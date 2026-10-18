package janus

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/agentium-lab/Janus/core"
)

// ProgressFn reports mid-task progress. Non-blocking: failures are logged
// and discarded. Call it like a log statement from inside the handler.
type ProgressFn func(message string, percent *int, data map[string]interface{})

// WorkerHandler is the application-supplied function that processes a task.
// It receives the pulled task, the agent ID, and a ProgressFn for real-time
// progress reporting. If it returns a non-nil error, the Worker NACKs the
// task as retriable. If it returns nil, the Worker ACKs.
// WorkerHandler receives (ctx, task, agentID, progress). Handlers that don't
// need progress reporting can use the 3-param variant via WorkerHandlerSimple.
type WorkerHandler func(ctx context.Context, task *core.Task, agentID string, progress ProgressFn) (resultRef string, usage *core.TokenUsage, err error)

// WorkerHandlerSimple is the pre-v1.4 signature for backward compatibility.
// Wrap with FromSimple() to use with Run().
type WorkerHandlerSimple func(ctx context.Context, task *core.Task, agentID string) (resultRef string, usage *core.TokenUsage, err error)

// FromSimple adapts a 3-param handler to the full WorkerHandler signature.
func FromSimple(h WorkerHandlerSimple) WorkerHandler {
	return func(ctx context.Context, task *core.Task, agentID string, _ ProgressFn) (string, *core.TokenUsage, error) {
		return h(ctx, task, agentID)
	}
}

// WorkerConfig configures a JanusWorker.
type WorkerConfig struct {
	AgentID      string        // The agent ID this worker pulls for.
	MailboxID    string        // The mailbox to poll.
	PollInterval time.Duration // Interval between polls when queue is empty (default 2s).
	Heartbeat    time.Duration // Interval for task heartbeats while running (default 30s).

	// ErrorBackoff controls the retry delay after a Pull error (default 1s,
	// doubling to MaxErrorBackoff). Prevents hot-looping against a
	// degraded API.
	ErrorBackoff    time.Duration
	MaxErrorBackoff time.Duration
	// AgentHeartbeat is the interval for agent-liveness heartbeats
	// (default 30s). Without periodic agent heartbeats the sweeper
	// offlines the agent while it is actively processing tasks.
	AgentHeartbeat time.Duration
}

// JanusWorker is a high-level helper that runs a poll-process-ack loop.
// The application provides a WorkerHandler; the Worker handles polling,
// agent heartbeats, task start/heartbeat, ACK/NACK, and empty-mailbox backoff.
type JanusWorker struct {
	client *Client
	config WorkerConfig
}

// NewJanusWorker creates a Worker bound to the given client and config.
func NewJanusWorker(client *Client, config WorkerConfig) *JanusWorker {
	if config.PollInterval == 0 {
		config.PollInterval = 2 * time.Second
	}
	if config.Heartbeat == 0 {
		config.Heartbeat = 30 * time.Second
	}
	if config.ErrorBackoff == 0 {
		config.ErrorBackoff = time.Second
	}
	if config.MaxErrorBackoff == 0 {
		config.MaxErrorBackoff = 30 * time.Second
	}
	if config.AgentHeartbeat == 0 {
		config.AgentHeartbeat = 30 * time.Second
	}
	return &JanusWorker{client: client, config: config}
}

// Run starts the worker loop. It blocks until ctx is cancelled. For each task:
//  1. Pull from the mailbox.
//  2. Start the task (move to running).
//  3. Send periodic heartbeats while the handler runs.
//  4. ACK on success or NACK (retriable) on error.
func (w *JanusWorker) Run(ctx context.Context, handler WorkerHandler) error {
	// Agent-liveness heartbeat: the sweeper offlines agents without a
	// recent heartbeat — a worker that only sends TASK heartbeats would
	// go offline while actively processing.
	agentCtx, agentCancel := context.WithCancel(ctx)
	defer agentCancel()
	go w.agentHeartbeatLoop(agentCtx)

	backoff := w.config.ErrorBackoff
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := w.processOne(ctx, handler)
		if err == nil {
			backoff = w.config.ErrorBackoff // reset on success
			continue
		}
		log.Printf("janus worker: %v", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > w.config.MaxErrorBackoff {
			backoff = w.config.MaxErrorBackoff
		}
	}
}

func (w *JanusWorker) processOne(ctx context.Context, handler WorkerHandler) error {
	result, err := w.client.PullTask(ctx, w.config.MailboxID, w.config.AgentID)
	if err != nil {
		return fmt.Errorf("pull: %w", err)
	}
	if result == nil || result.Task == nil {
		// Empty queue: back off.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(w.config.PollInterval):
		}
		return nil
	}

	task := result.Task
	leaseID := result.Lease.LeaseID
	attempt := result.Lease.Attempt

	// Start the task.
	if err := w.client.StartTask(ctx, task.ID, attempt, leaseID); err != nil {
		return fmt.Errorf("start task %s: %w", task.ID, err)
	}

	// Heartbeat goroutine while the handler runs. The handler receives
	// hbCtx: after sustained heartbeat failure the lease is gone, the task
	// will be redelivered, and continuing to burn compute is waste — the
	// worker cancels hbCtx so cooperative handlers stop.
	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	go w.heartbeatLoop(hbCtx, hbCancel, task.ID, attempt, leaseID)

	// Progress reporter: fire-and-forget, failures logged not propagated.
	progress := func(message string, percent *int, data map[string]interface{}) {
		if perr := w.client.ReportProgress(ctx, task.ID, message, w.config.AgentID, percent, data); perr != nil {
			log.Printf("janus worker progress %s: %v (non-fatal)", task.ID, perr)
		}
	}

	// Run the handler.
	resultRef, usage, hErr := handler(hbCtx, task, w.config.AgentID, progress)
	hbCancel()

	// ACK or NACK.
	if hErr != nil {
		nackErr := w.client.NackTask(ctx, task.ID, NackRequest{
			LeaseID:   leaseID,
			Attempt:   attempt,
			Retriable: true,
			Error:     &core.TaskError{Code: "HANDLER_ERROR", Message: hErr.Error()},
			AgentID:   w.config.AgentID,
		})
		if nackErr != nil {
			return fmt.Errorf("nack task %s: %w (handler error: %v)", task.ID, nackErr, hErr)
		}
		return nil
	}

	ackReq := AckRequest{
		LeaseID:   leaseID,
		Attempt:   attempt,
		ResultRef: resultRef,
		AgentID:   w.config.AgentID,
	}
	if usage != nil {
		ackReq.TokenUsage = usage
	}
	if err := w.client.AckTask(ctx, task.ID, ackReq); err != nil {
		return fmt.Errorf("ack task %s: %w", task.ID, err)
	}
	return nil
}

func (w *JanusWorker) heartbeatLoop(ctx context.Context, leaseLost context.CancelFunc, taskID string, attempt int, leaseID string) {
	ticker := time.NewTicker(w.config.Heartbeat)
	defer ticker.Stop()
	// Heartbeat failures are logged but DO NOT stop renewal — a single
	// transient error previously killed the goroutine while the handler
	// kept running, letting the lease expire and the task redeliver.
	var consecutiveFailures int
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.client.Heartbeat(ctx, taskID, attempt, leaseID); err != nil {
				consecutiveFailures++
				log.Printf("janus worker heartbeat %s: %v (streak %d)", taskID, err, consecutiveFailures)
				if consecutiveFailures >= 5 {
					log.Printf("janus worker heartbeat %s: lease presumed lost after 5 failures; cancelling handler context", taskID)
					leaseLost()
					return
				}
			} else {
				consecutiveFailures = 0
			}
		}
	}
}

func (w *JanusWorker) agentHeartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(w.config.AgentHeartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.client.HeartbeatAgent(ctx, w.config.AgentID); err != nil {
				log.Printf("janus worker agent heartbeat: %v (non-fatal)", err)
			}
		}
	}
}
