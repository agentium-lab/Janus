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
type WorkerHandler func(ctx context.Context, task *core.Task, agentID string, progress ProgressFn) (resultRef string, usage *core.TokenUsage, err error)

// WorkerConfig configures a JanusWorker.
type WorkerConfig struct {
	AgentID      string        // The agent ID this worker pulls for.
	MailboxID    string        // The mailbox to poll.
	PollInterval time.Duration // Interval between polls when queue is empty (default 2s).
	Heartbeat    time.Duration // Interval for task heartbeats while running (default 30s).
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
	return &JanusWorker{client: client, config: config}
}

// Run starts the worker loop. It blocks until ctx is cancelled. For each task:
//  1. Pull from the mailbox.
//  2. Start the task (move to running).
//  3. Send periodic heartbeats while the handler runs.
//  4. ACK on success or NACK (retriable) on error.
func (w *JanusWorker) Run(ctx context.Context, handler WorkerHandler) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := w.processOne(ctx, handler); err != nil {
			log.Printf("janus worker: %v", err)
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

	// Heartbeat goroutine while the handler runs.
	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	go w.heartbeatLoop(hbCtx, task.ID, attempt, leaseID)

	// Progress reporter: fire-and-forget, failures logged not propagated.
	progress := func(message string, percent *int, data map[string]interface{}) {
		if perr := w.client.ReportProgress(ctx, task.ID, message, w.config.AgentID, percent, data); perr != nil {
			log.Printf("janus worker progress %s: %v (non-fatal)", task.ID, perr)
		}
	}

	// Run the handler.
	resultRef, usage, hErr := handler(ctx, task, w.config.AgentID, progress)
	hbCancel()

	// ACK or NACK.
	if hErr != nil {
		nackErr := w.client.NackTask(ctx, task.ID, NackRequest{
			LeaseID:   leaseID,
			Attempt:   attempt,
			Retriable: true,
			Error:     &core.TaskError{Code: "HANDLER_ERROR", Message: hErr.Error()},
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
	}
	if usage != nil {
		ackReq.TokenUsage = usage
	}
	if err := w.client.AckTask(ctx, task.ID, ackReq); err != nil {
		return fmt.Errorf("ack task %s: %w", task.ID, err)
	}
	return nil
}

func (w *JanusWorker) heartbeatLoop(ctx context.Context, taskID string, attempt int, leaseID string) {
	ticker := time.NewTicker(w.config.Heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.client.Heartbeat(ctx, taskID, attempt, leaseID); err != nil {
				log.Printf("janus worker heartbeat %s: %v", taskID, err)
				return
			}
		}
	}
}
