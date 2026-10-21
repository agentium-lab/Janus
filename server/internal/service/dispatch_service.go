package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/auth"
	natsdriver "github.com/agentium-lab/Janus/server/internal/driver/nats"
	"github.com/agentium-lab/Janus/server/internal/driver/postgres"
	"github.com/agentium-lab/Janus/server/internal/metrics"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type PullResult struct {
	Task      *core.Task
	LeaseID   string
	ExpiresAt time.Time
}

type DispatchService struct {
	taskRepo    TaskRepo
	attemptRepo TaskAttemptRepo
	mailboxRepo MailboxRepo
	queueDriver QueueDriver
	policySvc   *PolicyService
	budgetSvc   *BudgetService

	// Unified transaction path (Priority 1): one code shape for PG and memory.
	lifecycle Lifecycle
	taskTx    TaskTxRepo
	attemptTx AttemptTxRepo
	txOutbox  OutboxDedupeWriter
	ledger    BudgetLedger
}

func NewDispatchService(
	taskRepo TaskRepo,
	attemptRepo TaskAttemptRepo,
	mailboxRepo MailboxRepo,
	queueDriver QueueDriver,
	policySvc *PolicyService,
	budgetSvc *BudgetService,
) *DispatchService {
	s := &DispatchService{
		taskRepo:    taskRepo,
		attemptRepo: attemptRepo,
		mailboxRepo: mailboxRepo,
		queueDriver: queueDriver,
		policySvc:   policySvc,
		budgetSvc:   budgetSvc,
	}
	s.initTxPath()
	return s
}

// initTxPath resolves the unified transaction path from the wired repos: PG
// repos contribute their native *Tx methods; anything else is bridged through
// TxAdapter + MemoryLifecycle + PublishingOutbox so tests run the production
// code shape.
func (s *DispatchService) initTxPath() {
	if pgTask, ok := s.taskRepo.(*postgres.TaskRepository); ok && pgTask != nil {
		s.taskTx = pgTask
	} else {
		s.taskTx = &TaskRepoTxAdapter{TaskRepo: s.taskRepo}
	}
	if pgAttempt, ok := s.attemptRepo.(*postgres.TaskAttemptRepository); ok && pgAttempt != nil {
		s.attemptTx = pgAttempt
	} else {
		s.attemptTx = AttemptRepoTxAdapter{s.attemptRepo}
	}
	if s.attemptRepo == nil {
		s.attemptTx = nil
	}
	s.lifecycle = NewMemoryLifecycle()
	s.txOutbox = NewPublishingOutbox(s.queueDriver)
}

// WithTxPath wires the production transaction components: PGLifecycle, the
// PG repos' native *Tx methods, the persistent outbox, and the idempotent
// budget ledger.
// WithQueueDriver swaps the queue driver (tests inject fault-shaped drivers).
func (s *DispatchService) WithQueueDriver(d QueueDriver) *DispatchService {
	s.queueDriver = d
	return s
}

func (s *DispatchService) WithTxPath(lc Lifecycle, outbox OutboxDedupeWriter, ledger BudgetLedger) *DispatchService {
	if lc != nil {
		s.lifecycle = lc
	}
	if outbox != nil {
		s.txOutbox = outbox
	}
	s.ledger = ledger
	return s
}

// activeTaskCounts returns tenant- and agent-scoped in-flight counts
// (claimed + running). The tenant side sums both statuses (CountByStatus
// is single-status); the agent side already covers both via
// CountRunningByAgent's attempt query. Counting only 'running' on the
// tenant side missed the pull-to-claimed window: N agents could all claim
// under a cap and then StartTask past it. Count errors propagate — they
// used to read as zero (unlimited).
func (s *DispatchService) activeTaskCounts(ctx context.Context, tenantID, agentID string) (tenant, agent int, err error) {
	tClaimed, err := s.taskRepo.CountByStatus(ctx, tenantID, core.TaskStatusClaimed)
	if err != nil {
		return 0, 0, fmt.Errorf("tenant claimed count: %w", err)
	}
	tRunning, err := s.taskRepo.CountByStatus(ctx, tenantID, core.TaskStatusRunning)
	if err != nil {
		return 0, 0, fmt.Errorf("tenant running count: %w", err)
	}
	aActive, err := s.taskRepo.CountRunningByAgent(ctx, tenantID, agentID)
	if err != nil {
		return 0, 0, fmt.Errorf("agent active count: %w", err)
	}
	return tClaimed + tRunning, aActive, nil
}

// errAgentAtCapacity marks the in-transaction capacity recheck inside the
// claim transaction; the delivery is requeued rather than lost.
var errAgentAtCapacity = errors.New("agent at capacity")

func (s *DispatchService) PullTask(ctx context.Context, tenantID, mailboxID, agentID string) (*PullResult, error) {
	ctx, span := otel.Tracer("janus").Start(ctx, "DispatchService.PullTask",
		trace.WithAttributes(
			attribute.String("tenant.id", tenantID),
			attribute.String("mailbox.id", mailboxID),
			attribute.String("agent.id", agentID),
		),
	)
	defer span.End()
	if tenantID == "" || mailboxID == "" || agentID == "" {
		return nil, fmt.Errorf("tenant id, mailbox id, and agent id are required")
	}

	// Ownership is a hard gate: a failed or missing mailbox lookup must
	// REJECT, not fall through — with the check skipped, any agent could
	// pull another agent's mail out of an existing consumer while the
	// ownership store was degraded.
	mb, mbErr := s.mailboxRepo.Get(ctx, tenantID, mailboxID)
	if mbErr != nil {
		return nil, fmt.Errorf("mailbox ownership lookup: %w", mbErr)
	}
	if mb == nil {
		return nil, fmt.Errorf("mailbox %s not found", mailboxID)
	}
	if mb.AgentID != "" && mb.AgentID != agentID {
		return nil, fmt.Errorf("agent %s is not the owner of mailbox %s", agentID, mailboxID)
	}

	ctx = natsdriver.ContextWithTenant(ctx, tenantID)

	decision, err := s.policySvc.Evaluate(ctx, core.PolicyInput{
		TenantID: tenantID,
		Actor:    core.PolicyActor{Type: "agent", ID: agentID},
		Action:   "dispatch",
		Resource: core.PolicyResource{Type: "mailbox", Value: mailboxID},
	})
	if err != nil {
		return nil, fmt.Errorf("policy check: %w", err)
	}
	if decision.Decision == core.PolicyDecisionDeny {
		metrics.PolicyDenied.WithLabelValues(tenantID).Inc()
		return nil, &core.BackpressureError{
			Reason:  core.ReasonApprovalRequired,
			Message: fmt.Sprintf("policy denied: %s", decision.Reason),
		}
	}

	tenantActive, agentActive, cerr := s.activeTaskCounts(ctx, tenantID, agentID)
	if cerr != nil {
		return nil, cerr
	}
	if err := s.budgetSvc.CheckConcurrency(ctx, tenantID, agentID, agentActive, tenantActive); err != nil {
		return nil, err
	}

	if err := s.ensureMailboxConsumer(ctx, tenantID, mailboxID); err != nil {
		return nil, err
	}
	metrics.PullRequests.WithLabelValues(tenantID, mailboxID).Inc()

	deliveries, err := s.queueDriver.FetchTasks(ctx, tenantID, mailboxID, core.FetchOptions{
		MaxMessages: 1,
		WaitTime:    2 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("fetch tasks: %w", err)
	}
	if len(deliveries) == 0 {
		return nil, nil
	}

	delivery := deliveries[0]
	task, err := s.taskRepo.Get(ctx, tenantID, delivery.TaskID)
	if err != nil {
		return nil, fmt.Errorf("get task %s: %w", delivery.TaskID, err)
	}

	// Redelivery reconciliation: a delivery may arrive for a task that has
	// already advanced past 'queued' (e.g. duplicate redelivery, or the outbox
	// republished after the task already completed). Decide what to do with the
	// broker delivery before creating a new attempt.
	switch {
	case task.Status.IsTerminal():
		// completed/dead_lettered/cancelled/expired: ACK to clear stale delivery.
		_ = s.queueDriver.AckTask(ctx, tenantID, core.DeliveryRef(delivery.DeliveryRef))
		return nil, nil
	case task.Status == core.TaskStatusFailed || task.Status == core.TaskStatusBlocked:
		// failed is terminal-by-contract (Fail's explicit unrecoverable
		// declaration) but deliberately sits OUTSIDE TerminalStates so Replay
		// keeps rejecting it; blocked tasks resume through their own unblock
		// path. In both states the task must never be handed out again off a
		// broker redelivery: ACK the stale delivery and stop. The pre-fix
		// fall-through minted a NEW attempt and CAS'd the task back to
		// claimed off a plain NATS redelivery, resurrecting failed work.
		_ = s.queueDriver.AckTask(ctx, tenantID, core.DeliveryRef(delivery.DeliveryRef))
		return nil, nil
	case task.Status == core.TaskStatusRetryScheduled:
		// retry_scheduled: the retry scheduler / outbox will re-publish at the
		// backoff time. ACK this stale delivery so NATS stops redelivering it.
		_ = s.queueDriver.AckTask(ctx, tenantID, core.DeliveryRef(delivery.DeliveryRef))
		return nil, nil
	case task.Status == core.TaskStatusCreated:
		// created-but-not-yet-queued: the outbox hasn't published yet. NACK to
		// preserve the broker message; it will be redelivered after the task
		// reaches 'queued'. Use a short delay to avoid a tight redelivery loop.
		_ = s.queueDriver.NackTask(ctx, tenantID, core.DeliveryRef(delivery.DeliveryRef), core.NackRetriable)
		return nil, nil
	case task.Status == core.TaskStatusClaimed || task.Status == core.TaskStatusRunning:
		// In-flight task. Check whether this delivery matches the current
		// attempt (a duplicate redelivery of the in-flight attempt) or is a
		// stale/older delivery.
		latest, lerr := s.attemptRepo.GetLatest(ctx, tenantID, task.ID)
		if lerr == nil && latest != nil && latest.DeliveryRef == string(delivery.DeliveryRef) {
			// Same in-flight attempt redelivered: ACK the duplicate.
			_ = s.queueDriver.AckTask(ctx, tenantID, core.DeliveryRef(delivery.DeliveryRef))
			return nil, nil
		}
		// Otherwise this is a delivery for a different/older attempt while a
		// newer attempt is in flight. ACK to clear it; the in-flight attempt
		// owns the task.
		_ = s.queueDriver.AckTask(ctx, tenantID, core.DeliveryRef(delivery.DeliveryRef))
		return nil, nil
	}

	dispatchDecision, dispatchErr := s.policySvc.Evaluate(ctx, core.PolicyInput{
		TenantID: tenantID,
		Actor:    core.PolicyActor{Type: "agent", ID: agentID},
		Action:   "dispatch",
		Resource: core.PolicyResource{Type: "task", Value: task.ID},
	})
	// Fail-closed on policy evaluation errors: an unreachable policy store
	// previously read as "no rule matched" and let every delivery through.
	// Only an explicit Allow proceeds; Deny AND ApprovalRequired both stop
	// the dispatch — an approval-gated task must not run before approval.
	if dispatchErr != nil {
		metrics.PolicyDenied.WithLabelValues(tenantID).Inc()
		s.publishEvent(ctx, core.JanusEvent{
			EventType: core.EventPolicyDenied,
			TenantID:  tenantID, TaskID: task.ID,
			Payload: mustMarshal(map[string]string{
				"agent_id": agentID, "delivery_ref": string(delivery.DeliveryRef),
				"reason": fmt.Sprintf("policy evaluation failed: %v", dispatchErr),
			}),
		})
		return nil, &core.BackpressureError{
			Reason:  core.ReasonApprovalRequired,
			Message: fmt.Sprintf("dispatch policy unavailable: %v", dispatchErr),
		}
	}
	if dispatchDecision.Decision == core.PolicyDecisionDeny || dispatchDecision.Decision == core.PolicyDecisionApprovalRequired {
		metrics.PolicyDenied.WithLabelValues(tenantID).Inc()
		s.publishEvent(ctx, core.JanusEvent{
			EventType: core.EventPolicyDenied,
			TenantID:  tenantID, TaskID: task.ID,
			Payload: mustMarshal(map[string]string{
				"agent_id": agentID, "delivery_ref": string(delivery.DeliveryRef),
				"reason": dispatchDecision.Reason,
			}),
		})
		deliveryRef := delivery.DeliveryRef
		time.AfterFunc(5*time.Second, func() {
			nackCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = s.queueDriver.NackTask(nackCtx, tenantID, core.DeliveryRef(deliveryRef), core.NackRetriable)
		})
		return nil, &core.BackpressureError{
			Reason:  core.ReasonApprovalRequired,
			Message: fmt.Sprintf("dispatch policy denied: %s", dispatchDecision.Reason),
		}
	}

	if err := s.budgetSvc.Reserve(ctx, tenantID, agentID, task.Envelope.Budget); err != nil {
		s.publishEvent(ctx, core.JanusEvent{
			EventType: core.EventType("budget.exceeded"),
			TenantID:  tenantID, TaskID: task.ID,
			Payload: mustMarshal(map[string]string{
				"agent_id": agentID, "delivery_ref": string(delivery.DeliveryRef),
				"reason": err.Error(),
			}),
		})
		deliveryRef := delivery.DeliveryRef
		time.AfterFunc(5*time.Second, func() {
			nackCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = s.queueDriver.NackTask(nackCtx, tenantID, core.DeliveryRef(deliveryRef), core.NackRetriable)
		})
		return nil, err
	}

	leaseID := generateLeaseID()
	// The lease must expire on the mailbox's ack-wait clock (the same clock
	// the lease scanner and the NATS consumer use), not a hardcoded 300s —
	// agents were told their lease lasted 300s while a mailbox with a
	// shorter ack_wait had already expired them.
	leaseTTL := 300
	if mb != nil && mb.ACKWaitSeconds > 0 {
		leaseTTL = mb.ACKWaitSeconds
	}
	expiresAt := time.Now().Add(time.Duration(leaseTTL) * time.Second)

	attempt := core.TaskAttempt{
		TenantID:    tenantID,
		TaskID:      task.ID,
		Attempt:     task.AttemptCount + 1,
		AgentID:     agentID,
		LeaseID:     leaseID,
		Status:      "claimed",
		StartedAt:   time.Now(),
		DeliveryRef: string(delivery.DeliveryRef),
	}

	// Attempt create + task claim + claimed event in one tx, serialized per
	// agent with a capacity re-check under the lock (closes the race between
	// the pre-fetch count check and attempt creation).
	// Tenant-scoped serialization: locking tenant:agent let two agents
	// read the same in-flight count and each claim past the TENANT cap.
	// Serializing per tenant keeps the capacity recheck correct at a
	// modest throughput cost (the claim transaction is milliseconds).
	err = s.lifecycle.ApplyTxLocked(ctx, tenantID+":concurrency", func(tx pgx.Tx) error {
		// All reads inside the transaction run ON the tx executor — nested
		// pool queries acquired a second connection while the tx held one
		// (pool self-starvation under saturation).
		tClaimed, cerr2 := s.taskTx.CountByStatusTx(ctx, tx, tenantID, core.TaskStatusClaimed)
		if cerr2 != nil {
			return fmt.Errorf("tenant claimed count: %w", cerr2)
		}
		tRunning, cerr3 := s.taskTx.CountByStatusTx(ctx, tx, tenantID, core.TaskStatusRunning)
		if cerr3 != nil {
			return fmt.Errorf("tenant running count: %w", cerr3)
		}
		aActive, cerr4 := s.taskTx.CountRunningByAgentTx(ctx, tx, tenantID, agentID)
		if cerr4 != nil {
			return fmt.Errorf("agent active count: %w", cerr4)
		}
		if cerr := s.budgetSvc.CheckConcurrencyTx(ctx, tx, tenantID, agentID, aActive, tClaimed+tRunning); cerr != nil {
			return fmt.Errorf("%w: %v", errAgentAtCapacity, cerr)
		}
		if cerr := s.attemptTx.CreateTx(ctx, tx, attempt); cerr != nil {
			return fmt.Errorf("create attempt: %w", cerr)
		}
		claimed, uerr := s.taskTx.UpdateStatusWithCheckTx(ctx, tx, tenantID, task.ID, task.Status, core.TaskStatusClaimed, 1)
		if uerr != nil {
			return fmt.Errorf("update task claimed: %w", uerr)
		}
		if !claimed {
			// The task moved between the pre-lock read and this CAS (e.g.
			// Cancel committed first). Committing anyway would leave a live
			// attempt+lease on a terminal task — roll back the claim.
			return fmt.Errorf("claim race: task %s status changed concurrently, expected %s", task.ID, task.Status)
		}
		claimedPayload := MarshalEvent(&core.JanusEvent{
			EventType: core.EventTaskClaimed, TenantID: tenantID, TaskID: task.ID,
			Payload: mustMarshal(map[string]string{"lease_id": leaseID, "agent_id": agentID}),
		})
		return s.txOutbox.Insert(ctx, tx, ulid(), tenantID, "event_publish", claimedPayload)
	})
	if err != nil {
		s.budgetSvc.Release(ctx, tenantID, agentID)
		if errors.Is(err, errAgentAtCapacity) {
			_ = s.queueDriver.NackTask(ctx, tenantID, delivery.DeliveryRef, core.NackRetriable)
			return nil, &core.BackpressureError{Reason: core.ReasonAgentConcurrencyExceeded,
				Message: "agent concurrency limit reached; delivery requeued"}
		}
		return nil, err
	}
	return &PullResult{Task: task, LeaseID: leaseID, ExpiresAt: expiresAt}, nil
}

func (s *DispatchService) StartTask(ctx context.Context, tenantID, taskID, leaseID string) error {
	return s.StartTaskAs(ctx, tenantID, taskID, leaseID, "")
}

// actingAgent resolves the agent identity for owner checks: a bound
// principal is authoritative (a self-reported agent_id cannot bypass it —
// omitting or forging the request field must not skip verification);
// without an authenticated bound identity the self-reported value applies
// (dev/no-auth mode and unbound tenant keys).
func actingAgent(ctx context.Context, selfReported string) string {
	if p, ok := auth.PrincipalFromContext(ctx); ok && p.BoundAgentID != "" {
		return p.BoundAgentID
	}
	return selfReported
}

// StartTaskAs additionally verifies the acting agent owns the attempt.
// agentID == "" skips the owner check (pre-v1.10 callers); the API handler
// always forwards the request's agent_id.
func (s *DispatchService) StartTaskAs(ctx context.Context, tenantID, taskID, leaseID, agentID string) error {
	agentID = actingAgent(ctx, agentID)
	if tenantID == "" || taskID == "" || leaseID == "" {
		return fmt.Errorf("tenant id, task id, and lease id are required")
	}

	// Cheap fast path only — the authoritative validation runs inside the
	// transaction below. Between this pool read and the tx body, Cancel ->
	// Replay -> a new Pull can swap the attempt (attempt numbers restart at 1
	// after a replay, under a NEW lease); a stale start reading the old
	// attempt here must not run the task once the tx re-checks.
	attempt, err := s.attemptRepo.GetLatest(ctx, tenantID, taskID)
	if err != nil {
		return fmt.Errorf("get latest attempt: %w", err)
	}
	if attempt.LeaseID != leaseID {
		return fmt.Errorf("lease mismatch")
	}
	if agentID != "" && attempt.AgentID != agentID {
		return fmt.Errorf("agent %s does not own the current attempt", agentID)
	}

	return s.lifecycle.ApplyTx(ctx, func(tx pgx.Tx) error {
		task, gerr := s.taskTx.GetTx(ctx, tx, tenantID, taskID)
		if gerr != nil {
			return fmt.Errorf("get task: %w", gerr)
		}
		// Authoritative re-validation ON the transaction: the attempt may
		// have been swapped since the pre-check read above.
		latest, aerr := s.attemptTx.GetLatestTx(ctx, tx, tenantID, taskID)
		if aerr != nil {
			return fmt.Errorf("get latest attempt in tx: %w", aerr)
		}
		if latest.LeaseID != leaseID {
			return fmt.Errorf("lease mismatch: task %s is held by a newer attempt", taskID)
		}
		if agentID != "" && latest.AgentID != agentID {
			return fmt.Errorf("agent %s does not own the current attempt", agentID)
		}
		if latest.Status != "claimed" && latest.Status != "running" {
			return fmt.Errorf("attempt %d of task %s is %s, not startable", latest.Attempt, taskID, latest.Status)
		}
		if task.Status.IsTerminal() {
			return fmt.Errorf("%w: task %s is %s, cannot start", core.ErrInvalidTransition, taskID, task.Status)
		}
		if !core.CanTransition(task.Status, core.TaskStatusRunning) {
			return fmt.Errorf("%w: %s -> running for task %s", core.ErrInvalidTransition, task.Status, taskID)
		}
		started, uerr := s.taskTx.UpdateStatusWithCheckTx(ctx, tx, tenantID, taskID, task.Status, core.TaskStatusRunning, 0)
		if uerr != nil {
			return fmt.Errorf("update task running: %w", uerr)
		}
		if !started {
			return fmt.Errorf("start race: task %s status changed concurrently, expected %s", taskID, task.Status)
		}
		startedPayload := MarshalEvent(&core.JanusEvent{
			EventType: core.EventTaskStarted, TenantID: tenantID, TaskID: taskID,
			Payload: mustMarshal(map[string]string{"lease_id": leaseID}),
		})
		return s.txOutbox.Insert(ctx, tx, ulid(), tenantID, "event_publish", startedPayload)
	})
}

func (s *DispatchService) TaskHeartbeat(ctx context.Context, tenantID, taskID, leaseID string) error {
	return s.TaskHeartbeatAs(ctx, tenantID, taskID, leaseID, "")
}

// TaskHeartbeatAs additionally verifies the acting agent owns the attempt.
func (s *DispatchService) TaskHeartbeatAs(ctx context.Context, tenantID, taskID, leaseID, agentID string) error {
	agentID = actingAgent(ctx, agentID)
	if tenantID == "" || taskID == "" || leaseID == "" {
		return fmt.Errorf("tenant id, task id, and lease id are required")
	}

	attempt, err := s.attemptRepo.GetLatest(ctx, tenantID, taskID)
	if err != nil {
		return fmt.Errorf("get latest attempt: %w", err)
	}
	if attempt.LeaseID != leaseID {
		return fmt.Errorf("lease mismatch")
	}
	if agentID != "" && attempt.AgentID != agentID {
		return fmt.Errorf("agent %s does not own the current attempt", agentID)
	}

	active, herr := s.attemptRepo.UpdateHeartbeat(ctx, tenantID, taskID, attempt.Attempt, leaseID)
	if herr != nil {
		return herr
	}
	if !active {
		return fmt.Errorf("attempt is no longer active; lease presumed lost")
	}
	return nil
}

func (s *DispatchService) AckTask(ctx context.Context, tenantID, taskID, leaseID string, resultRef string, usage *core.TokenUsage) error {
	return s.AckTaskAs(ctx, tenantID, taskID, leaseID, "", resultRef, usage)
}

// AckTaskAs additionally verifies the acting agent owns the attempt.
func (s *DispatchService) AckTaskAs(ctx context.Context, tenantID, taskID, leaseID, agentID, resultRef string, usage *core.TokenUsage) error {
	agentID = actingAgent(ctx, agentID)
	ctx, span := otel.Tracer("janus").Start(ctx, "DispatchService.AckTask",
		trace.WithAttributes(
			attribute.String("tenant.id", tenantID),
			attribute.String("task.id", taskID),
		),
	)
	defer span.End()
	if tenantID == "" || taskID == "" || leaseID == "" {
		return fmt.Errorf("tenant id, task id, and lease id are required")
	}

	attempt, err := s.attemptRepo.GetLatest(ctx, tenantID, taskID)
	if err != nil {
		return fmt.Errorf("get latest attempt: %w", err)
	}
	if attempt.LeaseID != leaseID {
		return fmt.Errorf("lease mismatch")
	}
	if agentID != "" && attempt.AgentID != agentID {
		return fmt.Errorf("agent %s does not own the current attempt", agentID)
	}

	var prompt, completion, total int64
	if usage != nil {
		prompt = int64(usage.PromptTokens)
		completion = int64(usage.CompletionTokens)
		total = int64(usage.TotalTokens)
	}
	usageJSON, _ := encodeJSON(usage)

	committed := false
	err = s.lifecycle.ApplyTx(ctx, func(tx pgx.Tx) error {
		ok, ferr := s.attemptTx.UpdateFinishedWithCheckTx(ctx, tx, tenantID, taskID, attempt.Attempt, leaseID, "completed", nil, usageJSON)
		if err := ferr; err != nil {
			return fmt.Errorf("finish attempt: %w", err)
		}
		if !ok {
			// Already finished (duplicate ACK). No-op.
			return nil
		}

		task, gerr := s.taskTx.GetTx(ctx, tx, tenantID, taskID)
		if gerr != nil {
			return fmt.Errorf("get task: %w", gerr)
		}
		if !core.CanTransition(task.Status, core.TaskStatusCompleted) {
			return fmt.Errorf("%w: %s -> completed for task %s", core.ErrInvalidTransition, task.Status, taskID)
		}
		taskOK, uerr := s.taskTx.UpdateStatusWithCheckTx(ctx, tx, tenantID, taskID, task.Status, core.TaskStatusCompleted, 0)
		if uerr != nil {
			return fmt.Errorf("complete task: %w", uerr)
		}
		if !taskOK {
			return nil
		}
		if resultRef != "" {
			if serr := s.taskTx.SetResultRefTx(ctx, tx, tenantID, taskID, resultRef); serr != nil {
				return fmt.Errorf("set result ref: %w", serr)
			}
		}

		// Idempotent settlement: ledger (two scopes), increment only on insert.
		costUSD := 0.0
		if usage != nil && total > 0 {
			costUSD = EstimateCostUSD(total)
		}
		for _, scope := range []struct{ Type, ID string }{
			{"tenant", tenantID},
			{"agent", attempt.AgentID},
		} {
			if s.ledger != nil {
				if ierr := s.ledger.SettleTx(ctx, tx, core.LedgerEntry{
					TenantID: tenantID, TaskID: taskID, Attempt: attempt.Attempt,
					ScopeType: scope.Type, ScopeID: scope.ID,
					PromptTokens: prompt, CompletionTokens: completion,
					TotalTokens: total, CostUSD: costUSD,
				}); ierr != nil {
					return fmt.Errorf("ledger settle %s: %w", scope.Type, ierr)
				}
			}
		}

		// completed event via outbox.
		completedPayload := MarshalEvent(&core.JanusEvent{
			EventType: core.EventTaskCompleted, TenantID: tenantID, TaskID: taskID,
			SourceAgent: attempt.AgentID,
			Payload:     mustMarshal(map[string]string{"result_ref": resultRef}),
		})
		if oerr := s.txOutbox.Insert(ctx, tx, ulid(), tenantID, "event_publish", completedPayload); oerr != nil {
			return fmt.Errorf("outbox completed: %w", oerr)
		}

		if task.Envelope.ToolInvocation != nil {
			toolPayload, _ := json.Marshal(map[string]string{
				"tool_name":  task.Envelope.ToolInvocation.Name,
				"result_ref": resultRef,
			})
			toolEvt := MarshalEvent(&core.JanusEvent{
				EventType: core.EventToolInvocationCompleted, TenantID: tenantID, TaskID: taskID,
				SourceAgent: attempt.AgentID, Payload: toolPayload,
			})
			if oerr := s.txOutbox.Insert(ctx, tx, ulid(), tenantID, "event_publish", toolEvt); oerr != nil {
				return fmt.Errorf("outbox tool completed: %w", oerr)
			}
		}
		committed = true
		return nil
	})
	if err != nil {
		return err
	}
	if !committed {
		// Duplicate ACK that was a no-op inside the tx.
		return nil
	}

	metrics.TasksCompleted.WithLabelValues(tenantID).Inc()
	metrics.AckTotal.WithLabelValues(tenantID).Inc()

	// ACK NATS only after DB commit.
	if attempt.DeliveryRef != "" {
		if aerr := s.queueDriver.AckTask(ctx, tenantID, core.DeliveryRef(attempt.DeliveryRef)); aerr != nil {
			log.Printf("ack queue message failed after task completed: tenant=%s task=%s attempt=%d delivery_ref=%s err=%v",
				tenantID, taskID, attempt.Attempt, attempt.DeliveryRef, aerr)
			warnPayload := MarshalEvent(&core.JanusEvent{
				EventType: core.EventTaskCompleted, TenantID: tenantID, TaskID: taskID,
				Payload: mustMarshal(map[string]string{"result_ref": resultRef, "ack_error": aerr.Error(), "delivery_ref": attempt.DeliveryRef}),
			})
			_ = s.txOutbox.InsertDirect(ctx, ulid(), tenantID, "event_publish", warnPayload)
		}
	}
	return nil
}

func (s *DispatchService) NackTask(ctx context.Context, tenantID, taskID, leaseID string, retriable bool, taskErr *core.TaskError) error {
	return s.NackTaskAs(ctx, tenantID, taskID, leaseID, "", retriable, taskErr)
}

// NackTaskAs additionally verifies the acting agent owns the attempt.
func (s *DispatchService) NackTaskAs(ctx context.Context, tenantID, taskID, leaseID, agentID string, retriable bool, taskErr *core.TaskError) error {
	agentID = actingAgent(ctx, agentID)
	ctx, span := otel.Tracer("janus").Start(ctx, "DispatchService.NackTask",
		trace.WithAttributes(
			attribute.String("tenant.id", tenantID),
			attribute.String("task.id", taskID),
			attribute.Bool("task.retriable", retriable),
		),
	)
	defer span.End()
	if tenantID == "" || taskID == "" || leaseID == "" {
		return fmt.Errorf("tenant id, task id, and lease id are required")
	}

	attempt, err := s.attemptRepo.GetLatest(ctx, tenantID, taskID)
	if err != nil {
		return fmt.Errorf("get latest attempt: %w", err)
	}
	if attempt.LeaseID != leaseID {
		return fmt.Errorf("lease mismatch")
	}
	if agentID != "" && attempt.AgentID != agentID {
		return fmt.Errorf("agent %s does not own the current attempt", agentID)
	}

	var errJSON []byte
	if taskErr != nil {
		errJSON, _ = encodeJSON(taskErr)
	}

	task, err := s.taskRepo.Get(ctx, tenantID, taskID)
	if err != nil {
		return fmt.Errorf("get task: %w", err)
	}
	mb, mbErr := s.mailboxRepo.Get(ctx, tenantID, task.MailboxID)
	if mbErr != nil {
		// An infrastructure failure reading the retry policy must NOT be
		// interpreted as "cannot retry" — that misrouted transient PG
		// faults to the DLQ. Surface it; the delivery stays in the broker's
		// redelivery window.
		return fmt.Errorf("nack: mailbox read for retry policy: %w", mbErr)
	}
	canRetry := retriable && mb != nil && !mb.RetryPolicy.ExceedsMaxAttempts(task.AttemptCount)

	committed := false
	attemptFinished := false
	err = s.lifecycle.ApplyTx(ctx, func(tx pgx.Tx) error {
		ok, ferr := s.attemptTx.UpdateFinishedWithCheckTx(ctx, tx, tenantID, taskID, attempt.Attempt, leaseID, "failed", errJSON, nil)
		if err := ferr; err != nil {
			return fmt.Errorf("finish attempt: %w", err)
		}
		if !ok {
			return nil // duplicate NACK, no-op
		}
		attemptFinished = true

		if !core.CanTransition(task.Status, core.TaskStatusRetryScheduled) &&
			!core.CanTransition(task.Status, core.TaskStatusDeadLettered) {
			return fmt.Errorf("%w: %s is terminal, cannot nack for task %s", core.ErrInvalidTransition, task.Status, taskID)
		}
		if canRetry {
			retryAt := time.Now().Add(mb.RetryPolicy.BackoffDuration(task.AttemptCount))
			retryOK, uerr := s.taskTx.UpdateStatusWithCheckTx(ctx, tx, tenantID, taskID, task.Status, core.TaskStatusRetryScheduled, 0)
			if uerr != nil {
				return fmt.Errorf("set retry_scheduled: %w", uerr)
			}
			if !retryOK {
				return nil
			}
			if rerr := s.taskTx.UpdateRetryAtTx(ctx, tx, tenantID, taskID, retryAt); rerr != nil {
				return fmt.Errorf("set retry_at: %w", rerr)
			}
			retryPayload := MarshalEvent(&core.JanusEvent{
				EventType: core.EventTaskRetryScheduled, TenantID: tenantID, TaskID: taskID,
				Payload: mustMarshal(map[string]string{"attempt": fmt.Sprintf("%d", task.AttemptCount)}),
			})
			if oerr := s.txOutbox.Insert(ctx, tx, ulid(), tenantID, "event_publish", retryPayload); oerr != nil {
				return fmt.Errorf("outbox retry: %w", oerr)
			}
		} else {
			dlOK, uerr := s.taskTx.UpdateStatusWithCheckTx(ctx, tx, tenantID, taskID, task.Status, core.TaskStatusDeadLettered, 0)
			if uerr != nil {
				return fmt.Errorf("dead letter: %w", uerr)
			}
			if !dlOK {
				return nil
			}
			// dlq_publish + dead_lettered event outbox
			envelopeJSON, _ := json.Marshal(task.Envelope)
			dlqHeaders := map[string]string{"attempt_count": fmt.Sprintf("%d", task.AttemptCount)}
			if len(errJSON) > 0 {
				dlqHeaders["error"] = string(errJSON)
			}
			dlqPayload, _ := json.Marshal(core.TaskMessage{
				TenantID: tenantID, MailboxID: task.MailboxID, TaskID: taskID,
				Priority: task.Priority, Payload: envelopeJSON, Headers: dlqHeaders,
			})
			if oerr := s.txOutbox.Insert(ctx, tx, ulid(), tenantID, "dlq_publish", dlqPayload); oerr != nil {
				return fmt.Errorf("outbox dlq: %w", oerr)
			}
			dlEventPayload := MarshalEvent(&core.JanusEvent{
				EventType: core.EventTaskDeadLettered, TenantID: tenantID, TaskID: taskID,
				Payload: errJSON,
			})
			if oerr := s.txOutbox.Insert(ctx, tx, ulid(), tenantID, "event_publish", dlEventPayload); oerr != nil {
				return fmt.Errorf("outbox dead_lettered: %w", oerr)
			}
		}
		committed = true
		return nil
	})
	if err != nil {
		return err
	}
	if attemptFinished {
		_ = s.budgetSvc.Release(ctx, tenantID, attempt.AgentID)
		metrics.NackTotal.WithLabelValues(tenantID).Inc()
	}
	if !committed {
		return nil
	}

	// NATS side effects AFTER DB commit (invariant #5: DB before NATS).
	if attempt.DeliveryRef != "" {
		if retriable {
			// retriable: ACK original so redelivery/scheduler controls retry
			if aerr := s.queueDriver.AckTask(ctx, tenantID, core.DeliveryRef(attempt.DeliveryRef)); aerr != nil {
				log.Printf("ack queue failed after retry_scheduled: tenant=%s task=%s err=%v", tenantID, taskID, aerr)
			}
		} else {
			if nerr := s.queueDriver.NackTask(ctx, tenantID, core.DeliveryRef(attempt.DeliveryRef), core.NackNonRetriable); nerr != nil {
				log.Printf("nack queue failed after dead-letter: tenant=%s task=%s err=%v", tenantID, taskID, nerr)
			}
		}
	}
	return nil
}

func (s *DispatchService) publishEvent(ctx context.Context, event core.JanusEvent) {
	_ = enrichEvent(&event)
	_ = s.queueDriver.PublishEvent(ctx, event)
}

// ensureMailboxConsumer guarantees the tenant streams + mailbox DLQ +
// durable consumer exist BEFORE the pull, and PROPAGATES errors: a pull
// against an uninitialized tenant used to fail opaquely (or wait for the
// 5-minute reconcile) when another replica created the tenant — errors
// here now surface immediately with a clear cause.
func (s *DispatchService) ensureMailboxConsumer(ctx context.Context, tenantID, mailboxID string) error {
	if err := s.queueDriver.EnsureTenant(ctx, tenantID); err != nil {
		return fmt.Errorf("ensure tenant %s: %w", tenantID, err)
	}
	mb, err := s.mailboxRepo.Get(ctx, tenantID, mailboxID)
	if err != nil {
		return fmt.Errorf("mailbox lookup %s: %w", mailboxID, err)
	}
	if mb == nil {
		return fmt.Errorf("mailbox %s not found", mailboxID)
	}
	if err := s.queueDriver.EnsureMailbox(ctx, core.MailboxSpec{
		TenantID:         tenantID,
		MailboxID:        mailboxID,
		AgentID:          mb.AgentID,
		MaxConcurrency:   mb.MaxConcurrency,
		ACKWaitSeconds:   mb.ACKWaitSeconds,
		MaxDeliver:       mb.MaxDeliver,
		RetentionSeconds: mb.RetentionSeconds,
	}); err != nil {
		return fmt.Errorf("ensure mailbox %s: %w", mailboxID, err)
	}
	// Reuse the CREATE-path spec builder: a hand-rolled spec here once
	// omitted MaxACKPending, so a cold-cache replica pulled with a DIFFERENT
	// consumer config than the creator used — NATS answers ErrConsumerExists
	// and pulls fail until the reconcile converges.
	if err := s.queueDriver.EnsureConsumer(ctx, consumerSpecFor(*mb)); err != nil {
		return fmt.Errorf("ensure consumer %s: %w", mailboxID, err)
	}
	return nil
}

func generateLeaseID() string {
	b := make([]byte, 10)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func encodeJSON(v interface{}) ([]byte, error) {
	return mustMarshal(v), nil
}
