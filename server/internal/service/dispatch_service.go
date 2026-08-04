package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/agentium-lab/Janus/core"
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
	lifecycle   *LifecycleService
	outboxRepo  *postgres.OutboxRepo
	budgetUsage *postgres.BudgetUsageRepo
}

func NewDispatchService(
	taskRepo TaskRepo,
	attemptRepo TaskAttemptRepo,
	mailboxRepo MailboxRepo,
	queueDriver QueueDriver,
	policySvc *PolicyService,
	budgetSvc *BudgetService,
) *DispatchService {
	return &DispatchService{
		taskRepo:    taskRepo,
		attemptRepo: attemptRepo,
		mailboxRepo: mailboxRepo,
		queueDriver: queueDriver,
		policySvc:   policySvc,
		budgetSvc:   budgetSvc,
	}
}

// WithLifecycle wires the transaction wrapper + outbox + budget-usage repos.
// When set, ACK/NACK/Pull/Start go through ApplyTx + outbox (production path).
// When nil, the service falls back to direct publish (test path).
func (s *DispatchService) WithLifecycle(lc *LifecycleService, outboxRepo *postgres.OutboxRepo, budgetUsage *postgres.BudgetUsageRepo) *DispatchService {
	s.lifecycle = lc
	s.outboxRepo = outboxRepo
	s.budgetUsage = budgetUsage
	return s
}

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

	mb, mbErr := s.mailboxRepo.Get(ctx, tenantID, mailboxID)
	if mbErr == nil && mb != nil && mb.AgentID != "" && mb.AgentID != agentID {
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
		return nil, &core.BackpressureError{
			Reason:  core.ReasonApprovalRequired,
			Message: fmt.Sprintf("policy denied: %s", decision.Reason),
		}
	}

	tenantRunning, _ := s.taskRepo.CountByStatus(ctx, tenantID, core.TaskStatusRunning)
	agentRunning, _ := s.taskRepo.CountRunningByAgent(ctx, tenantID, agentID)
	if err := s.budgetSvc.CheckConcurrency(ctx, tenantID, agentID, agentRunning, tenantRunning); err != nil {
		return nil, err
	}

	s.ensureMailboxConsumer(ctx, tenantID, mailboxID)

	deliveries, err := s.queueDriver.FetchTasks(ctx, mailboxID, core.FetchOptions{
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
		_ = s.queueDriver.AckTask(ctx, core.DeliveryRef(delivery.DeliveryRef))
		return nil, nil
	case task.Status == core.TaskStatusRetryScheduled:
		// retry_scheduled: the retry scheduler / outbox will re-publish at the
		// backoff time. ACK this stale delivery so NATS stops redelivering it.
		_ = s.queueDriver.AckTask(ctx, core.DeliveryRef(delivery.DeliveryRef))
		return nil, nil
	case task.Status == core.TaskStatusCreated:
		// created-but-not-yet-queued: the outbox hasn't published yet. NACK to
		// preserve the broker message; it will be redelivered after the task
		// reaches 'queued'. Use a short delay to avoid a tight redelivery loop.
		_ = s.queueDriver.NackTask(ctx, core.DeliveryRef(delivery.DeliveryRef), core.NackRetriable)
		return nil, nil
	case task.Status == core.TaskStatusClaimed || task.Status == core.TaskStatusRunning:
		// In-flight task. Check whether this delivery matches the current
		// attempt (a duplicate redelivery of the in-flight attempt) or is a
		// stale/older delivery.
		latest, lerr := s.attemptRepo.GetLatest(ctx, tenantID, task.ID)
		if lerr == nil && latest != nil && latest.DeliveryRef == string(delivery.DeliveryRef) {
			// Same in-flight attempt redelivered: ACK the duplicate.
			_ = s.queueDriver.AckTask(ctx, core.DeliveryRef(delivery.DeliveryRef))
			return nil, nil
		}
		// Otherwise this is a delivery for a different/older attempt while a
		// newer attempt is in flight. ACK to clear it; the in-flight attempt
		// owns the task.
		_ = s.queueDriver.AckTask(ctx, core.DeliveryRef(delivery.DeliveryRef))
		return nil, nil
	}

	dispatchDecision, dispatchErr := s.policySvc.Evaluate(ctx, core.PolicyInput{
		TenantID: tenantID,
		Actor:    core.PolicyActor{Type: "agent", ID: agentID},
		Action:   "dispatch",
		Resource: core.PolicyResource{Type: "task", Value: task.ID},
	})
	if dispatchErr == nil && dispatchDecision.Decision == core.PolicyDecisionDeny {
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
			_ = s.queueDriver.NackTask(nackCtx, core.DeliveryRef(deliveryRef), core.NackRetriable)
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
			_ = s.queueDriver.NackTask(nackCtx, core.DeliveryRef(deliveryRef), core.NackRetriable)
		})
		return nil, err
	}

	leaseID := generateLeaseID()
	expiresAt := time.Now().Add(300 * time.Second)

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

	// Lifecycle path: attempt create + task status + claimed event in one tx.
	if s.lifecycle != nil {
		if pgTaskRepo, ok := s.taskRepo.(*postgres.TaskRepository); ok {
			if pgAttemptRepo, ok := s.attemptRepo.(*postgres.TaskAttemptRepository); ok {
				err = s.lifecycle.ApplyTx(ctx, func(tx pgx.Tx) error {
					if cerr := pgAttemptRepo.CreateTx(ctx, tx, attempt); cerr != nil {
						return fmt.Errorf("create attempt: %w", cerr)
					}
					if _, uerr := pgTaskRepo.UpdateStatusWithCheckTx(ctx, tx, tenantID, task.ID, task.Status, core.TaskStatusClaimed, 1); uerr != nil {
						return fmt.Errorf("update task claimed: %w", uerr)
					}
					claimedPayload, _ := json.Marshal(core.JanusEvent{
						EventType: core.EventTaskClaimed, TenantID: tenantID, TaskID: task.ID,
						Payload: mustMarshal(map[string]string{"lease_id": leaseID, "agent_id": agentID}),
					})
					return s.outboxRepo.Insert(ctx, tx, ulid(), tenantID, "event_publish", claimedPayload)
				})
			if err != nil {
				s.budgetSvc.Release(ctx, tenantID, agentID)
				return nil, err
			}
			return &PullResult{Task: task, LeaseID: leaseID, ExpiresAt: expiresAt}, nil
			}
		}
	}

	// Fallback path (no lifecycle or non-PG repos).
	if err := s.attemptRepo.Create(ctx, attempt); err != nil {
		s.budgetSvc.Release(ctx, tenantID, agentID)
		return nil, fmt.Errorf("create attempt: %w", err)
	}

	if err := s.taskRepo.UpdateStatus(ctx, tenantID, task.ID, core.TaskStatusClaimed, 1); err != nil {
		s.budgetSvc.Release(ctx, tenantID, agentID)
		return nil, fmt.Errorf("update task claimed: %w", err)
	}

	s.publishEvent(ctx, core.JanusEvent{
		EventType: core.EventTaskClaimed,
		TenantID:  tenantID,
		TaskID:    task.ID,
		Payload:   mustMarshal(map[string]string{"lease_id": leaseID, "agent_id": agentID}),
	})

	return &PullResult{
		Task:      task,
		LeaseID:   leaseID,
		ExpiresAt: expiresAt,
	}, nil
}

func (s *DispatchService) StartTask(ctx context.Context, tenantID, taskID, leaseID string) error {
	if tenantID == "" || taskID == "" || leaseID == "" {
		return fmt.Errorf("tenant id, task id, and lease id are required")
	}

	attempt, err := s.attemptRepo.GetLatest(ctx, tenantID, taskID)
	if err != nil {
		return fmt.Errorf("get latest attempt: %w", err)
	}
	if attempt.LeaseID != leaseID {
		return fmt.Errorf("lease mismatch: expected %s, got %s", attempt.LeaseID, leaseID)
	}

	// Lifecycle path: task status + started event in one tx.
	if s.lifecycle != nil {
		if pgTaskRepo, ok := s.taskRepo.(*postgres.TaskRepository); ok {
			err = s.lifecycle.ApplyTx(ctx, func(tx pgx.Tx) error {
				task, gerr := s.taskRepo.Get(ctx, tenantID, taskID)
				if gerr != nil {
					return fmt.Errorf("get task: %w", gerr)
				}
				if _, uerr := pgTaskRepo.UpdateStatusWithCheckTx(ctx, tx, tenantID, taskID, task.Status, core.TaskStatusRunning, 0); uerr != nil {
					return fmt.Errorf("update task running: %w", uerr)
				}
				startedPayload, _ := json.Marshal(core.JanusEvent{
					EventType: core.EventTaskStarted, TenantID: tenantID, TaskID: taskID,
					Payload: mustMarshal(map[string]string{"lease_id": leaseID}),
				})
				return s.outboxRepo.Insert(ctx, tx, ulid(), tenantID, "event_publish", startedPayload)
			})
			return err
		}
	}

	// Fallback path.
	if err := s.taskRepo.UpdateStatus(ctx, tenantID, taskID, core.TaskStatusRunning, 0); err != nil {
		return fmt.Errorf("update task running: %w", err)
	}

	s.publishEvent(ctx, core.JanusEvent{
		EventType: core.EventTaskStarted,
		TenantID:  tenantID,
		TaskID:    taskID,
		Payload:   mustMarshal(map[string]string{"lease_id": leaseID}),
	})

	return nil
}

func (s *DispatchService) TaskHeartbeat(ctx context.Context, tenantID, taskID, leaseID string) error {
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

	return s.attemptRepo.UpdateHeartbeat(ctx, tenantID, taskID, attempt.Attempt)
}

func (s *DispatchService) AckTask(ctx context.Context, tenantID, taskID, leaseID string, resultRef string, usage *core.TokenUsage) error {
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

	// Fallback path (no lifecycle): keep legacy direct behavior.
	if s.lifecycle == nil {
		return s.ackTaskDirect(ctx, tenantID, taskID, attempt, resultRef, usage)
	}

	// The lifecycle path needs concrete PG repos for the *Tx methods. This
	// mirrors TaskService.createWithOutbox's type assertion pattern.
	pgTaskRepo, ok := s.taskRepo.(*postgres.TaskRepository)
	if !ok {
		return s.ackTaskDirect(ctx, tenantID, taskID, attempt, resultRef, usage)
	}
	pgAttemptRepo, ok := s.attemptRepo.(*postgres.TaskAttemptRepository)
	if !ok {
		return s.ackTaskDirect(ctx, tenantID, taskID, attempt, resultRef, usage)
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
		ok, ferr := pgAttemptRepo.UpdateFinishedWithCheckTx(ctx, tx, tenantID, taskID, attempt.Attempt, "completed", nil, usageJSON)
		if err := ferr; err != nil {
			return fmt.Errorf("finish attempt: %w", err)
		}
		if !ok {
			// Already finished (duplicate ACK). No-op.
			return nil
		}

		task, gerr := s.taskRepo.Get(ctx, tenantID, taskID)
		if gerr != nil {
			return fmt.Errorf("get task: %w", gerr)
		}
		taskOK, uerr := pgTaskRepo.UpdateStatusWithCheckTx(ctx, tx, tenantID, taskID, task.Status, core.TaskStatusCompleted, 0)
		if uerr != nil {
			return fmt.Errorf("complete task: %w", uerr)
		}
		if !taskOK {
			return nil
		}
		if resultRef != "" {
			if serr := pgTaskRepo.SetResultRefTx(ctx, tx, tenantID, taskID, resultRef); serr != nil {
				return fmt.Errorf("set result ref: %w", serr)
			}
		}

		// Idempotent settlement: ledger (two scopes), increment only on insert.
		for _, scope := range []struct{ Type, ID string }{
			{"tenant", tenantID},
			{"agent", attempt.AgentID},
		} {
			inserted, ierr := s.budgetUsage.InsertLedgerTx(ctx, tx, core.LedgerEntry{
				TenantID: tenantID, TaskID: taskID, Attempt: attempt.Attempt,
				ScopeType: scope.Type, ScopeID: scope.ID,
				PromptTokens: prompt, CompletionTokens: completion,
				TotalTokens: total, CostUSD: 0,
			})
			if ierr != nil {
				return fmt.Errorf("ledger insert %s: %w", scope.Type, ierr)
			}
			if inserted {
				if ierr := s.budgetUsage.IncrementUsageTx(ctx, tx, tenantID, scope.Type, scope.ID, prompt, completion, total, 0); ierr != nil {
					return fmt.Errorf("increment usage %s: %w", scope.Type, ierr)
				}
			}
		}

		// completed event via outbox.
		completedPayload, _ := json.Marshal(core.JanusEvent{
			EventType: core.EventTaskCompleted, TenantID: tenantID, TaskID: taskID,
			SourceAgent: attempt.AgentID,
			Payload:     mustMarshal(map[string]string{"result_ref": resultRef}),
		})
		if oerr := s.outboxRepo.Insert(ctx, tx, ulid(), tenantID, "event_publish", completedPayload); oerr != nil {
			return fmt.Errorf("outbox completed: %w", oerr)
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

	// ACK NATS only after DB commit.
	if attempt.DeliveryRef != "" {
		if aerr := s.queueDriver.AckTask(ctx, core.DeliveryRef(attempt.DeliveryRef)); aerr != nil {
			log.Printf("ack queue message failed after task completed: tenant=%s task=%s attempt=%d delivery_ref=%s err=%v",
				tenantID, taskID, attempt.Attempt, attempt.DeliveryRef, aerr)
			warnPayload, _ := json.Marshal(core.JanusEvent{
				EventType: core.EventTaskCompleted, TenantID: tenantID, TaskID: taskID,
				Payload: mustMarshal(map[string]string{"result_ref": resultRef, "ack_error": aerr.Error(), "delivery_ref": attempt.DeliveryRef}),
			})
			_ = s.outboxRepo.InsertDirect(ctx, ulid(), tenantID, "event_publish", warnPayload)
		}
	}
	return nil
}

// ackTaskDirect is the fallback ACK path (no LifecycleService). Preserves the
// pre-lifecycle behavior for unit tests / in-memory driver scenarios.
func (s *DispatchService) ackTaskDirect(ctx context.Context, tenantID, taskID string, attempt *core.TaskAttempt, resultRef string, usage *core.TokenUsage) error {
	var usageJSON []byte
	if usage != nil {
		usageJSON, _ = encodeJSON(usage)
	}

	ok, err := s.attemptRepo.UpdateFinishedWithCheck(ctx, tenantID, taskID, attempt.Attempt, "completed", nil, usageJSON)
	if err != nil {
		return fmt.Errorf("finish attempt: %w", err)
	}
	if !ok {
		return nil
	}

	if err := s.taskRepo.UpdateStatus(ctx, tenantID, taskID, core.TaskStatusCompleted, 0); err != nil {
		return fmt.Errorf("complete task: %w", err)
	}

	if resultRef != "" {
		_ = s.taskRepo.SetResultRef(ctx, tenantID, taskID, resultRef)
	}

	_ = s.budgetSvc.Settle(ctx, tenantID, attempt.AgentID, usage)

	if attempt.DeliveryRef != "" {
		if err := s.queueDriver.AckTask(ctx, core.DeliveryRef(attempt.DeliveryRef)); err != nil {
			s.publishEvent(ctx, core.JanusEvent{
				EventType: core.EventTaskCompleted,
				TenantID:  tenantID,
				TaskID:    taskID,
				Payload:   mustMarshal(map[string]string{"result_ref": resultRef, "ack_warn": err.Error()}),
			})
			return fmt.Errorf("ack queue message: %w", err)
		}
	}

	s.publishEvent(ctx, core.JanusEvent{
		EventType: core.EventTaskCompleted,
		TenantID:  tenantID,
		TaskID:    taskID,
		Payload:   mustMarshal(map[string]string{"result_ref": resultRef}),
	})

	return nil
}

func (s *DispatchService) NackTask(ctx context.Context, tenantID, taskID, leaseID string, retriable bool, taskErr *core.TaskError) error {
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

	// Fallback path (no lifecycle): keep legacy direct behavior.
	if s.lifecycle == nil {
		return s.nackTaskDirect(ctx, tenantID, taskID, attempt, retriable, taskErr)
	}

	// Lifecycle path needs concrete PG repos for *Tx methods.
	pgTaskRepo, ok := s.taskRepo.(*postgres.TaskRepository)
	if !ok {
		return s.nackTaskDirect(ctx, tenantID, taskID, attempt, retriable, taskErr)
	}
	pgAttemptRepo, ok := s.attemptRepo.(*postgres.TaskAttemptRepository)
	if !ok {
		return s.nackTaskDirect(ctx, tenantID, taskID, attempt, retriable, taskErr)
	}

	var errJSON []byte
	if taskErr != nil {
		errJSON, _ = encodeJSON(taskErr)
	}

	task, err := s.taskRepo.Get(ctx, tenantID, taskID)
	if err != nil {
		return fmt.Errorf("get task: %w", err)
	}
	mb, _ := s.mailboxRepo.Get(ctx, tenantID, task.MailboxID)
	canRetry := retriable && mb != nil && !mb.RetryPolicy.ExceedsMaxAttempts(task.AttemptCount)

	committed := false
	attemptFinished := false
	err = s.lifecycle.ApplyTx(ctx, func(tx pgx.Tx) error {
		ok, ferr := pgAttemptRepo.UpdateFinishedWithCheckTx(ctx, tx, tenantID, taskID, attempt.Attempt, "failed", errJSON, nil)
		if err := ferr; err != nil {
			return fmt.Errorf("finish attempt: %w", err)
		}
		if !ok {
			return nil // duplicate NACK, no-op
		}
		attemptFinished = true

		if canRetry {
			retryAt := time.Now().Add(mb.RetryPolicy.BackoffDuration(task.AttemptCount))
			retryOK, uerr := pgTaskRepo.UpdateStatusWithCheckTx(ctx, tx, tenantID, taskID, task.Status, core.TaskStatusRetryScheduled, 0)
			if uerr != nil {
				return fmt.Errorf("set retry_scheduled: %w", uerr)
			}
			if !retryOK {
				return nil
			}
			if rerr := pgTaskRepo.UpdateRetryAtTx(ctx, tx, tenantID, taskID, retryAt); rerr != nil {
				return fmt.Errorf("set retry_at: %w", rerr)
			}
			retryPayload, _ := json.Marshal(core.JanusEvent{
				EventType: core.EventTaskRetryScheduled, TenantID: tenantID, TaskID: taskID,
				Payload: mustMarshal(map[string]string{"attempt": fmt.Sprintf("%d", task.AttemptCount)}),
			})
			if oerr := s.outboxRepo.Insert(ctx, tx, ulid(), tenantID, "event_publish", retryPayload); oerr != nil {
				return fmt.Errorf("outbox retry: %w", oerr)
			}
		} else {
			dlOK, uerr := pgTaskRepo.UpdateStatusWithCheckTx(ctx, tx, tenantID, taskID, task.Status, core.TaskStatusDeadLettered, 0)
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
			if oerr := s.outboxRepo.Insert(ctx, tx, ulid(), tenantID, "dlq_publish", dlqPayload); oerr != nil {
				return fmt.Errorf("outbox dlq: %w", oerr)
			}
			dlEventPayload, _ := json.Marshal(core.JanusEvent{
				EventType: core.EventTaskDeadLettered, TenantID: tenantID, TaskID: taskID,
				Payload: errJSON,
			})
			if oerr := s.outboxRepo.Insert(ctx, tx, ulid(), tenantID, "event_publish", dlEventPayload); oerr != nil {
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
	}
	if !committed {
		return nil
	}

	// NATS side effects AFTER DB commit (invariant #5: DB before NATS).
	if attempt.DeliveryRef != "" {
		if retriable {
			// retriable: ACK original so redelivery/scheduler controls retry
			if aerr := s.queueDriver.AckTask(ctx, core.DeliveryRef(attempt.DeliveryRef)); aerr != nil {
				log.Printf("ack queue failed after retry_scheduled: tenant=%s task=%s err=%v", tenantID, taskID, aerr)
			}
		} else {
			if nerr := s.queueDriver.NackTask(ctx, core.DeliveryRef(attempt.DeliveryRef), core.NackNonRetriable); nerr != nil {
				log.Printf("nack queue failed after dead-letter: tenant=%s task=%s err=%v", tenantID, taskID, nerr)
			}
		}
	}
	return nil
}

// nackTaskDirect is the fallback NACK path (no LifecycleService). Preserves the
// pre-lifecycle behavior. NOTE: this path ACKs NATS before updating DB, which
// is the ordering issue the lifecycle path fixes; kept only for tests.
func (s *DispatchService) nackTaskDirect(ctx context.Context, tenantID, taskID string, attempt *core.TaskAttempt, retriable bool, taskErr *core.TaskError) error {
	var errJSON []byte
	if taskErr != nil {
		errJSON, _ = encodeJSON(taskErr)
	}

	_ = s.budgetSvc.Release(ctx, tenantID, attempt.AgentID)

	if attempt.DeliveryRef != "" {
		if retriable {
			_ = s.queueDriver.AckTask(ctx, core.DeliveryRef(attempt.DeliveryRef))
		} else {
			_ = s.queueDriver.NackTask(ctx, core.DeliveryRef(attempt.DeliveryRef), core.NackNonRetriable)
		}
	}

	ok, err := s.attemptRepo.UpdateFinishedWithCheck(ctx, tenantID, taskID, attempt.Attempt, "failed", errJSON, nil)
	if err != nil {
		return fmt.Errorf("finish attempt: %w", err)
	}
	if !ok {
		return nil
	}

	task, err := s.taskRepo.Get(ctx, tenantID, taskID)
	if err != nil {
		return fmt.Errorf("get task: %w", err)
	}

	if retriable {
		mb, mbErr := s.mailboxRepo.Get(ctx, tenantID, task.MailboxID)
		if mbErr == nil && !mb.RetryPolicy.ExceedsMaxAttempts(task.AttemptCount) {
			if err := s.taskRepo.UpdateRetryAt(ctx, tenantID, taskID,
				time.Now().Add(mb.RetryPolicy.BackoffDuration(task.AttemptCount)),
			); err != nil {
				return fmt.Errorf("schedule retry: %w", err)
			}
			s.publishEvent(ctx, core.JanusEvent{
				EventType: core.EventTaskRetryScheduled,
				TenantID:  tenantID,
				TaskID:    taskID,
				Payload:   mustMarshal(map[string]string{"attempt": fmt.Sprintf("%d", task.AttemptCount)}),
			})
			return nil
		}
	}

	if err := s.taskRepo.UpdateStatus(ctx, tenantID, taskID, core.TaskStatusDeadLettered, 0); err != nil {
		return fmt.Errorf("dead letter: %w", err)
	}

	envelopeJSON, _ := json.Marshal(task.Envelope)
	_ = s.queueDriver.PublishDLQ(ctx, core.TaskMessage{
		TenantID:  tenantID,
		MailboxID: task.MailboxID,
		TaskID:    taskID,
		Priority:  task.Priority,
		Payload:   envelopeJSON,
		Headers:   map[string]string{"attempt_count": fmt.Sprintf("%d", task.AttemptCount)},
	}, errJSON)

	s.publishEvent(ctx, core.JanusEvent{
		EventType: core.EventTaskDeadLettered,
		TenantID:  tenantID,
		TaskID:    taskID,
		Payload:   errJSON,
	})

	return nil
}

func (s *DispatchService) publishEvent(ctx context.Context, event core.JanusEvent) {
	_ = enrichEvent(&event)
	_ = s.queueDriver.PublishEvent(ctx, event)
}

func (s *DispatchService) ensureMailboxConsumer(ctx context.Context, tenantID, mailboxID string) {
	mb, err := s.mailboxRepo.Get(ctx, tenantID, mailboxID)
	if err != nil || mb == nil {
		return
	}
	_ = s.queueDriver.EnsureMailbox(ctx, core.MailboxSpec{
		TenantID:         tenantID,
		MailboxID:        mailboxID,
		AgentID:          mb.AgentID,
		MaxConcurrency:   mb.MaxConcurrency,
		ACKWaitSeconds:   mb.ACKWaitSeconds,
		MaxDeliver:       mb.MaxDeliver,
		RetentionSeconds: mb.RetentionSeconds,
	})
	_ = s.queueDriver.EnsureConsumer(ctx, core.ConsumerSpec{
		TenantID:       tenantID,
		MailboxID:      mailboxID,
		DurableName:    mailboxID,
		ACKWaitSeconds: mb.ACKWaitSeconds,
		MaxDeliver:     mb.MaxDeliver,
	})
}

func generateLeaseID() string {
	b := make([]byte, 10)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func encodeJSON(v interface{}) ([]byte, error) {
	return mustMarshal(v), nil
}
