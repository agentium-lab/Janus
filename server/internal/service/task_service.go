package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	crand "crypto/rand"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/auth"
	"github.com/agentium-lab/Janus/server/internal/driver/postgres"
	"github.com/agentium-lab/Janus/server/internal/metrics"
	"github.com/agentium-lab/Janus/server/internal/service/routing"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type IntentResolver interface {
	Resolve(ctx context.Context, tenantID, intentValue string, payload core.Payload, contextRefs []core.ContextRef, policyHints []string) (*IntentResolveResult, error)
}

type IntentResolveResult struct {
	ResolvedCapability string
	Confidence         float64
	Reason             string
}

// AgentExistenceChecker verifies an agent is registered under the tenant.
// It backs the server-side source_agent ownership check.
type AgentExistenceChecker interface {
	AgentExists(ctx context.Context, tenantID, agentID string) (bool, error)
}

type TaskService struct {
	taskRepo       TaskRepo
	queueDriver    QueueDriver
	pool           *pgxpool.Pool
	outboxRepo     *postgres.OutboxRepo
	policySvc      *PolicyService
	approvalSvc    *ApprovalService
	lifecycle      *LifecycleService
	intentResolver IntentResolver
	contextRefSvc  *ContextRefService
	router         *routing.Router

	agentExistence AgentExistenceChecker
	attemptRepo    TaskAttemptRepo
}

func NewTaskService(taskRepo TaskRepo, queueDriver QueueDriver, pool *pgxpool.Pool, outboxRepo *postgres.OutboxRepo) *TaskService {
	return &TaskService{
		taskRepo:    taskRepo,
		queueDriver: queueDriver,
		pool:        pool,
		outboxRepo:  outboxRepo,
	}
}

func (s *TaskService) WithPolicy(policySvc *PolicyService) *TaskService {
	s.policySvc = policySvc
	return s
}

func (s *TaskService) WithApproval(approvalSvc *ApprovalService) *TaskService {
	s.approvalSvc = approvalSvc
	return s
}

// WithLifecycle wires the transaction wrapper so management transitions
// (cancel/block/unblock/replay) route their events through the outbox inside a
// transaction. When nil, the service falls back to direct publish.
func (s *TaskService) WithLifecycle(lc *LifecycleService) *TaskService {
	s.lifecycle = lc
	return s
}

func (s *TaskService) WithIntentResolver(r IntentResolver) *TaskService {
	s.intentResolver = r
	return s
}

func (s *TaskService) WithContextRefService(svc *ContextRefService) *TaskService {
	s.contextRefSvc = svc
	return s
}

func (s *TaskService) WithAttemptRepo(r TaskAttemptRepo) *TaskService {
	s.attemptRepo = r
	return s
}

func (s *TaskService) WithAgentExistence(c AgentExistenceChecker) *TaskService {
	s.agentExistence = c
	return s
}

func (s *TaskService) WithRouter(r *routing.Router) *TaskService {
	s.router = r
	return s
}

func (s *TaskService) Create(ctx context.Context, task core.Task) (*core.Task, error) {
	ctx, span := otel.Tracer("janus").Start(ctx, "TaskService.Create",
		trace.WithAttributes(
			attribute.String("tenant.id", task.TenantID),
			attribute.String("task.id", task.ID),
			attribute.String("task.target_type", string(task.TargetType)),
		),
	)
	defer span.End()
	if task.TenantID == "" {
		return nil, fmt.Errorf("tenant id is required")
	}
	if task.ID == "" {
		return nil, fmt.Errorf("task id is required")
	}
	if task.SourceAgent == "" {
		return nil, fmt.Errorf("source agent is required")
	}
	if s.agentExistence != nil {
		exists, err := s.agentExistence.AgentExists(ctx, task.TenantID, task.SourceAgent)
		if err != nil {
			return nil, fmt.Errorf("verify source agent: %w", err)
		}
		if !exists {
			return nil, fmt.Errorf("unknown source_agent %q: agents must be registered under the publishing tenant", task.SourceAgent)
		}
	}
	if task.TargetType == "" {
		return nil, fmt.Errorf("target type is required")
	}

	if task.TargetType == core.TargetType("intent") && s.intentResolver == nil {
		return nil, fmt.Errorf("intent routing not available; configure LLM (JANUS_LLM_ENABLED=true) or use target_type: capability")
	}
	if task.TargetType == core.TargetType("intent") && s.intentResolver != nil {
		hints := []string{}
		if task.Envelope.Policy != nil {
			hints = task.Envelope.Policy.AllowedTools
		}
		result, err := s.intentResolver.Resolve(ctx, task.TenantID, task.TargetValue, task.Envelope.Payload, task.Envelope.ContextRefs, hints)
		if err != nil {
			return nil, fmt.Errorf("intent resolution failed: %w", err)
		}
		if result.ResolvedCapability == "" {
			return nil, fmt.Errorf("intent resolution failed: %s", result.Reason)
		}
		task.TargetType = core.TargetTypeCapability
		task.TargetValue = result.ResolvedCapability
		task.Envelope.Target = core.Target{Type: core.TargetTypeCapability, Value: result.ResolvedCapability}
	}
	if task.TargetValue == "" {
		return nil, fmt.Errorf("target value is required")
	}
	if s.router != nil {
		result, err := s.router.Route(ctx, task.TenantID, core.Target{Type: task.TargetType, Value: task.TargetValue}, task.Envelope)
		if err != nil {
			return nil, fmt.Errorf("routing: %w", err)
		}
		task.MailboxID = result.MailboxID
	}
	if task.Status == "" {
		task.Status = core.TaskStatusCreated
	}
	if task.Priority == "" {
		task.Priority = core.PriorityNormal
	}
	if err := task.Envelope.Validate(); err != nil {
		return nil, fmt.Errorf("envelope validation: %w", err)
	}

	if s.policySvc != nil {
		decision, err := s.policySvc.Evaluate(ctx, core.PolicyInput{
			TenantID: task.TenantID,
			Actor:    core.PolicyActor{Type: "agent", ID: task.SourceAgent},
			Action:   "task.publish",
			Resource: core.PolicyResource{Type: string(task.TargetType), Value: task.TargetValue},
		})
		if err != nil {
			return nil, fmt.Errorf("policy check: %w", err)
		}
		s.emitToolPolicyEvent(ctx, &task, decision.Decision, decision.Reason)
		if decision.Decision == core.PolicyDecisionDeny {
			return nil, fmt.Errorf("policy denied: %s", decision.Reason)
		}
		if decision.Decision == core.PolicyDecisionApprovalRequired {
			task.Status = core.TaskStatusApprovalPending
		}
	}

	if task.IdempotencyKey != "" {
		existing, err := s.taskRepo.GetByIdempotencyKey(ctx, task.TenantID, task.IdempotencyKey)
		if err != nil && err != pgx.ErrNoRows {
			return nil, fmt.Errorf("idempotency key lookup: %w", err)
		}
		if existing != nil {
			return existing, nil
		}
	}

	var result *core.Task
	if s.outboxRepo != nil && s.pool != nil {
		err := s.createWithOutbox(ctx, task)
		if err == nil {
			metrics.TasksCreated.WithLabelValues(task.TenantID).Inc()
			created, _ := s.taskRepo.Get(ctx, task.TenantID, task.ID)
			if created != nil {
				result = created
			} else {
				result = &task
			}
		} else {
			return nil, err
		}
	} else {
		err := s.createDirect(ctx, task)
		if err == nil {
			metrics.TasksCreated.WithLabelValues(task.TenantID).Inc()
			created, _ := s.taskRepo.Get(ctx, task.TenantID, task.ID)
			if created != nil {
				result = created
			} else {
				result = &task
			}
		} else {
			return nil, err
		}
	}

	if task.Status == core.TaskStatusApprovalPending && s.approvalSvc != nil {
		if _, err := s.approvalSvc.RequestApproval(ctx, core.Approval{
			TenantID:    task.TenantID,
			TaskID:      task.ID,
			RequestedBy: task.SourceAgent,
			Reason:      "policy: approval required",
		}); err != nil {
			log.Printf("task %s: request approval: %v", task.ID, err)
		}
	}

	if s.contextRefSvc != nil && len(task.Envelope.ContextRefs) > 0 {
		if err := s.contextRefSvc.NormalizeAndBind(ctx, task.TenantID, task.ID, task.Envelope.ContextRefs); err != nil {
			return result, fmt.Errorf("context ref bind: %w", err)
		}
	}

	return result, nil
}

func (s *TaskService) createWithOutbox(ctx context.Context, task core.Task) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	taskRepo, ok := s.taskRepo.(*postgres.TaskRepository)
	if !ok {
		return s.createDirect(ctx, task)
	}

	if err := taskRepo.CreateTx(ctx, tx, task); err != nil {
		return fmt.Errorf("create task: %w", err)
	}

	createdPayload, _ := json.Marshal(core.JanusEvent{
		EventType:   core.EventTaskCreated,
		TenantID:    task.TenantID,
		TaskID:      task.ID,
		SourceAgent: task.SourceAgent,
		Payload:     mustMarshal(map[string]string{"status": string(task.Status)}),
	})
	if err := s.outboxRepo.Insert(ctx, tx, ulid(), task.TenantID, "event_publish", createdPayload); err != nil {
		return fmt.Errorf("outbox insert created: %w", err)
	}

	if task.MailboxID != "" && task.Status != core.TaskStatusApprovalPending {
		payload, _ := json.Marshal(task.Envelope)
		queuePayload, _ := json.Marshal(core.TaskMessage{
			TenantID:  task.TenantID,
			MailboxID: task.MailboxID,
			TaskID:    task.ID,
			Priority:  task.Priority,
			Payload:   payload,
		})
		if err := s.outboxRepo.Insert(ctx, tx, ulid(), task.TenantID, "task_publish", queuePayload); err != nil {
			return fmt.Errorf("outbox insert task: %w", err)
		}

		if err := taskRepo.UpdateStatusTx(ctx, tx, task.TenantID, task.ID, core.TaskStatusQueued, 0); err != nil {
			return fmt.Errorf("update to queued: %w", err)
		}

		queuedPayload, _ := json.Marshal(core.JanusEvent{
			EventType:   core.EventTaskQueued,
			TenantID:    task.TenantID,
			TaskID:      task.ID,
			SourceAgent: task.SourceAgent,
			Payload:     mustMarshal(map[string]string{"mailbox": task.MailboxID}),
		})
		if err := s.outboxRepo.Insert(ctx, tx, ulid(), task.TenantID, "event_publish", queuedPayload); err != nil {
			return fmt.Errorf("outbox insert queued: %w", err)
		}
	}

	return tx.Commit(ctx)
}

func (s *TaskService) createDirect(ctx context.Context, task core.Task) error {
	if err := s.taskRepo.Create(ctx, task); err != nil {
		return fmt.Errorf("create task: %w", err)
	}

	if err := s.publishEvent(ctx, core.JanusEvent{
		EventType:   core.EventTaskCreated,
		TenantID:    task.TenantID,
		TaskID:      task.ID,
		SourceAgent: task.SourceAgent,
		Payload:     mustMarshal(map[string]string{"status": string(task.Status)}),
	}); err != nil {
		return fmt.Errorf("publish created event: %w", err)
	}

	if task.MailboxID != "" && task.Status != core.TaskStatusApprovalPending {
		payload, err := json.Marshal(task.Envelope)
		if err != nil {
			return fmt.Errorf("marshal envelope: %w", err)
		}
		if err := s.queueDriver.PublishTask(ctx, core.TaskMessage{
			TenantID:  task.TenantID,
			MailboxID: task.MailboxID,
			TaskID:    task.ID,
			Priority:  task.Priority,
			Payload:   payload,
		}); err != nil {
			return fmt.Errorf("publish to queue: %w", err)
		}

		if err := s.taskRepo.UpdateStatus(ctx, task.TenantID, task.ID, core.TaskStatusQueued, 0); err != nil {
			return fmt.Errorf("update to queued: %w", err)
		}

		if err := s.publishEvent(ctx, core.JanusEvent{
			EventType:   core.EventTaskQueued,
			TenantID:    task.TenantID,
			TaskID:      task.ID,
			SourceAgent: task.SourceAgent,
			Payload:     mustMarshal(map[string]string{"mailbox": task.MailboxID}),
		}); err != nil {
			return fmt.Errorf("publish queued event: %w", err)
		}
	}

	return nil
}

func (s *TaskService) Get(ctx context.Context, tenantID, taskID string) (*core.Task, error) {
	if tenantID == "" || taskID == "" {
		return nil, fmt.Errorf("tenant id and task id are required")
	}
	return s.taskRepo.Get(ctx, tenantID, taskID)
}

func (s *TaskService) Start(ctx context.Context, tenantID, taskID string) error {
	return s.transition(ctx, tenantID, taskID, core.TaskStatusRunning, core.EventTaskStarted, 0)
}

func (s *TaskService) Complete(ctx context.Context, tenantID, taskID string) error {
	return s.transition(ctx, tenantID, taskID, core.TaskStatusCompleted, core.EventTaskCompleted, 0)
}

func (s *TaskService) Fail(ctx context.Context, tenantID, taskID string, taskErr *core.TaskError) error {
	if err := s.transition(ctx, tenantID, taskID, core.TaskStatusFailed, core.EventTaskFailed, 1); err != nil {
		return err
	}

	if taskErr != nil {
		errBytes, _ := json.Marshal(taskErr)
		_ = s.publishEvent(ctx, core.JanusEvent{
			EventType: core.EventTaskFailed,
			TenantID:  tenantID,
			TaskID:    taskID,
			Payload:   errBytes,
		})
	}
	return nil
}

func (s *TaskService) Cancel(ctx context.Context, tenantID, taskID string) error {
	return s.transition(ctx, tenantID, taskID, core.TaskStatusCancelled, core.EventTaskCancelled, 0)
}

func (s *TaskService) Block(ctx context.Context, tenantID, taskID, reason string) error {
	if tenantID == "" || taskID == "" {
		return fmt.Errorf("tenant id and task id are required")
	}

	// Lifecycle path: status update + blocked event in one tx via outbox.
	if s.lifecycle != nil {
		if pgTaskRepo, ok := s.taskRepo.(*postgres.TaskRepository); ok {
			err := s.lifecycle.ApplyTx(ctx, func(tx pgx.Tx) error {
				if uerr := pgTaskRepo.UpdateStatusTx(ctx, tx, tenantID, taskID, core.TaskStatusBlocked, 0); uerr != nil {
					return fmt.Errorf("block task: %w", uerr)
				}
				payload, _ := json.Marshal(core.JanusEvent{
					EventType: core.EventTaskBlocked, TenantID: tenantID, TaskID: taskID,
					Payload: mustMarshal(map[string]string{"reason": reason}),
				})
				return s.outboxRepo.Insert(ctx, tx, ulid(), tenantID, "event_publish", payload)
			})
			return err
		}
	}

	// Fallback path.
	if err := s.taskRepo.UpdateStatus(ctx, tenantID, taskID, core.TaskStatusBlocked, 0); err != nil {
		return fmt.Errorf("block task: %w", err)
	}
	return s.publishEvent(ctx, core.JanusEvent{
		EventType: core.EventTaskBlocked,
		TenantID:  tenantID,
		TaskID:    taskID,
		Payload:   mustMarshal(map[string]string{"reason": reason}),
	})
}

func (s *TaskService) Unblock(ctx context.Context, tenantID, taskID string) error {
	return s.transition(ctx, tenantID, taskID, core.TaskStatusRunning, core.EventTaskStarted, 0)
}

func (s *TaskService) Replay(ctx context.Context, tenantID, taskID string) (*core.Task, error) {
	if tenantID == "" || taskID == "" {
		return nil, fmt.Errorf("tenant id and task id are required")
	}

	task, err := s.taskRepo.Get(ctx, tenantID, taskID)
	if err != nil {
		return nil, fmt.Errorf("get task: %w", err)
	}
	if !task.Status.IsTerminal() {
		return nil, fmt.Errorf("only terminal tasks can be replayed, current status: %s", task.Status)
	}

	if err := s.taskRepo.ResetForReplay(ctx, tenantID, taskID); err != nil {
		return nil, fmt.Errorf("reset task: %w", err)
	}

	if task.MailboxID != "" {
		payload, _ := json.Marshal(task.Envelope)
		msg := core.TaskMessage{
			TenantID:  tenantID,
			MailboxID: task.MailboxID,
			TaskID:    taskID,
			Priority:  task.Priority,
			Payload:   payload,
		}
		if s.outboxRepo != nil {
			queuePayload, _ := json.Marshal(msg)
			dedupeKey := fmt.Sprintf("task_publish:%s:%s:replay", tenantID, taskID)
			if err := s.outboxRepo.InsertDirectWithDedupe(ctx, ulid(), tenantID, "task_publish", dedupeKey, queuePayload); err != nil {
				return nil, fmt.Errorf("outbox insert replay: %w", err)
			}
		} else {
			if err := s.queueDriver.PublishTask(ctx, msg); err != nil {
				return nil, fmt.Errorf("re-publish to queue: %w", err)
			}
		}
		if err := s.taskRepo.UpdateStatus(ctx, tenantID, taskID, core.TaskStatusQueued, 0); err != nil {
			return nil, fmt.Errorf("update task queued after replay: %w", err)
		}
	}

	metrics.TasksCreated.WithLabelValues(tenantID).Inc()
	createdEvent := core.JanusEvent{
		EventType:   core.EventTaskCreated,
		TenantID:    tenantID,
		TaskID:      taskID,
		SourceAgent: task.SourceAgent,
		Payload:     mustMarshal(map[string]string{"status": "replayed"}),
	}
	if s.outboxRepo != nil {
		payload, _ := json.Marshal(createdEvent)
		_ = s.outboxRepo.InsertDirect(ctx, ulid(), tenantID, "event_publish", payload)
	} else {
		_ = s.publishEvent(ctx, createdEvent)
	}

	return s.taskRepo.Get(ctx, tenantID, taskID)
}

// ReportProgress validates that the reporting agent holds the latest attempt
// on the task, persists the progress event through the outbox (audit trail),
// and returns the event for in-memory fanout.
func (s *TaskService) ReportProgress(ctx context.Context, tenantID, taskID, agentID string, prog core.TaskProgress) (*core.JanusEvent, error) {
	task, err := s.taskRepo.Get(ctx, tenantID, taskID)
	if err != nil {
		return nil, fmt.Errorf("task not found: %w", err)
	}
	// Task must be actively executing.
	if task.Status != core.TaskStatusClaimed && task.Status != core.TaskStatusRunning {
		return nil, fmt.Errorf("task %s is %s, progress only accepted while claimed or running", taskID, task.Status)
	}
	// Reporter must be the agent processing this task (latest attempt).
	if s.attemptRepo != nil {
		attempt, err := s.attemptRepo.GetLatest(ctx, tenantID, taskID)
		if err != nil || attempt.AgentID != agentID {
			return nil, fmt.Errorf("agent %s does not hold the latest attempt on task %s", agentID, taskID)
		}
	}
	// One event, one identity: the SAME EventID travels the fast lane
	// (broadcaster) and the slow lane (outbox → queue → broadcaster loopback).
	// The broadcaster dedupes by EventID within a bounded window, giving
	// at-least-once delivery with near-duplicate suppression — a loopback
	// delayed beyond the window may redeliver.
	payload, _ := json.Marshal(prog)
	evt := core.JanusEvent{
		EventID:     ulid(),
		EventType:   core.EventTaskProgress,
		TenantID:    tenantID,
		TaskID:      taskID,
		SourceAgent: agentID,
		Payload:     payload,
	}
	if s.outboxRepo != nil {
		evtPayload, _ := json.Marshal(evt)
		if err := s.outboxRepo.InsertDirect(ctx, ulid(), tenantID, "event_publish", evtPayload); err != nil {
			// Audit write failure shouldn't block real-time delivery.
			log.Printf("task %s progress: outbox write failed: %v", taskID, err)
		}
	}
	return &evt, nil
}

func (s *TaskService) ListByStatus(ctx context.Context, tenantID string, status core.TaskStatus, limit int) ([]*core.Task, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("tenant id is required")
	}
	if limit <= 0 {
		limit = 50
	}
	return s.taskRepo.ListByStatus(ctx, tenantID, status, limit)
}

func (s *TaskService) transition(ctx context.Context, tenantID, taskID string, status core.TaskStatus, eventType core.EventType, attemptInc int) error {
	if tenantID == "" || taskID == "" {
		return fmt.Errorf("tenant id and task id are required")
	}
	current, err := s.taskRepo.Get(ctx, tenantID, taskID)
	if err != nil {
		return fmt.Errorf("get task for transition: %w", err)
	}
	if current.Status.IsTerminal() {
		return fmt.Errorf("task %s is in terminal state %s, cannot transition to %s", taskID, current.Status, status)
	}
	if !core.CanTransition(current.Status, status) {
		return fmt.Errorf("invalid transition: %s -> %s for task %s", current.Status, status, taskID)
	}

	// Lifecycle path: CAS + event outbox in one tx (when PG repos + lifecycle).
	if s.lifecycle != nil {
		if _, ok := s.taskRepo.(*postgres.TaskRepository); ok {
			err = s.lifecycle.ApplyTx(ctx, func(tx pgx.Tx) error {
				return s.TransitionInTx(ctx, tx, tenantID, taskID, current.Status, status, eventType, attemptInc)
			})
			if err != nil {
				return err
			}
			recordTaskMetric(tenantID, status)
			return nil
		}
	}

	// Fallback path (no lifecycle or non-PG repo).
	ok, err := s.taskRepo.UpdateStatusWithCheck(ctx, tenantID, taskID, current.Status, status, attemptInc)
	if err != nil {
		return fmt.Errorf("update task status to %s: %w", status, err)
	}
	if !ok {
		return fmt.Errorf("conflict: task %s status changed concurrently, expected %s", taskID, current.Status)
	}
	recordTaskMetric(tenantID, status)
	return s.publishEvent(ctx, core.JanusEvent{
		EventType: eventType,
		TenantID:  tenantID,
		TaskID:    taskID,
		Payload:   mustMarshal(map[string]string{"status": string(status)}),
	})
}

// appendClaimedActor annotates an object-shaped event payload with the
// non-authoritative claimed_actor hint. Non-object payloads and payloads that
// already carry the field pass through untouched.
func appendClaimedActor(payload []byte, actor string) []byte {
	m := map[string]interface{}{}
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &m); err != nil || m == nil {
			return payload
		}
	}
	if _, exists := m["claimed_actor"]; exists {
		return payload
	}
	m["claimed_actor"] = actor
	out, err := json.Marshal(m)
	if err != nil {
		return payload
	}
	return out
}

// TransitionInTx applies a guarded status change and records the lifecycle
// event on the caller's transaction so decision tables and task state commit
// atomically. expectedStatus is what the caller read before opening the tx;
// a mismatch fails the whole transaction (retryable).
func (s *TaskService) TransitionInTx(ctx context.Context, tx pgx.Tx, tenantID, taskID string, expected, status core.TaskStatus, eventType core.EventType, attemptInc int) error {
	pgTaskRepo, ok := s.taskRepo.(*postgres.TaskRepository)
	if !ok {
		return fmt.Errorf("transition in tx requires postgres task repo")
	}
	if !core.CanTransition(expected, status) {
		return fmt.Errorf("invalid transition: %s -> %s for task %s", expected, status, taskID)
	}
	ok2, err := pgTaskRepo.UpdateStatusWithCheckTx(ctx, tx, tenantID, taskID, expected, status, attemptInc)
	if err != nil {
		return fmt.Errorf("update task status to %s: %w", status, err)
	}
	if !ok2 {
		return fmt.Errorf("conflict: task %s status changed concurrently, expected %s", taskID, expected)
	}
	if s.outboxRepo != nil {
		payload, _ := json.Marshal(core.JanusEvent{
			EventType: eventType, TenantID: tenantID, TaskID: taskID,
			Payload: mustMarshal(map[string]string{"status": string(status)}),
		})
		if ierr := s.outboxRepo.Insert(ctx, tx, ulid(), tenantID, "event_publish", payload); ierr != nil {
			return fmt.Errorf("record event: %w", ierr)
		}
	}
	recordTaskMetric(tenantID, status)
	return nil
}

func (s *TaskService) publishEvent(ctx context.Context, event core.JanusEvent) error {
	if actor := auth.ActingUserFromContext(ctx); actor != "" {
		event.Payload = appendClaimedActor(event.Payload, actor)
	}
	if err := enrichEvent(&event); err != nil {
		return err
	}
	return s.queueDriver.PublishEvent(ctx, event)
}

func (s *TaskService) emitToolPolicyEvent(ctx context.Context, task *core.Task, decision core.PolicyDecisionType, reason string) {
	ti := task.Envelope.ToolInvocation
	if ti == nil {
		return
	}
	var typ core.EventType
	switch decision {
	case core.PolicyDecisionDeny:
		typ = core.EventToolInvocationDenied
	case core.PolicyDecisionAllow, core.PolicyDecisionApprovalRequired:
		typ = core.EventToolInvocationAllowed
	default:
		return
	}
	payload, _ := json.Marshal(map[string]string{"tool_name": ti.Name, "reason": reason})
	_ = s.publishEvent(ctx, core.JanusEvent{
		EventType: typ, TenantID: task.TenantID, TaskID: task.ID,
		SourceAgent: task.SourceAgent, Payload: payload,
	})
}

type IdempotentError struct {
	ExistingTaskID string
}

func (e *IdempotentError) Error() string {
	return fmt.Sprintf("task already exists with idempotency key: %s", e.ExistingTaskID)
}

func mustMarshal(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}

func recordTaskMetric(tenantID string, status core.TaskStatus) {
	switch status {
	case core.TaskStatusCompleted:
		metrics.TasksCompleted.WithLabelValues(tenantID).Inc()
	case core.TaskStatusFailed:
		metrics.TasksFailed.WithLabelValues(tenantID).Inc()
	case core.TaskStatusDeadLettered:
		metrics.TasksDeadLettered.WithLabelValues(tenantID).Inc()
	}
}

func ulid() string {
	now := time.Now()
	t := uint64(now.UnixMilli())
	b := make([]byte, 10)
	for i := 9; i >= 0; i-- {
		b[i] = byte(t & 0xff)
		t >>= 8
	}
	// Use crypto/rand for the entropy portion so IDs generated within the same
	// millisecond do not collide.
	randBytes := make([]byte, 6)
	_, _ = crand.Read(randBytes)
	return fmt.Sprintf("%x%x", b, randBytes)
}
