package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/driver/postgres"
)

type ApprovalRepo interface {
	Create(ctx context.Context, approval core.Approval) error
	Get(ctx context.Context, tenantID, approvalID string) (*core.Approval, error)
	GetPendingByTask(ctx context.Context, tenantID, taskID string) (*core.Approval, error)
	UpdateDecision(ctx context.Context, tenantID, approvalID, decision, approver, reason string) error
	ListPending(ctx context.Context, tenantID string, limit int) ([]*core.Approval, error)
}

type ApprovalService struct {
	repo       ApprovalRepo
	taskSvc    *TaskService
	queueDrv   core.QueueEventDriver
	outbox     OutboxWriter
	outboxRepo *postgres.OutboxRepo
	pool       *pgxpool.Pool
}

func NewApprovalService(repo ApprovalRepo, taskSvc *TaskService, queueDrv core.QueueEventDriver) *ApprovalService {
	return &ApprovalService{repo: repo, taskSvc: taskSvc, queueDrv: queueDrv}
}

func (s *ApprovalService) WithOutbox(outbox OutboxWriter) *ApprovalService {
	s.outbox = outbox
	return s
}

// WithOutboxRepo wires the transactional outbox repo + pool so Approve can
// commit the approval decision together with the task_publish/event_publish
// outbox rows in a single transaction (mirrors LifecycleService.ApplyTx).
func (s *ApprovalService) WithOutboxRepo(outboxRepo *postgres.OutboxRepo, pool *pgxpool.Pool) *ApprovalService {
	s.outboxRepo = outboxRepo
	s.pool = pool
	return s
}

func (s *ApprovalService) RequestApproval(ctx context.Context, approval core.Approval) (*core.Approval, error) {
	if approval.TenantID == "" || approval.TaskID == "" || approval.RequestedBy == "" {
		return nil, fmt.Errorf("tenant id, task id, and requested_by are required")
	}
	if approval.ID == "" {
		approval.ID = ulid()
	}
	if approval.Status == "" {
		approval.Status = "pending"
	}
	if approval.ExpiresAt.IsZero() {
		approval.ExpiresAt = time.Now().Add(24 * time.Hour)
	}
	if err := s.repo.Create(ctx, approval); err != nil {
		return nil, fmt.Errorf("create approval: %w", err)
	}
	return &approval, nil
}

func (s *ApprovalService) Approve(ctx context.Context, tenantID, approvalID, approver, reason string) error {
	approval, err := s.repo.Get(ctx, tenantID, approvalID)
	if err != nil {
		return fmt.Errorf("get approval: %w", err)
	}
	if approval.Status != "pending" {
		return fmt.Errorf("approval already decided: %s", approval.Status)
	}
	if !approval.ExpiresAt.IsZero() && time.Now().After(approval.ExpiresAt) {
		return s.Expire(ctx, tenantID, approvalID)
	}

	// Atomic path: when wired with a pool + tx outbox, commit the approval
	// decision together with the task_publish/event_publish outbox rows in a
	// single transaction. Falls back to the legacy non-atomic path otherwise
	// (unit tests / in-memory driver).
	if s.pool != nil && s.outboxRepo != nil {
		return s.approveAtomic(ctx, tenantID, approvalID, approver, reason, approval)
	}

	if err := s.repo.UpdateDecision(ctx, tenantID, approvalID, "approved", approver, reason); err != nil {
		return fmt.Errorf("update approval: %w", err)
	}
	if err := s.taskSvc.transition(ctx, tenantID, approval.TaskID, core.TaskStatusQueued, core.EventTaskQueued, 0); err != nil {
		return fmt.Errorf("queue task: %w", err)
	}
	task, err := s.taskSvc.Get(ctx, tenantID, approval.TaskID)
	if err == nil && task != nil && task.MailboxID != "" {
		payload, _ := json.Marshal(task.Envelope)
		msg := core.TaskMessage{
			TenantID:  tenantID,
			MailboxID: task.MailboxID,
			TaskID:    approval.TaskID,
			Priority:  task.Priority,
			Payload:   payload,
		}
		if s.outbox != nil {
			queuePayload, _ := json.Marshal(msg)
			_ = s.outbox.InsertDirect(ctx, ulid(), tenantID, "task_publish", queuePayload)
		} else if s.queueDrv != nil {
			_ = s.queueDrv.PublishTask(ctx, msg)
		}
	}
	return nil
}

// approveAtomic runs the approval decision + task status + outbox writes for
// the queued delivery in a single PG transaction. Mirrors the lifecycle path
// used by DispatchService.PullTask.
func (s *ApprovalService) approveAtomic(ctx context.Context, tenantID, approvalID, approver, reason string, approval *core.Approval) error {
	pgApprovalRepo, ok := s.repo.(*postgres.ApprovalRepo)
	if !ok {
		return fmt.Errorf("approveAtomic: postgres approval repo required")
	}

	task, err := s.taskSvc.Get(ctx, tenantID, approval.TaskID)
	if err != nil {
		return fmt.Errorf("get task: %w", err)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin approve tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	if err := pgApprovalRepo.UpdateDecisionTx(ctx, tx, tenantID, approvalID, "approved", approver, reason); err != nil {
		return fmt.Errorf("update approval: %w", err)
	}

	queuedPayload, _ := json.Marshal(core.JanusEvent{
		EventType: core.EventTaskQueued, TenantID: tenantID, TaskID: approval.TaskID,
		Payload: mustMarshal(map[string]string{"mailbox": task.MailboxID, "approval": approvalID}),
	})
	if err := s.outboxRepo.Insert(ctx, tx, ulid(), tenantID, "event_publish", queuedPayload); err != nil {
		return fmt.Errorf("outbox queued event: %w", err)
	}

	if task.MailboxID != "" {
		envelopeJSON, _ := json.Marshal(task.Envelope)
		queuePayload, _ := json.Marshal(core.TaskMessage{
			TenantID: tenantID, MailboxID: task.MailboxID, TaskID: approval.TaskID,
			Priority: task.Priority, Payload: envelopeJSON,
		})
		if err := s.outboxRepo.Insert(ctx, tx, ulid(), tenantID, "task_publish", queuePayload); err != nil {
			return fmt.Errorf("outbox task publish: %w", err)
		}
	}

	if terr := s.taskSvc.TransitionInTx(ctx, tx, tenantID, approval.TaskID,
		task.Status, core.TaskStatusQueued, core.EventTaskQueued, 0); terr != nil {
		return fmt.Errorf("queue task in approval tx: %w", terr)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit approve tx: %w", err)
	}
	committed = true
	return nil
}

func (s *ApprovalService) Reject(ctx context.Context, tenantID, approvalID, approver, reason string) error {
	approval, err := s.repo.Get(ctx, tenantID, approvalID)
	if err != nil {
		return fmt.Errorf("get approval: %w", err)
	}
	if approval.Status != "pending" {
		return fmt.Errorf("approval already decided: %s", approval.Status)
	}
	if s.pool != nil && s.outboxRepo != nil {
		return s.decideAtomic(ctx, tenantID, approvalID, approver, reason, "rejected",
			approval, core.TaskStatusCancelled, core.EventTaskCancelled)
	}
	if err := s.repo.UpdateDecision(ctx, tenantID, approvalID, "rejected", approver, reason); err != nil {
		return fmt.Errorf("update approval: %w", err)
	}
	if err := s.taskSvc.transition(ctx, tenantID, approval.TaskID, core.TaskStatusCancelled, core.EventTaskCancelled, 0); err != nil {
		return fmt.Errorf("cancel task: %w", err)
	}
	return nil
}

// decideAtomic commits a terminal decision together with the guarded task
// status change on one transaction, so an approval can never read decided
// while its task still reads pending (or vice versa).
func (s *ApprovalService) decideAtomic(ctx context.Context, tenantID, approvalID, approver, reason, decision string, approval *core.Approval, target core.TaskStatus, event core.EventType) error {
	pgRepo, ok := s.repo.(*postgres.ApprovalRepo)
	if !ok {
		return fmt.Errorf("decideAtomic: postgres approval repo required")
	}
	task, err := s.taskSvc.Get(ctx, tenantID, approval.TaskID)
	if err != nil {
		return fmt.Errorf("get task: %w", err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin decision tx: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := pgRepo.UpdateDecisionTx(ctx, tx, tenantID, approvalID, decision, approver, reason); err != nil {
		return fmt.Errorf("update approval: %w", err)
	}
	if terr := s.taskSvc.TransitionInTx(ctx, tx, tenantID, approval.TaskID,
		task.Status, target, event, 0); terr != nil {
		return fmt.Errorf("transition task in decision tx: %w", terr)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit decision tx: %w", err)
	}
	return nil
}

func (s *ApprovalService) Expire(ctx context.Context, tenantID, approvalID string) error {
	approval, err := s.repo.Get(ctx, tenantID, approvalID)
	if err != nil || approval == nil {
		return err
	}
	if s.pool != nil && s.outboxRepo != nil {
		return s.decideAtomic(ctx, tenantID, approvalID, "system", "approval timeout", "expired",
			approval, core.TaskStatusCancelled, core.EventTaskCancelled)
	}
	if err := s.repo.UpdateDecision(ctx, tenantID, approvalID, "expired", "system", "approval timeout"); err != nil {
		return fmt.Errorf("expire approval: %w", err)
	}
	if err := s.taskSvc.transition(ctx, tenantID, approval.TaskID, core.TaskStatusCancelled, core.EventTaskCancelled, 0); err != nil {
		log.Printf("approval expire: cancel task %s: %v", approval.TaskID, err)
	}
	return nil
}

func (s *ApprovalService) Get(ctx context.Context, tenantID, approvalID string) (*core.Approval, error) {
	return s.repo.Get(ctx, tenantID, approvalID)
}

func (s *ApprovalService) ListPending(ctx context.Context, tenantID string, limit int) ([]*core.Approval, error) {
	if limit <= 0 {
		limit = 50
	}
	return s.repo.ListPending(ctx, tenantID, limit)
}
