package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

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
	repo     ApprovalRepo
	taskSvc  *TaskService
	queueDrv core.QueueEventDriver

	// Unified transaction path (Priority 1): one code shape for PG and memory.
	lifecycle  Lifecycle
	approvalTx ApprovalTxRepo
	txOutbox   OutboxDedupeWriter
}

func NewApprovalService(repo ApprovalRepo, taskSvc *TaskService, queueDrv core.QueueEventDriver) *ApprovalService {
	s := &ApprovalService{repo: repo, taskSvc: taskSvc, queueDrv: queueDrv}
	s.initTxPath()
	return s
}

func (s *ApprovalService) initTxPath() {
	s.lifecycle = NewMemoryLifecycle()
	if pgRepo, ok := s.repo.(*postgres.ApprovalRepo); ok && pgRepo != nil {
		s.approvalTx = pgRepo
	} else {
		s.approvalTx = ApprovalRepoTxAdapter{s.repo}
	}
	s.txOutbox = NewPublishingOutbox(s.queueDrv)
}

// WithTxPath wires the production transaction components: PGLifecycle, the
// PG approval repo's native *Tx method, and the persistent outbox.
func (s *ApprovalService) WithTxPath(lc Lifecycle, outbox OutboxDedupeWriter) *ApprovalService {
	if lc != nil {
		s.lifecycle = lc
	}
	if outbox != nil {
		s.txOutbox = outbox
	}
	if pgRepo, ok := s.repo.(*postgres.ApprovalRepo); ok {
		s.approvalTx = pgRepo
	}
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

	return s.approveAtomic(ctx, tenantID, approvalID, approver, reason, "approved", approval)
}

// approveAtomic commits the approval decision together with the task status
// change and the task_publish/event_publish outbox rows in one transaction,
// so an approval can never read approved while its task still reads pending.
func (s *ApprovalService) approveAtomic(ctx context.Context, tenantID, approvalID, approver, reason, decision string, approval *core.Approval) error {
	task, err := s.taskSvc.Get(ctx, tenantID, approval.TaskID)
	if err != nil {
		return fmt.Errorf("get task: %w", err)
	}

	var terminalStatus core.TaskStatus
	var terminalEvent core.EventType
	switch decision {
	case "approved":
		terminalStatus, terminalEvent = core.TaskStatusQueued, core.EventTaskQueued
	case "rejected":
		terminalStatus, terminalEvent = core.TaskStatusCancelled, core.EventTaskCancelled
	default:
		terminalStatus, terminalEvent = core.TaskStatusCancelled, core.EventTaskCancelled
	}

	committed := false
	err = s.lifecycle.ApplyTx(ctx, func(tx pgx.Tx) error {
		if uerr := s.approvalTx.UpdateDecisionTx(ctx, tx, tenantID, approvalID, decision, approver, reason); uerr != nil {
			return fmt.Errorf("update approval: %w", uerr)
		}

		if terr := s.taskSvc.TransitionInTx(ctx, tx, tenantID, approval.TaskID,
			task.Status, terminalStatus, terminalEvent, 0); terr != nil {
			return fmt.Errorf("transition task in approval tx: %w", terr)
		}

		if task.MailboxID == "" {
			committed = true
			return nil
		}

		queuedPayload := MarshalEvent(&core.JanusEvent{
			EventType: terminalEvent, TenantID: tenantID, TaskID: approval.TaskID,
			Payload: mustMarshal(map[string]string{"mailbox": task.MailboxID, "approval": approvalID}),
		})
		if oerr := s.txOutbox.Insert(ctx, tx, ulid(), tenantID, "event_publish", queuedPayload); oerr != nil {
			return fmt.Errorf("outbox queued event: %w", oerr)
		}

		if decision == "approved" {
			envelopeJSON, _ := json.Marshal(task.Envelope)
			queuePayload, _ := json.Marshal(core.TaskMessage{
				TenantID: tenantID, MailboxID: task.MailboxID, TaskID: approval.TaskID,
				Priority: task.Priority, Payload: envelopeJSON,
			})
			if oerr := s.txOutbox.Insert(ctx, tx, ulid(), tenantID, "task_publish", queuePayload); oerr != nil {
				return fmt.Errorf("outbox task publish: %w", oerr)
			}
		}
		committed = true
		return nil
	})
	if err != nil {
		return err
	}
	_ = committed
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
	return s.approveAtomic(ctx, tenantID, approvalID, approver, reason, "rejected", approval)
}

func (s *ApprovalService) Expire(ctx context.Context, tenantID, approvalID string) error {
	approval, err := s.repo.Get(ctx, tenantID, approvalID)
	if err != nil || approval == nil {
		return err
	}
	return s.approveAtomic(ctx, tenantID, approvalID, "system", "approval timeout", "expired", approval)
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
