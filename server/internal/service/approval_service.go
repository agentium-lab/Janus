package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/agentium-lab/Janus/core"
)

type ApprovalRepo interface {
	Create(ctx context.Context, approval core.Approval) error
	Get(ctx context.Context, tenantID, approvalID string) (*core.Approval, error)
	GetPendingByTask(ctx context.Context, tenantID, taskID string) (*core.Approval, error)
	UpdateDecision(ctx context.Context, tenantID, approvalID, decision, approver, reason string) error
	ListPending(ctx context.Context, tenantID string, limit int) ([]*core.Approval, error)
}

type ApprovalService struct {
	repo        ApprovalRepo
	taskSvc     *TaskService
	queueDrv    core.QueueEventDriver
	outbox      OutboxWriter
}

func NewApprovalService(repo ApprovalRepo, taskSvc *TaskService, queueDrv core.QueueEventDriver) *ApprovalService {
	return &ApprovalService{repo: repo, taskSvc: taskSvc, queueDrv: queueDrv}
}

func (s *ApprovalService) WithOutbox(outbox OutboxWriter) *ApprovalService {
	s.outbox = outbox
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

func (s *ApprovalService) Reject(ctx context.Context, tenantID, approvalID, approver, reason string) error {
	approval, err := s.repo.Get(ctx, tenantID, approvalID)
	if err != nil {
		return fmt.Errorf("get approval: %w", err)
	}
	if approval.Status != "pending" {
		return fmt.Errorf("approval already decided: %s", approval.Status)
	}
	if err := s.repo.UpdateDecision(ctx, tenantID, approvalID, "rejected", approver, reason); err != nil {
		return fmt.Errorf("update approval: %w", err)
	}
	if err := s.taskSvc.transition(ctx, tenantID, approval.TaskID, core.TaskStatusCancelled, core.EventTaskCancelled, 0); err != nil {
		return fmt.Errorf("cancel task: %w", err)
	}
	return nil
}

func (s *ApprovalService) Expire(ctx context.Context, tenantID, approvalID string) error {
	if err := s.repo.UpdateDecision(ctx, tenantID, approvalID, "expired", "system", "approval timeout"); err != nil {
		return fmt.Errorf("expire approval: %w", err)
	}
	approval, _ := s.repo.Get(ctx, tenantID, approvalID)
	if approval != nil {
		_ = s.taskSvc.transition(ctx, tenantID, approval.TaskID, core.TaskStatusCancelled, core.EventTaskCancelled, 0)
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
