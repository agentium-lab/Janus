package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/agentium-lab/Janus/core"
	natsdriver "github.com/agentium-lab/Janus/server/internal/driver/nats"
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

func (s *DispatchService) PullTask(ctx context.Context, tenantID, mailboxID, agentID string) (*PullResult, error) {
	if tenantID == "" || mailboxID == "" || agentID == "" {
		return nil, fmt.Errorf("tenant id, mailbox id, and agent id are required")
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

	if err := s.budgetSvc.CheckConcurrency(ctx, tenantID, agentID, 0); err != nil {
		return nil, err
	}

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

	leaseID := generateLeaseID()
	expiresAt := time.Now().Add(300 * time.Second)

	attempt := core.TaskAttempt{
		TenantID:  tenantID,
		TaskID:    task.ID,
		Attempt:   task.AttemptCount + 1,
		AgentID:   agentID,
		LeaseID:   leaseID,
		Status:    "claimed",
		StartedAt: time.Now(),
	}
	if err := s.attemptRepo.Create(ctx, attempt); err != nil {
		return nil, fmt.Errorf("create attempt: %w", err)
	}

	if err := s.taskRepo.UpdateStatus(ctx, tenantID, task.ID, core.TaskStatusClaimed, 1); err != nil {
		return nil, fmt.Errorf("update task claimed: %w", err)
	}

	_ = s.queueDriver.PublishEvent(ctx, core.JanusEvent{
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

	if err := s.taskRepo.UpdateStatus(ctx, tenantID, taskID, core.TaskStatusRunning, 0); err != nil {
		return fmt.Errorf("update task running: %w", err)
	}

	_ = s.queueDriver.PublishEvent(ctx, core.JanusEvent{
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

	var usageJSON []byte
	if usage != nil {
		usageJSON, _ = encodeJSON(usage)
	}

	if err := s.attemptRepo.UpdateFinished(ctx, tenantID, taskID, attempt.Attempt, "completed", nil, usageJSON); err != nil {
		return fmt.Errorf("finish attempt: %w", err)
	}

	if err := s.taskRepo.UpdateStatus(ctx, tenantID, taskID, core.TaskStatusCompleted, 0); err != nil {
		return fmt.Errorf("complete task: %w", err)
	}

	_ = s.queueDriver.PublishEvent(ctx, core.JanusEvent{
		EventType: core.EventTaskCompleted,
		TenantID:  tenantID,
		TaskID:    taskID,
		Payload:   mustMarshal(map[string]string{"result_ref": resultRef}),
	})

	return nil
}

func (s *DispatchService) NackTask(ctx context.Context, tenantID, taskID, leaseID string, retriable bool, taskErr *core.TaskError) error {
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

	var errJSON []byte
	if taskErr != nil {
		errJSON, _ = encodeJSON(taskErr)
	}

	if err := s.attemptRepo.UpdateFinished(ctx, tenantID, taskID, attempt.Attempt, "failed", errJSON, nil); err != nil {
		return fmt.Errorf("finish attempt: %w", err)
	}

	task, err := s.taskRepo.Get(ctx, tenantID, taskID)
	if err != nil {
		return fmt.Errorf("get task: %w", err)
	}

	if retriable {
		mb, mbErr := s.mailboxRepo.Get(ctx, tenantID, task.MailboxID)
		if mbErr == nil && !mb.RetryPolicy.ExceedsMaxAttempts(task.AttemptCount) {
			if err := s.taskRepo.UpdateStatus(ctx, tenantID, taskID, core.TaskStatusRetryScheduled, 0); err != nil {
				return fmt.Errorf("schedule retry: %w", err)
			}
			_ = s.queueDriver.PublishEvent(ctx, core.JanusEvent{
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

	_ = s.queueDriver.PublishEvent(ctx, core.JanusEvent{
		EventType: core.EventTaskDeadLettered,
		TenantID:  tenantID,
		TaskID:    taskID,
		Payload:   errJSON,
	})

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
