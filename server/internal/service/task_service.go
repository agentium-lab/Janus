package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/driver/postgres"
)

type TaskService struct {
	taskRepo    TaskRepo
	queueDriver QueueDriver
	pool        *pgxpool.Pool
	outboxRepo  *postgres.OutboxRepo
}

func NewTaskService(taskRepo TaskRepo, queueDriver QueueDriver, pool *pgxpool.Pool, outboxRepo *postgres.OutboxRepo) *TaskService {
	return &TaskService{
		taskRepo:    taskRepo,
		queueDriver: queueDriver,
		pool:        pool,
		outboxRepo:  outboxRepo,
	}
}

func (s *TaskService) Create(ctx context.Context, task core.Task) error {
	if task.TenantID == "" {
		return fmt.Errorf("tenant id is required")
	}
	if task.ID == "" {
		return fmt.Errorf("task id is required")
	}
	if task.SourceAgent == "" {
		return fmt.Errorf("source agent is required")
	}
	if task.TargetType == "" {
		return fmt.Errorf("target type is required")
	}
	if task.TargetValue == "" {
		return fmt.Errorf("target value is required")
	}
	if task.Status == "" {
		task.Status = core.TaskStatusCreated
	}
	if task.Priority == "" {
		task.Priority = core.PriorityNormal
	}
	if err := task.Envelope.Validate(); err != nil {
		return fmt.Errorf("envelope validation: %w", err)
	}

	if task.IdempotencyKey != "" {
		existing, err := s.taskRepo.GetByIdempotencyKey(ctx, task.TenantID, task.IdempotencyKey)
		if err == nil && existing != nil {
			return &IdempotentError{ExistingTaskID: existing.ID}
		}
	}

	if s.outboxRepo != nil && s.pool != nil {
		return s.createWithOutbox(ctx, task)
	}
	return s.createDirect(ctx, task)
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

	if task.MailboxID != "" {
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

	if task.MailboxID != "" {
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
	if err := s.taskRepo.UpdateStatus(ctx, tenantID, taskID, status, attemptInc); err != nil {
		return fmt.Errorf("update task status to %s: %w", status, err)
	}
	return s.publishEvent(ctx, core.JanusEvent{
		EventType: eventType,
		TenantID:  tenantID,
		TaskID:    taskID,
		Payload:   mustMarshal(map[string]string{"status": string(status)}),
	})
}

func (s *TaskService) publishEvent(ctx context.Context, event core.JanusEvent) error {
	return s.queueDriver.PublishEvent(ctx, event)
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

func ulid() string {
	now := time.Now()
	t := uint64(now.UnixMilli())
	b := make([]byte, 10)
	for i := 9; i >= 0; i-- {
		b[i] = byte(t & 0xff)
		t >>= 8
	}
	randBytes := make([]byte, 6)
	for i := range randBytes {
		randBytes[i] = byte(t & 0xff)
		t = t>>8 + uint64(i)
	}
	return fmt.Sprintf("%x%x", b, randBytes)
}
