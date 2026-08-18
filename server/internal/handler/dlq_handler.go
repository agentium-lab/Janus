package handler

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/driver/postgres"
)

type DLQService interface {
	QueryDLQ(ctx context.Context, tenantID, mailboxID string, limit int) ([]*core.Task, error)
	ReplayDLQ(ctx context.Context, tenantID, taskID string) (*core.Task, error)
	DiscardDLQ(ctx context.Context, tenantID, taskID string) error
}

type DLQHandler struct {
	svc DLQService
}

func NewDLQHandler(svc DLQService) *DLQHandler {
	return &DLQHandler{svc: svc}
}

func (h *DLQHandler) Query(w http.ResponseWriter, r *http.Request) {
	tenantID := tenantIDFromPath(r.URL.Path)
	mailboxID := r.URL.Query().Get("mailbox")
	limit := intQuery(r, "limit", 50)

	tasks, err := h.svc.QueryDLQ(r.Context(), tenantID, mailboxID, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if tasks == nil {
		tasks = []*core.Task{}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"tasks": tasks})
}

func (h *DLQHandler) Replay(w http.ResponseWriter, r *http.Request) {
	tenantID, taskID := tenantAndTaskFromDLQPath(r.URL.Path)

	result, err := h.svc.ReplayDLQ(r.Context(), tenantID, taskID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *DLQHandler) Discard(w http.ResponseWriter, r *http.Request) {
	tenantID, taskID := tenantAndTaskFromDLQPath(r.URL.Path)

	if err := h.svc.DiscardDLQ(r.Context(), tenantID, taskID); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "discarded"})
}

func tenantAndTaskFromDLQPath(path string) (string, string) {
	parts := strings.Split(path, "/")
	tenantID, taskID := "", ""
	for i, p := range parts {
		if p == "tenants" && i+1 < len(parts) {
			tenantID = parts[i+1]
		}
		if p == "dlq" && i+1 < len(parts) {
			taskID = parts[i+1]
		}
	}
	return tenantID, taskID
}

// dlqServiceAdapter adapts task service to DLQService
type DLQServiceAdapter struct {
	taskRepo    DLQTaskRepo
	queueDriver DLQQueueDriver
	outboxRepo  *postgres.OutboxRepo
	pool        *pgxpool.Pool
}

type DLQTaskRepo interface {
	ListDeadLettered(ctx context.Context, tenantID, mailboxID string, limit int) ([]*core.Task, error)
	UpdateStatus(ctx context.Context, tenantID, taskID string, status core.TaskStatus, attemptIncrement int) error
	Get(ctx context.Context, tenantID, taskID string) (*core.Task, error)
}

type DLQQueueDriver interface {
	PublishTask(ctx context.Context, msg core.TaskMessage) error
	PublishEvent(ctx context.Context, event core.JanusEvent) error
}

func NewDLQServiceAdapter(repo DLQTaskRepo, driver DLQQueueDriver) *DLQServiceAdapter {
	return &DLQServiceAdapter{taskRepo: repo, queueDriver: driver}
}

// WithOutbox wires the transactional outbox + pool so ReplayDLQ commits the
// status transition together with the task_publish/event_publish outbox rows
// in a single transaction instead of publishing directly to the queue driver.
func (a *DLQServiceAdapter) WithOutbox(outboxRepo *postgres.OutboxRepo, pool *pgxpool.Pool) *DLQServiceAdapter {
	a.outboxRepo = outboxRepo
	a.pool = pool
	return a
}

func (a *DLQServiceAdapter) QueryDLQ(ctx context.Context, tenantID, mailboxID string, limit int) ([]*core.Task, error) {
	if tenantID == "" {
		return nil, nil
	}
	return a.taskRepo.ListDeadLettered(ctx, tenantID, mailboxID, limit)
}

func (a *DLQServiceAdapter) ReplayDLQ(ctx context.Context, tenantID, taskID string) (*core.Task, error) {
	task, err := a.taskRepo.Get(ctx, tenantID, taskID)
	if err != nil {
		return nil, err
	}
	if task.Status != core.TaskStatusDeadLettered {
		return nil, fmt.Errorf("task is not dead_lettered, status: %s", task.Status)
	}

	// Atomic path: route through outbox inside a single tx when wired.
	// Mirrors LifecycleService.ApplyTx + outbox pattern.
	if a.pool != nil && a.outboxRepo != nil {
		return a.replayDLQAtomic(ctx, tenantID, taskID, task)
	}

	if err := a.taskRepo.UpdateStatus(ctx, tenantID, taskID, core.TaskStatusCreated, 0); err != nil {
		return nil, err
	}

	if task.MailboxID != "" {
		payload, _ := json.Marshal(task.Envelope)
		if err := a.queueDriver.PublishTask(ctx, core.TaskMessage{
			TenantID:  tenantID,
			MailboxID: task.MailboxID,
			TaskID:    taskID,
			Priority:  task.Priority,
			Payload:   payload,
		}); err != nil {
			return nil, err
		}
		_ = a.taskRepo.UpdateStatus(ctx, tenantID, taskID, core.TaskStatusQueued, 0)
	}

	_ = a.queueDriver.PublishEvent(ctx, core.JanusEvent{
		EventType: core.EventTaskCreated,
		TenantID:  tenantID,
		TaskID:    taskID,
		Payload:   dlqMustMarshal(map[string]string{"status": "replayed_from_dlq"}),
	})

	return a.taskRepo.Get(ctx, tenantID, taskID)
}

// replayDLQAtomic commits the task status transitions and the task_publish /
// event_publish outbox rows in a single PG transaction. The direct queue
// driver is bypassed; the outbox publisher will dispatch the rows reliably.
func (a *DLQServiceAdapter) replayDLQAtomic(ctx context.Context, tenantID, taskID string, task *core.Task) (*core.Task, error) {
	pgTaskRepo, ok := a.taskRepo.(*postgres.TaskRepository)
	if !ok {
		return nil, fmt.Errorf("replayDLQAtomic: postgres task repo required")
	}

	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin dlq replay tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	if err := pgTaskRepo.UpdateStatusTx(ctx, tx, tenantID, taskID, core.TaskStatusCreated, 0); err != nil {
		return nil, fmt.Errorf("set created: %w", err)
	}

	if task.MailboxID != "" {
		envelopeJSON, _ := json.Marshal(task.Envelope)
		queuePayload, _ := json.Marshal(core.TaskMessage{
			TenantID: tenantID, MailboxID: task.MailboxID, TaskID: taskID,
			Priority: task.Priority, Payload: envelopeJSON,
		})
		dedupeKey := fmt.Sprintf("task_publish:%s:%s:dlq_replay", tenantID, taskID)
		if err := a.outboxRepo.InsertWithDedupe(ctx, tx, dlqULID(), tenantID, "task_publish", dedupeKey, queuePayload); err != nil {
			return nil, fmt.Errorf("outbox task_publish: %w", err)
		}
		if err := pgTaskRepo.UpdateStatusTx(ctx, tx, tenantID, taskID, core.TaskStatusQueued, 0); err != nil {
			return nil, fmt.Errorf("set queued: %w", err)
		}
	}

	createdPayload, _ := json.Marshal(core.JanusEvent{
		EventType: core.EventTaskCreated, TenantID: tenantID, TaskID: taskID,
		Payload: dlqMustMarshal(map[string]string{"status": "replayed_from_dlq"}),
	})
	if err := a.outboxRepo.Insert(ctx, tx, dlqULID(), tenantID, "event_publish", createdPayload); err != nil {
		return nil, fmt.Errorf("outbox event_publish: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit dlq replay tx: %w", err)
	}
	committed = true

	return a.taskRepo.Get(ctx, tenantID, taskID)
}

func dlqULID() string {
	now := time.Now()
	t := uint64(now.UnixMilli())
	b := make([]byte, 10)
	for i := 9; i >= 0; i-- {
		b[i] = byte(t & 0xff)
		t >>= 8
	}
	randBytes := make([]byte, 6)
	_, _ = cryptorand.Read(randBytes)
	return fmt.Sprintf("%x%x", b, randBytes)
}

func (a *DLQServiceAdapter) DiscardDLQ(ctx context.Context, tenantID, taskID string) error {
	return a.taskRepo.UpdateStatus(ctx, tenantID, taskID, core.TaskStatusCancelled, 0)
}

func dlqMustMarshal(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}
