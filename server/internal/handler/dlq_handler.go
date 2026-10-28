package handler

import (
	"errors"

	"context"
	cryptorand "crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/driver/postgres"
	"github.com/agentium-lab/Janus/server/internal/service"
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
	tenantID := r.PathValue("tenantID")
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
	tenantID, taskID := r.PathValue("tenantID"), r.PathValue("taskID")

	result, err := h.svc.ReplayDLQ(r.Context(), tenantID, taskID)
	if err != nil {
		if errors.Is(err, postgres.ErrTaskNotReplayable) {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *DLQHandler) Discard(w http.ResponseWriter, r *http.Request) {
	tenantID, taskID := r.PathValue("tenantID"), r.PathValue("taskID")

	if err := h.svc.DiscardDLQ(r.Context(), tenantID, taskID); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "discarded"})
}

// dlqServiceAdapter adapts task service to DLQService
type DLQServiceAdapter struct {
	taskRepo    DLQTaskRepo
	queueDriver DLQQueueDriver
	outboxRepo  *postgres.OutboxRepo
	pool        *pgxpool.Pool
	replayGate  DLQReplayGate
}

// DLQReplayGate checks approval, publish policy, and mailbox routing
// admission before allowing DLQ replay. Mirrors the gates in
// TaskService.Replay so DLQ replay cannot bypass them.
type DLQReplayGate interface {
	CheckApproval(ctx context.Context, tenantID, taskID string) error
	CheckPolicy(ctx context.Context, tenantID, sourceAgent, targetType, targetValue string) error
	CheckAdmission(ctx context.Context, tenantID, mailboxID string, envelope core.TaskEnvelope) error
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

// WithReplayGate wires the approval/policy gate so DLQ replay cannot bypass
// the same checks that gate ordinary Replay.
//
// CONTRACT: If the gate is nil (not wired), DLQ replay proceeds WITHOUT
// approval/policy checks. This is a deliberate backdoor for dev/test setups
// where no approval service is configured. Production wiring MUST call
// WithReplayGate with a non-nil gate to enforce governance.
func (a *DLQServiceAdapter) WithReplayGate(gate DLQReplayGate) *DLQServiceAdapter {
	a.replayGate = gate
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
	// DLQ replay must pass the same gates as ordinary replay: a task whose
	// approval was rejected or whose policy denies must not re-enter the
	// queue through the DLQ backdoor.
	if a.replayGate != nil {
		if gerr := a.replayGate.CheckApproval(ctx, tenantID, taskID); gerr != nil {
			return nil, fmt.Errorf("dlq replay approval gate: %w", gerr)
		}
		if gerr := a.replayGate.CheckPolicy(ctx, tenantID, task.SourceAgent, string(task.TargetType), task.TargetValue); gerr != nil {
			return nil, fmt.Errorf("dlq replay policy gate: %w", gerr)
		}
		if gerr := a.replayGate.CheckAdmission(ctx, tenantID, task.MailboxID, task.Envelope); gerr != nil {
			return nil, fmt.Errorf("dlq replay admission gate: %w", gerr)
		}
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

	generation, err := pgTaskRepo.ResetForReplayTx(ctx, tx, tenantID, taskID)
	if err != nil {
		return nil, fmt.Errorf("reset task: %w", err)
	}

	if task.MailboxID != "" {
		envelopeJSON, _ := json.Marshal(task.Envelope)
		queuePayload, _ := json.Marshal(core.TaskMessage{
			TenantID: tenantID, MailboxID: task.MailboxID, TaskID: taskID,
			Priority: task.Priority, Payload: envelopeJSON,
		})
		// DLQ replay resets attempt_count to 1; include it in the dedupe key
		// so it matches the format used by retry scheduler (replay:N:attempt:M).
		dedupeKey := core.BuildReplayDedupeKey(tenantID, taskID, "dlq_replay", generation, 1)
		if err := a.outboxRepo.InsertWithDedupe(ctx, tx, dlqULID(), tenantID, "task_publish", dedupeKey, queuePayload); err != nil {
			return nil, fmt.Errorf("outbox task_publish: %w", err)
		}
		if err := pgTaskRepo.UpdateStatusTx(ctx, tx, tenantID, taskID, core.TaskStatusQueued, 0); err != nil {
			return nil, fmt.Errorf("set queued: %w", err)
		}
	}

	createdEvent := core.JanusEvent{
		EventType: core.EventTaskCreated, TenantID: tenantID, TaskID: taskID,
		Payload: dlqMustMarshal(map[string]string{"status": "replayed_from_dlq"}),
	}
	createdPayload := service.MarshalEvent(&createdEvent)
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
	task, err := a.taskRepo.Get(ctx, tenantID, taskID)
	if err != nil {
		return fmt.Errorf("get task for discard: %w", err)
	}
	if task.Status != core.TaskStatusDeadLettered {
		return fmt.Errorf("task is not dead_lettered, status: %s", task.Status)
	}
	// Discard is a terminal transition: no live attempt may remain. A
	// dead_lettered task normally has no active attempt, but a race between
	// the lease scanner's dead-letter and a late heartbeat could leave one.
	if a.pool != nil {
		tx, terr := a.pool.Begin(ctx)
		if terr != nil {
			return fmt.Errorf("begin discard tx: %w", terr)
		}
		committed := false
		defer func() {
			if !committed {
				_ = tx.Rollback(ctx)
			}
		}()
		if _, uerr := tx.Exec(ctx,
			`UPDATE task_attempts SET status = 'cancelled', finished_at = now()
			 WHERE tenant_id = $1 AND task_id = $2 AND status IN ('claimed', 'running')`,
			tenantID, taskID); uerr != nil {
			return fmt.Errorf("finish open attempts for discard: %w", uerr)
		}
		tag, uerr := tx.Exec(ctx,
			`UPDATE tasks SET status = 'cancelled', updated_at = now()
			 WHERE tenant_id = $1 AND id = $2 AND status = 'dead_lettered'`,
			tenantID, taskID)
		if uerr != nil {
			return fmt.Errorf("update task cancelled: %w", uerr)
		}
		// Check if the task was actually updated. If not, a concurrent operation
		// (e.g., Replay) has already changed the status, so we must rollback to
		// avoid leaving task_attempts cancelled but task in a different state.
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("task %s is no longer dead_lettered (concurrent modification)", taskID)
		}
		if cerr := tx.Commit(ctx); cerr != nil {
			return fmt.Errorf("commit discard tx: %w", cerr)
		}
		committed = true
		return nil
	}
	return a.taskRepo.UpdateStatus(ctx, tenantID, taskID, core.TaskStatusCancelled, 0)
}

func dlqMustMarshal(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}
