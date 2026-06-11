package postgres

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/agentium-lab/Janus/core"
)

type TaskRepository struct {
	pool *pgxpool.Pool
}

func NewTaskRepository(pool *pgxpool.Pool) *TaskRepository {
	return &TaskRepository{pool: pool}
}

func (r *TaskRepository) Create(ctx context.Context, task core.Task) error {
	envelopeJSON, _ := json.Marshal(task.Envelope)

	var deadline interface{}
	if task.Deadline != nil {
		deadline = *task.Deadline
	}

	_, err := r.pool.Exec(ctx,
		`INSERT INTO tasks (tenant_id, id, idempotency_key, source_agent, target_type, target_value,
		  mailbox_id, status, priority, deadline, ttl_seconds, envelope, attempt_count)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
		task.TenantID, task.ID, nilIfEmpty(task.IdempotencyKey),
		task.SourceAgent, string(task.TargetType), task.TargetValue,
		nilIfEmpty(task.MailboxID),
		string(task.Status), string(task.Priority), deadline, task.TTLSeconds,
		envelopeJSON, task.AttemptCount,
	)
	return err
}

func (r *TaskRepository) CreateTx(ctx context.Context, tx pgx.Tx, task core.Task) error {
	envelopeJSON, _ := json.Marshal(task.Envelope)

	var deadline interface{}
	if task.Deadline != nil {
		deadline = *task.Deadline
	}

	_, err := tx.Exec(ctx,
		`INSERT INTO tasks (tenant_id, id, idempotency_key, source_agent, target_type, target_value,
		  mailbox_id, status, priority, deadline, ttl_seconds, envelope, attempt_count)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
		task.TenantID, task.ID, nilIfEmpty(task.IdempotencyKey),
		task.SourceAgent, string(task.TargetType), task.TargetValue,
		nilIfEmpty(task.MailboxID),
		string(task.Status), string(task.Priority), deadline, task.TTLSeconds,
		envelopeJSON, task.AttemptCount,
	)
	return err
}

func (r *TaskRepository) Get(ctx context.Context, tenantID, taskID string) (*core.Task, error) {
	var t core.Task
	var status, priority, targetType, targetValue string
	var idempotencyKey, mailboxID, resultRef *string
	var deadline *time.Time
	var ttlSeconds *int
	var envelopeJSON []byte
	var errorJSON []byte
	var completedAt *time.Time

	err := r.pool.QueryRow(ctx,
		`SELECT tenant_id, id, idempotency_key, source_agent, target_type, target_value,
		        mailbox_id, status, priority, deadline, ttl_seconds, envelope,
		        result_ref, error, attempt_count, created_at, updated_at, completed_at
		 FROM tasks WHERE tenant_id = $1 AND id = $2`,
		tenantID, taskID,
	).Scan(
		&t.TenantID, &t.ID, &idempotencyKey, &t.SourceAgent, &targetType, &targetValue,
		&mailboxID, &status, &priority, &deadline, &ttlSeconds, &envelopeJSON,
		&resultRef, &errorJSON, &t.AttemptCount, &t.CreatedAt, &t.UpdatedAt, &completedAt,
	)
	if err != nil {
		return nil, err
	}

	if idempotencyKey != nil {
		t.IdempotencyKey = *idempotencyKey
	}
	t.TargetType = core.TargetType(targetType)
	t.TargetValue = targetValue
	if mailboxID != nil {
		t.MailboxID = *mailboxID
	}
	t.Status = core.TaskStatus(status)
	t.Priority = core.Priority(priority)
	if resultRef != nil {
		t.ResultRef = *resultRef
	}
	t.Deadline = deadline
	if ttlSeconds != nil {
		t.TTLSeconds = *ttlSeconds
	}
	t.CompletedAt = completedAt
	if errorJSON != nil {
		var taskErr core.TaskError
		_ = json.Unmarshal(errorJSON, &taskErr)
		t.Error = &taskErr
	}
	_ = json.Unmarshal(envelopeJSON, &t.Envelope)

	return &t, nil
}

func (r *TaskRepository) GetByIdempotencyKey(ctx context.Context, tenantID, key string) (*core.Task, error) {
	var taskID string
	err := r.pool.QueryRow(ctx,
		"SELECT id FROM tasks WHERE tenant_id = $1 AND idempotency_key = $2",
		tenantID, key,
	).Scan(&taskID)
	if err != nil {
		return nil, err
	}
	return r.Get(ctx, tenantID, taskID)
}

func (r *TaskRepository) UpdateStatus(ctx context.Context, tenantID, taskID string, status core.TaskStatus, attemptIncrement int) error {
	if attemptIncrement > 0 {
		_, err := r.pool.Exec(ctx,
			`UPDATE tasks SET status = $1, attempt_count = attempt_count + $2, updated_at = now()
			 WHERE tenant_id = $3 AND id = $4`,
			string(status), attemptIncrement, tenantID, taskID,
		)
		return err
	}

	if status == core.TaskStatusCompleted {
		_, err := r.pool.Exec(ctx,
			`UPDATE tasks SET status = $1, updated_at = now(), completed_at = now()
			 WHERE tenant_id = $2 AND id = $3`,
			string(status), tenantID, taskID,
		)
		return err
	}

	_, err := r.pool.Exec(ctx,
		`UPDATE tasks SET status = $1, updated_at = now() WHERE tenant_id = $2 AND id = $3`,
		string(status), tenantID, taskID,
	)
	return err
}

func (r *TaskRepository) UpdateStatusTx(ctx context.Context, tx pgx.Tx, tenantID, taskID string, status core.TaskStatus, attemptIncrement int) error {
	if attemptIncrement > 0 {
		_, err := tx.Exec(ctx,
			`UPDATE tasks SET status = $1, attempt_count = attempt_count + $2, updated_at = now()
			 WHERE tenant_id = $3 AND id = $4`,
			string(status), attemptIncrement, tenantID, taskID,
		)
		return err
	}

	if status == core.TaskStatusCompleted {
		_, err := tx.Exec(ctx,
			`UPDATE tasks SET status = $1, updated_at = now(), completed_at = now()
			 WHERE tenant_id = $2 AND id = $3`,
			string(status), tenantID, taskID,
		)
		return err
	}

	_, err := tx.Exec(ctx,
		`UPDATE tasks SET status = $1, updated_at = now() WHERE tenant_id = $2 AND id = $3`,
		string(status), tenantID, taskID,
	)
	return err
}

func (r *TaskRepository) UpdateRetryAt(ctx context.Context, tenantID, taskID string, retryAt time.Time) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE tasks SET status = 'retry_scheduled', retry_at = $1, updated_at = now() WHERE tenant_id = $2 AND id = $3`,
		retryAt, tenantID, taskID,
	)
	return err
}

func (r *TaskRepository) ListByStatus(ctx context.Context, tenantID string, status core.TaskStatus, limit int) ([]*core.Task, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT tenant_id, id, idempotency_key, source_agent, target_type, target_value,
		        mailbox_id, status, priority, deadline, ttl_seconds, envelope,
		        result_ref, error, attempt_count, created_at, updated_at, completed_at
		 FROM tasks WHERE tenant_id = $1 AND status = $2
		 ORDER BY priority ASC, created_at ASC LIMIT $3`,
		tenantID, string(status), limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTasks(rows)
}

func scanTasks(rows pgx.Rows) ([]*core.Task, error) {
	var tasks []*core.Task
	for rows.Next() {
		var t core.Task
		var status, priority, targetType, targetValue string
		var idempotencyKey, mailboxID, resultRef *string
		var deadline *time.Time
		var ttlSeconds *int
		var envelopeJSON []byte
		var errorJSON []byte
		var completedAt *time.Time

		err := rows.Scan(
			&t.TenantID, &t.ID, &idempotencyKey, &t.SourceAgent, &targetType, &targetValue,
			&mailboxID, &status, &priority, &deadline, &ttlSeconds, &envelopeJSON,
			&resultRef, &errorJSON, &t.AttemptCount, &t.CreatedAt, &t.UpdatedAt, &completedAt,
		)
		if err != nil {
			return nil, err
		}
		if idempotencyKey != nil {
			t.IdempotencyKey = *idempotencyKey
		}
		t.TargetType = core.TargetType(targetType)
		t.TargetValue = targetValue
		if mailboxID != nil {
			t.MailboxID = *mailboxID
		}
		t.Status = core.TaskStatus(status)
		t.Priority = core.Priority(priority)
		if resultRef != nil {
			t.ResultRef = *resultRef
		}
		t.Deadline = deadline
		if ttlSeconds != nil {
			t.TTLSeconds = *ttlSeconds
		}
		t.CompletedAt = completedAt
		if errorJSON != nil {
			var taskErr core.TaskError
			_ = json.Unmarshal(errorJSON, &taskErr)
			t.Error = &taskErr
		}
		_ = json.Unmarshal(envelopeJSON, &t.Envelope)
		tasks = append(tasks, &t)
	}
	return tasks, rows.Err()
}

func nilIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func nilIfZero(v int) interface{} {
	if v == 0 {
		return nil
	}
	return v
}
