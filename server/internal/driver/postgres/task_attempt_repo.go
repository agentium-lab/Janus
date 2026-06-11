package postgres

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/agentium-lab/Janus/core"
)

type TaskAttemptRepository struct {
	pool *pgxpool.Pool
}

func NewTaskAttemptRepository(pool *pgxpool.Pool) *TaskAttemptRepository {
	return &TaskAttemptRepository{pool: pool}
}

func (r *TaskAttemptRepository) Create(ctx context.Context, a core.TaskAttempt) error {
	var errorJSON, usageJSON []byte
	if a.Error != nil {
		errorJSON, _ = json.Marshal(a.Error)
	}
	if a.TokenUsage != nil {
		usageJSON, _ = json.Marshal(a.TokenUsage)
	}
	_, err := r.pool.Exec(ctx,
		`INSERT INTO task_attempts (tenant_id, task_id, attempt, agent_id, lease_id, status,
		  started_at, heartbeat_at, finished_at, error, token_usage)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		a.TenantID, a.TaskID, a.Attempt, a.AgentID, a.LeaseID, a.Status,
		a.StartedAt, a.HeartbeatAt, a.FinishedAt, jsonOrNull(errorJSON), jsonOrNull(usageJSON),
	)
	return err
}

func (r *TaskAttemptRepository) GetLatest(ctx context.Context, tenantID, taskID string) (*core.TaskAttempt, error) {
	var a core.TaskAttempt
	var errorJSON, usageJSON []byte
	var heartbeatAt, finishedAt *time.Time

	err := r.pool.QueryRow(ctx,
		`SELECT tenant_id, task_id, attempt, agent_id, lease_id, status,
		        started_at, heartbeat_at, finished_at, error, token_usage
		 FROM task_attempts
		 WHERE tenant_id = $1 AND task_id = $2
		 ORDER BY attempt DESC LIMIT 1`,
		tenantID, taskID,
	).Scan(
		&a.TenantID, &a.TaskID, &a.Attempt, &a.AgentID, &a.LeaseID, &a.Status,
		&a.StartedAt, &heartbeatAt, &finishedAt, &errorJSON, &usageJSON,
	)
	if err != nil {
		return nil, err
	}
	a.HeartbeatAt = heartbeatAt
	a.FinishedAt = finishedAt
	if errorJSON != nil {
		var taskErr core.TaskError
		_ = json.Unmarshal(errorJSON, &taskErr)
		a.Error = &taskErr
	}
	if usageJSON != nil {
		var usage core.TokenUsage
		_ = json.Unmarshal(usageJSON, &usage)
		a.TokenUsage = &usage
	}
	return &a, nil
}

func (r *TaskAttemptRepository) UpdateHeartbeat(ctx context.Context, tenantID, taskID string, attempt int) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE task_attempts SET heartbeat_at = now()
		 WHERE tenant_id = $1 AND task_id = $2 AND attempt = $3`,
		tenantID, taskID, attempt,
	)
	return err
}

func (r *TaskAttemptRepository) UpdateFinished(ctx context.Context, tenantID, taskID string, attempt int, status string, errJSON []byte, usageJSON []byte) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE task_attempts SET status = $1, finished_at = now(), error = $2, token_usage = $3
		 WHERE tenant_id = $4 AND task_id = $5 AND attempt = $6`,
		status, jsonOrNull(errJSON), jsonOrNull(usageJSON),
		tenantID, taskID, attempt,
	)
	return err
}

func jsonOrNull(b []byte) interface{} {
	if len(b) == 0 {
		return nil
	}
	return b
}
