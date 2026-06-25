package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/agentium-lab/Janus/core"
)

type ApprovalRepo struct {
	pool *pgxpool.Pool
}

func NewApprovalRepo(pool *pgxpool.Pool) *ApprovalRepo {
	return &ApprovalRepo{pool: pool}
}

func (r *ApprovalRepo) Create(ctx context.Context, a core.Approval) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO approvals (tenant_id, id, task_id, status, requested_by, expires_at)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		a.TenantID, a.ID, a.TaskID, a.Status, a.RequestedBy, a.ExpiresAt,
	)
	return err
}

func (r *ApprovalRepo) Get(ctx context.Context, tenantID, approvalID string) (*core.Approval, error) {
	var a core.Approval
	var decidedAt *time.Time
	var requestedBy, approver, reason, decision *string
	err := r.pool.QueryRow(ctx,
		`SELECT tenant_id, id, task_id, status, requested_by, approver, reason, decision, expires_at, created_at, decided_at
		 FROM approvals WHERE tenant_id = $1 AND id = $2`,
		tenantID, approvalID,
	).Scan(&a.TenantID, &a.ID, &a.TaskID, &a.Status, &requestedBy, &approver, &reason, &decision, &a.ExpiresAt, &a.CreatedAt, &decidedAt)
	if err != nil {
		return nil, err
	}
	if requestedBy != nil {
		a.RequestedBy = *requestedBy
	}
	if approver != nil {
		a.Approver = *approver
	}
	if reason != nil {
		a.Reason = *reason
	}
	if decision != nil {
		a.Decision = *decision
	}
	a.DecidedAt = decidedAt
	return &a, nil
}

func (r *ApprovalRepo) GetPendingByTask(ctx context.Context, tenantID, taskID string) (*core.Approval, error) {
	var a core.Approval
	var decidedAt *time.Time
	err := r.pool.QueryRow(ctx,
		`SELECT tenant_id, id, task_id, status, requested_by, approver, reason, decision, expires_at, created_at, decided_at
		 FROM approvals WHERE tenant_id = $1 AND task_id = $2 AND status = 'pending'
		 ORDER BY created_at DESC LIMIT 1`,
		tenantID, taskID,
	).Scan(&a.TenantID, &a.ID, &a.TaskID, &a.Status, &a.RequestedBy, &a.Approver, &a.Reason, &a.Decision, &a.ExpiresAt, &a.CreatedAt, &decidedAt)
	if err != nil {
		return nil, err
	}
	a.DecidedAt = decidedAt
	return &a, nil
}

func (r *ApprovalRepo) UpdateDecision(ctx context.Context, tenantID, approvalID, decision, approver, reason string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE approvals SET status = $1, decision = $1, approver = $2, reason = $3, decided_at = now()
		 WHERE tenant_id = $4 AND id = $5`,
		decision, approver, reason, tenantID, approvalID,
	)
	return err
}

func (r *ApprovalRepo) ListPending(ctx context.Context, tenantID string, limit int) ([]*core.Approval, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT tenant_id, id, task_id, status, requested_by, approver, reason, decision, expires_at, created_at, decided_at
		 FROM approvals WHERE tenant_id = $1 AND status = 'pending'
		 ORDER BY created_at ASC LIMIT $2`,
		tenantID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*core.Approval
	for rows.Next() {
		var a core.Approval
		var decidedAt *time.Time
		var requestedBy, approver, reason, decision *string
		if err := rows.Scan(&a.TenantID, &a.ID, &a.TaskID, &a.Status, &requestedBy, &approver, &reason, &decision, &a.ExpiresAt, &a.CreatedAt, &decidedAt); err != nil {
			return nil, err
		}
		if requestedBy != nil {
			a.RequestedBy = *requestedBy
		}
		if approver != nil {
			a.Approver = *approver
		}
		if reason != nil {
			a.Reason = *reason
		}
		if decision != nil {
			a.Decision = *decision
		}
		a.DecidedAt = decidedAt
		result = append(result, &a)
	}
	return result, nil
}
