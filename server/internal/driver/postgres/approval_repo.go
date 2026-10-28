package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
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
	var expiresAt interface{}
	if !a.ExpiresAt.IsZero() {
		expiresAt = a.ExpiresAt
	}
	_, err := r.pool.Exec(ctx,
		`INSERT INTO approvals (tenant_id, id, task_id, status, requested_by, decision, expires_at)
		 VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''), $7)`,
		a.TenantID, a.ID, a.TaskID, a.Status, a.RequestedBy, a.Decision, expiresAt,
	)
	return err
}

// CreateTx inserts the approval on the caller's transaction — the task and
// its gating approval must commit atomically or neither does.
func (r *ApprovalRepo) CreateTx(ctx context.Context, tx pgx.Tx, a core.Approval) error {
	var expiresAt interface{}
	if !a.ExpiresAt.IsZero() {
		expiresAt = a.ExpiresAt
	}
	_, err := tx.Exec(ctx,
		`INSERT INTO approvals (tenant_id, id, task_id, status, requested_by, decision, expires_at)
		 VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''), $7)`,
		a.TenantID, a.ID, a.TaskID, a.Status, a.RequestedBy, a.Decision, expiresAt,
	)
	return err
}

func (r *ApprovalRepo) Get(ctx context.Context, tenantID, approvalID string) (*core.Approval, error) {
	var a core.Approval
	var decidedAt, expiresAt *time.Time
	var requestedBy, approver, reason, decision *string
	err := r.pool.QueryRow(ctx,
		`SELECT tenant_id, id, task_id, status, requested_by, approver, reason, decision, expires_at, created_at, decided_at
		 FROM approvals WHERE tenant_id = $1 AND id = $2`,
		tenantID, approvalID,
	).Scan(&a.TenantID, &a.ID, &a.TaskID, &a.Status, &requestedBy, &approver, &reason, &decision, &expiresAt, &a.CreatedAt, &decidedAt)
	if err != nil {
		return nil, err
	}
	if expiresAt != nil {
		a.ExpiresAt = *expiresAt
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

// GetLatestByTask returns the task's most recent approval record with its
// decision (nil when none exists) — replay gating needs the verdict, not
// just the pending set.
func (r *ApprovalRepo) GetLatestByTask(ctx context.Context, tenantID, taskID string) (*core.Approval, error) {
	var a core.Approval
	var decidedAt, expiresAt *time.Time
	var requestedBy, approver, reason, decision *string
	err := r.pool.QueryRow(ctx,
		`SELECT tenant_id, id, task_id, status, requested_by, approver, reason, decision, expires_at, created_at, decided_at
		 FROM approvals WHERE tenant_id = $1 AND task_id = $2
		 ORDER BY created_at DESC LIMIT 1`,
		tenantID, taskID,
	).Scan(&a.TenantID, &a.ID, &a.TaskID, &a.Status, &requestedBy, &approver, &reason, &decision, &expiresAt, &a.CreatedAt, &decidedAt)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if expiresAt != nil {
		a.ExpiresAt = *expiresAt
	}
	if decision != nil {
		a.Decision = *decision
	}
	return &a, nil
}

func (r *ApprovalRepo) GetPendingByTask(ctx context.Context, tenantID, taskID string) (*core.Approval, error) {
	var a core.Approval
	var decidedAt, expiresAt *time.Time
	err := r.pool.QueryRow(ctx,
		`SELECT tenant_id, id, task_id, status, requested_by, approver, reason, decision, expires_at, created_at, decided_at
		 FROM approvals WHERE tenant_id = $1 AND task_id = $2 AND status = 'pending'
		 ORDER BY created_at DESC LIMIT 1`,
		tenantID, taskID,
	).Scan(&a.TenantID, &a.ID, &a.TaskID, &a.Status, &a.RequestedBy, &a.Approver, &a.Reason, &a.Decision, &expiresAt, &a.CreatedAt, &decidedAt)
	if err != nil {
		return nil, err
	}
	if expiresAt != nil {
		a.ExpiresAt = *expiresAt
	}
	a.DecidedAt = decidedAt
	return &a, nil
}

func (r *ApprovalRepo) UpdateDecision(ctx context.Context, tenantID, approvalID, decision, approver, reason string) error {
	tag, err := r.pool.Exec(ctx,
		`UPDATE approvals SET status = $1, decision = $1, approver = $2, reason = $3, decided_at = now()
		 WHERE tenant_id = $4 AND id = $5 AND status = 'pending'
		   AND (expires_at IS NULL OR expires_at > now())`,
		decision, approver, reason, tenantID, approvalID,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("approval already decided, expired, or not found")
	}
	return nil
}

// UpdateDecisionTx is the transactional variant of UpdateDecision for use
// inside a caller-supplied tx (e.g. lifecycle.ApplyTx). The expires_at
// predicate runs on the database clock so a decision cannot land past the
// deadline even when the service-side pre-check raced it.
func (r *ApprovalRepo) UpdateDecisionTx(ctx context.Context, tx pgx.Tx, tenantID, approvalID, decision, approver, reason string) error {
	tag, err := tx.Exec(ctx,
		`UPDATE approvals SET status = $1, decision = $1, approver = $2, reason = $3, decided_at = now()
		 WHERE tenant_id = $4 AND id = $5 AND status = 'pending'
		   AND (expires_at IS NULL OR expires_at > now())`,
		decision, approver, reason, tenantID, approvalID,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("approval already decided, expired, or not found")
	}
	return nil
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
		var decidedAt, expiresAt *time.Time
		var requestedBy, approver, reason, decision *string
		if err := rows.Scan(&a.TenantID, &a.ID, &a.TaskID, &a.Status, &requestedBy, &approver, &reason, &decision, &expiresAt, &a.CreatedAt, &decidedAt); err != nil {
			return nil, err
		}
		if expiresAt != nil {
			a.ExpiresAt = *expiresAt
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
