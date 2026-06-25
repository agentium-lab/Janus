package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/agentium-lab/Janus/core"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ContextRefRepo struct {
	pool *pgxpool.Pool
}

func NewContextRefRepo(pool *pgxpool.Pool) *ContextRefRepo {
	return &ContextRefRepo{pool: pool}
}

func (r *ContextRefRepo) Insert(ctx context.Context, ref core.ContextRef) error {
	scopeJSON, _ := json.Marshal(ref.AccessScope)
	var expiresAt *string
	if ref.ExpiresAt != "" {
		expiresAt = &ref.ExpiresAt
	}
	var createdAt interface{}
	if ref.CreatedAt != "" {
		createdAt = ref.CreatedAt
	} else {
		createdAt = time.Now()
	}
	_, err := r.pool.Exec(ctx,
		`INSERT INTO context_refs (tenant_id, id, type, uri, hash, classification, access_scope, expires_at, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8::timestamptz, $9)`,
		ref.TenantID, ref.ID, ref.Type, ref.URI, ref.Hash, ref.Classification, scopeJSON, expiresAt, createdAt,
	)
	return err
}

func (r *ContextRefRepo) Get(ctx context.Context, tenantID, id string) (*core.ContextRef, error) {
	var ref core.ContextRef
	var scopeJSON []byte
	var expiresAt, createdAt *string
	err := r.pool.QueryRow(ctx,
		`SELECT tenant_id, id, type, uri, hash, classification, access_scope, expires_at::text, created_at::text
		 FROM context_refs WHERE tenant_id = $1 AND id = $2`,
		tenantID, id,
	).Scan(&ref.TenantID, &ref.ID, &ref.Type, &ref.URI, &ref.Hash, &ref.Classification, &scopeJSON, &expiresAt, &createdAt)
	if err != nil {
		return nil, fmt.Errorf("get context ref: %w", err)
	}
	_ = json.Unmarshal(scopeJSON, &ref.AccessScope)
	if expiresAt != nil {
		ref.ExpiresAt = *expiresAt
	}
	if createdAt != nil {
		ref.CreatedAt = *createdAt
	}
	return &ref, nil
}

func (r *ContextRefRepo) ListByTask(ctx context.Context, tenantID, taskID string) ([]*core.ContextRef, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT cr.tenant_id, cr.id, cr.type, cr.uri, cr.hash, cr.classification, cr.access_scope, cr.expires_at::text, cr.created_at::text
		 FROM context_refs cr
		 JOIN task_context_refs tcr ON cr.tenant_id = tcr.tenant_id AND cr.id = tcr.context_ref_id
		 WHERE cr.tenant_id = $1 AND tcr.task_id = $2
		 ORDER BY tcr.attached_at ASC`,
		tenantID, taskID,
	)
	if err != nil {
		return nil, fmt.Errorf("list context refs by task: %w", err)
	}
	defer rows.Close()

	var refs []*core.ContextRef
	for rows.Next() {
		var ref core.ContextRef
		var scopeJSON []byte
		var expiresAt, createdAt *string
		if err := rows.Scan(&ref.TenantID, &ref.ID, &ref.Type, &ref.URI, &ref.Hash, &ref.Classification, &scopeJSON, &expiresAt, &createdAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(scopeJSON, &ref.AccessScope)
		if expiresAt != nil {
			ref.ExpiresAt = *expiresAt
		}
		if createdAt != nil {
			ref.CreatedAt = *createdAt
		}
		refs = append(refs, &ref)
	}
	return refs, rows.Err()
}

func (r *ContextRefRepo) Delete(ctx context.Context, tenantID, id string) error {
	_, err := r.pool.Exec(ctx, `DELETE FROM context_refs WHERE tenant_id = $1 AND id = $2`, tenantID, id)
	return err
}
