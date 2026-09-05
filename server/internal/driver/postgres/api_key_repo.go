package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/agentium-lab/Janus/core"
)

type APIKeyRepo struct {
	pool *pgxpool.Pool
}

func NewAPIKeyRepo(pool *pgxpool.Pool) *APIKeyRepo {
	return &APIKeyRepo{pool: pool}
}

func (r *APIKeyRepo) CreateAPIKey(ctx context.Context, tenantID, keyHash, name, prefix string, scopes []string, boundAgentID string) (core.APIKey, error) {
	k := core.APIKey{
		TenantID:     tenantID,
		Name:         name,
		Prefix:       prefix,
		Scopes:       scopes,
		BoundAgentID: boundAgentID,
	}
	err := r.pool.QueryRow(ctx,
		`INSERT INTO api_keys (tenant_id, key_hash, name, prefix, scopes, bound_agent_id)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 RETURNING id, created_at`,
		tenantID, keyHash, name, prefix, scopes, boundAgentID,
	).Scan(&k.ID, &k.CreatedAt)
	return k, err
}

func (r *APIKeyRepo) ListAPIKeys(ctx context.Context, tenantID string) ([]core.APIKey, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, name, prefix, scopes, created_at, last_used_at, revoked_at
		 FROM api_keys WHERE tenant_id = $1 AND revoked_at IS NULL
		 ORDER BY created_at DESC`,
		tenantID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	keys := []core.APIKey{}
	for rows.Next() {
		var k core.APIKey
		if err := rows.Scan(&k.ID, &k.Name, &k.Prefix, &k.Scopes, &k.CreatedAt, &k.LastUsedAt, &k.RevokedAt); err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	return keys, rows.Err()
}

func (r *APIKeyRepo) RevokeAPIKey(ctx context.Context, tenantID, keyID string) (*core.APIKey, error) {
	var k core.APIKey
	err := r.pool.QueryRow(ctx,
		`UPDATE api_keys SET revoked_at = now()
		 WHERE tenant_id = $1 AND id = $2 AND revoked_at IS NULL
		 RETURNING id, name, prefix, scopes, created_at, last_used_at, revoked_at`,
		tenantID, keyID,
	).Scan(&k.ID, &k.Name, &k.Prefix, &k.Scopes, &k.CreatedAt, &k.LastUsedAt, &k.RevokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &k, nil
}
