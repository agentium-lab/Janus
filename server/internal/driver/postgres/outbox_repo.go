package postgres

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type OutboxRepo struct {
	pool *pgxpool.Pool
}

func NewOutboxRepo(pool *pgxpool.Pool) *OutboxRepo {
	return &OutboxRepo{pool: pool}
}

type OutboxEntry struct {
	ID        string
	TenantID  string
	Kind      string
	Payload   json.RawMessage
	Status    string
	Attempts  int
	CreatedAt time.Time
}

func (r *OutboxRepo) Insert(ctx context.Context, tx pgx.Tx, id, tenantID, kind string, payload json.RawMessage) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO outbox_events (id, tenant_id, kind, payload) VALUES ($1, $2, $3, $4)`,
		id, tenantID, kind, payload,
	)
	return err
}

func (r *OutboxRepo) FetchPending(ctx context.Context, limit int) ([]OutboxEntry, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, tenant_id, kind, payload, status, attempts, created_at
		 FROM outbox_events
		 WHERE status = 'pending'
		 ORDER BY created_at ASC
		 LIMIT $1 FOR UPDATE SKIP LOCKED`, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []OutboxEntry
	for rows.Next() {
		var e OutboxEntry
		if err := rows.Scan(&e.ID, &e.TenantID, &e.Kind, &e.Payload, &e.Status, &e.Attempts, &e.CreatedAt); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

func (r *OutboxRepo) MarkPublished(ctx context.Context, id string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE outbox_events SET status = 'published', published_at = now(), attempts = attempts + 1 WHERE id = $1`,
		id,
	)
	return err
}

func (r *OutboxRepo) MarkFailed(ctx context.Context, id string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE outbox_events SET status = 'failed', attempts = attempts + 1 WHERE id = $1`,
		id,
	)
	return err
}
