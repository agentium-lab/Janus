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

func (r *OutboxRepo) InsertDirect(ctx context.Context, id, tenantID, kind string, payload json.RawMessage) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO outbox_events (id, tenant_id, kind, payload) VALUES ($1, $2, $3, $4)`,
		id, tenantID, kind, payload,
	)
	return err
}

func (r *OutboxRepo) FetchPending(ctx context.Context, limit int) ([]OutboxEntry, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}

	rows, err := tx.Query(ctx,
		`SELECT id, tenant_id, kind, payload, status, attempts, created_at
		 FROM outbox_events
		 WHERE status IN ('pending', 'retry')
		   AND (next_attempt_at IS NULL OR next_attempt_at <= now())
		 ORDER BY created_at ASC
		 LIMIT $1 FOR UPDATE SKIP LOCKED`, limit,
	)
	if err != nil {
		tx.Rollback(ctx)
		return nil, err
	}
	defer rows.Close()

	var entries []OutboxEntry
	for rows.Next() {
		var e OutboxEntry
		if err := rows.Scan(&e.ID, &e.TenantID, &e.Kind, &e.Payload, &e.Status, &e.Attempts, &e.CreatedAt); err != nil {
			tx.Rollback(ctx)
			return nil, err
		}
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		tx.Rollback(ctx)
		return nil, err
	}

	for _, e := range entries {
		_, err := tx.Exec(ctx,
			`UPDATE outbox_events SET status = 'publishing', attempts = attempts + 1 WHERE id = $1`,
			e.ID,
		)
		if err != nil {
			tx.Rollback(ctx)
			return nil, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return entries, nil
}

func (r *OutboxRepo) MarkPublished(ctx context.Context, id string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE outbox_events SET status = 'published', published_at = now(), attempts = attempts + 1 WHERE id = $1`,
		id,
	)
	return err
}

const maxOutboxRetries = 5

func (r *OutboxRepo) MarkFailed(ctx context.Context, id string) error {
	return r.MarkFailedWithReason(ctx, id, "")
}

func (r *OutboxRepo) MarkFailedWithReason(ctx context.Context, id string, lastErr string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE outbox_events
		 SET status = CASE WHEN attempts >= $2 THEN 'dead' ELSE 'retry' END,
		     last_error = CASE WHEN $3 != '' THEN $3 ELSE last_error END,
		     next_attempt_at = CASE WHEN attempts < $2 THEN now() + interval '5 seconds' * attempts ^ 2 ELSE NULL END
		 WHERE id = $1`,
		id, maxOutboxRetries, lastErr,
	)
	return err
}
