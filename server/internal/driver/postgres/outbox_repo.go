package postgres

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type OutboxRepo struct {
	pool         *pgxpool.Pool
	workerID     string
	leaseDuration time.Duration
}

func NewOutboxRepo(pool *pgxpool.Pool) *OutboxRepo {
	return &OutboxRepo{pool: pool}
}

// SetWorker identifies this repo instance's outbox worker for lease tracking.
// Call once at startup with a stable, unique-per-process id.
func (r *OutboxRepo) SetWorker(workerID string, leaseDuration time.Duration) {
	r.workerID = workerID
	if leaseDuration > 0 {
		r.leaseDuration = leaseDuration
	}
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

// InsertDirectWithDedupe is like InsertDirect but sets a stable dedupe_key so the
// same delivery cannot be enqueued twice. If dedupeKey already exists for this
// tenant, the insert is a no-op (returns nil).
func (r *OutboxRepo) InsertDirectWithDedupe(ctx context.Context, id, tenantID, kind, dedupeKey string, payload json.RawMessage) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO outbox_events (id, tenant_id, kind, payload, dedupe_key)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (tenant_id, dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING`,
		id, tenantID, kind, payload, nilIfEmpty(dedupeKey),
	)
	return err
}

// InsertWithDedupe is the transactional variant of InsertDirectWithDedupe.
func (r *OutboxRepo) InsertWithDedupe(ctx context.Context, tx pgx.Tx, id, tenantID, kind, dedupeKey string, payload json.RawMessage) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO outbox_events (id, tenant_id, kind, payload, dedupe_key)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (tenant_id, dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING`,
		id, tenantID, kind, payload, nilIfEmpty(dedupeKey),
	)
	return err
}

// defaultOutboxLeaseDuration is how long a 'publishing' row is held by a worker
// before another worker may reclaim it. Override via SetLeaseDuration.
const defaultOutboxLeaseDuration = 60 * time.Second

// FetchPending claims up to limit pending/retry rows (and reclaims any
// 'publishing' rows whose lease has expired). Each claimed row is atomically
// moved to 'publishing' with attempts+1, locked_by, locked_at and
// lease_expires_at set, so a crashed worker's rows eventually become reclaimable.
func (r *OutboxRepo) FetchPending(ctx context.Context, limit int) ([]OutboxEntry, error) {
	lease := defaultOutboxLeaseDuration
	if r.leaseDuration > 0 {
		lease = r.leaseDuration
	}
	workerID := r.workerID

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}

	rows, err := tx.Query(ctx,
		`SELECT id, tenant_id, kind, payload, status, attempts, created_at
		 FROM outbox_events
		 WHERE (status IN ('pending', 'retry')
		          AND (next_attempt_at IS NULL OR next_attempt_at <= now()))
		    OR (status = 'publishing' AND lease_expires_at IS NOT NULL AND lease_expires_at <= now())
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
		// Only increment attempts when transitioning into 'publishing' from a
		// non-publishing state. Reclaiming an expired 'publishing' row does NOT
		// bump attempts again (the original claim already counted it).
		attemptInc := ""
		if e.Status != "publishing" {
			attemptInc = ", attempts = attempts + 1"
		}
		_, err := tx.Exec(ctx,
			`UPDATE outbox_events
			    SET status = 'publishing'`+attemptInc+`,
			        locked_by = $2, locked_at = now(), lease_expires_at = now() + $3
			  WHERE id = $1`,
			e.ID, workerID, lease,
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

// MarkPublished marks a row as published and clears the worker lease fields.
// NOTE: attempts is NOT incremented here — FetchPending already counted the
// delivery attempt when it moved the row to 'publishing'. (Previously this
// double-incremented attempts.)
func (r *OutboxRepo) MarkPublished(ctx context.Context, id string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE outbox_events
		    SET status = 'published', published_at = now(),
		        locked_by = NULL, locked_at = NULL, lease_expires_at = NULL
		  WHERE id = $1`,
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
		     next_attempt_at = CASE WHEN attempts < $2 THEN now() + interval '5 seconds' * attempts ^ 2 ELSE NULL END,
		     locked_by = NULL, locked_at = NULL, lease_expires_at = NULL
		 WHERE id = $1`,
		id, maxOutboxRetries, lastErr,
	)
	return err
}
