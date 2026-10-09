package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/agentium-lab/Janus/core"
)

type MailboxRepository struct {
	pool *pgxpool.Pool
}

func NewMailboxRepository(pool *pgxpool.Pool) *MailboxRepository {
	return &MailboxRepository{pool: pool}
}

func (r *MailboxRepository) Create(ctx context.Context, mb core.Mailbox) error {
	retryJSON, _ := json.Marshal(mb.RetryPolicy)

	_, err := r.pool.Exec(ctx,
		`INSERT INTO mailboxes (tenant_id, id, agent_id, status, priority, max_concurrency,
		  ack_wait_seconds, max_deliver, retention_seconds, retry_policy)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		mb.TenantID, mb.ID, mb.AgentID, string(mb.Status), string(mb.Priority),
		mb.MaxConcurrency, mb.ACKWaitSeconds, mb.MaxDeliver, mb.RetentionSeconds, retryJSON,
	)
	return err
}

func (r *MailboxRepository) Get(ctx context.Context, tenantID, mailboxID string) (*core.Mailbox, error) {
	var mb core.Mailbox
	var status, priority string
	var retryJSON []byte

	err := r.pool.QueryRow(ctx,
		`SELECT tenant_id, id, agent_id, status, priority, max_concurrency,
		        ack_wait_seconds, max_deliver, retention_seconds, retry_policy,
		        created_at, updated_at, config_version
		 FROM mailboxes WHERE tenant_id = $1 AND id = $2`,
		tenantID, mailboxID,
	).Scan(
		&mb.TenantID, &mb.ID, &mb.AgentID, &status, &priority, &mb.MaxConcurrency,
		&mb.ACKWaitSeconds, &mb.MaxDeliver, &mb.RetentionSeconds, &retryJSON,
		&mb.CreatedAt, &mb.UpdatedAt, &mb.ConfigVersion,
	)
	if err != nil {
		return nil, err
	}

	mb.Status = core.MailboxStatus(status)
	mb.Priority = core.Priority(priority)
	_ = json.Unmarshal(retryJSON, &mb.RetryPolicy)
	return &mb, nil
}

// AcquireMailboxLock takes a session advisory lock on the mailbox key,
// serializing the read-version→write-broker sequence ACROSS REPLICAS: any
// instance applying a config for this mailbox does so exclusively, so an
// older revision cannot interleave past a newer one between the version
// check and the broker write.
func (r *MailboxRepository) AcquireMailboxLock(ctx context.Context, tenantID, mailboxID string) (release func(), ok bool, err error) {
	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return nil, false, err
	}
	// FNV-1a over the logical key, cast to the 64-bit advisory-lock space.
	// hashtext() was 32-bit (rare cross-mailbox false contention) and NOT
	// stable across PG major versions — during a mixed-version rolling
	// upgrade old and new replicas computed DIFFERENT lock keys for the
	// same mailbox, silently reopening the lost-update window.
	lockKey := advisoryLockKey64("mailbox:" + tenantID + ":" + mailboxID)
	var locked bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, lockKey).Scan(&locked); err != nil {
		conn.Release()
		return nil, false, err
	}
	if !locked {
		conn.Release()
		return nil, false, nil
	}
	return func() {
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, lockKey)
		conn.Release()
	}, true, nil
}

// advisoryLockKey64 maps a logical lock name to a stable 64-bit key for
// pg_try_advisory_lock(bigint). FNV-1a is deterministic across PG versions.
func advisoryLockKey64(name string) int64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for _, b := range []byte(name) {
		h ^= uint64(b)
		h *= prime64
	}
	// pg advisory bigint locks treat the value as signed; map to non-negative.
	return int64(h & 0x7fffffffffffffff)
}

func (r *MailboxRepository) ListAll(ctx context.Context) ([]*core.Mailbox, error) {
	// Must read the FULL config: the startup reconcile replays these rows
	// onto the NATS consumers, and a partial read replays zero values —
	// resetting every custom ack_wait/max_deliver/max_concurrency to the
	// defaults on each boot.
	rows, err := r.pool.Query(ctx,
		`SELECT tenant_id, id, agent_id, status, max_concurrency,
		        ack_wait_seconds, max_deliver, retry_policy, config_version
		 FROM mailboxes ORDER BY tenant_id, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*core.Mailbox
	for rows.Next() {
		var m core.Mailbox
		var status string
		var retryJSON []byte
		if err := rows.Scan(&m.TenantID, &m.ID, &m.AgentID, &status, &m.MaxConcurrency,
			&m.ACKWaitSeconds, &m.MaxDeliver, &retryJSON, &m.ConfigVersion); err != nil {
			return nil, err
		}
		m.Status = core.MailboxStatus(status)
		_ = json.Unmarshal(retryJSON, &m.RetryPolicy)
		out = append(out, &m)
	}
	return out, rows.Err()
}

func (r *MailboxRepository) ListByAgent(ctx context.Context, tenantID, agentID string) ([]*core.Mailbox, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT tenant_id, id, agent_id, status, priority, max_concurrency,
		        ack_wait_seconds, max_deliver, retention_seconds, retry_policy,
		        created_at, updated_at
		 FROM mailboxes WHERE tenant_id = $1 AND agent_id = $2 ORDER BY id`,
		tenantID, agentID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var mailboxes []*core.Mailbox
	for rows.Next() {
		var mb core.Mailbox
		var status, priority string
		var retryJSON []byte
		err := rows.Scan(
			&mb.TenantID, &mb.ID, &mb.AgentID, &status, &priority, &mb.MaxConcurrency,
			&mb.ACKWaitSeconds, &mb.MaxDeliver, &mb.RetentionSeconds, &retryJSON,
			&mb.CreatedAt, &mb.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		mb.Status = core.MailboxStatus(status)
		mb.Priority = core.Priority(priority)
		_ = json.Unmarshal(retryJSON, &mb.RetryPolicy)
		mailboxes = append(mailboxes, &mb)
	}
	return mailboxes, rows.Err()
}

func (r *MailboxRepository) Backlog(ctx context.Context, tenantID, mailboxID string) (int, error) {
	var count int
	err := r.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM tasks WHERE tenant_id = $1 AND mailbox_id = $2 AND status IN ('queued', 'retry_scheduled')`,
		tenantID, mailboxID,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("backlog count: %w", err)
	}
	return count, nil
}

func (r *MailboxRepository) UpdateStatus(ctx context.Context, tenantID, mailboxID string, status core.MailboxStatus) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE mailboxes SET status = $1, updated_at = now() WHERE tenant_id = $2 AND id = $3`,
		string(status), tenantID, mailboxID,
	)
	return err
}

// UpdateConfig applies new settings and returns the new config_version —
// callers embed it in parked reconcile specs so retries can refuse to
// apply a superseded revision.
func (r *MailboxRepository) UpdateConfig(ctx context.Context, tenantID, mailboxID string, maxConcurrency, ackWaitSeconds, maxDeliver, retentionSeconds int) (int, error) {
	var version int
	err := r.pool.QueryRow(ctx,
		`UPDATE mailboxes SET max_concurrency = $3, ack_wait_seconds = $4,
		        max_deliver = $5, retention_seconds = $6, updated_at = now(),
		        config_version = config_version + 1
		 WHERE tenant_id = $1 AND id = $2
		 RETURNING config_version`,
		tenantID, mailboxID, maxConcurrency, ackWaitSeconds, maxDeliver, retentionSeconds,
	).Scan(&version)
	if err != nil {
		return 0, err
	}
	return version, nil
}
