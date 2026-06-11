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
		        created_at, updated_at
		 FROM mailboxes WHERE tenant_id = $1 AND id = $2`,
		tenantID, mailboxID,
	).Scan(
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
	return &mb, nil
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
