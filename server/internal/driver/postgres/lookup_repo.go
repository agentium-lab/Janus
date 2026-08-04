package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/agentium-lab/Janus/server/internal/service/routing"
)

type agentLookupRepo struct {
	pool *pgxpool.Pool
}

func NewAgentLookupRepo(pool *pgxpool.Pool) routing.AgentLookup {
	return &agentLookupRepo{pool: pool}
}

func (r *agentLookupRepo) ListOnlineByCapability(ctx context.Context, tenantID, capability string) ([]routing.AgentCandidate, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT a.id, m.id, m.status, a.max_concurrency,
		        COALESCE(m.status, '') as mailbox_status
		 FROM agents a
		 JOIN agent_capabilities ac ON a.tenant_id = ac.tenant_id AND a.id = ac.agent_id
		 LEFT JOIN mailboxes m ON a.tenant_id = m.tenant_id AND m.agent_id = a.id
		 WHERE a.tenant_id = $1 AND ac.capability = $2 AND a.status = 'online'
		 ORDER BY a.id`,
		tenantID, capability,
	)
	if err != nil {
		return nil, fmt.Errorf("list online by capability: %w", err)
	}
	defer rows.Close()

	var candidates []routing.AgentCandidate
	for rows.Next() {
		var c routing.AgentCandidate
		var capRow string
		if err := rows.Scan(&c.AgentID, &c.MailboxID, &capRow, &c.MaxConcurrency, &c.Status); err != nil {
			return nil, fmt.Errorf("scan candidate: %w", err)
		}
		if capRow == "active" || capRow == "" {
			candidates = append(candidates, c)
		}
	}
	return candidates, nil
}

func (r *agentLookupRepo) GetAgentMailbox(ctx context.Context, tenantID, agentID string) (string, error) {
	var mailboxID string
	err := r.pool.QueryRow(ctx,
		`SELECT m.id FROM mailboxes m
		 JOIN agents a ON a.tenant_id = m.tenant_id AND a.id = m.agent_id
		 WHERE a.tenant_id = $1 AND a.id = $2 AND m.status = 'active'
		 LIMIT 1`,
		tenantID, agentID,
	).Scan(&mailboxID)
	if err != nil {
		return "", fmt.Errorf("agent %s mailbox not found: %w", agentID, err)
	}
	return mailboxID, nil
}

func (r *agentLookupRepo) ValidateMailbox(ctx context.Context, tenantID, mailboxID string) (bool, error) {
	var status string
	err := r.pool.QueryRow(ctx,
		`SELECT status FROM mailboxes WHERE tenant_id = $1 AND id = $2`,
		tenantID, mailboxID,
	).Scan(&status)
	if err != nil {
		return false, fmt.Errorf("validate mailbox: %w", err)
	}
	return status == "active", nil
}

func (r *agentLookupRepo) GetGroupMailboxes(ctx context.Context, tenantID, groupID string) ([]string, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT m.id
		 FROM agents a
		 JOIN mailboxes m ON a.tenant_id = m.tenant_id AND a.id = m.agent_id
		 WHERE a.tenant_id = $1 AND a.team_id = $2 AND a.status = 'online' AND m.status = 'active'
		 ORDER BY m.id`,
		tenantID, groupID,
	)
	if err != nil {
		return nil, fmt.Errorf("list group mailboxes: %w", err)
	}
	defer rows.Close()

	var mailboxes []string
	for rows.Next() {
		var mb string
		if err := rows.Scan(&mb); err != nil {
			return nil, fmt.Errorf("scan mailbox: %w", err)
		}
		mailboxes = append(mailboxes, mb)
	}
	if len(mailboxes) == 0 {
		return nil, fmt.Errorf("no group mailbox mapping for %s", groupID)
	}
	return mailboxes, nil
}

func (r *agentLookupRepo) GetHumanMailboxes(ctx context.Context, tenantID, humanID string) ([]string, error) {
	return nil, fmt.Errorf("human routing is not supported; use the approval workflow for human-in-the-loop")
}
