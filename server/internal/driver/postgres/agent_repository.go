package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/agentium-lab/Janus/core"
)

type AgentRepository struct {
	pool *pgxpool.Pool
}

func NewAgentRepository(pool *pgxpool.Pool) *AgentRepository {
	return &AgentRepository{pool: pool}
}

func (r *AgentRepository) Register(ctx context.Context, agent core.Agent) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO agents (id, tenant_id, display_name, protocol, endpoint, status, description, max_concurrency, rpm, tpm)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		agent.ID, agent.TenantID, agent.DisplayName, string(agent.Protocol),
		nilIfEmpty(agent.Endpoint), string(agent.Status), nilIfEmpty(agent.Description),
		agent.MaxConcurrency, nilIfZero(agent.RPM), nilIfZero(agent.TPM),
	)
	return err
}

func (r *AgentRepository) Get(ctx context.Context, tenantID, agentID string) (*core.Agent, error) {
	var a core.Agent
	var protocol, status string
	var endpoint, description *string
	var maxConc int
	var rpm, tpm *int
	var lastHB *time.Time

	err := r.pool.QueryRow(ctx,
		`SELECT id, tenant_id, display_name, protocol, endpoint, status, description,
		        max_concurrency, rpm, tpm, created_at, updated_at, last_heartbeat_at
		 FROM agents WHERE tenant_id = $1 AND id = $2`,
		tenantID, agentID,
	).Scan(
		&a.ID, &a.TenantID, &a.DisplayName, &protocol, &endpoint, &status, &description,
		&maxConc, &rpm, &tpm, &a.CreatedAt, &a.UpdatedAt, &lastHB,
	)
	if err != nil {
		return nil, err
	}

	a.Protocol = core.AgentProtocol(protocol)
	a.Status = core.AgentStatus(status)
	if endpoint != nil {
		a.Endpoint = *endpoint
	}
	if description != nil {
		a.Description = *description
	}
	a.MaxConcurrency = maxConc
	if rpm != nil {
		a.RPM = *rpm
	}
	if tpm != nil {
		a.TPM = *tpm
	}
	if lastHB != nil {
		a.LastHeartbeatAt = lastHB
	}
	return &a, nil
}

func (r *AgentRepository) UpdateStatus(ctx context.Context, tenantID, agentID string, status core.AgentStatus) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE agents SET status = $1, updated_at = now() WHERE tenant_id = $2 AND id = $3`,
		string(status), tenantID, agentID,
	)
	return err
}

func (r *AgentRepository) UpdateHeartbeat(ctx context.Context, tenantID, agentID string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE agents SET last_heartbeat_at = now(), updated_at = now(), status = 'online'
		 WHERE tenant_id = $1 AND id = $2`,
		tenantID, agentID,
	)
	return err
}

func (r *AgentRepository) List(ctx context.Context, tenantID string) ([]*core.Agent, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, tenant_id, display_name, protocol, endpoint, status, description,
		        max_concurrency, rpm, tpm, created_at, updated_at, last_heartbeat_at
		 FROM agents WHERE tenant_id = $1 ORDER BY id`,
		tenantID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanAgents(rows)
}

func (r *AgentRepository) ListByStatus(ctx context.Context, tenantID string, status core.AgentStatus) ([]*core.Agent, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, tenant_id, display_name, protocol, endpoint, status, description,
		        max_concurrency, rpm, tpm, created_at, updated_at, last_heartbeat_at
		 FROM agents WHERE tenant_id = $1 AND status = $2 ORDER BY id`,
		tenantID, string(status),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanAgents(rows)
}

func (r *AgentRepository) ListAllByStatus(ctx context.Context, status core.AgentStatus) ([]*core.Agent, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, tenant_id, display_name, protocol, endpoint, status, description,
		        max_concurrency, rpm, tpm, created_at, updated_at, last_heartbeat_at
		 FROM agents WHERE status = $1 ORDER BY id`,
		string(status),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanAgents(rows)
}

func scanAgents(rows pgx.Rows) ([]*core.Agent, error) {
	var agents []*core.Agent
	for rows.Next() {
		var a core.Agent
		var protocol, status string
		var endpoint, description *string
		var maxConc int
		var rpm, tpm *int
		var lastHB *time.Time

		err := rows.Scan(
			&a.ID, &a.TenantID, &a.DisplayName, &protocol, &endpoint, &status, &description,
			&maxConc, &rpm, &tpm, &a.CreatedAt, &a.UpdatedAt, &lastHB,
		)
		if err != nil {
			return nil, err
		}
		a.Protocol = core.AgentProtocol(protocol)
		a.Status = core.AgentStatus(status)
		if endpoint != nil {
			a.Endpoint = *endpoint
		}
		if description != nil {
			a.Description = *description
		}
		a.MaxConcurrency = maxConc
		if rpm != nil {
			a.RPM = *rpm
		}
		if tpm != nil {
			a.TPM = *tpm
		}
		if lastHB != nil {
			a.LastHeartbeatAt = lastHB
		}
		agents = append(agents, &a)
	}
	return agents, rows.Err()
}
