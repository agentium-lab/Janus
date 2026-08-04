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
		`INSERT INTO agents (id, tenant_id, team_id, display_name, protocol, endpoint, status, description, max_concurrency, rpm, tpm)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		agent.ID, agent.TenantID, nilIfEmpty(agent.TeamID), agent.DisplayName, string(agent.Protocol),
		nilIfEmpty(agent.Endpoint), string(agent.Status), nilIfEmpty(agent.Description),
		agent.MaxConcurrency, nilIfZero(agent.RPM), nilIfZero(agent.TPM),
	)
	return err
}

func (r *AgentRepository) Get(ctx context.Context, tenantID, agentID string) (*core.Agent, error) {
	var a core.Agent
	var protocol, status string
	var teamID, endpoint, description *string
	var maxConc int
	var rpm, tpm *int
	var lastHB *time.Time

	err := r.pool.QueryRow(ctx,
		`SELECT id, tenant_id, team_id, display_name, protocol, endpoint, status, description,
		        max_concurrency, rpm, tpm, created_at, updated_at, last_heartbeat_at
		 FROM agents WHERE tenant_id = $1 AND id = $2`,
		tenantID, agentID,
	).Scan(
		&a.ID, &a.TenantID, &teamID, &a.DisplayName, &protocol, &endpoint, &status, &description,
		&maxConc, &rpm, &tpm, &a.CreatedAt, &a.UpdatedAt, &lastHB,
	)
	if err != nil {
		return nil, err
	}

	a.Protocol = core.AgentProtocol(protocol)
	a.Status = core.AgentStatus(status)
	if teamID != nil {
		a.TeamID = *teamID
	}
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
		`SELECT id, tenant_id, team_id, display_name, protocol, endpoint, status, description,
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
		`SELECT id, tenant_id, team_id, display_name, protocol, endpoint, status, description,
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
		`SELECT id, tenant_id, team_id, display_name, protocol, endpoint, status, description,
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

func (r *AgentRepository) FindByCapability(ctx context.Context, tenantID, capability string) ([]*core.Agent, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT DISTINCT a.id, a.tenant_id, a.team_id, a.display_name, a.protocol, a.endpoint, a.status, a.description,
		        a.max_concurrency, a.rpm, a.tpm, a.created_at, a.updated_at, a.last_heartbeat_at
		 FROM agents a
		 JOIN agent_capabilities ac ON a.tenant_id = ac.tenant_id AND a.id = ac.agent_id
		 WHERE a.tenant_id = $1 AND ac.capability = $2 AND a.status = 'online'
		 ORDER BY a.id`,
		tenantID, capability,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanAgents(rows)
}

func (r *AgentRepository) ListOnlineWithCapabilities(ctx context.Context, tenantID string) ([]*core.Agent, error) {
	agents, err := r.ListByStatus(ctx, tenantID, core.AgentStatusOnline)
	if err != nil {
		return nil, err
	}
	if len(agents) == 0 {
		return agents, nil
	}

	ids := make([]string, len(agents))
	for i, a := range agents {
		ids[i] = a.ID
	}
	rows, err := r.pool.Query(ctx,
		`SELECT agent_id, capability, schema::text, description
		 FROM agent_capabilities
		 WHERE tenant_id = $1 AND agent_id = ANY($2)
		 ORDER BY agent_id, capability`,
		tenantID, ids,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	caps := make(map[string][]core.AgentCapability)
	for rows.Next() {
		var c core.AgentCapability
		if err := rows.Scan(&c.AgentID, &c.Capability, &c.Schema, &c.Description); err != nil {
			return nil, err
		}
		caps[c.AgentID] = append(caps[c.AgentID], c)
	}
	for _, a := range agents {
		a.Capabilities = caps[a.ID]
	}
	return agents, nil
}

func scanAgents(rows pgx.Rows) ([]*core.Agent, error) {
	var agents []*core.Agent
	for rows.Next() {
		var a core.Agent
		var protocol, status string
		var teamID, endpoint, description *string
		var maxConc int
		var rpm, tpm *int
		var lastHB *time.Time

		err := rows.Scan(
			&a.ID, &a.TenantID, &teamID, &a.DisplayName, &protocol, &endpoint, &status, &description,
			&maxConc, &rpm, &tpm, &a.CreatedAt, &a.UpdatedAt, &lastHB,
		)
		if err != nil {
			return nil, err
		}
		a.Protocol = core.AgentProtocol(protocol)
		a.Status = core.AgentStatus(status)
		if teamID != nil {
			a.TeamID = *teamID
		}
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
