package postgres

import (
	"context"
	"database/sql"
	"time"

	"github.com/agentium-lab/Janus/core"
)

type AgentRepository struct {
	db *sql.DB
}

func NewAgentRepository(db *sql.DB) *AgentRepository {
	return &AgentRepository{db: db}
}

func (r *AgentRepository) Register(ctx context.Context, agent core.Agent) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO agents (id, tenant_id, display_name, protocol, endpoint, status, description, max_concurrency, rpm, tpm)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		agent.ID, agent.TenantID, agent.DisplayName, string(agent.Protocol),
		agent.Endpoint, string(agent.Status), agent.Description,
		agent.MaxConcurrency, agent.RPM, agent.TPM,
	)
	return err
}

func (r *AgentRepository) Get(ctx context.Context, tenantID, agentID string) (*core.Agent, error) {
	var a core.Agent
	var protocol, status string
	var endpoint, description sql.NullString
	var maxConc int
	var rpm, tpm sql.NullInt32
	var lastHB sql.NullTime

	err := r.db.QueryRowContext(ctx,
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
	a.Endpoint = endpoint.String
	a.Description = description.String
	a.MaxConcurrency = maxConc
	if rpm.Valid {
		a.RPM = int(rpm.Int32)
	}
	if tpm.Valid {
		a.TPM = int(tpm.Int32)
	}
	if lastHB.Valid {
		a.LastHeartbeatAt = &lastHB.Time
	}
	return &a, nil
}

func (r *AgentRepository) UpdateStatus(ctx context.Context, tenantID, agentID string, status core.AgentStatus) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE agents SET status = $1, updated_at = now() WHERE tenant_id = $2 AND id = $3`,
		string(status), tenantID, agentID,
	)
	return err
}

func (r *AgentRepository) UpdateHeartbeat(ctx context.Context, tenantID, agentID string) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE agents SET last_heartbeat_at = now(), updated_at = now(), status = 'online'
		 WHERE tenant_id = $1 AND id = $2`,
		tenantID, agentID,
	)
	return err
}

func (r *AgentRepository) List(ctx context.Context, tenantID string) ([]*core.Agent, error) {
	rows, err := r.db.QueryContext(ctx,
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
	rows, err := r.db.QueryContext(ctx,
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

func scanAgents(rows *sql.Rows) ([]*core.Agent, error) {
	var agents []*core.Agent
	for rows.Next() {
		var a core.Agent
		var protocol, status string
		var endpoint, description sql.NullString
		var maxConc int
		var rpm, tpm sql.NullInt32
		var lastHB sql.NullTime

		err := rows.Scan(
			&a.ID, &a.TenantID, &a.DisplayName, &protocol, &endpoint, &status, &description,
			&maxConc, &rpm, &tpm, &a.CreatedAt, &a.UpdatedAt, &lastHB,
		)
		if err != nil {
			return nil, err
		}
		a.Protocol = core.AgentProtocol(protocol)
		a.Status = core.AgentStatus(status)
		a.Endpoint = endpoint.String
		a.Description = description.String
		a.MaxConcurrency = maxConc
		if rpm.Valid {
			a.RPM = int(rpm.Int32)
		}
		if tpm.Valid {
			a.TPM = int(tpm.Int32)
		}
		if lastHB.Valid {
			t := lastHB.Time
			a.LastHeartbeatAt = &t
		}
		agents = append(agents, &a)
	}
	return agents, rows.Err()
}

var _ = time.Sleep
