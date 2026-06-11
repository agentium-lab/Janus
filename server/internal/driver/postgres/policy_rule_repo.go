package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/agentium-lab/Janus/core"
)

type PolicyRuleRepository struct {
	pool *pgxpool.Pool
}

func NewPolicyRuleRepository(pool *pgxpool.Pool) *PolicyRuleRepository {
	return &PolicyRuleRepository{pool: pool}
}

func (r *PolicyRuleRepository) Create(ctx context.Context, rule core.PolicyRule) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO policy_rules (tenant_id, id, name, status, priority, condition, action, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, now(), now())`,
		rule.TenantID, rule.ID, rule.Name, rule.Status, rule.Priority,
		rule.Condition, rule.Action,
	)
	return err
}

func (r *PolicyRuleRepository) ListActive(ctx context.Context, tenantID string) ([]*core.PolicyRule, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT tenant_id, id, name, status, priority, condition, action, created_at, updated_at
		 FROM policy_rules
		 WHERE tenant_id = $1 AND status = 'active'
		 ORDER BY priority ASC`,
		tenantID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPolicyRules(rows)
}

func scanPolicyRules(rows pgx.Rows) ([]*core.PolicyRule, error) {
	var rules []*core.PolicyRule
	for rows.Next() {
		var rule core.PolicyRule
		err := rows.Scan(
			&rule.TenantID, &rule.ID, &rule.Name, &rule.Status, &rule.Priority,
			&rule.Condition, &rule.Action, &rule.CreatedAt, &rule.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		rules = append(rules, &rule)
	}
	return rules, rows.Err()
}
