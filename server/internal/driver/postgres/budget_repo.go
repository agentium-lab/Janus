package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/agentium-lab/Janus/core"
)

type BudgetRepository struct {
	pool *pgxpool.Pool
}

func NewBudgetRepository(pool *pgxpool.Pool) *BudgetRepository {
	return &BudgetRepository{pool: pool}
}

func (r *BudgetRepository) Upsert(ctx context.Context, spec core.BudgetSpec) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO budgets (tenant_id, scope_type, scope_id, rpm, tpm, max_concurrency,
		  daily_cost_usd, monthly_cost_usd, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now(), now())
		 ON CONFLICT (tenant_id, scope_type, scope_id)
		 DO UPDATE SET rpm = EXCLUDED.rpm, tpm = EXCLUDED.tpm, max_concurrency = EXCLUDED.max_concurrency,
		  daily_cost_usd = EXCLUDED.daily_cost_usd, monthly_cost_usd = EXCLUDED.monthly_cost_usd,
		  updated_at = now()`,
		spec.TenantID, string(spec.ScopeType), spec.ScopeID,
		nilIfZero(spec.RPM), nilIfZero(spec.TPM), nilIfZero(spec.MaxConcurrency),
		nilIfZeroF(spec.DailyCostUSD), nilIfZeroF(spec.MonthlyCostUSD),
	)
	return err
}

func (r *BudgetRepository) Get(ctx context.Context, tenantID string, scopeType core.BudgetScopeType, scopeID string) (*core.BudgetSpec, error) {
	var spec core.BudgetSpec
	var rpm, tpm, maxConc *int
	var dailyCost, monthlyCost *float64
	var createdAt, updatedAt time.Time

	err := r.pool.QueryRow(ctx,
		`SELECT tenant_id, scope_type, scope_id, rpm, tpm, max_concurrency,
		        daily_cost_usd, monthly_cost_usd, created_at, updated_at
		 FROM budgets WHERE tenant_id = $1 AND scope_type = $2 AND scope_id = $3`,
		tenantID, string(scopeType), scopeID,
	).Scan(
		&spec.TenantID, &spec.ScopeType, &spec.ScopeID,
		&rpm, &tpm, &maxConc, &dailyCost, &monthlyCost,
		&createdAt, &updatedAt,
	)
	if err != nil {
		return nil, err
	}
	if rpm != nil {
		spec.RPM = *rpm
	}
	if tpm != nil {
		spec.TPM = *tpm
	}
	if maxConc != nil {
		spec.MaxConcurrency = *maxConc
	}
	if dailyCost != nil {
		spec.DailyCostUSD = *dailyCost
	}
	if monthlyCost != nil {
		spec.MonthlyCostUSD = *monthlyCost
	}
	spec.CreatedAt = createdAt
	spec.UpdatedAt = updatedAt
	return &spec, nil
}

func (r *BudgetRepository) ListByTenant(ctx context.Context, tenantID string) ([]*core.BudgetSpec, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT tenant_id, scope_type, scope_id, rpm, tpm, max_concurrency,
		        daily_cost_usd, monthly_cost_usd, created_at, updated_at
		 FROM budgets WHERE tenant_id = $1 ORDER BY scope_type, scope_id`,
		tenantID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanBudgets(rows)
}

func scanBudgets(rows pgx.Rows) ([]*core.BudgetSpec, error) {
	var specs []*core.BudgetSpec
	for rows.Next() {
		var spec core.BudgetSpec
		var rpm, tpm, maxConc *int
		var dailyCost, monthlyCost *float64

		err := rows.Scan(
			&spec.TenantID, &spec.ScopeType, &spec.ScopeID,
			&rpm, &tpm, &maxConc, &dailyCost, &monthlyCost,
			&spec.CreatedAt, &spec.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		if rpm != nil {
			spec.RPM = *rpm
		}
		if tpm != nil {
			spec.TPM = *tpm
		}
		if maxConc != nil {
			spec.MaxConcurrency = *maxConc
		}
		if dailyCost != nil {
			spec.DailyCostUSD = *dailyCost
		}
		if monthlyCost != nil {
			spec.MonthlyCostUSD = *monthlyCost
		}
		specs = append(specs, &spec)
	}
	return specs, rows.Err()
}

func nilIfZeroF(v float64) interface{} {
	if v == 0 {
		return nil
	}
	return v
}
