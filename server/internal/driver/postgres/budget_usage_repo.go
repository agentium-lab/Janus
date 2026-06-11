package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type BudgetUsageRepo struct {
	pool *pgxpool.Pool
}

func NewBudgetUsageRepo(pool *pgxpool.Pool) *BudgetUsageRepo {
	return &BudgetUsageRepo{pool: pool}
}

func (r *BudgetUsageRepo) ReserveTask(ctx context.Context, tenantID, scopeType, scopeID string) error {
	periodKey := time.Now().Format("2006-01-02")
	_, err := r.pool.Exec(ctx,
		`INSERT INTO budget_usage (tenant_id, scope_type, scope_id, period, period_key, task_count)
		 VALUES ($1, $2, $3, 'daily', $4, 1)
		 ON CONFLICT (tenant_id, scope_type, scope_id, period, period_key)
		 DO UPDATE SET task_count = budget_usage.task_count + 1`,
		tenantID, scopeType, scopeID, periodKey,
	)
	return err
}

func (r *BudgetUsageRepo) SettleUsage(ctx context.Context, tenantID, scopeType, scopeID string, tokens int, costUSD float64) error {
	periodKey := time.Now().Format("2006-01-02")
	_, err := r.pool.Exec(ctx,
		`INSERT INTO budget_usage (tenant_id, scope_type, scope_id, period, period_key, tokens_used, cost_used, task_count)
		 VALUES ($1, $2, $3, 'daily', $4, $5, $6, 0)
		 ON CONFLICT (tenant_id, scope_type, scope_id, period, period_key)
		 DO UPDATE SET tokens_used = budget_usage.tokens_used + $5, cost_used = budget_usage.cost_used + $6`,
		tenantID, scopeType, scopeID, periodKey, tokens, costUSD,
	)
	return err
}

func (r *BudgetUsageRepo) ReleaseTask(ctx context.Context, tenantID, scopeType, scopeID string) error {
	periodKey := time.Now().Format("2006-01-02")
	tag, err := r.pool.Exec(ctx,
		`UPDATE budget_usage SET task_count = GREATEST(task_count - 1, 0)
		 WHERE tenant_id = $1 AND scope_type = $2 AND scope_id = $3 AND period = 'daily' AND period_key = $4`,
		tenantID, scopeType, scopeID, periodKey,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("no budget usage row to release")
	}
	return nil
}

func (r *BudgetUsageRepo) GetDailyUsage(ctx context.Context, tenantID, scopeType, scopeID string) (tokens int, costUSD float64, taskCount int, err error) {
	periodKey := time.Now().Format("2006-01-02")
	err = r.pool.QueryRow(ctx,
		`SELECT COALESCE(tokens_used, 0), COALESCE(cost_used, 0), COALESCE(task_count, 0)
		 FROM budget_usage
		 WHERE tenant_id = $1 AND scope_type = $2 AND scope_id = $3 AND period = 'daily' AND period_key = $4`,
		tenantID, scopeType, scopeID, periodKey,
	).Scan(&tokens, &costUSD, &taskCount)
	if err != nil {
		return 0, 0, 0, nil
	}
	return
}
