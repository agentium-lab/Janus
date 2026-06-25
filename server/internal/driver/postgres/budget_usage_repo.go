package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/agentium-lab/Janus/core"
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

// InsertLedgerTx inserts a ledger entry inside a transaction. Returns true if a
// new row was inserted, false if it already existed (ON CONFLICT DO NOTHING).
func (r *BudgetUsageRepo) InsertLedgerTx(ctx context.Context, tx pgx.Tx, e core.LedgerEntry) (bool, error) {
	tag, err := tx.Exec(ctx,
		`INSERT INTO budget_usage_ledger
		   (tenant_id, task_id, attempt, scope_type, scope_id,
		    prompt_tokens, completion_tokens, total_tokens, cost_usd)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		 ON CONFLICT (tenant_id, task_id, attempt, scope_type, scope_id) DO NOTHING`,
		e.TenantID, e.TaskID, e.Attempt, e.ScopeType, e.ScopeID,
		e.PromptTokens, e.CompletionTokens, e.TotalTokens, e.CostUSD,
	)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// InsertLedger is the non-transactional variant for fallback paths.
func (r *BudgetUsageRepo) InsertLedger(ctx context.Context, e core.LedgerEntry) (bool, error) {
	tag, err := r.pool.Exec(ctx,
		`INSERT INTO budget_usage_ledger
		   (tenant_id, task_id, attempt, scope_type, scope_id,
		    prompt_tokens, completion_tokens, total_tokens, cost_usd)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		 ON CONFLICT (tenant_id, task_id, attempt, scope_type, scope_id) DO NOTHING`,
		e.TenantID, e.TaskID, e.Attempt, e.ScopeType, e.ScopeID,
		e.PromptTokens, e.CompletionTokens, e.TotalTokens, e.CostUSD,
	)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// IncrementUsageTx adds token/cost usage to the budget_usage aggregate inside a
// transaction. Call only after InsertLedgerTx returned true (new row).
func (r *BudgetUsageRepo) IncrementUsageTx(ctx context.Context, tx pgx.Tx, tenantID, scopeType, scopeID string, prompt, completion, total int64, cost float64) error {
	periodKey := time.Now().Format("2006-01-02")
	_, err := tx.Exec(ctx,
		`INSERT INTO budget_usage (tenant_id, scope_type, scope_id, period, period_key, tokens_used, cost_used, task_count)
		 VALUES ($1, $2, $3, 'daily', $4, $5, $6, 0)
		 ON CONFLICT (tenant_id, scope_type, scope_id, period, period_key)
		 DO UPDATE SET tokens_used = budget_usage.tokens_used + $5, cost_used = budget_usage.cost_used + $6`,
		tenantID, scopeType, scopeID, periodKey, total, cost,
	)
	return err
}

// IncrementUsage is the non-transactional variant.
func (r *BudgetUsageRepo) IncrementUsage(ctx context.Context, tenantID, scopeType, scopeID string, prompt, completion, total int64, cost float64) error {
	periodKey := time.Now().Format("2006-01-02")
	_, err := r.pool.Exec(ctx,
		`INSERT INTO budget_usage (tenant_id, scope_type, scope_id, period, period_key, tokens_used, cost_used, task_count)
		 VALUES ($1, $2, $3, 'daily', $4, $5, $6, 0)
		 ON CONFLICT (tenant_id, scope_type, scope_id, period, period_key)
		 DO UPDATE SET tokens_used = budget_usage.tokens_used + $5, cost_used = budget_usage.cost_used + $6`,
		tenantID, scopeType, scopeID, periodKey, total, cost,
	)
	return err
}
