package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

func TestBudgetUsageRepo_InsertLedgerTx_Idempotent(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewBudgetUsageRepo(pool)
	ctx := context.Background()

	entry := core.LedgerEntry{
		TenantID: "t1", TaskID: "task-1", Attempt: 1,
		ScopeType: "tenant", ScopeID: "t1",
		PromptTokens: 100, CompletionTokens: 50, TotalTokens: 150, CostUSD: 0.5,
	}

	// First insert: new row.
	tx1, err := pool.Begin(ctx)
	require.NoError(t, err)
	inserted, err := repo.InsertLedgerTx(ctx, tx1, entry)
	require.NoError(t, err)
	require.True(t, inserted, "first insert should report inserted=true")
	require.NoError(t, tx1.Commit(ctx))

	// Second insert of same entry: ON CONFLICT, not inserted.
	tx2, err := pool.Begin(ctx)
	require.NoError(t, err)
	inserted2, err := repo.InsertLedgerTx(ctx, tx2, entry)
	require.NoError(t, err)
	require.False(t, inserted2, "duplicate insert should report inserted=false")
	require.NoError(t, tx2.Commit(ctx))
}

func TestBudgetUsageRepo_IncrementUsageTx_Accumulates(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewBudgetUsageRepo(pool)
	ctx := context.Background()

	// Two distinct tasks settled into the same scope accumulate.
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	require.NoError(t, repo.IncrementUsageTx(ctx, tx, "t1", "tenant", "t1", 10, 5, 15, 0.1))
	require.NoError(t, repo.IncrementUsageTx(ctx, tx, "t1", "tenant", "t1", 20, 10, 30, 0.2))
	require.NoError(t, tx.Commit(ctx))

	tokens, cost, _, err := repo.GetDailyUsage(ctx, "t1", "tenant", "t1")
	require.NoError(t, err)
	require.Equal(t, 45, tokens, "tokens should accumulate")
	if cost < 0.29 || cost > 0.31 {
		t.Fatalf("cost should be ~0.3, got %v", cost)
	}
}
