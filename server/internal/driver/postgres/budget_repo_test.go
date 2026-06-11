package postgres

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

func TestBudgetRepo_UpsertAndGet(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "acme")
	repo := NewBudgetRepository(pool)
	ctx := context.Background()

	spec := core.BudgetSpec{
		TenantID:       "acme",
		ScopeType:      core.BudgetScopeTenant,
		ScopeID:        "acme",
		RPM:            100,
		TPM:            50000,
		MaxConcurrency: 10,
		DailyCostUSD:   50.0,
		MonthlyCostUSD: 1000.0,
	}
	require.NoError(t, repo.Upsert(ctx, spec))

	got, err := repo.Get(ctx, "acme", core.BudgetScopeTenant, "acme")
	require.NoError(t, err)
	assert.Equal(t, "acme", got.TenantID)
	assert.Equal(t, core.BudgetScopeTenant, got.ScopeType)
	assert.Equal(t, 100, got.RPM)
	assert.Equal(t, 50000, got.TPM)
	assert.Equal(t, 10, got.MaxConcurrency)
	assert.Equal(t, 50.0, got.DailyCostUSD)
	assert.Equal(t, 1000.0, got.MonthlyCostUSD)
}

func TestBudgetRepo_UpsertOverwrite(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "acme")
	repo := NewBudgetRepository(pool)
	ctx := context.Background()

	spec := core.BudgetSpec{
		TenantID:  "acme",
		ScopeType: core.BudgetScopeTenant,
		ScopeID:   "acme",
		RPM:       100,
		TPM:       50000,
	}
	require.NoError(t, repo.Upsert(ctx, spec))

	spec.RPM = 200
	spec.TPM = 100000
	require.NoError(t, repo.Upsert(ctx, spec))

	got, err := repo.Get(ctx, "acme", core.BudgetScopeTenant, "acme")
	require.NoError(t, err)
	assert.Equal(t, 200, got.RPM)
	assert.Equal(t, 100000, got.TPM)
}

func TestBudgetRepo_GetNotFound(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewBudgetRepository(pool)
	ctx := context.Background()

	_, err := repo.Get(ctx, "acme", core.BudgetScopeTenant, "nonexistent")
	assert.Equal(t, pgx.ErrNoRows, err)
}

func TestBudgetRepo_ListByTenant(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "acme")
	repo := NewBudgetRepository(pool)
	ctx := context.Background()

	require.NoError(t, repo.Upsert(ctx, core.BudgetSpec{
		TenantID: "acme", ScopeType: core.BudgetScopeTenant, ScopeID: "acme", RPM: 100,
	}))
	require.NoError(t, repo.Upsert(ctx, core.BudgetSpec{
		TenantID: "acme", ScopeType: core.BudgetScopeAgent, ScopeID: "agent-a", TPM: 5000,
	}))

	specs, err := repo.ListByTenant(ctx, "acme")
	require.NoError(t, err)
	assert.Len(t, specs, 2)
}

func TestBudgetRepo_ListByTenantEmpty(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewBudgetRepository(pool)
	ctx := context.Background()

	specs, err := repo.ListByTenant(ctx, "acme")
	require.NoError(t, err)
	assert.Len(t, specs, 0)
}

func TestBudgetRepo_NullFields(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "acme")
	repo := NewBudgetRepository(pool)
	ctx := context.Background()

	require.NoError(t, repo.Upsert(ctx, core.BudgetSpec{
		TenantID:  "acme",
		ScopeType: core.BudgetScopeTenant,
		ScopeID:   "acme",
	}))

	got, err := repo.Get(ctx, "acme", core.BudgetScopeTenant, "acme")
	require.NoError(t, err)
	assert.Equal(t, 0, got.RPM)
	assert.Equal(t, 0, got.TPM)
	assert.Equal(t, 0, got.MaxConcurrency)
	assert.Equal(t, 0.0, got.DailyCostUSD)
	assert.Equal(t, 0.0, got.MonthlyCostUSD)
}

func TestBudgetRepo_NullFieldsViaList(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "acme")
	repo := NewBudgetRepository(pool)
	ctx := context.Background()

	require.NoError(t, repo.Upsert(ctx, core.BudgetSpec{
		TenantID: "acme", ScopeType: core.BudgetScopeTenant, ScopeID: "acme",
	}))

	specs, err := repo.ListByTenant(ctx, "acme")
	require.NoError(t, err)
	require.Len(t, specs, 1)
	assert.Equal(t, 0, specs[0].RPM)
}
