package postgres

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func insertTestTenant(t *testing.T, pool *pgxpool.Pool, id string) {
	_, err := pool.Exec(context.Background(), "INSERT INTO tenants (id, name) VALUES ($1, $2)", id, "Tenant "+id)
	require.NoError(t, err)
}

func TestTenantRepo_CreateAndGet(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewTenantRepository(pool)
	ctx := context.Background()

	err := repo.Create(ctx, "acme", "Acme Corp")
	require.NoError(t, err)

	name, err := repo.GetName(ctx, "acme")
	require.NoError(t, err)
	if name != "Acme Corp" {
		t.Errorf("expected Acme Corp, got %s", name)
	}
}

func TestTenantRepo_CreateDuplicate(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewTenantRepository(pool)
	ctx := context.Background()

	err := repo.Create(ctx, "acme", "Acme Corp")
	require.NoError(t, err)

	err = repo.Create(ctx, "acme", "Acme Corp 2")
	if err == nil {
		t.Error("expected error for duplicate tenant")
	}
}
