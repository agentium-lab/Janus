package postgres

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func insertTestTenant(t *testing.T, db *sql.DB, id string) {
	_, err := db.Exec("INSERT INTO tenants (id, name) VALUES ($1, $2)", id, "Tenant "+id)
	require.NoError(t, err)
}

func TestTenantRepo_CreateAndGet(t *testing.T) {
	db := openTestDB(t)
	runMigration(t, db)
	repo := NewTenantRepository(db)
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
	db := openTestDB(t)
	runMigration(t, db)
	repo := NewTenantRepository(db)
	ctx := context.Background()

	err := repo.Create(ctx, "acme", "Acme Corp")
	require.NoError(t, err)

	err = repo.Create(ctx, "acme", "Acme Corp 2")
	if err == nil {
		t.Error("expected error for duplicate tenant")
	}
}
