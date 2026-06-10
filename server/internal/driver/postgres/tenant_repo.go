package postgres

import (
	"context"
	"database/sql"
)

type TenantRepository struct {
	db *sql.DB
}

func NewTenantRepository(db *sql.DB) *TenantRepository {
	return &TenantRepository{db: db}
}

func (r *TenantRepository) Create(ctx context.Context, id, name string) error {
	_, err := r.db.ExecContext(ctx,
		"INSERT INTO tenants (id, name) VALUES ($1, $2)",
		id, name,
	)
	return err
}

func (r *TenantRepository) GetName(ctx context.Context, id string) (string, error) {
	var name string
	err := r.db.QueryRowContext(ctx, "SELECT name FROM tenants WHERE id = $1", id).Scan(&name)
	return name, err
}
