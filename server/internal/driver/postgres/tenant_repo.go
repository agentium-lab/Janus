package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type TenantRepository struct {
	pool *pgxpool.Pool
}

func NewTenantRepository(pool *pgxpool.Pool) *TenantRepository {
	return &TenantRepository{pool: pool}
}

func (r *TenantRepository) Create(ctx context.Context, id, name string) error {
	_, err := r.pool.Exec(ctx,
		"INSERT INTO tenants (id, name) VALUES ($1, $2)",
		id, name,
	)
	return err
}

func (r *TenantRepository) GetName(ctx context.Context, id string) (string, error) {
	var name string
	err := r.pool.QueryRow(ctx, "SELECT name FROM tenants WHERE id = $1", id).Scan(&name)
	return name, err
}
