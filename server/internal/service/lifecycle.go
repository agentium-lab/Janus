package service

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PGLifecycle implements Lifecycle with real PostgreSQL transactions.
type PGLifecycle struct {
	pool *pgxpool.Pool
}

func NewPGLifecycle(pool *pgxpool.Pool) *PGLifecycle {
	return &PGLifecycle{pool: pool}
}

func (s *PGLifecycle) ApplyTx(ctx context.Context, fn func(tx pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// MemoryLifecycle implements Lifecycle without a database — for tests.
// It runs fn with a nil transaction, so in-memory repos (which ignore the
// tx parameter) execute the SAME logical path as production.
type MemoryLifecycle struct{}

func NewMemoryLifecycle() *MemoryLifecycle {
	return &MemoryLifecycle{}
}

func (s *MemoryLifecycle) ApplyTx(ctx context.Context, fn func(tx pgx.Tx) error) error {
	return fn(nil) // in-memory repos ignore the tx parameter
}
