package service

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// LifecycleService is a thin transaction wrapper for task state transitions.
// It opens a PG transaction, runs the caller-supplied fn (which orchestrates
// repo writes + outbox rows), and commits. Business logic stays in the caller.
//
// Callers MUST perform non-transactional side effects (e.g. NATS ACK/TERM)
// only after ApplyTx returns nil, to satisfy the "DB commits before NATS"
// invariant.
type LifecycleService struct {
	pool *pgxpool.Pool
}

func NewLifecycleService(pool *pgxpool.Pool) *LifecycleService {
	return &LifecycleService{pool: pool}
}

// ApplyTx runs fn inside a PG transaction. All writes in fn commit atomically
// or roll back together. Returns fn's error or commit error.
//
// The deferred Rollback is a no-op once Commit succeeds.
func (s *LifecycleService) ApplyTx(ctx context.Context, fn func(tx pgx.Tx) error) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("lifecycle service not configured")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin lifecycle tx: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit lifecycle tx: %w", err)
	}
	return nil
}
