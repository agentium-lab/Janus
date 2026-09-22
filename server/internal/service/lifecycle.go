package service

import (
	"sync"

	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Lifecycle is the unified transaction boundary for task state transitions.
// Production wires PGLifecycle (real PG transactions); tests wire
// MemoryLifecycle (fn runs with a nil tx so in-memory repos execute the SAME
// logical path as production). This is the core of the Priority-1 unified
// transaction path: one code shape, two environments.
type Lifecycle interface {
	// ApplyTx runs fn inside a transaction. All writes in fn commit atomically
	// or roll back together. Callers MUST perform non-transactional side
	// effects (e.g. queue ACK/TERM) only after ApplyTx returns nil, to satisfy
	// the "DB commits before NATS" invariant.
	ApplyTx(ctx context.Context, fn func(tx pgx.Tx) error) error

	// ApplyTxLocked is ApplyTx plus a serializing lock on key: two concurrent
	// calls with the same key cannot run fn's body at the same time. Used to
	// close check-then-act races (e.g. agent concurrency re-check inside the
	// claim transaction). The PG implementation takes a transaction-scoped
	// advisory lock; the memory implementation is a no-op (tests are
	// single-threaded per key).
	ApplyTxLocked(ctx context.Context, key string, fn func(tx pgx.Tx) error) error
}

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

func (s *PGLifecycle) ApplyTxLocked(ctx context.Context, key string, fn func(tx pgx.Tx) error) error {
	return s.ApplyTx(ctx, func(tx pgx.Tx) error {
		if _, lerr := tx.Exec(ctx, `select pg_advisory_xact_lock(hashtext($1))`, key); lerr != nil {
			return fmt.Errorf("advisory lock: %w", lerr)
		}
		return fn(tx)
	})
}

// MemoryLifecycle implements Lifecycle without a database — for tests.
// It runs fn with a nil transaction, so in-memory repos (which ignore the
// tx parameter) execute the SAME logical path as production. There is no
// rollback: writes that already ran stay visible when fn fails (memory
// repos are best-effort in tests). ApplyTxLocked DOES serialize per key,
// mirroring the production advisory lock so concurrency-sensitive tests
// exercise the same mutual exclusion.
type MemoryLifecycle struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

func NewMemoryLifecycle() *MemoryLifecycle {
	return &MemoryLifecycle{locks: make(map[string]*sync.Mutex)}
}

func (s *MemoryLifecycle) ApplyTx(ctx context.Context, fn func(tx pgx.Tx) error) error {
	return fn(nil) // in-memory repos ignore the tx parameter
}

func (s *MemoryLifecycle) ApplyTxLocked(ctx context.Context, key string, fn func(tx pgx.Tx) error) error {
	s.mu.Lock()
	lk, ok := s.locks[key]
	if !ok {
		lk = &sync.Mutex{}
		s.locks[key] = lk
	}
	s.mu.Unlock()
	lk.Lock()
	defer lk.Unlock()
	return s.ApplyTx(ctx, fn)
}
