package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Executor is the pgx statement surface shared by pgx.Tx and *pgxpool.Conn
// (and by *pgxpool.Pool). Advisory-lock holders hand their callback the
// held connection as an Executor so DB work under the lock runs on the
// SAME pooled connection: a pool-scoped query inside the lock borrows a
// second connection, and a small or saturated pool then self-deadlocks —
// the lock pins the very connection the query is waiting for.
type Executor interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}
