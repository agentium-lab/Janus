package postgres

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func repoRoot() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..")
}

func testDSN() string {
	host := os.Getenv("JANUS_PG_HOST")
	if host == "" {
		host = "/tmp"
	}
	port := os.Getenv("JANUS_PG_PORT")
	if port == "" {
		port = "5432"
	}
	user := os.Getenv("JANUS_PG_USER")
	if user == "" {
		user = "silv"
	}
	dbname := os.Getenv("JANUS_PG_DBNAME")
	if dbname == "" {
		dbname = "janus_test"
	}
	return fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable", host, port, user, dbname)
}

func openTestDB(t *testing.T) *pgxpool.Pool {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testDSN())
	require.NoError(t, err)
	t.Cleanup(func() { pool.Close() })
	require.NoError(t, pool.Ping(ctx))
	return pool
}

func runMigration(t *testing.T, pool *pgxpool.Pool) {
	ctx := context.Background()
	up, err := os.ReadFile(filepath.Join(repoRoot(), "migrations", "000001_initial_schema.up.sql"))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, string(up))
	require.NoError(t, err)
	t.Cleanup(func() {
		down, _ := os.ReadFile(filepath.Join(repoRoot(), "migrations", "000001_initial_schema.down.sql"))
		pool.Exec(ctx, string(down))
	})
}
