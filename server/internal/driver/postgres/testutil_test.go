package postgres

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func repoRoot() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..")
}

func pgConfig() (host, port, user, dbname string) {
	host = os.Getenv("JANUS_PG_HOST")
	if host == "" {
		host = "/tmp"
	}
	port = os.Getenv("JANUS_PG_PORT")
	if port == "" {
		port = "5432"
	}
	user = os.Getenv("JANUS_PG_USER")
	if user == "" {
		user = "silv"
	}
	dbname = os.Getenv("JANUS_PG_DBNAME")
	if dbname == "" {
		dbname = "janus_test"
	}
	return
}

func adminDSN() string {
	host, port, user, dbname := pgConfig()
	return fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable", host, port, user, dbname)
}

func openTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	host, port, user, _ := pgConfig()

	testDB := fmt.Sprintf("janus_pgtest_%d", time.Now().UnixNano())

	adminConn, err := pgx.Connect(ctx, adminDSN())
	require.NoError(t, err, "connect to admin DB")
	_, err = adminConn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", testDB))
	require.NoError(t, err, "create test DB")
	adminConn.Close(ctx)

	dsn := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable", host, port, user, testDB)
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err, "connect to test DB")
	require.NoError(t, pool.Ping(ctx), "ping test DB")

	t.Cleanup(func() {
		pool.Close()
		adminConn, err := pgx.Connect(ctx, adminDSN())
		if err == nil {
			adminConn.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDB))
			adminConn.Close(ctx)
		}
	})

	return pool
}

func runMigration(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	migrations := []string{
		"000001_initial_schema.up.sql",
		"000002_delivery_ref.up.sql",
		"000003_api_keys.up.sql",
		"000004_budget_usage.up.sql",
		"000005_retry_at.up.sql",
		"000006_context_refs.up.sql",
		"000007_outbox.up.sql",
		"000008_outbox_retry.up.sql",
	}
	for _, m := range migrations {
		up, err := os.ReadFile(filepath.Join(repoRoot(), "migrations", m))
		require.NoError(t, err, "read migration %s", m)
		_, err = pool.Exec(ctx, string(up))
		require.NoError(t, err, "run migration %s", m)
	}
	t.Cleanup(func() {
		downFiles := []string{
			"000008_outbox_retry.down.sql",
			"000007_outbox.down.sql",
			"000006_context_refs.down.sql",
			"000005_retry_at.down.sql",
			"000004_budget_usage.down.sql",
			"000003_api_keys.down.sql",
			"000002_delivery_ref.down.sql",
			"000001_initial_schema.down.sql",
		}
		for _, m := range downFiles {
			down, _ := os.ReadFile(filepath.Join(repoRoot(), "migrations", m))
			if len(down) > 0 {
				pool.Exec(ctx, strings.TrimSpace(string(down)))
			}
		}
	})
}
