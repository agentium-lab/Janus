package postgres

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
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

	// Skip the whole package when PostgreSQL is not reachable, so local runs and
	// CI without the postgres service do not fail. Set JANUS_PG_HOST / etc. to
	// point at a running instance.
	adminConn, err := pgx.Connect(ctx, adminDSN())
	if err != nil {
		t.Skipf("postgres not reachable (set JANUS_PG_HOST/PORT/USER/DBNAME to enable): %v", err)
	}
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

	// Discover all migrations by listing the migrations/ directory rather than
	// hard-coding filenames. This keeps the test harness in sync with the
	// migration set without manual edits whenever a new migration is added.
	migrationsDir := filepath.Join(repoRoot(), "migrations")
	entries, err := os.ReadDir(migrationsDir)
	require.NoError(t, err, "read migrations dir")

	var upFiles []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".up.sql") {
			upFiles = append(upFiles, e.Name())
		}
	}
	sort.Strings(upFiles)

	for _, m := range upFiles {
		up, err := os.ReadFile(filepath.Join(migrationsDir, m))
		require.NoError(t, err, "read migration %s", m)
		_, err = pool.Exec(ctx, string(up))
		require.NoError(t, err, "run migration %s", m)
	}

	t.Cleanup(func() {
		// Run down migrations in reverse order. Missing down files are skipped.
		var downFiles []string
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".down.sql") {
				downFiles = append(downFiles, e.Name())
			}
		}
		sort.Sort(sort.Reverse(sort.StringSlice(downFiles)))
		for _, m := range downFiles {
			down, err := os.ReadFile(filepath.Join(migrationsDir, m))
			if err == nil && len(down) > 0 {
				pool.Exec(ctx, strings.TrimSpace(string(down)))
			}
		}
	})
}
