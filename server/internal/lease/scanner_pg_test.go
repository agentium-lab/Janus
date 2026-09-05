package lease

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openLeaseTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
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
		user = "janus"
	}
	testDB := fmt.Sprintf("janus_leasetest_%d", time.Now().UnixNano())

	ctx := context.Background()
	adminDSN := fmt.Sprintf("host=%s port=%s user=%s dbname=janus_test sslmode=disable", host, port, user)
	adminConn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Skipf("postgres not reachable (set JANUS_PG_HOST/PORT/USER to enable): %v", err)
	}
	_, err = adminConn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", testDB))
	require.NoError(t, err, "create test DB")
	adminConn.Close(ctx)

	dsn := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable", host, port, user, testDB)
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	require.NoError(t, pool.Ping(ctx))

	migrationsDir := findLeaseMigrationsDir()
	entries, _ := os.ReadDir(migrationsDir)
	for _, e := range entries {
		if !e.IsDir() && len(e.Name()) > 7 && e.Name()[len(e.Name())-7:] == ".up.sql" {
			up, _ := os.ReadFile(migrationsDir + "/" + e.Name())
			_, err := pool.Exec(ctx, string(up))
			require.NoError(t, err, "migration %s", e.Name())
		}
	}

	t.Cleanup(func() {
		pool.Close()
		ctx := context.Background()
		adminConn, err := pgx.Connect(ctx, adminDSN)
		if err == nil {
			adminConn.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDB))
			adminConn.Close(ctx)
		}
	})

	return pool
}

func findLeaseMigrationsDir() string {
	for _, d := range []string{"../../../migrations", "../../../../migrations"} {
		if _, err := os.Stat(d); err == nil {
			return d
		}
	}
	return "../../../migrations"
}

func seedLeaseTestData(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()

	_, err := pool.Exec(ctx, `INSERT INTO tenants (id, name, created_at, updated_at) VALUES ('acme', 'Acme', now(), now())`)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `INSERT INTO agents (tenant_id, id, display_name, protocol, endpoint, status, max_concurrency, created_at, updated_at)
		VALUES ('acme', 'agent-1', 'Review Bot', 'a2a', 'http://localhost', 'active', 1, now(), now())`)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `INSERT INTO mailboxes (tenant_id, id, agent_id, status, priority, max_concurrency, ack_wait_seconds, max_deliver, retention_seconds, created_at, updated_at)
		VALUES ('acme', 'mb-1', 'agent-1', 'active', 'normal', 1, 2, 3, 3600, now(), now())`)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `INSERT INTO tasks (tenant_id, id, source_agent, target_type, target_value, mailbox_id, status, priority, envelope, attempt_count, created_at, updated_at)
		VALUES ('acme', 'task-1', 'agent-1', 'agent', 'agent-1', 'mb-1', 'claimed', 'normal', '{}'::jsonb, 1, now(), now())`)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `INSERT INTO task_attempts (tenant_id, task_id, attempt, agent_id, lease_id, status, started_at, heartbeat_at)
		VALUES ('acme', 'task-1', 1, 'agent-1', 'lease-1', 'claimed', now() - interval '10 minutes', now() - interval '10 minutes')`)
	require.NoError(t, err)
}

func TestExpireLeases_TransitionsToRetry(t *testing.T) {
	pool := openLeaseTestDB(t)
	seedLeaseTestData(t, pool)

	ctx := context.Background()
	scanner := NewScanner(pool, time.Hour)
	n, err := scanner.ExpireLeases(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	var taskStatus string
	err = pool.QueryRow(ctx, `SELECT status FROM tasks WHERE tenant_id='acme' AND id='task-1'`).Scan(&taskStatus)
	require.NoError(t, err)
	assert.Equal(t, "retry_scheduled", taskStatus)

	var attemptStatus string
	err = pool.QueryRow(ctx, `SELECT status FROM task_attempts WHERE tenant_id='acme' AND task_id='task-1' AND attempt=1`).Scan(&attemptStatus)
	require.NoError(t, err)
	assert.Equal(t, "failed", attemptStatus)
}

func TestExpireLeases_DeadLetterWhenMaxExceeded(t *testing.T) {
	pool := openLeaseTestDB(t)
	seedLeaseTestData(t, pool)

	ctx := context.Background()
	// Default mailbox retry_policy is max_attempts=5, so attempt_count=5
	// (5 >= 5) exceeds the policy and must dead-letter the task.
	_, err := pool.Exec(ctx, `UPDATE tasks SET attempt_count = 5 WHERE tenant_id='acme' AND id='task-1'`)
	require.NoError(t, err)

	scanner := NewScanner(pool, time.Hour)
	n, err := scanner.ExpireLeases(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	var taskStatus string
	err = pool.QueryRow(ctx, `SELECT status FROM tasks WHERE tenant_id='acme' AND id='task-1'`).Scan(&taskStatus)
	require.NoError(t, err)
	assert.Equal(t, "dead_lettered", taskStatus)
}

func TestExpireLeases_NoExpiredLeases(t *testing.T) {
	pool := openLeaseTestDB(t)
	seedLeaseTestData(t, pool)

	ctx := context.Background()
	_, err := pool.Exec(ctx, `UPDATE task_attempts SET heartbeat_at = now() WHERE tenant_id='acme' AND task_id='task-1'`)
	require.NoError(t, err)

	scanner := NewScanner(pool, time.Hour)
	n, err := scanner.ExpireLeases(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestExpireLeases_ConcurrentACK_NoDoubleExpire(t *testing.T) {
	pool := openLeaseTestDB(t)
	seedLeaseTestData(t, pool)

	ctx := context.Background()
	_, err := pool.Exec(ctx, `UPDATE task_attempts SET status = 'succeeded', finished_at = now() WHERE tenant_id='acme' AND task_id='task-1'`)
	require.NoError(t, err)

	scanner := NewScanner(pool, time.Hour)
	n, err := scanner.ExpireLeases(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}
