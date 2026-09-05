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

func openLeaseBadTestDB(t *testing.T) *pgxpool.Pool {
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
		user = "silv"
	}
	testDB := fmt.Sprintf("janus_leasetest_%d", time.Now().UnixNano())
	ctx := context.Background()
	adminDSN := fmt.Sprintf("host=%s port=%s user=%s dbname=janus_test sslmode=disable", host, port, user)
	adminConn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Skipf("postgres not reachable: %v", err)
	}
	_, err = adminConn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", testDB))
	require.NoError(t, err)
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

func TestExpireLeases_QueryError(t *testing.T) {
	pool := openLeaseBadTestDB(t)
	require.NotNil(t, pool)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	scanner := NewScanner(pool, time.Hour)
	_, err := scanner.ExpireLeases(ctx)
	assert.Error(t, err)
}

func TestExpireLeases_RowsErr(t *testing.T) {
	pool := openLeaseBadTestDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := pool.Exec(ctx, `INSERT INTO tenants (id, name, created_at, updated_at) VALUES ('acme', 'Acme', now(), now())`)
	assert.Error(t, err)

	scanner := NewScanner(pool, time.Hour)
	_, err = scanner.ExpireLeases(ctx)
	assert.Error(t, err)
}

func TestExpireLeases_DeadLetteredNoMailbox(t *testing.T) {
	pool := openLeaseBadTestDB(t)
	ctx := context.Background()

	_, err := pool.Exec(ctx, `INSERT INTO tenants (id, name, created_at, updated_at) VALUES ('acme', 'Acme', now(), now())`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO agents (tenant_id, id, display_name, protocol, endpoint, status, max_concurrency, created_at, updated_at)
		VALUES ('acme', 'agent-1', 'Bot', 'a2a', 'http://localhost', 'active', 1, now(), now())`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO mailboxes (tenant_id, id, agent_id, status, priority, max_concurrency, ack_wait_seconds, max_deliver, retention_seconds, created_at, updated_at)
		VALUES ('acme', 'mb-1', 'agent-1', 'active', 'normal', 1, 2, 2, 3600, now(), now())`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO tasks (tenant_id, id, source_agent, target_type, target_value, mailbox_id, status, priority, envelope, attempt_count, created_at, updated_at)
		VALUES ('acme', 'task-1', 'agent-1', 'agent', 'agent-1', 'mb-1', 'claimed', 'normal', '{}'::jsonb, 3, now(), now())`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO task_attempts (tenant_id, task_id, attempt, agent_id, lease_id, status, started_at, heartbeat_at)
		VALUES ('acme', 'task-1', 1, 'agent-1', 'lease-1', 'claimed', now() - interval '10 minutes', now() - interval '10 minutes')`)
	require.NoError(t, err)

	scanner := NewScanner(pool, time.Hour)
	n, err := scanner.ExpireLeases(ctx)
	assert.Equal(t, 1, n)
	assert.NoError(t, err)
}

func TestScanner_StartShortInterval_TriggersScan(t *testing.T) {
	pool := openLeaseBadTestDB(t)
	s := NewScanner(pool, 10*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		s.Start(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not exit after context timeout")
	}
}

func TestExpireLeases_ConcurrentAckZeroRows(t *testing.T) {
	pool := openLeaseBadTestDB(t)
	require.NotNil(t, pool)
	ctx := context.Background()

	_, err := pool.Exec(ctx, `INSERT INTO tenants (id, name, created_at, updated_at) VALUES ('acme', 'Acme', now(), now())`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO agents (tenant_id, id, display_name, protocol, endpoint, status, max_concurrency, created_at, updated_at)
		VALUES ('acme', 'agent-1', 'Bot', 'a2a', 'http://localhost', 'active', 1, now(), now())`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO mailboxes (tenant_id, id, agent_id, status, priority, max_concurrency, ack_wait_seconds, max_deliver, retention_seconds, created_at, updated_at)
		VALUES ('acme', 'mb-1', 'agent-1', 'active', 'normal', 1, 2, 5, 3600, now(), now())`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO tasks (tenant_id, id, source_agent, target_type, target_value, mailbox_id, status, priority, envelope, attempt_count, created_at, updated_at)
		VALUES ('acme', 'task-1', 'agent-1', 'agent', 'agent-1', 'mb-1', 'claimed', 'normal', '{}'::jsonb, 1, now(), now())`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO task_attempts (tenant_id, task_id, attempt, agent_id, lease_id, status, started_at, heartbeat_at)
		VALUES ('acme', 'task-1', 1, 'agent-1', 'lease-1', 'succeeded', now() - interval '10 minutes', now() - interval '10 minutes')`)
	require.NoError(t, err)

	scanner := NewScanner(pool, time.Hour)
	n, err := scanner.ExpireLeases(ctx)
	assert.Equal(t, 0, n)
	assert.NoError(t, err)
}

func TestScan_NilPoolErrorPath(t *testing.T) {
	s := &Scanner{pool: nil, stopCh: make(chan struct{})}
	s.scan(context.Background())
}

func TestScanner_ScanLogsError(t *testing.T) {
	pool := openLeaseBadTestDB(t)
	require.NotNil(t, pool)

	scanner := NewScanner(pool, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	scanner.scan(ctx)
}

func TestScanner_ScanWithExpiredLease(t *testing.T) {
	pool := openLeaseBadTestDB(t)
	require.NotNil(t, pool)
	ctx := context.Background()

	_, err := pool.Exec(ctx, `INSERT INTO tenants (id, name, created_at, updated_at) VALUES ('acme', 'Acme', now(), now())`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO agents (tenant_id, id, display_name, protocol, endpoint, status, max_concurrency, created_at, updated_at)
		VALUES ('acme', 'agent-1', 'Bot', 'a2a', 'http://localhost', 'active', 1, now(), now())`)
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

	scanner := NewScanner(pool, time.Hour)
	scanner.scan(ctx)
}

func TestExpireLeases_ScanRowError(t *testing.T) {
	pool := openLeaseBadTestDB(t)
	ctx := context.Background()

	_, err := pool.Exec(ctx, `INSERT INTO tenants (id, name, created_at, updated_at) VALUES ('acme', 'Acme', now(), now())`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO agents (tenant_id, id, display_name, protocol, endpoint, status, max_concurrency, created_at, updated_at)
		VALUES ('acme', 'agent-1', 'Bot', 'a2a', 'http://localhost', 'active', 1, now(), now())`)
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

	_, err = pool.Exec(ctx, `ALTER TABLE task_attempts DROP COLUMN heartbeat_at`)
	require.NoError(t, err)

	scanner := NewScanner(pool, time.Hour)
	_, err = scanner.ExpireLeases(ctx)
	assert.Error(t, err)
}

func TestExpireLeases_RowsErrAfterIteration(t *testing.T) {
	pool := openLeaseBadTestDB(t)
	ctx := context.Background()

	_, err := pool.Exec(ctx, `INSERT INTO tenants (id, name, created_at, updated_at) VALUES ('acme', 'Acme', now(), now())`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO agents (tenant_id, id, display_name, protocol, endpoint, status, max_concurrency, created_at, updated_at)
		VALUES ('acme', 'agent-1', 'Bot', 'a2a', 'http://localhost', 'active', 1, now(), now())`)
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

	ctxCancel, cancel := context.WithCancel(context.Background())
	cancel()

	scanner := NewScanner(pool, time.Hour)
	_, err = scanner.ExpireLeases(ctxCancel)
	assert.Error(t, err)
}

func TestExpireLeases_MarkAttemptFailedError(t *testing.T) {
	pool := openLeaseBadTestDB(t)
	require.NotNil(t, pool)
	ctx := context.Background()

	_, err := pool.Exec(ctx, `INSERT INTO tenants (id, name, created_at, updated_at) VALUES ('acme', 'Acme', now(), now())`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO agents (tenant_id, id, display_name, protocol, endpoint, status, max_concurrency, created_at, updated_at)
		VALUES ('acme', 'agent-1', 'Bot', 'a2a', 'http://localhost', 'active', 1, now(), now())`)
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

	_, err = pool.Exec(ctx, `ALTER TABLE task_attempts DROP COLUMN finished_at`)
	require.NoError(t, err)

	scanner := NewScanner(pool, time.Hour)
	n, err := scanner.ExpireLeases(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 0, n)
}
