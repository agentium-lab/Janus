package expiry

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

	"github.com/agentium-lab/Janus/server/internal/driver/postgres"
)

// openExpiryTestDB provisions an ephemeral PostgreSQL database, applies all
// migrations, returns a pool, and drops the DB on cleanup. If PG is not
// reachable the test is SKIPPED (not failed) so the suite stays usable in
// environments without a database; set JANUS_PG_HOST/PORT/USER to enable.
func openExpiryTestDB(t *testing.T) *pgxpool.Pool {
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
	testDB := fmt.Sprintf("janus_expirytest_%d", time.Now().UnixNano())

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

	migrationsDir := findExpiryMigrationsDir()
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

func findExpiryMigrationsDir() string {
	for _, d := range []string{"../../../migrations", "../../../../migrations"} {
		if _, err := os.Stat(d); err == nil {
			return d
		}
	}
	return "../../../migrations"
}

func seedTask(t *testing.T, pool *pgxpool.Pool, taskID, status string, deadlineOffsetSecs int, ttlSeconds int, createdOffsetSecs int) {
	t.Helper()
	ctx := context.Background()

	_, err := pool.Exec(ctx, `INSERT INTO tenants (id, name, created_at, updated_at) VALUES ('acme', 'Acme', now(), now())`)
	require.NoError(t, err)

	var deadlineExpr string
	switch {
	case deadlineOffsetSecs > 0:
		deadlineExpr = fmt.Sprintf("now() + interval '%d seconds'", deadlineOffsetSecs)
	case deadlineOffsetSecs < 0:
		deadlineExpr = fmt.Sprintf("now() - interval '%d seconds'", -deadlineOffsetSecs)
	default:
		deadlineExpr = "NULL"
	}

	var ttlExpr string
	if ttlSeconds > 0 {
		ttlExpr = fmt.Sprintf("%d", ttlSeconds)
	} else {
		ttlExpr = "NULL"
	}

	createdExpr := "now()"
	if createdOffsetSecs > 0 {
		createdExpr = fmt.Sprintf("now() - interval '%d seconds'", createdOffsetSecs)
	}

	stmt := fmt.Sprintf(`
		INSERT INTO tasks (tenant_id, id, source_agent, target_type, target_value,
		                   mailbox_id, status, priority, deadline, ttl_seconds,
		                   envelope, attempt_count, created_at, updated_at)
		VALUES ('acme', $1, 'agent-1', 'agent', 'agent-1', NULL, $2, 'normal',
		        %s, %s, '{}'::jsonb, 0, %s, now())`,
		deadlineExpr, ttlExpr, createdExpr)

	_, err = pool.Exec(ctx, stmt, taskID, status)
	require.NoError(t, err, "seed task %s", taskID)
}

func taskStatus(t *testing.T, pool *pgxpool.Pool, taskID string) string {
	t.Helper()
	ctx := context.Background()
	var status string
	err := pool.QueryRow(ctx, `SELECT status FROM tasks WHERE tenant_id='acme' AND id=$1`, taskID).Scan(&status)
	require.NoError(t, err, "re-read task %s", taskID)
	return status
}

func TestScanner_ExpiresTasksWithPastDeadline(t *testing.T) {
	pool := openExpiryTestDB(t)
	seedTask(t, pool, "task-deadline", "pending", -3600, 0, 0)

	repo := postgres.NewTaskRepository(pool)
	ctx := context.Background()

	n, err := repo.ExpireTasks(ctx)
	require.NoError(t, err)
	assert.Greater(t, n, int64(0), "ExpireTasks should report >0 affected rows")

	assert.Equal(t, "expired", taskStatus(t, pool, "task-deadline"))
}

func TestScanner_ExpiresTasksWithPastTTL(t *testing.T) {
	pool := openExpiryTestDB(t)
	// ttl=60s, created_at = 1 hour ago -> clearly past, deterministic.
	seedTask(t, pool, "task-ttl", "pending", 0, 60, 3600)

	repo := postgres.NewTaskRepository(pool)
	ctx := context.Background()

	n, err := repo.ExpireTasks(ctx)
	require.NoError(t, err)
	assert.Greater(t, n, int64(0), "ExpireTasks should report >0 affected rows for past TTL")

	assert.Equal(t, "expired", taskStatus(t, pool, "task-ttl"))
}

func TestScanner_NoExpiryWhenFutureDeadline(t *testing.T) {
	pool := openExpiryTestDB(t)
	seedTask(t, pool, "task-future", "pending", 3600, 0, 0)

	repo := postgres.NewTaskRepository(pool)
	ctx := context.Background()

	n, err := repo.ExpireTasks(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n, "no rows should be expired")

	assert.Equal(t, "pending", taskStatus(t, pool, "task-future"))
}

func TestScanner_SkipsTerminalStatuses(t *testing.T) {
	pool := openExpiryTestDB(t)
	seedTask(t, pool, "task-done", "completed", -3600, 0, 0)

	repo := postgres.NewTaskRepository(pool)
	ctx := context.Background()

	n, err := repo.ExpireTasks(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n, "terminal-status tasks must be excluded")

	assert.Equal(t, "completed", taskStatus(t, pool, "task-done"))
}

func TestScanner_LoopExpiresTask(t *testing.T) {
	pool := openExpiryTestDB(t)
	seedTask(t, pool, "task-loop", "pending", -3600, 0, 0)

	repo := postgres.NewTaskRepository(pool)
	scanner := NewScanner(repo, 30*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		scanner.Start(ctx)
		close(done)
	}()

	// Poll up to ~2s; the scanner ticks every 30ms so a single tick suffices.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if taskStatus(t, pool, "task-loop") == "expired" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	<-done

	assert.Equal(t, "expired", taskStatus(t, pool, "task-loop"))
}
