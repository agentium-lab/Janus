package retry

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func execRetry(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	_, err := pool.Exec(context.Background(), sql, args...)
	require.NoError(t, err)
}

const retryRaiseFn = `CREATE OR REPLACE FUNCTION retry_cov_raise() RETURNS trigger AS $$
	BEGIN
		RAISE EXCEPTION 'retry_cov boom';
	END;
$$ LANGUAGE plpgsql`

const retrySkipFn = `CREATE OR REPLACE FUNCTION retry_cov_skip() RETURNS trigger AS $$
	BEGIN
		RETURN NULL;
	END;
$$ LANGUAGE plpgsql`

// openRetryLeakyTestDB opens a throwaway retry DB whose pool IS closed
// normally at cleanup: the zero-rows path used to abandon its transaction
// and hold the connection forever (pool.Close would block), which the
// defer-Rollback fix on the race-loser branch eliminated.
func openRetryLeakyTestDB(t *testing.T) *pgxpool.Pool {
	pool := openRetryTestDB(t)
	t.Cleanup(pool.Close)
	return pool
}

func TestProcessReadyRetries_ZeroRowsAffectedStillCommits(t *testing.T) {
	pool := openRetryLeakyTestDB(t)
	seedRetryTask(t, pool, "task-zero-rows", true)
	execRetry(t, pool, retrySkipFn)
	execRetry(t, pool, `CREATE TRIGGER retry_cov_skip_update BEFORE UPDATE ON tasks
		FOR EACH ROW EXECUTE FUNCTION retry_cov_skip()`)
	drv := &fakeQueueDriver{}
	sched := NewScheduler(pool, drv)
	sched.useOutbox = true

	sched.processReadyRetries(context.Background())

	ctx := context.Background()
	var status string
	require.NoError(t, pool.QueryRow(ctx, `SELECT status FROM tasks WHERE id = 'task-zero-rows'`).Scan(&status))
	assert.Equal(t, "retry_scheduled", status, "skipped UPDATE means 0 rows affected; task must stay retry_scheduled")

	var outboxCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM outbox_events WHERE kind = 'task_publish'`).Scan(&outboxCount))
	assert.Equal(t, 0, outboxCount)
	assert.Empty(t, drv.publishedTasks)
}

func TestProcessReadyRetries_OutboxInsertErrorRollsBack(t *testing.T) {
	pool := openRetryTestDB(t)
	seedRetryTask(t, pool, "task-outbox-err", true)
	execRetry(t, pool, retryRaiseFn)
	execRetry(t, pool, `CREATE TRIGGER retry_cov_outbox_boom BEFORE INSERT ON outbox_events
		FOR EACH ROW WHEN (NEW.kind = 'task_publish') EXECUTE FUNCTION retry_cov_raise()`)

	sched := NewScheduler(pool, &fakeQueueDriver{})
	sched.useOutbox = true
	sched.processReadyRetries(context.Background())

	var status string
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT status FROM tasks WHERE id = 'task-outbox-err'`).Scan(&status))
	assert.Equal(t, "retry_scheduled", status, "outbox failure must roll back the promotion")
}

func TestProcessReadyRetries_CommitErrorRollsBack(t *testing.T) {
	pool := openRetryTestDB(t)
	seedRetryTask(t, pool, "task-commit-err", true)
	execRetry(t, pool, retryRaiseFn)
	execRetry(t, pool, `CREATE CONSTRAINT TRIGGER retry_cov_commit_boom
		AFTER UPDATE ON tasks DEFERRABLE INITIALLY DEFERRED
		FOR EACH ROW EXECUTE FUNCTION retry_cov_raise()`)

	sched := NewScheduler(pool, nil)
	sched.useOutbox = true
	sched.processReadyRetries(context.Background())

	var status string
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT status FROM tasks WHERE id = 'task-commit-err'`).Scan(&status))
	assert.Equal(t, "retry_scheduled", status, "commit failure must roll back the promotion")
}

func TestProcessReadyRetries_EmptyMailboxIDSkipsPublish(t *testing.T) {
	pool := openRetryTestDB(t)
	ctx := context.Background()
	execRetry(t, pool, `INSERT INTO tenants (id, name) VALUES ('acme', 'Acme') ON CONFLICT DO NOTHING`)
	execRetry(t, pool, `INSERT INTO agents (id, tenant_id, display_name, protocol, status)
		VALUES ('agent-1', 'acme', 'A1', 'a2a', 'offline') ON CONFLICT DO NOTHING`)

	env := `{"task_id":"task-no-mailbox","tenant_id":"acme"}`
	// mailbox_id must be '' rather than NULL: a NULL mailbox_id cannot be
	// scanned into *string and kills the whole sweep (see report).
	execRetry(t, pool, `INSERT INTO tasks (id, tenant_id, source_agent, target_type, target_value, mailbox_id,
		  status, priority, envelope, retry_at, attempt_count)
		VALUES ('task-no-mailbox', 'acme', 'agent-1', 'mailbox', 'mb-1', '',
		  'retry_scheduled', 'normal', $1::jsonb, now() - interval '1 minute', 1)`, env)

	drv := &fakeQueueDriver{}
	sched := NewScheduler(pool, drv)
	sched.useOutbox = true
	sched.processReadyRetries(ctx)

	var status string
	require.NoError(t, pool.QueryRow(ctx, `SELECT status FROM tasks WHERE id = 'task-no-mailbox'`).Scan(&status))
	assert.Equal(t, "queued", status, "task is still promoted when mailbox_id is empty")

	var outboxCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM outbox_events WHERE kind = 'task_publish'`).Scan(&outboxCount))
	assert.Equal(t, 0, outboxCount)
	assert.Empty(t, drv.publishedTasks, "no re-publish may happen without a mailbox")
}

func TestProcessReadyRetries_ScanErrorNullAttemptCount(t *testing.T) {
	pool := openRetryTestDB(t)
	seedRetryTask(t, pool, "task-scan-null", true)
	execRetry(t, pool, `ALTER TABLE tasks ALTER COLUMN attempt_count DROP NOT NULL`)
	execRetry(t, pool, `UPDATE tasks SET attempt_count = NULL WHERE id = 'task-scan-null'`)

	sched := NewScheduler(pool, &fakeQueueDriver{})
	sched.processReadyRetries(context.Background())

	var status string
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT status FROM tasks WHERE id = 'task-scan-null'`).Scan(&status))
	assert.Equal(t, "retry_scheduled", status, "NULL attempt_count must abort the scan before any promotion")
}

func TestProcessReadyRetries_RowsErrMidStream(t *testing.T) {
	pool := openRetryTestDB(t)
	ctx := context.Background()
	execRetry(t, pool, `INSERT INTO tenants (id, name) VALUES ('acme', 'Acme') ON CONFLICT DO NOTHING`)
	execRetry(t, pool, `INSERT INTO agents (id, tenant_id, display_name, protocol, status)
		VALUES ('agent-1', 'acme', 'A1', 'a2a', 'offline') ON CONFLICT DO NOTHING`)

	bigEnvelope := `{"pad":"` + strings.Repeat("x", 16*1024*1024) + `"}`
	for i := 0; i < 16; i++ {
		execRetry(t, pool, `INSERT INTO tasks (id, tenant_id, source_agent, target_type, target_value, mailbox_id,
			  status, priority, envelope, retry_at, attempt_count)
			VALUES ($1, 'acme', 'agent-1', 'mailbox', 'mb-1', 'mb-1',
			  'retry_scheduled', 'normal', $2::jsonb, now() - interval '1 minute', 1)`,
			fmt.Sprintf("task-big-%d", i), bigEnvelope)
	}

	// 16 rows x 16MB cannot finish streaming before the deadline, so
	// rows.Err() fires mid-iteration and no task is processed.
	scanCtx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	sched := NewScheduler(pool, nil)
	sched.processReadyRetries(scanCtx)

	var remaining int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM tasks WHERE id LIKE 'task-big-%' AND status = 'retry_scheduled'`).Scan(&remaining))
	assert.Equal(t, 16, remaining)
}

func TestScheduler_StartTickerProcessesReadyTask(t *testing.T) {
	pool := openRetryTestDB(t)
	seedRetryTask(t, pool, "task-ticker", true)
	drv := &fakeQueueDriver{}
	sched := NewScheduler(pool, drv)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		sched.Start(ctx, 10*time.Millisecond)
		close(done)
	}()

	deadline := time.Now().Add(3 * time.Second)
	promoted := false
	for time.Now().Before(deadline) {
		var status string
		require.NoError(t, pool.QueryRow(context.Background(),
			`SELECT status FROM tasks WHERE id = 'task-ticker'`).Scan(&status))
		if status == "queued" {
			promoted = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not exit after context cancel")
	}
	assert.True(t, promoted, "ticker-driven loop should promote the ready task")
}
