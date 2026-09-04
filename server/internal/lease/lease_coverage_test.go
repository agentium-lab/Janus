package lease

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

// seedLeaseExpired inserts one tenant/agent/mailbox fixture and a task whose
// single attempt went silent 10 minutes ago. retryPolicy == "" keeps the
// schema default (max_attempts=5); attemptCount >= 5 dead-letters.
func seedLeaseExpired(t *testing.T, pool *pgxpool.Pool, taskID string, attemptCount int, retryPolicy string) {
	t.Helper()
	ctx := context.Background()
	_, err := pool.Exec(ctx, `INSERT INTO tenants (id, name) VALUES ('acme', 'Acme') ON CONFLICT DO NOTHING`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO agents (tenant_id, id, display_name, protocol, endpoint, status)
		VALUES ('acme', 'agent-1', 'Bot', 'a2a', 'http://localhost', 'offline') ON CONFLICT DO NOTHING`)
	require.NoError(t, err)
	if retryPolicy == "" {
		_, err = pool.Exec(ctx, `INSERT INTO mailboxes (tenant_id, id, agent_id, status, ack_wait_seconds, max_deliver)
			VALUES ('acme', 'mb-1', 'agent-1', 'active', 2, 3) ON CONFLICT DO NOTHING`)
	} else {
		_, err = pool.Exec(ctx, `INSERT INTO mailboxes (tenant_id, id, agent_id, status, ack_wait_seconds, max_deliver, retry_policy)
			VALUES ('acme', 'mb-1', 'agent-1', 'active', 2, 3, $1) ON CONFLICT DO NOTHING`, retryPolicy)
	}
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO tasks (tenant_id, id, source_agent, target_type, target_value, mailbox_id, status, priority, envelope, attempt_count)
		VALUES ('acme', $1, 'agent-1', 'mailbox', 'mb-1', 'mb-1', 'claimed', 'normal', '{}'::jsonb, $2)`, taskID, attemptCount)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO task_attempts (tenant_id, task_id, attempt, agent_id, lease_id, status, started_at, heartbeat_at)
		VALUES ('acme', $1, 1, 'agent-1', 'lease-1', 'claimed', now() - interval '10 minutes', now() - interval '10 minutes')`, taskID)
	require.NoError(t, err)
}

func execLease(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	_, err := pool.Exec(context.Background(), sql, args...)
	require.NoError(t, err)
}

const raiseTriggerFn = `CREATE OR REPLACE FUNCTION lease_cov_raise() RETURNS trigger AS $$
	BEGIN
		RAISE EXCEPTION 'lease_cov boom';
	END;
$$ LANGUAGE plpgsql`

const skipUpdateFn = `CREATE OR REPLACE FUNCTION lease_cov_skip() RETURNS trigger AS $$
	BEGIN
		RETURN NULL;
	END;
$$ LANGUAGE plpgsql`

func TestExpireLeases_DeadLetterWritesBothOutboxEvents(t *testing.T) {
	pool := openLeaseTestDB(t)
	seedLeaseExpired(t, pool, "task-dlq", 5, "")

	n, err := NewScanner(pool, time.Hour).ExpireLeases(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	ctx := context.Background()
	var taskStatus, attemptStatus string
	require.NoError(t, pool.QueryRow(ctx, `SELECT status FROM tasks WHERE id='task-dlq'`).Scan(&taskStatus))
	require.NoError(t, pool.QueryRow(ctx, `SELECT status FROM task_attempts WHERE task_id='task-dlq'`).Scan(&attemptStatus))
	assert.Equal(t, "dead_lettered", taskStatus)
	assert.Equal(t, "failed", attemptStatus)

	var dlqPayload, eventPayload []byte
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT payload FROM outbox_events WHERE kind='dlq_publish'`).Scan(&dlqPayload))
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT payload FROM outbox_events WHERE kind='event_publish'`).Scan(&eventPayload))

	var msg core.TaskMessage
	require.NoError(t, json.Unmarshal(dlqPayload, &msg))
	assert.Equal(t, "task-dlq", msg.TaskID)
	assert.Equal(t, "5", msg.Headers["attempt_count"])
	assert.Equal(t, "lease_expired", msg.Headers["reason"])

	var evt core.JanusEvent
	require.NoError(t, json.Unmarshal(eventPayload, &evt))
	assert.Equal(t, core.EventTaskDeadLettered, evt.EventType)
	assert.Equal(t, "task-dlq", evt.TaskID)
}

func TestExpireLeases_RetryUpdateError(t *testing.T) {
	pool := openLeaseTestDB(t)
	seedLeaseExpired(t, pool, "task-re-upd-err", 1, "")
	execLease(t, pool, raiseTriggerFn)
	execLease(t, pool, `CREATE TRIGGER lease_cov_retry_boom BEFORE UPDATE ON tasks
		FOR EACH ROW WHEN (NEW.status = 'retry_scheduled') EXECUTE FUNCTION lease_cov_raise()`)

	n, err := NewScanner(pool, time.Hour).ExpireLeases(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, n)

	var taskStatus, attemptStatus string
	ctx := context.Background()
	require.NoError(t, pool.QueryRow(ctx, `SELECT status FROM tasks WHERE id='task-re-upd-err'`).Scan(&taskStatus))
	require.NoError(t, pool.QueryRow(ctx, `SELECT status FROM task_attempts WHERE task_id='task-re-upd-err'`).Scan(&attemptStatus))
	assert.Equal(t, "claimed", taskStatus, "update failure must roll back the whole transaction")
	assert.Equal(t, "claimed", attemptStatus)
}

func TestExpireLeases_DeadLetterUpdateError(t *testing.T) {
	pool := openLeaseTestDB(t)
	seedLeaseExpired(t, pool, "task-dl-upd-err", 5, "")
	execLease(t, pool, raiseTriggerFn)
	execLease(t, pool, `CREATE TRIGGER lease_cov_dl_boom BEFORE UPDATE ON tasks
		FOR EACH ROW WHEN (NEW.status = 'dead_lettered') EXECUTE FUNCTION lease_cov_raise()`)

	n, err := NewScanner(pool, time.Hour).ExpireLeases(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestExpireLeases_DLQOutboxInsertError(t *testing.T) {
	pool := openLeaseTestDB(t)
	seedLeaseExpired(t, pool, "task-dlq-oerr", 5, "")
	execLease(t, pool, raiseTriggerFn)
	execLease(t, pool, `CREATE TRIGGER lease_cov_dlq_boom BEFORE INSERT ON outbox_events
		FOR EACH ROW WHEN (NEW.kind = 'dlq_publish') EXECUTE FUNCTION lease_cov_raise()`)

	n, err := NewScanner(pool, time.Hour).ExpireLeases(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, n)

	var status string
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT status FROM tasks WHERE id='task-dlq-oerr'`).Scan(&status))
	assert.Equal(t, "claimed", status)
}

func TestExpireLeases_EventOutboxInsertError(t *testing.T) {
	pool := openLeaseTestDB(t)
	seedLeaseExpired(t, pool, "task-evt-oerr", 5, "")
	execLease(t, pool, raiseTriggerFn)
	execLease(t, pool, `CREATE TRIGGER lease_cov_evt_boom BEFORE INSERT ON outbox_events
		FOR EACH ROW WHEN (NEW.kind = 'event_publish') EXECUTE FUNCTION lease_cov_raise()`)

	n, err := NewScanner(pool, time.Hour).ExpireLeases(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestExpireLeases_CommitError(t *testing.T) {
	pool := openLeaseTestDB(t)
	seedLeaseExpired(t, pool, "task-commit-err", 1, "")
	execLease(t, pool, raiseTriggerFn)
	execLease(t, pool, `CREATE CONSTRAINT TRIGGER lease_cov_commit_boom
		AFTER UPDATE ON tasks DEFERRABLE INITIALLY DEFERRED
		FOR EACH ROW EXECUTE FUNCTION lease_cov_raise()`)

	n, err := NewScanner(pool, time.Hour).ExpireLeases(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, n)

	var taskStatus, attemptStatus string
	ctx := context.Background()
	require.NoError(t, pool.QueryRow(ctx, `SELECT status FROM tasks WHERE id='task-commit-err'`).Scan(&taskStatus))
	require.NoError(t, pool.QueryRow(ctx, `SELECT status FROM task_attempts WHERE task_id='task-commit-err'`).Scan(&attemptStatus))
	assert.Equal(t, "claimed", taskStatus, "commit failure must roll back")
	assert.Equal(t, "claimed", attemptStatus)
}

func TestExpireLeases_RowsAffectedZeroSkipsTask(t *testing.T) {
	pool := openLeaseTestDB(t)
	seedLeaseExpired(t, pool, "task-zero", 1, "")
	execLease(t, pool, skipUpdateFn)
	execLease(t, pool, `CREATE TRIGGER lease_cov_skip_attempt BEFORE UPDATE ON task_attempts
		FOR EACH ROW EXECUTE FUNCTION lease_cov_skip()`)

	n, err := NewScanner(pool, time.Hour).ExpireLeases(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, n)

	var status string
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT status FROM tasks WHERE id='task-zero'`).Scan(&status))
	assert.Equal(t, "claimed", status)
}

func TestExpireLeases_MissingMailboxScanError(t *testing.T) {
	pool := openLeaseTestDB(t)
	seedLeaseExpired(t, pool, "task-orphan", 1, "")
	execLease(t, pool, `UPDATE tasks SET mailbox_id = 'ghost-mailbox' WHERE id = 'task-orphan'`)

	n, err := NewScanner(pool, time.Hour).ExpireLeases(context.Background())
	assert.Error(t, err, "LEFT JOIN miss makes m.ack_wait_seconds NULL, which cannot scan into *int")
	assert.Equal(t, 0, n)

	var status string
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT status FROM tasks WHERE id='task-orphan'`).Scan(&status))
	assert.Equal(t, "claimed", status)
}

func TestExpireLeases_RetryAtUsesMailboxPolicyBackoff(t *testing.T) {
	pool := openLeaseTestDB(t)
	seedLeaseExpired(t, pool, "task-backoff", 2,
		`{"max_attempts":10,"backoff_type":"exponential","initial_seconds":60,"max_seconds":900,"jitter":false}`)

	n, err := NewScanner(pool, time.Hour).ExpireLeases(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	var retryAt time.Time
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT retry_at FROM tasks WHERE id='task-backoff'`).Scan(&retryAt))
	delay := time.Until(retryAt)
	assert.Greater(t, delay, 30*time.Second, "retry_at should be scheduled ~120s out (60s * 2^1)")
	assert.Less(t, delay, 4*time.Minute)
}

func TestExpireLeases_RowsErrMidStream(t *testing.T) {
	pool := openLeaseTestDB(t)
	ctx := context.Background()
	execLease(t, pool, `INSERT INTO tenants (id, name) VALUES ('acme', 'Acme') ON CONFLICT DO NOTHING`)
	execLease(t, pool, `INSERT INTO agents (tenant_id, id, display_name, protocol, endpoint, status)
		VALUES ('acme', 'agent-1', 'Bot', 'a2a', 'http://localhost', 'offline') ON CONFLICT DO NOTHING`)

	bigEnvelope := `{"pad":"` + strings.Repeat("x", 16*1024*1024) + `"}`
	for i := 0; i < 16; i++ {
		execLease(t, pool, `INSERT INTO tasks (tenant_id, id, source_agent, target_type, target_value, mailbox_id, status, priority, envelope, attempt_count)
			VALUES ('acme', $1, 'agent-1', 'mailbox', 'mb-1', 'mb-1', 'claimed', 'normal', $2::jsonb, 1)`,
			fmt.Sprintf("task-big-%d", i), bigEnvelope)
		execLease(t, pool, `INSERT INTO task_attempts (tenant_id, task_id, attempt, agent_id, lease_id, status, started_at, heartbeat_at)
			VALUES ('acme', $1, 1, 'agent-1', 'lease-1', 'claimed', now() - interval '10 minutes', now() - interval '10 minutes')`,
			fmt.Sprintf("task-big-%d", i))
	}

	// 16 rows x 16MB cannot finish streaming before the deadline, so
	// rows.Err() fires mid-iteration and ExpireLeases surfaces the error.
	scanCtx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	_, err := NewScanner(pool, time.Hour).ExpireLeases(scanCtx)
	assert.Error(t, err)

	var remaining int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM tasks WHERE id LIKE 'task-big-%' AND status = 'claimed'`).Scan(&remaining))
	assert.Equal(t, 16, remaining, "no task may transition when row streaming fails")
}

func TestBackoff_EdgeCases(t *testing.T) {
	assert.Equal(t, 10*time.Second, backoff(-1), "negative attempt behaves like 0")
	assert.Equal(t, 10*time.Second, backoff(0))
	assert.Equal(t, 160*time.Second, backoff(4))
	assert.Equal(t, 320*time.Second, backoff(5))
	assert.Equal(t, 640*time.Second, backoff(6))
	assert.Equal(t, 15*time.Minute, backoff(7), "1280s caps at 15m")
	assert.Equal(t, 15*time.Minute, backoff(1000))
}

func TestLeaseGenerateULID_UniqueAndShaped(t *testing.T) {
	ids := make(map[string]bool, 200)
	for i := 0; i < 200; i++ {
		id := generateULID()
		assert.Len(t, id, 32, "10-byte timestamp + 6-byte entropy in hex")
		ids[id] = true
	}
	assert.Len(t, ids, 200, "ULIDs generated in the same millisecond must not collide")
}
