package retry

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

func openRetryTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	host := os.Getenv("JANUS_PG_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("JANUS_PG_PORT")
	if port == "" {
		port = "5432"
	}
	user := os.Getenv("JANUS_PG_USER")
	if user == "" {
		user = "janus"
	}
	testDB := fmt.Sprintf("janus_retrytest_%d", time.Now().UnixNano())

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

	// Run migrations.
	migrationsDir := findRetryMigrationsDir()
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
		c, err := pgx.Connect(ctx, adminDSN)
		if err == nil {
			c.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDB))
			c.Close(ctx)
		}
	})
	return pool
}

func findRetryMigrationsDir() string {
	for _, d := range []string{"../../../migrations", "../../../../migrations"} {
		if _, err := os.Stat(d); err == nil {
			return d
		}
	}
	return "../../../migrations"
}

// seedRetryTask inserts a tenant, mailbox, agent, and a retry_scheduled task.
func seedRetryTask(t *testing.T, pool *pgxpool.Pool, taskID string, retryInPast bool) {
	t.Helper()
	ctx := context.Background()
	pool.Exec(ctx, "INSERT INTO tenants (id, name) VALUES ($1, $2) ON CONFLICT DO NOTHING", "acme", "Acme")
	pool.Exec(ctx, `INSERT INTO agents (id, tenant_id, display_name, protocol, status)
		VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING`, "agent-1", "acme", "A1", "a2a", "offline")
	pool.Exec(ctx, `INSERT INTO mailboxes (id, tenant_id, agent_id, status)
		VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`, "mb-1", "acme", "agent-1", "active")

	envelope := core.TaskEnvelope{
		TaskID: taskID, TenantID: "acme", SourceAgent: "agent-1",
		Target: core.Target{Type: "mailbox", Value: "mb-1"},
	}
	envJSON, _ := json.Marshal(envelope)

	retryAt := time.Now().Add(1 * time.Hour) // future by default
	if retryInPast {
		retryAt = time.Now().Add(-1 * time.Minute) // past → ready for retry
	}
	_, err := pool.Exec(ctx,
		`INSERT INTO tasks (id, tenant_id, source_agent, target_type, target_value, mailbox_id,
		  status, priority, envelope, retry_at, attempt_count)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 1)`,
		taskID, "acme", "agent-1", "mailbox", "mb-1", "mb-1",
		"retry_scheduled", "normal", envJSON, retryAt)
	require.NoError(t, err)
}

func TestProcessReadyRetries_PromotesReadyTask(t *testing.T) {
	pool := openRetryTestDB(t)
	sched := NewScheduler(pool, &fakeQueueDriver{})
	sched.useOutbox = true
	ctx := context.Background()

	seedRetryTask(t, pool, "task-retry-1", true) // retry_at in past → ready

	sched.processReadyRetries(ctx)

	var status string
	pool.QueryRow(ctx, "SELECT status FROM tasks WHERE id = $1", "task-retry-1").Scan(&status)
	assert.Equal(t, "queued", status, "ready task should be promoted to queued")

	// Verify outbox has a task_publish entry for the retry.
	var count int
	pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE kind = 'task_publish'").Scan(&count)
	assert.GreaterOrEqual(t, count, 1, "outbox should have a task_publish entry")
}

func TestProcessReadyRetries_SkipsFutureRetry(t *testing.T) {
	pool := openRetryTestDB(t)
	sched := NewScheduler(pool, &fakeQueueDriver{})
	sched.useOutbox = true
	ctx := context.Background()

	seedRetryTask(t, pool, "task-retry-future", false) // retry_at in future → not ready

	sched.processReadyRetries(ctx)

	var status string
	pool.QueryRow(ctx, "SELECT status FROM tasks WHERE id = $1", "task-retry-future").Scan(&status)
	assert.Equal(t, "retry_scheduled", status, "future retry should not be promoted")
}

func TestProcessReadyRetries_WithDirectQueue(t *testing.T) {
	pool := openRetryTestDB(t)
	drv := &fakeQueueDriver{}
	sched := NewScheduler(pool, drv)
	sched.useOutbox = false
	ctx := context.Background()

	seedRetryTask(t, pool, "task-retry-direct", true)

	sched.processReadyRetries(ctx)

	var status string
	pool.QueryRow(ctx, "SELECT status FROM tasks WHERE id = $1", "task-retry-direct").Scan(&status)
	assert.Equal(t, "queued", status)
	assert.Len(t, drv.publishedTasks, 1, "task should be published to queue directly")
	assert.Equal(t, "task-retry-direct", drv.publishedTasks[0].TaskID)
}

func TestProcessReadyRetries_QueryError(t *testing.T) {
	pool := openRetryTestDB(t)
	pool.Close()

	sched := NewScheduler(pool, nil)
	ctx := context.Background()

	sched.processReadyRetries(ctx)
}

func TestProcessReadyRetries_ScanError(t *testing.T) {
	pool := openRetryTestDB(t)
	ctx := context.Background()

	pool.Exec(ctx, "INSERT INTO tenants (id, name) VALUES ($1, $2) ON CONFLICT DO NOTHING", "acme", "Acme")
	pool.Exec(ctx, `INSERT INTO agents (id, tenant_id, display_name, protocol, status)
		VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING`, "agent-1", "acme", "A1", "a2a", "offline")
	pool.Exec(ctx, `INSERT INTO mailboxes (id, tenant_id, agent_id, status)
		VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`, "mb-1", "acme", "agent-1", "active")

	envelope := []byte(`{}`)

	_, err := pool.Exec(ctx,
		`INSERT INTO tasks (id, tenant_id, source_agent, target_type, target_value, mailbox_id,
		  status, priority, envelope, retry_at, attempt_count)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 1)`,
		"task-scan-error", "acme", "agent-1", "mailbox", "mb-1", "mb-1",
		"retry_scheduled", "invalid_priority_not_an_enum", envelope, time.Now().Add(-1*time.Minute))
	require.NoError(t, err)

	sched := NewScheduler(pool, &fakeQueueDriver{})
	sched.processReadyRetries(ctx)
}

func TestProcessReadyRetries_UpdateError(t *testing.T) {
	pool := openRetryTestDB(t)
	ctx := context.Background()

	pool.Exec(ctx, "INSERT INTO tenants (id, name) VALUES ($1, $2) ON CONFLICT DO NOTHING", "acme", "Acme")
	pool.Exec(ctx, `INSERT INTO agents (id, tenant_id, display_name, protocol, status)
		VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING`, "agent-1", "acme", "A1", "a2a", "offline")
	pool.Exec(ctx, `INSERT INTO mailboxes (id, tenant_id, agent_id, status)
		VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`, "mb-1", "acme", "agent-1", "active")

	envelope := []byte(`{}`)
	_, err := pool.Exec(ctx,
		`INSERT INTO tasks (id, tenant_id, source_agent, target_type, target_value, mailbox_id,
		  status, priority, envelope, retry_at, attempt_count)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 1)`,
		"task-update-error", "acme", "agent-1", "mailbox", "mb-1", "mb-1",
		"retry_scheduled", "normal", envelope, time.Now().Add(-1*time.Minute))
	require.NoError(t, err)

	pool.Exec(ctx, `CREATE OR REPLACE FUNCTION raise_update_error() RETURNS trigger AS $$
	BEGIN
		RAISE EXCEPTION 'intentional update error';
	END;
	$$ LANGUAGE plpgsql`)

	pool.Exec(ctx, `CREATE TRIGGER task_update_error_trigger
		BEFORE UPDATE ON tasks
		FOR EACH ROW
		WHEN (NEW.id = 'task-update-error')
		EXECUTE FUNCTION raise_update_error()`)

	sched := NewScheduler(pool, &fakeQueueDriver{})
	sched.processReadyRetries(ctx)

	pool.Exec(ctx, "DROP TRIGGER IF EXISTS task_update_error_trigger ON tasks")
	pool.Exec(ctx, "DROP FUNCTION IF EXISTS raise_update_error()")

	var status string
	pool.QueryRow(ctx, "SELECT status FROM tasks WHERE id = 'task-update-error'").Scan(&status)
	assert.Equal(t, "retry_scheduled", status, "task should remain in retry_scheduled due to update error")
}

func TestProcessReadyRetries_BuildRetryMessageFails(t *testing.T) {
	pool := openRetryTestDB(t)
	ctx := context.Background()

	pool.Exec(ctx, "INSERT INTO tenants (id, name) VALUES ($1, $2) ON CONFLICT DO NOTHING", "acme", "Acme")
	pool.Exec(ctx, `INSERT INTO agents (id, tenant_id, display_name, protocol, status)
		VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING`, "agent-1", "acme", "A1", "a2a", "offline")
	pool.Exec(ctx, `INSERT INTO mailboxes (id, tenant_id, agent_id, status)
		VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`, "mb-1", "acme", "agent-1", "active")

	envelope := []byte(`{"janus_version": 123}`)
	_, err := pool.Exec(ctx,
		`INSERT INTO tasks (id, tenant_id, source_agent, target_type, target_value, mailbox_id,
		  status, priority, envelope, retry_at, attempt_count)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 1)`,
		"task-invalid-env", "acme", "agent-1", "mailbox", "mb-1", "mb-1",
		"retry_scheduled", "normal", envelope, time.Now().Add(-1*time.Minute))
	require.NoError(t, err)

	sched := NewScheduler(pool, &fakeQueueDriver{})
	sched.useOutbox = true
	sched.processReadyRetries(ctx)

	var status string
	pool.QueryRow(ctx, "SELECT status FROM tasks WHERE id = $1", "task-invalid-env").Scan(&status)
	assert.Equal(t, "queued", status, "task status should be updated to queued")

	var outboxCount int
	pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE tenant_id = 'acme' AND kind = 'task_publish'").Scan(&outboxCount)
	assert.Equal(t, 0, outboxCount, "no outbox event should be created for invalid envelope")
}

func TestProcessReadyRetries_MultipleTasksProcessed(t *testing.T) {
	pool := openRetryTestDB(t)
	ctx := context.Background()

	pool.Exec(ctx, "INSERT INTO tenants (id, name) VALUES ($1, $2) ON CONFLICT DO NOTHING", "acme", "Acme")
	pool.Exec(ctx, `INSERT INTO agents (id, tenant_id, display_name, protocol, status)
		VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING`, "agent-1", "acme", "A1", "a2a", "offline")
	pool.Exec(ctx, `INSERT INTO mailboxes (id, tenant_id, agent_id, status)
		VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`, "mb-1", "acme", "agent-1", "active")

	envelope := []byte(`{}`)
	for i := 0; i < 3; i++ {
		taskID := fmt.Sprintf("task-multi-%d", i)
		_, err := pool.Exec(ctx,
			`INSERT INTO tasks (id, tenant_id, source_agent, target_type, target_value, mailbox_id,
			  status, priority, envelope, retry_at, attempt_count)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 1)`,
			taskID, "acme", "agent-1", "mailbox", "mb-1", "mb-1",
			"retry_scheduled", "normal", envelope, time.Now().Add(-1*time.Minute))
		require.NoError(t, err)
	}

	sched := NewScheduler(pool, &fakeQueueDriver{})
	sched.useOutbox = true
	sched.processReadyRetries(ctx)

	var count int
	pool.QueryRow(ctx, "SELECT COUNT(*) FROM tasks WHERE status = 'queued' AND id LIKE 'task-multi-%'").Scan(&count)
	assert.Equal(t, 3, count, "all 3 tasks should be promoted to queued")
}

// fakeQueueDriver implements core.QueueEventDriver for retry tests.
type fakeQueueDriver struct {
	publishedTasks []core.TaskMessage
}

func (d *fakeQueueDriver) PublishTask(_ context.Context, msg core.TaskMessage) error {
	d.publishedTasks = append(d.publishedTasks, msg)
	return nil
}
func (d *fakeQueueDriver) FetchTasks(_ context.Context, _, _ string, _ core.FetchOptions) ([]core.TaskDelivery, error) {
	return nil, nil
}
func (d *fakeQueueDriver) AckTask(_ context.Context, _ string, _ core.DeliveryRef) error { return nil }
func (d *fakeQueueDriver) NackTask(_ context.Context, _ string, _ core.DeliveryRef, _ core.NackReason) error {
	return nil
}
func (d *fakeQueueDriver) PublishDLQ(_ context.Context, _ core.TaskMessage, _ []byte) error {
	return nil
}
func (d *fakeQueueDriver) PublishEvent(_ context.Context, _ core.JanusEvent) error { return nil }
func (d *fakeQueueDriver) ReplayEvents(_ context.Context, _ core.EventReplayFilter) (core.EventIterator, error) {
	return nil, nil
}
func (d *fakeQueueDriver) EnsureTenant(_ context.Context, _ string) error              { return nil }
func (d *fakeQueueDriver) EnsureMailbox(_ context.Context, _ core.MailboxSpec) error   { return nil }
func (d *fakeQueueDriver) EnsureConsumer(_ context.Context, _ core.ConsumerSpec) error { return nil }
func (d *fakeQueueDriver) Close() error                                                { return nil }
