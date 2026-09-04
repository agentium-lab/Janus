package outbox

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
	"github.com/agentium-lab/Janus/server/internal/driver/postgres"
)

func openOutboxTestDB(t *testing.T) *pgxpool.Pool {
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
	testDB := fmt.Sprintf("janus_outboxtest_%d", time.Now().UnixNano())

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
	migrationsDir := findOutboxMigrationsDir()
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

func findOutboxMigrationsDir() string {
	for _, d := range []string{"../../../migrations", "../../../../migrations"} {
		if _, err := os.Stat(d); err == nil {
			return d
		}
	}
	return "../../../migrations"
}

func insertOutboxEntry(t *testing.T, pool *pgxpool.Pool, id, tenantID, kind, status string, payload json.RawMessage) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO outbox_events (id, tenant_id, kind, payload, status) VALUES ($1, $2, $3, $4, $5)`,
		id, tenantID, kind, payload, status)
	require.NoError(t, err)
}

func TestOutboxRepo_FetchPending_ClaimsAndMarksPublishing(t *testing.T) {
	pool := openOutboxTestDB(t)
	repo := postgres.NewOutboxRepo(pool)
	repo.SetWorker("test-worker", 60*time.Second)
	ctx := context.Background()

	payload := json.RawMessage(`{"task_id":"task-1"}`)
	insertOutboxEntry(t, pool, "ob-1", "acme", "task_publish", "pending", payload)

	entries, err := repo.FetchPending(ctx, 10)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "ob-1", entries[0].ID)
	assert.Equal(t, "task_publish", entries[0].Kind)
	// The returned entry has the pre-update status/attempts (the UPDATE happens
	// after the SELECT in the same tx). Verify the DB state instead.
	var status string
	pool.QueryRow(ctx, "SELECT status FROM outbox_events WHERE id = $1", "ob-1").Scan(&status)
	assert.Equal(t, "publishing", status, "entry should be moved to publishing")
}

func TestOutboxRepo_MarkPublished_ClearsLease(t *testing.T) {
	pool := openOutboxTestDB(t)
	repo := postgres.NewOutboxRepo(pool)
	repo.SetWorker("test-worker", 60*time.Second)
	ctx := context.Background()

	insertOutboxEntry(t, pool, "ob-2", "acme", "event_publish", "pending", json.RawMessage(`{}`))
	_, _ = repo.FetchPending(ctx, 10)

	err := repo.MarkPublished(ctx, "ob-2")
	require.NoError(t, err)

	var status, lockedBy *string
	pool.QueryRow(ctx, "SELECT status, locked_by FROM outbox_events WHERE id = $1", "ob-2").Scan(&status, &lockedBy)
	assert.Equal(t, "published", *status)
	assert.Nil(t, lockedBy, "locked_by should be cleared after MarkPublished")
}

func TestOutboxRepo_MarkFailedWithReason_RetriesThenDead(t *testing.T) {
	pool := openOutboxTestDB(t)
	repo := postgres.NewOutboxRepo(pool)
	repo.SetWorker("test-worker", 60*time.Second)
	ctx := context.Background()

	insertOutboxEntry(t, pool, "ob-3", "acme", "task_publish", "pending", json.RawMessage(`{}`))

	// Fetch to move to 'publishing' (attempts=1).
	_, _ = repo.FetchPending(ctx, 10)

	// Mark failed → should go to 'retry' (attempts < maxOutboxRetries).
	err := repo.MarkFailedWithReason(ctx, "ob-3", "NATS timeout")
	require.NoError(t, err)

	var status string
	pool.QueryRow(ctx, "SELECT status FROM outbox_events WHERE id = $1", "ob-3").Scan(&status)
	assert.Equal(t, "retry", status, "first failure should go to retry")

	// Simulate exhausting retries by setting attempts high.
	pool.Exec(ctx, "UPDATE outbox_events SET attempts = $1, status = 'publishing' WHERE id = $2", 999, "ob-3")
	err = repo.MarkFailedWithReason(ctx, "ob-3", "permanent failure")
	require.NoError(t, err)

	pool.QueryRow(ctx, "SELECT status FROM outbox_events WHERE id = $1", "ob-3").Scan(&status)
	assert.Equal(t, "dead", status, "exhausted retries should go to dead")
}

func TestOutboxRepo_FetchPending_SkipsPublishedAndDead(t *testing.T) {
	pool := openOutboxTestDB(t)
	repo := postgres.NewOutboxRepo(pool)
	repo.SetWorker("test-worker", 60*time.Second)
	ctx := context.Background()

	insertOutboxEntry(t, pool, "ob-pub", "acme", "task_publish", "published", json.RawMessage(`{}`))
	insertOutboxEntry(t, pool, "ob-dead", "acme", "task_publish", "dead", json.RawMessage(`{}`))
	insertOutboxEntry(t, pool, "ob-pending", "acme", "task_publish", "pending", json.RawMessage(`{}`))

	entries, err := repo.FetchPending(ctx, 10)
	require.NoError(t, err)
	require.Len(t, entries, 1, "only pending should be fetched")
	assert.Equal(t, "ob-pending", entries[0].ID)
}

func TestOutboxRepo_InsertDirect(t *testing.T) {
	pool := openOutboxTestDB(t)
	repo := postgres.NewOutboxRepo(pool)
	ctx := context.Background()

	err := repo.InsertDirect(ctx, "ob-direct-1", "acme", "event_publish", json.RawMessage(`{"type":"test"}`))
	require.NoError(t, err)

	var count int
	pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE id = $1", "ob-direct-1").Scan(&count)
	assert.Equal(t, 1, count)
}

func TestPublisher_PublishBatch_FullCycle(t *testing.T) {
	pool := openOutboxTestDB(t)
	repo := postgres.NewOutboxRepo(pool)
	repo.SetWorker("test-worker", 60*time.Second)
	ctx := context.Background()

	// Insert a pending task_publish entry.
	msg := core.TaskMessage{TenantID: "acme", MailboxID: "mb-1", TaskID: "task-pub-1"}
	payload, _ := json.Marshal(msg)
	insertOutboxEntry(t, pool, "ob-cycle", "acme", "task_publish", "pending", payload)

	// Create publisher with a fake driver that records publishes.
	drv := &recordingDriver{}
	pub := NewPublisher(repo, drv)

	// Manually call publishBatch (normally called by Start loop).
	pub.publishBatch(ctx)

	// Verify the task was published.
	assert.Len(t, drv.publishedTasks, 1)
	assert.Equal(t, "task-pub-1", drv.publishedTasks[0].TaskID)

	// Verify the entry is now 'published'.
	var status string
	pool.QueryRow(ctx, "SELECT status FROM outbox_events WHERE id = $1", "ob-cycle").Scan(&status)
	assert.Equal(t, "published", status)
}

// recordingDriver implements core.QueueEventDriver for outbox integration tests.
type recordingDriver struct {
	publishedTasks []core.TaskMessage
	publishedEvent []core.JanusEvent
}

func (d *recordingDriver) PublishTask(_ context.Context, msg core.TaskMessage) error {
	d.publishedTasks = append(d.publishedTasks, msg)
	return nil
}
func (d *recordingDriver) FetchTasks(_ context.Context, _, _ string, _ core.FetchOptions) ([]core.TaskDelivery, error) {
	return nil, nil
}
func (d *recordingDriver) AckTask(_ context.Context, _ string, _ core.DeliveryRef) error { return nil }
func (d *recordingDriver) NackTask(_ context.Context, _ string, _ core.DeliveryRef, _ core.NackReason) error {
	return nil
}
func (d *recordingDriver) PublishDLQ(_ context.Context, _ core.TaskMessage, _ []byte) error {
	return nil
}
func (d *recordingDriver) PublishEvent(_ context.Context, event core.JanusEvent) error {
	d.publishedEvent = append(d.publishedEvent, event)
	return nil
}
func (d *recordingDriver) ReplayEvents(_ context.Context, _ core.EventReplayFilter) (core.EventIterator, error) {
	return nil, nil
}
func (d *recordingDriver) EnsureTenant(_ context.Context, _ string) error              { return nil }
func (d *recordingDriver) EnsureMailbox(_ context.Context, _ core.MailboxSpec) error   { return nil }
func (d *recordingDriver) EnsureConsumer(_ context.Context, _ core.ConsumerSpec) error { return nil }
func (d *recordingDriver) Close() error                                                { return nil }

type errorDriver struct{}

func (d *errorDriver) PublishTask(_ context.Context, _ core.TaskMessage) error {
	return fmt.Errorf("nats publish failed")
}
func (d *errorDriver) FetchTasks(_ context.Context, _, _ string, _ core.FetchOptions) ([]core.TaskDelivery, error) {
	return nil, nil
}
func (d *errorDriver) AckTask(_ context.Context, _ string, _ core.DeliveryRef) error { return nil }
func (d *errorDriver) NackTask(_ context.Context, _ string, _ core.DeliveryRef, _ core.NackReason) error {
	return nil
}
func (d *errorDriver) PublishDLQ(_ context.Context, _ core.TaskMessage, _ []byte) error {
	return fmt.Errorf("dlq publish failed")
}
func (d *errorDriver) PublishEvent(_ context.Context, _ core.JanusEvent) error {
	return fmt.Errorf("event publish failed")
}
func (d *errorDriver) ReplayEvents(_ context.Context, _ core.EventReplayFilter) (core.EventIterator, error) {
	return nil, nil
}
func (d *errorDriver) EnsureTenant(_ context.Context, _ string) error              { return nil }
func (d *errorDriver) EnsureMailbox(_ context.Context, _ core.MailboxSpec) error   { return nil }
func (d *errorDriver) EnsureConsumer(_ context.Context, _ core.ConsumerSpec) error { return nil }
func (d *errorDriver) Close() error                                                { return nil }

func TestPublisher_PublishBatch_PublishError(t *testing.T) {
	pool := openOutboxTestDB(t)
	repo := postgres.NewOutboxRepo(pool)
	repo.SetWorker("test-worker", 60*time.Second)
	ctx := context.Background()

	msg := core.TaskMessage{TenantID: "acme", MailboxID: "mb-1", TaskID: "task-err-1"}
	payload, _ := json.Marshal(msg)
	insertOutboxEntry(t, pool, "ob-err", "acme", "task_publish", "pending", payload)

	drv := &errorDriver{}
	pub := NewPublisher(repo, drv)
	pub.publishBatch(ctx)

	var status string
	pool.QueryRow(ctx, "SELECT status FROM outbox_events WHERE id = $1", "ob-err").Scan(&status)
	assert.Equal(t, "retry", status)
}

func TestPublisher_PublishBatch_EventPublishError(t *testing.T) {
	pool := openOutboxTestDB(t)
	repo := postgres.NewOutboxRepo(pool)
	repo.SetWorker("test-worker", 60*time.Second)
	ctx := context.Background()

	evt := core.JanusEvent{EventID: "e1", TenantID: "acme", EventType: core.EventTaskCreated}
	payload, _ := json.Marshal(evt)
	insertOutboxEntry(t, pool, "ob-evt-err", "acme", "event_publish", "pending", payload)

	drv := &errorDriver{}
	pub := NewPublisher(repo, drv)
	pub.publishBatch(ctx)

	var status string
	pool.QueryRow(ctx, "SELECT status FROM outbox_events WHERE id = $1", "ob-evt-err").Scan(&status)
	assert.Equal(t, "retry", status)
}

func TestPublisher_PublishBatch_DLQPublishError(t *testing.T) {
	pool := openOutboxTestDB(t)
	repo := postgres.NewOutboxRepo(pool)
	repo.SetWorker("test-worker", 60*time.Second)
	ctx := context.Background()

	msg := core.TaskMessage{TenantID: "acme", MailboxID: "mb-1", TaskID: "task-dlq-1"}
	payload, _ := json.Marshal(msg)
	insertOutboxEntry(t, pool, "ob-dlq-err", "acme", "dlq_publish", "pending", payload)

	drv := &errorDriver{}
	pub := NewPublisher(repo, drv)
	pub.publishBatch(ctx)

	var status string
	pool.QueryRow(ctx, "SELECT status FROM outbox_events WHERE id = $1", "ob-dlq-err").Scan(&status)
	assert.Equal(t, "retry", status)
}

func TestPublisher_PublishBatch_UnknownKind_NoOp(t *testing.T) {
	pool := openOutboxTestDB(t)
	repo := postgres.NewOutboxRepo(pool)
	repo.SetWorker("test-worker", 60*time.Second)
	ctx := context.Background()

	insertOutboxEntry(t, pool, "ob-unknown", "acme", "unknown_kind", "pending", []byte(`{}`))

	drv := &recordingDriver{}
	pub := NewPublisher(repo, drv)
	pub.publishBatch(ctx)

	// Unknown kind: publishOne returns nil (no-op), MarkPublished should be called.
	var status string
	pool.QueryRow(ctx, "SELECT status FROM outbox_events WHERE id = $1", "ob-unknown").Scan(&status)
	assert.Equal(t, "published", status)
}

// mockOutboxRepo allows controlling error returns for testing.
type mockOutboxRepo struct {
	entries             []postgres.OutboxEntry
	fetchErr            error
	markPublishedErr    error
	markFailedErr       error
	markFailedCalled    bool
	markFailedID        string
	markPublishedCalled bool
	markPublishedID     string
}

func (m *mockOutboxRepo) FetchPending(_ context.Context, _ int) ([]postgres.OutboxEntry, error) {
	return m.entries, m.fetchErr
}

func (m *mockOutboxRepo) MarkPublished(_ context.Context, id string) error {
	m.markPublishedCalled = true
	m.markPublishedID = id
	return m.markPublishedErr
}

func (m *mockOutboxRepo) MarkFailedWithReason(_ context.Context, id string, _ string) error {
	m.markFailedCalled = true
	m.markFailedID = id
	return m.markFailedErr
}

func TestPublisher_PublishBatch_FetchPendingError(t *testing.T) {
	drv := &fakeDriver{}
	repo := &mockOutboxRepo{
		fetchErr: fmt.Errorf("database connection lost"),
	}
	pub := NewPublisher(repo, drv)

	pub.publishBatch(context.Background())

	assert.Empty(t, drv.publishedTasks)
	assert.False(t, repo.markPublishedCalled)
}

func TestPublisher_PublishBatch_MarkPublishedError(t *testing.T) {
	drv := &fakeDriver{}
	repo := &mockOutboxRepo{
		entries: []postgres.OutboxEntry{
			{ID: "ob-mp-err", TenantID: "acme", Kind: "task_publish", Payload: json.RawMessage(`{"task_id":"task-x"}`)},
		},
		markPublishedErr: fmt.Errorf("connection pool closed"),
	}
	pub := NewPublisher(repo, drv)

	pub.publishBatch(context.Background())

	assert.Len(t, drv.publishedTasks, 1)
	assert.True(t, repo.markPublishedCalled)
	assert.Equal(t, "ob-mp-err", repo.markPublishedID)
}

func TestPublisher_PublishBatch_MarkFailedError(t *testing.T) {
	drv := &fakeDriver{publishErr: fmt.Errorf("NATS down")}
	repo := &mockOutboxRepo{
		entries: []postgres.OutboxEntry{
			{ID: "ob-mf-err", TenantID: "acme", Kind: "task_publish", Payload: json.RawMessage(`{"task_id":"task-y"}`)},
		},
		markFailedErr: fmt.Errorf("db write failed"),
	}
	pub := NewPublisher(repo, drv)

	pub.publishBatch(context.Background())

	assert.Empty(t, drv.publishedTasks)
	assert.True(t, repo.markFailedCalled)
	assert.Equal(t, "ob-mf-err", repo.markFailedID)
}
