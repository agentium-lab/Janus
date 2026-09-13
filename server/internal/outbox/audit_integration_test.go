package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/driver/postgres"
)

// INTEGRATION TEST: requires PostgreSQL. Verifies that the Publisher (NATS
// path, status cursor) and the AuditProjector (projected_at cursor) work
// CONCURRENTLY on the same outbox table without competing or swallowing
// each other's entries — the P0 regression from v1.6.6.
//
// This is the test that would have caught the v1.6.6 P0s: it uses the real
// OutboxRepo against real PostgreSQL, not fakes.

func openOutboxIntegrationDB(t *testing.T) (*pgxpool.Pool, *postgres.OutboxRepo) {
	t.Helper()
	// Reuse the same connection logic as the existing PG-backed tests
	dsn := getTestDSN(t)
	if dsn == "" {
		t.Skip("PostgreSQL not reachable (set JANUS_PG_DSN to enable)")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	repo := postgres.NewOutboxRepo(pool)
	repo.SetWorker("integration-test", 5*time.Second)
	return pool, repo
}

func getTestDSN(t *testing.T) string {
	t.Helper()
	if v := osGetenv("JANUS_PG_DSN"); v != "" {
		return v
	}
	// try standard local PG
	if _, err := pgx.Connect(context.Background(), "postgres://janus:janus@localhost:5432/janus_test?sslmode=disable"); err == nil {
		return "postgres://janus:janus@localhost:5432/janus_test?sslmode=disable"
	}
	return ""
}

func osGetenv(key string) string {
	v, _ := os.LookupEnv(key)
	return v
}

func TestIntegration_PublisherAndProjector_Independent(t *testing.T) {
	pool, repo := openOutboxIntegrationDB(t)
	ctx := context.Background()
	tenant := fmt.Sprintf("itest_%d", time.Now().UnixNano())

	// Insert mixed entries: some task_publish, some event_publish
	const n = 20
	for i := 0; i < n; i++ {
		evt := core.JanusEvent{
			EventID:   fmt.Sprintf("evt-%s-%d", tenant, i),
			TenantID:  tenant,
			TaskID:    fmt.Sprintf("task-%d", i),
			EventType: core.EventTaskCompleted,
			Payload:   json.RawMessage(`{"status":"completed"}`),
		}
		payload, _ := json.Marshal(evt)
		require.NoError(t, repo.InsertDirectWithDedupe(ctx, fmt.Sprintf("ob-evt-%s-%d", tenant, i), tenant, "event_publish", fmt.Sprintf("evt-%s-%d", tenant, i), payload))
		require.NoError(t, repo.InsertDirectWithDedupe(ctx, fmt.Sprintf("ob-task-%s-%d", tenant, i), tenant, "task_publish", fmt.Sprintf("task-%s-%d", tenant, i), payload))
	}

	// Run Publisher batches (consumes via FetchPending / MarkPublished)
	pub := NewPublisher(repo, &countingQueueDriver{})
	for i := 0; i < 5; i++ {
		pub.publishBatch(ctx)
	}

	// Run AuditProjector batch (consumes via FetchUnprojected / MarkProjected)
	proj := NewAuditProjector(repo, &directAuditWriter{pool: pool})
	proj.projectBatch(ctx)

	// ASSERT: all event_publish entries have projected_at set
	var unprojected int
	err := pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox_events WHERE tenant_id = $1 AND kind = 'event_publish' AND projected_at IS NULL`,
		tenant).Scan(&unprojected)
	require.NoError(t, err)
	assert.Zero(t, unprojected, "all event_publish entries must be projected")

	// ASSERT: all task_publish entries are published (status = published)
	var unpublished int
	err = pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox_events WHERE tenant_id = $1 AND kind = 'task_publish' AND status != 'published'`,
		tenant).Scan(&unpublished)
	require.NoError(t, err)
	assert.Zero(t, unpublished, "all task_publish entries must be published")

	// ASSERT: all event_publish entries also have status = published (Publisher processed them too)
	var evNotPublished int
	err = pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox_events WHERE tenant_id = $1 AND kind = 'event_publish' AND status != 'published'`,
		tenant).Scan(&evNotPublished)
	require.NoError(t, err)
	assert.Zero(t, evNotPublished, "event_publish entries must also be NATS-published (dual-cursor independence)")
}

func TestIntegration_ProjectorAfterPublisher(t *testing.T) {
	pool, repo := openOutboxIntegrationDB(t)
	ctx := context.Background()
	tenant := fmt.Sprintf("itest2_%d", time.Now().UnixNano())

	// Insert event_publish entries
	for i := 0; i < 10; i++ {
		evt := core.JanusEvent{EventID: fmt.Sprintf("e-%s-%d", tenant, i), TenantID: tenant, EventType: core.EventTaskCompleted, Payload: json.RawMessage(`{"status":"done"}`)}
		payload, _ := json.Marshal(evt)
		require.NoError(t, repo.InsertDirectWithDedupe(ctx, fmt.Sprintf("ob-%s-%d", tenant, i), tenant, "event_publish", fmt.Sprintf("e-%s-%d", tenant, i), payload))
	}

	// Publisher runs FIRST and marks them published
	pub := NewPublisher(repo, &countingQueueDriver{})
	pub.publishBatch(ctx)

	// THEN the projector runs — it must still see them (projected_at is
	// independent of status)
	proj := NewAuditProjector(repo, &directAuditWriter{pool: pool})
	proj.projectBatch(ctx)

	var projected, published int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox_events WHERE tenant_id = $1 AND kind = 'event_publish' AND projected_at IS NOT NULL`,
		tenant).Scan(&projected))
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox_events WHERE tenant_id = $1 AND kind = 'event_publish' AND status = 'published'`,
		tenant).Scan(&published))
	assert.Equal(t, 10, projected, "projector must project even after publisher marked published")
	assert.Equal(t, 10, published, "publisher must have published them")
}

// countingQueueDriver counts NATS publishes (no real NATS needed).
type countingQueueDriver struct {
	mu   sync.Mutex
	pubs int
}

func (d *countingQueueDriver) PublishTask(_ context.Context, _ core.TaskMessage) error {
	d.mu.Lock()
	d.pubs++
	d.mu.Unlock()
	return nil
}
func (d *countingQueueDriver) PublishEvent(_ context.Context, _ core.JanusEvent) error {
	d.mu.Lock()
	d.pubs++
	d.mu.Unlock()
	return nil
}
func (d *countingQueueDriver) PublishDLQ(_ context.Context, _ core.TaskMessage, _ []byte) error {
	return nil
}
func (d *countingQueueDriver) FetchTasks(_ context.Context, _, _ string, _ core.FetchOptions) ([]core.TaskDelivery, error) {
	return nil, nil
}
func (d *countingQueueDriver) AckTask(_ context.Context, _ string, _ core.DeliveryRef) error {
	return nil
}
func (d *countingQueueDriver) NackTask(_ context.Context, _ string, _ core.DeliveryRef, _ core.NackReason) error {
	return nil
}
func (d *countingQueueDriver) ReplayEvents(_ context.Context, _ core.EventReplayFilter) (core.EventIterator, error) {
	return nil, fmt.Errorf("not supported")
}
func (d *countingQueueDriver) EnsureTenant(_ context.Context, _ string) error            { return nil }
func (d *countingQueueDriver) EnsureMailbox(_ context.Context, _ core.MailboxSpec) error { return nil }
func (d *countingQueueDriver) EnsureConsumer(_ context.Context, _ core.ConsumerSpec) error {
	return nil
}
func (d *countingQueueDriver) Close() error { return nil }

// directAuditWriter writes directly to the audit table (no channel).
type directAuditWriter struct {
	pool *pgxpool.Pool
}

func (w *directAuditWriter) RecordIdempotent(ctx context.Context, evt core.JanusEvent) error {
	_, err := w.pool.Exec(ctx,
		`INSERT INTO audit_event_projection (tenant_id, event_id, event_type, task_id, agent_id, trace_id, occurred_at, payload)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 ON CONFLICT (tenant_id, event_id) DO NOTHING`,
		evt.TenantID, evt.EventID, evt.EventType, evt.TaskID, evt.SourceAgent, evt.TraceID, evt.Timestamp, evt.Payload,
	)
	return err
}
