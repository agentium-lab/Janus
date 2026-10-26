package recovery

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

// PG harness mirrored from service/dispatch_pg_test.go: throwaway database
// per test, all migrations applied, skip when JANUS_PG_* points nowhere.

func openScannerTestDB(t *testing.T) *pgxpool.Pool {
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
	password := os.Getenv("JANUS_PG_PASSWORD")
	testDB := fmt.Sprintf("janus_scannertest_%d", time.Now().UnixNano())

	ctx := context.Background()
	adminDSN := fmt.Sprintf("host=%s port=%s user=%s dbname=janus_test sslmode=disable", host, port, user)
	if password != "" {
		adminDSN = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=janus_test sslmode=disable", host, port, user, password)
	}
	adminConn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Skipf("postgres not reachable (set JANUS_PG_HOST/PORT/USER to enable): %v", err)
	}
	_, err = adminConn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", testDB))
	require.NoError(t, err, "create test DB")
	adminConn.Close(ctx)

	dsn := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable", host, port, user, testDB)
	if password != "" {
		dsn = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, testDB)
	}
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	require.NoError(t, pool.Ping(ctx))

	runScannerMigrations(t, pool)

	t.Cleanup(func() {
		pool.Close()
		c, err := pgx.Connect(context.Background(), adminDSN)
		if err == nil {
			c.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDB))
			c.Close(context.Background())
		}
	})

	return pool
}

func runScannerMigrations(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	dir := scannerMigrationsDir()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err, "read migrations dir: %s", dir)
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".up.sql") {
			continue
		}
		up, err := os.ReadFile(dir + "/" + e.Name())
		require.NoError(t, err, "read migration %s", e.Name())
		_, err = pool.Exec(ctx, string(up))
		require.NoError(t, err, "run migration %s", e.Name())
	}
}

func scannerMigrationsDir() string {
	for _, candidate := range []string{
		"../../../migrations",
		"../../../../migrations",
		os.Getenv("JANUS_MIGRATION_PATH"),
	} {
		if candidate == "" {
			continue
		}
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	return "../../../migrations"
}

// 150 queued tasks past minAge: 60 share one created_at (forcing the
// (created_at, id) tiebreak), 90 are staggered. Pages of 100 + keyset
// cursor must publish each task exactly once, in (created_at, id) order.
func TestTaskScanner_ScanKeysetPagination_AllPagesRecovered_PG(t *testing.T) {
	pool := openScannerTestDB(t)
	ctx := context.Background()

	const tenant = "scantenant"
	_, err := pool.Exec(ctx, `INSERT INTO tenants (id, name) VALUES ($1, 'Scanner Pagination Test')`, tenant)
	require.NoError(t, err)

	tieBase := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	type seededTask struct {
		id        string
		createdAt time.Time
		mailbox   string
	}
	var seeds []seededTask
	for i := 0; i < 60; i++ {
		seeds = append(seeds, seededTask{fmt.Sprintf("tie-%03d", i), tieBase, fmt.Sprintf("mb-%02d", i%10)})
	}
	for i := 1; i <= 90; i++ {
		seeds = append(seeds, seededTask{
			fmt.Sprintf("stag-%03d", i), tieBase.Add(time.Duration(i) * 30 * time.Second),
			fmt.Sprintf("mb-%02d", i%10),
		})
	}

	expected := append([]seededTask(nil), seeds...)
	sort.Slice(expected, func(a, b int) bool {
		if !expected[a].createdAt.Equal(expected[b].createdAt) {
			return expected[a].createdAt.Before(expected[b].createdAt)
		}
		return expected[a].id < expected[b].id
	})
	expectedIDs := make([]string, len(expected))
	for i, s := range expected {
		expectedIDs[i] = s.id
	}

	batch := &pgx.Batch{}
	for _, s := range seeds {
		env, err := json.Marshal(core.TaskEnvelope{
			TaskID: s.id, TenantID: tenant, SourceAgent: "src-agent",
			Target: core.Target{Type: "agent", Value: "dst-agent"},
		})
		require.NoError(t, err)
		batch.Queue(`INSERT INTO tasks (tenant_id, id, source_agent, target_type, target_value, mailbox_id, status, priority, envelope, created_at)
			VALUES ($1, $2, 'src-agent', 'agent', 'dst-agent', $3, 'queued', 'normal', $4, $5)`,
			tenant, s.id, s.mailbox, string(env), s.createdAt)
	}
	br := pool.SendBatch(ctx, batch)
	for i := 0; i < batch.Len(); i++ {
		_, err := br.Exec()
		require.NoError(t, err, "seed insert %d", i)
	}
	require.NoError(t, br.Close())

	var queued int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM tasks WHERE tenant_id = $1 AND status = 'queued'`, tenant).Scan(&queued))
	require.Equal(t, 150, queued, "seeding must produce exactly 150 queued tasks")

	queue := &mockQueueDriver{}
	scanner := &TaskScanner{
		pool:     pool,
		queue:    queue,
		interval: time.Second,
		minAge:   5 * time.Minute,
		stopCh:   make(chan struct{}),
	}
	require.True(t, scanner.lastProcessedAt.IsZero(), "fresh scanner must start without a cursor")

	publishedIDs := func(from, to int) []string {
		ids := make([]string, 0, to-from)
		for _, m := range queue.published[from:to] {
			ids = append(ids, m.TaskID)
		}
		return ids
	}

	scanner.scan(ctx)
	require.Len(t, queue.published, 100, "page 1 must publish the oldest 100 tasks")
	assert.Equal(t, expectedIDs[:100], publishedIDs(0, 100),
		"page 1 order must be (created_at, id), ties broken by id")
	assert.Equal(t, fmt.Sprintf("recovery:%s:%s", tenant, expectedIDs[0]), queue.published[0].DedupeKey)

	scanner.scan(ctx)
	require.Len(t, queue.published, 150, "page 2 must continue past the cursor with the remaining 50")
	assert.Equal(t, expectedIDs[100:], publishedIDs(100, 150),
		"union of pages 1+2 must be all 150 ids, exactly once, in (created_at, id) order")
	assert.Equal(t, "stag-090", scanner.lastProcessedID,
		"cursor must hold the last row of the previous page")

	scanner.scan(ctx)
	require.Len(t, queue.published, 150, "empty page must publish nothing")
	assert.True(t, scanner.lastProcessedAt.IsZero(), "empty page must reset the cursor")
	assert.Empty(t, scanner.lastProcessedID, "reset cursor must clear the id too")

	// Rescan semantic: the reset cursor restarts from the oldest page;
	// downstream dedupe (recovery:<tenant>:<task>) absorbs the repeats.
	scanner.scan(ctx)
	require.Len(t, queue.published, 250, "post-reset scan must re-publish page 1")
	assert.Equal(t, expectedIDs[:100], publishedIDs(150, 250),
		"post-reset scan restarts from the oldest 100 tasks")
	assert.Equal(t, expectedIDs[99], scanner.lastProcessedID, "cursor again at last row of page 1")
}
