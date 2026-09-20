//go:build !race

package recovery

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// CRASH RECOVERY INVARIANT: kill -9 the process mid-flight, restart, and
// verify zero task loss + zero audit loss. The outbox table IS the durable
// record; nothing in memory matters.

const crashTenant = "crash-test"

func TestCrashRecovery_ZeroTaskLoss(t *testing.T) {
	pool := setupCrashDB(t)
	bin := buildJanusAPI(t)
	port := 18500

	// Phase 1: start, create tasks, kill -9
	p1 := startJanus(t, bin, port, pool)
	waitHealthy(t, port)
	setupViaAPI(t, port)
	createTasks(t, port, 20)
	killHard(t, p1)

	// Phase 2: restart, verify all tasks + outbox + audit survive
	p2 := startJanus(t, bin, port, pool)
	defer stopJanus(t, p2)
	waitHealthy(t, port)
	setupViaAPI(t, port)

	// Wait for outbox publisher + audit projector to catch up
	time.Sleep(3 * time.Second)

	var taskCount, outboxCount, projectedCount, auditCount int
	require.NoError(t, pool.QueryRow(context.TODO(),
		`SELECT count(*) FROM tasks WHERE tenant_id = $1`, crashTenant).Scan(&taskCount))
	require.NoError(t, pool.QueryRow(context.TODO(),
		`SELECT count(*) FROM outbox_events WHERE tenant_id = $1`, crashTenant).Scan(&outboxCount))
	require.NoError(t, pool.QueryRow(context.TODO(),
		`SELECT count(*) FROM outbox_events WHERE tenant_id = $1 AND projected_at IS NOT NULL`, crashTenant).Scan(&projectedCount))
	require.NoError(t, pool.QueryRow(context.TODO(),
		`SELECT count(*) FROM audit_event_projection WHERE tenant_id = $1`, crashTenant).Scan(&auditCount))

	assert.Equal(t, 20, taskCount, "all 20 tasks must survive crash")
	assert.Greater(t, outboxCount, 0, "outbox entries must exist")
	assert.Equal(t, outboxCount, projectedCount, "all outbox entries must be projected after restart")
	assert.Greater(t, auditCount, 0, "audit events must be projected")
}

func setupCrashDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("JANUS_PG_DSN")
	if dsn == "" {
		if _, err := pgx.Connect(context.TODO(), "postgres://janus:janus@localhost:5432/janus_test?sslmode=disable"); err != nil {
			t.Skip("PostgreSQL not reachable")
		}
		dsn = "postgres://janus:janus@localhost:5432/janus_test?sslmode=disable"
	}
	pool, err := pgxpool.New(context.TODO(), dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	// Clean slate for this tenant
	pool.Exec(context.TODO(), `DELETE FROM tasks WHERE tenant_id = $1`, crashTenant)
	pool.Exec(context.TODO(), `DELETE FROM outbox_events WHERE tenant_id = $1`, crashTenant)
	pool.Exec(context.TODO(), `DELETE FROM audit_event_projection WHERE tenant_id = $1`, crashTenant)
	return pool
}

func buildJanusAPI(t *testing.T) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "janus-api-crash")
	cmd := exec.Command("go", "build", "-o", out, "./server/cmd/janus-api")
	cmd.Dir = repoRootCrash(t)
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	b, err := cmd.CombinedOutput()
	require.NoError(t, err, "build: %s", b)
	return out
}

func repoRootCrash(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.work")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.work not found")
		}
		dir = parent
	}
}

func startJanus(t *testing.T, bin string, port int, pool *pgxpool.Pool) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(),
		"JANUS_QUEUE_DRIVER=pg",
		"JANUS_PG_HOST=localhost", "JANUS_PG_USER=janus", "JANUS_PG_PASSWORD=janus",
		"JANUS_PG_DATABASE=janus_test", "JANUS_PG_SSLMODE=disable",
		"JANUS_REDIS_ADDR=localhost:59999",
		"JANUS_MIGRATION_AUTO=true",
		"JANUS_MIGRATION_PATH="+filepath.Join(repoRootCrash(t), "migrations"),
		"JANUS_AUTH_ENABLED=false",
		"JANUS_HTTP_HOST=localhost",
		fmt.Sprintf("JANUS_HTTP_PORT=%d", port),
	)
	require.NoError(t, cmd.Start())
	return cmd
}

func setupViaAPI(t *testing.T, port int) {
	t.Helper()
	base := fmt.Sprintf("http://localhost:%d", port)

	requests := []struct {
		path, body string
	}{
		{"/v1/tenants", `{"id":"` + crashTenant + `","name":"Crash Test"}`},
		{"/v1/tenants/" + crashTenant + "/agents", `{"id":"crash-agent","display_name":"Crash Agent","protocol":"http","endpoint":"http://localhost:9"}`},
		{"/v1/tenants/" + crashTenant + "/mailboxes", `{"id":"crash-mb","agent_id":"crash-agent"}`},
	}
	for _, r := range requests {
		resp, err := http.Post(base+r.path, "application/json", strings.NewReader(r.body))
		require.NoError(t, err, "setup %s", r.path)
		resp.Body.Close()
		if resp.StatusCode >= 400 {
			t.Fatalf("setup %s: got %d", r.path, resp.StatusCode)
		}
	}
}

func createTasks(t *testing.T, port int, n int) {
	t.Helper()
	base := fmt.Sprintf("http://localhost:%d", port)
	for i := 0; i < n; i++ {
		body := fmt.Sprintf(`{"id":"crash-%d","source_agent":"crash-agent","target_type":"mailbox","target_value":"crash-mb","mailbox_id":"crash-mb","envelope":{"janus_version":"1","task_id":"crash-%d","tenant_id":"%s","source_agent":"crash-agent","target":{"type":"mailbox","value":"crash-mb"},"priority":"normal","payload":{"type":"text","content":"crash"},"trace":{"trace_id":"crash-%d"}}}`, i, i, crashTenant, i)
		resp, err := http.Post(base+"/v1/tenants/"+crashTenant+"/tasks", "application/json", strings.NewReader(body))
		require.NoError(t, err, "create task %d", i)
		resp.Body.Close()
		require.Equal(t, http.StatusCreated, resp.StatusCode, "task %d", i)
	}
}

func killHard(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	require.NoError(t, cmd.Process.Kill())
	cmd.Wait()
}

func stopJanus(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	cmd.Process.Kill()
	cmd.Wait()
}

func waitHealthy(t *testing.T, port int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(fmt.Sprintf("http://localhost:%d/healthz", port))
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			return
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("server did not become healthy")
}
