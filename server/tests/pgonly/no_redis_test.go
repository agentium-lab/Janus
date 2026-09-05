//go:build !race

package pgonly

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPGOnlyNoRedis_AgentRegister exercises the exact regression from the
// v1.5.0 review: with Redis unreachable and JANUS_QUEUE_DRIVER=pg, a typed-nil
// *redisdriver.Driver used to reach AgentService.Register and panic on
// hbDriver.Ping. This test builds the real binary, boots it against real
// PostgreSQL with a dead Redis address, and registers an agent over HTTP.
func TestPGOnlyNoRedis_AgentRegister(t *testing.T) {
	host := envOr("JANUS_PG_HOST", "localhost")
	port := envOr("JANUS_PG_PORT", "5432")
	user := envOr("JANUS_PG_USER", "janus")
	password := envOr("JANUS_PG_PASSWORD", "janus")

	adminDSN := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=janus_test sslmode=disable", host, port, user, password)
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Skipf("postgres not reachable (set JANUS_PG_* to enable): %v", err)
	}

	dbName := fmt.Sprintf("janus_pgonly_%d", time.Now().UnixNano())
	_, err = admin.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dbName))
	require.NoError(t, err, "create test DB")
	admin.Close(ctx)
	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), adminDSN)
		if err == nil {
			c.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))
			c.Close(context.Background())
		}
	})

	bin := buildJanusAPI(t)
	port2 := freePort(t)

	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(),
		"JANUS_QUEUE_DRIVER=pg",
		fmt.Sprintf("JANUS_PG_HOST=%s", host),
		fmt.Sprintf("JANUS_PG_PORT=%s", port),
		fmt.Sprintf("JANUS_PG_USER=%s", user),
		fmt.Sprintf("JANUS_PG_PASSWORD=%s", password),
		fmt.Sprintf("JANUS_PG_DATABASE=%s", dbName),
		"JANUS_PG_SSLMODE=disable",
		"JANUS_MIGRATION_AUTO=true",
		"JANUS_MIGRATION_PATH=migrations",
		"JANUS_REDIS_ADDR=localhost:59999",
		"JANUS_AUTH_ENABLED=false",
		"JANUS_HTTP_HOST=localhost",
		fmt.Sprintf("JANUS_HTTP_PORT=%d", port2),
		"JANUS_GRPC_PORT=0",
	)
	var bootLog strings.Builder
	cmd.Stdout = &bootLog
	cmd.Stderr = &bootLog
	require.NoError(t, cmd.Start(), "start janus-api")
	dead := make(chan struct{})
	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
		<-dead
	})
	go func() { cmd.Wait(); close(dead) }()

	base := fmt.Sprintf("http://localhost:%d", port2)
	waitHealthy(t, base)

	t.Run("agent_register_does_not_panic", func(t *testing.T) {
		body := `{"id":"cs-agent-1","display_name":"Customer Service","protocol":"http","endpoint":"http://localhost:9"}`
		resp, err := httpPost(base+"/v1/tenants/acme/agents", body)
		require.NoError(t, err, "server log:\n%s", bootLog.String())
		assert.Equal(t, http.StatusOK, resp.statusCode, "body=%s log=\n%s", resp.body, bootLog.String())
	})

	t.Run("task_create_budget_nil_ratelimiter", func(t *testing.T) {
		body := `{"id":"task-pgonly-1","source_agent":"cs-agent-1","target_type":"mailbox","target_value":"cs-mb","mailbox_id":"cs-mb","envelope":{"janus_version":"1","task_id":"task-pgonly-1","tenant_id":"acme","source_agent":"cs-agent-1","target":{"type":"mailbox","value":"cs-mb"},"priority":"normal","payload":{"type":"text","content":"hello"},"trace":{"trace_id":"tr-1"}}}`
		resp, err := httpPost(base+"/v1/tenants/acme/tasks", body)
		require.NoError(t, err)
		assert.Equal(t, http.StatusCreated, resp.statusCode, "body=%s log=\n%s", resp.body, bootLog.String())
	})

	assert.NotContains(t, bootLog.String(), "panic:", "server must not panic; log:\n%s", bootLog.String())
}

func buildJanusAPI(t *testing.T) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "janus-api")
	cmd := exec.Command("go", "build", "-o", out, "./server/cmd/janus-api")
	cmd.Dir = repoRoot(t)
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	b, err := cmd.CombinedOutput()
	require.NoError(t, err, "go build: %s", b)
	return out
}

func repoRoot(t *testing.T) string {
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

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func waitHealthy(t *testing.T, base string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(base + "/healthz")
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("server did not become healthy in 60s")
}

type httpResp struct {
	statusCode int
	body       string
}

func httpPost(url, body string) (*httpResp, error) {
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b := make([]byte, 4096)
	n, _ := resp.Body.Read(b)
	return &httpResp{statusCode: resp.StatusCode, body: string(b[:n])}, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
