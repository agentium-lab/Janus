package e2e

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	natsdriver "github.com/agentium-lab/Janus/server/internal/driver/nats"
	pgdriver "github.com/agentium-lab/Janus/server/internal/driver/postgres"
	redisdriver "github.com/agentium-lab/Janus/server/internal/driver/redis"
	"github.com/agentium-lab/Janus/server/internal/handler"
	"github.com/agentium-lab/Janus/server/internal/service"

	"github.com/agentium-lab/Janus/core"
)

const testTenant = "e2e-tenant"

var (
	db       *sql.DB
	server   *httptest.Server
	natsDrv  *natsdriver.Driver
	redisDrv *redisdriver.Driver
)

func TestMain(m *testing.M) {
	pgDSN := envOr("JANUS_PG_DSN", "postgres://janus:janus@localhost:5432/janus_test?sslmode=disable")
	natsURL := envOr("JANUS_NATS_URL", "nats://localhost:4222")
	redisAddr := envOr("JANUS_REDIS_ADDR", "localhost:6379")
	migrationPath := envOr("JANUS_MIGRATION_PATH", "../../../migrations/")

	var err error

	db, err = sql.Open("postgres", pgDSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pg open: %v\n", err)
		os.Exit(1)
	}
	if err := db.Ping(); err != nil {
		fmt.Fprintf(os.Stderr, "pg ping: %v (is postgres running?)\n", err)
		os.Exit(1)
	}

	cleanDB(db)

	mi, err := migrate.New("file://"+migrationPath, pgDSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "migrate init: %v\n", err)
		os.Exit(1)
	}
	if err := mi.Up(); err != nil && err != migrate.ErrNoChange {
		fmt.Fprintf(os.Stderr, "migrate up: %v\n", err)
		os.Exit(1)
	}
	mi.Close()

	natsDrv, err = natsdriver.NewDriver(natsdriver.Config{URL: natsURL})
	if err != nil {
		fmt.Fprintf(os.Stderr, "nats: %v\n", err)
		os.Exit(1)
	}

	redisDrv, err = redisdriver.NewDriver(redisdriver.Config{Addr: redisAddr})
	if err != nil {
		fmt.Fprintf(os.Stderr, "redis: %v\n", err)
		os.Exit(1)
	}

	tenantRepo := pgdriver.NewTenantRepository(db)
	agentRepo := pgdriver.NewAgentRepository(db)
	taskRepo := pgdriver.NewTaskRepository(db)
	mailboxRepo := pgdriver.NewMailboxRepository(db)

	tenantSvc := service.NewTenantService(tenantRepo)
	agentSvc := service.NewAgentService(agentRepo, mailboxRepo, redisDrv, natsDrv)
	taskSvc := service.NewTaskService(taskRepo, natsDrv)
	mailboxSvc := service.NewMailboxService(mailboxRepo, natsDrv)

	mux := newTestRouter(
		handler.NewTenantHandler(tenantSvc),
		handler.NewAgentHandler(agentSvc),
		handler.NewTaskHandler(taskSvc),
		handler.NewMailboxHandler(mailboxSvc),
	)

	server = httptest.NewServer(mux)
	defer server.Close()

	code := m.Run()

	server.Close()
	natsDrv.Close()
	redisDrv.Close()
	db.Close()
	os.Exit(code)
}

func TestE2E_TenantCreateAndGet(t *testing.T) {
	body := map[string]string{"id": testTenant, "name": "E2E Test Tenant"}
	resp := mustRequest(t, "POST", "/v1/tenants", body)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
	resp.Body.Close()

	resp = mustRequest(t, "GET", "/v1/tenants/"+testTenant, nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var tenant map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&tenant)
	assert.Equal(t, testTenant, tenant["id"])
}

func TestE2E_AgentRegisterAndGet(t *testing.T) {
	body := map[string]interface{}{
		"id":             "agent-1",
		"display_name":   "Test Agent",
		"protocol":       "a2a",
	}
	resp := mustRequest(t, "POST", "/v1/tenants/"+testTenant+"/agents", body)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
	resp.Body.Close()

	resp = mustRequest(t, "GET", "/v1/tenants/"+testTenant+"/agents/agent-1", nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var agent map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&agent)
	assert.Equal(t, "agent-1", agent["id"])
	assert.Equal(t, "online", agent["status"])
}

func TestE2E_AgentHeartbeat(t *testing.T) {
	resp := mustRequest(t, "POST", "/v1/tenants/"+testTenant+"/agents/agent-1/heartbeat", nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestE2E_AgentList(t *testing.T) {
	resp := mustRequest(t, "GET", "/v1/tenants/"+testTenant+"/agents", nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	agents, ok := result["agents"].([]interface{})
	assert.True(t, ok)
	assert.GreaterOrEqual(t, len(agents), 1)
}

func TestE2E_MailboxCreateAndGet(t *testing.T) {
	body := map[string]interface{}{
		"id":                "mb-1",
		"agent_id":          "agent-1",
		"max_concurrency":   5,
		"ack_wait_seconds":  300,
		"max_deliver":       3,
		"retention_seconds": 86400,
	}
	resp := mustRequest(t, "POST", "/v1/tenants/"+testTenant+"/mailboxes", body)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
	resp.Body.Close()

	resp = mustRequest(t, "GET", "/v1/tenants/"+testTenant+"/mailboxes/mb-1", nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var mb map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&mb)
	assert.Equal(t, "mb-1", mb["id"])
}

func TestE2E_TaskCreateAndGet(t *testing.T) {
	body := map[string]interface{}{
		"id":              "task-1",
		"source_agent":    "agent-1",
		"target_type":     "agent",
		"target_value":    "agent-2",
		"mailbox_id":      "mb-1",
		"idempotency_key": "e2e-key-1",
		"priority":        "normal",
		"envelope": map[string]interface{}{
			"janus_version": "0.1.0",
			"task_id":       "task-1",
			"tenant_id":     testTenant,
			"source_agent":  "agent-1",
			"target":        map[string]string{"type": "agent", "value": "agent-2"},
			"payload":       map[string]string{"type": "json", "content": `{"hello":"world"}`},
			"trace":         map[string]string{"trace_id": "trace-1"},
		},
	}
	resp := mustRequest(t, "POST", "/v1/tenants/"+testTenant+"/tasks", body)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
	resp.Body.Close()

	resp = mustRequest(t, "GET", "/v1/tenants/"+testTenant+"/tasks/task-1", nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var task map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&task)
	assert.Equal(t, "task-1", task["id"])
	assert.Equal(t, "queued", task["status"])
}

func TestE2E_TaskLifecycle(t *testing.T) {
	resp := mustRequest(t, "POST", "/v1/tenants/"+testTenant+"/tasks/task-1/start", nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp = mustRequest(t, "GET", "/v1/tenants/"+testTenant+"/tasks/task-1", nil)
	defer resp.Body.Close()
	var task map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&task)
	assert.Equal(t, "running", task["status"])

	resp = mustRequest(t, "POST", "/v1/tenants/"+testTenant+"/tasks/task-1/complete", nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp = mustRequest(t, "GET", "/v1/tenants/"+testTenant+"/tasks/task-1", nil)
	defer resp.Body.Close()
	json.NewDecoder(resp.Body).Decode(&task)
	assert.Equal(t, "completed", task["status"])
}

func TestE2E_TaskFail(t *testing.T) {
	body := map[string]interface{}{
		"id":              "task-2",
		"source_agent":    "agent-1",
		"target_type":     "agent",
		"target_value":    "agent-2",
		"mailbox_id":      "mb-1",
		"idempotency_key": "e2e-key-2",
		"envelope": map[string]interface{}{
			"janus_version": "0.1.0",
			"task_id":       "task-2",
			"tenant_id":     testTenant,
			"source_agent":  "agent-1",
			"target":        map[string]string{"type": "agent", "value": "agent-2"},
			"payload":       map[string]string{"type": "json", "content": `{}`},
			"trace":         map[string]string{"trace_id": "trace-2"},
		},
	}
	resp := mustRequest(t, "POST", "/v1/tenants/"+testTenant+"/tasks", body)
	resp.Body.Close()
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	resp = mustRequest(t, "POST", "/v1/tenants/"+testTenant+"/tasks/task-2/start", nil)
	resp.Body.Close()

	failBody := map[string]string{"code": "TIMEOUT", "message": "agent timed out"}
	resp = mustRequest(t, "POST", "/v1/tenants/"+testTenant+"/tasks/task-2/fail", failBody)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp = mustRequest(t, "GET", "/v1/tenants/"+testTenant+"/tasks/task-2", nil)
	defer resp.Body.Close()
	var task map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&task)
	assert.Equal(t, "failed", task["status"])
}

func TestE2E_TaskCancel(t *testing.T) {
	body := map[string]interface{}{
		"id":              "task-3",
		"source_agent":    "agent-1",
		"target_type":     "agent",
		"target_value":    "agent-2",
		"mailbox_id":      "mb-1",
		"idempotency_key": "e2e-key-3",
		"envelope": map[string]interface{}{
			"janus_version": "0.1.0",
			"task_id":       "task-3",
			"tenant_id":     testTenant,
			"source_agent":  "agent-1",
			"target":        map[string]string{"type": "agent", "value": "agent-2"},
			"payload":       map[string]string{"type": "json", "content": `{}`},
			"trace":         map[string]string{"trace_id": "trace-3"},
		},
	}
	resp := mustRequest(t, "POST", "/v1/tenants/"+testTenant+"/tasks", body)
	resp.Body.Close()

	resp = mustRequest(t, "POST", "/v1/tenants/"+testTenant+"/tasks/task-3/cancel", nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp = mustRequest(t, "GET", "/v1/tenants/"+testTenant+"/tasks/task-3", nil)
	defer resp.Body.Close()
	var task map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&task)
	assert.Equal(t, "cancelled", task["status"])
}

func TestE2E_TaskIdempotency(t *testing.T) {
	body := map[string]interface{}{
		"id":              "task-idem-1",
		"source_agent":    "agent-1",
		"target_type":     "agent",
		"target_value":    "agent-2",
		"mailbox_id":      "mb-1",
		"idempotency_key": "e2e-idem-key",
		"envelope": map[string]interface{}{
			"janus_version": "0.1.0",
			"task_id":       "task-idem-1",
			"tenant_id":     testTenant,
			"source_agent":  "agent-1",
			"target":        map[string]string{"type": "agent", "value": "agent-2"},
			"payload":       map[string]string{"type": "json", "content": `{}`},
			"trace":         map[string]string{"trace_id": "trace-idem"},
		},
	}
	resp := mustRequest(t, "POST", "/v1/tenants/"+testTenant+"/tasks", body)
	resp.Body.Close()
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	resp = mustRequest(t, "POST", "/v1/tenants/"+testTenant+"/tasks", body)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	assert.Equal(t, "existing", result["status"])
}

func TestE2E_HeartbeatDriver(t *testing.T) {
	ctx := natsdriver.ContextWithTenant(context.Background(), testTenant)
	err := redisDrv.Ping(ctx, testTenant, "agent-1")
	require.NoError(t, err)

	ts, err := redisDrv.GetLastHeartbeat(ctx, testTenant, "agent-1")
	require.NoError(t, err)
	assert.WithinDuration(t, time.Now(), *ts, 2*time.Second)
}

func TestE2E_NATSQueueDriver(t *testing.T) {
	ctx := natsdriver.ContextWithTenant(context.Background(), testTenant)

	err := natsDrv.EnsureTenant(ctx, testTenant)
	require.NoError(t, err)

	err = natsDrv.EnsureMailbox(ctx, core.MailboxSpec{
		TenantID:         testTenant,
		MailboxID:        "mb-e2e-nats",
		AgentID:          "agent-1",
		MaxConcurrency:   1,
		ACKWaitSeconds:   30,
		MaxDeliver:       3,
		RetentionSeconds: 3600,
	})
	require.NoError(t, err)

	err = natsDrv.EnsureConsumer(ctx, core.ConsumerSpec{
		TenantID:       testTenant,
		MailboxID:      "mb-e2e-nats",
		ACKWaitSeconds: 30,
		MaxDeliver:     3,
		MaxACKPending:  10,
	})
	require.NoError(t, err)

	err = natsDrv.PublishTask(ctx, core.TaskMessage{
		TenantID:  testTenant,
		MailboxID: "mb-e2e-nats",
		TaskID:    "nats-task-1",
		Priority:  core.PriorityNormal,
		Payload:   []byte(`{"task":"nats-test"}`),
	})
	require.NoError(t, err)

	deliveries, err := natsDrv.FetchTasks(ctx, "mb-e2e-nats", core.FetchOptions{
		MaxMessages: 1,
		WaitTime:    5 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, deliveries, 1)
	assert.Equal(t, "nats-task-1", deliveries[0].TaskID)

	err = natsDrv.AckTask(ctx, deliveries[0].DeliveryRef)
	require.NoError(t, err)
}

func mustRequest(t *testing.T, method, path string, body interface{}) *http.Response {
	t.Helper()
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, server.URL+path, reader)
	require.NoError(t, err)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func newTestRouter(tenantH *handler.TenantHandler, agentH *handler.AgentHandler, taskH *handler.TaskHandler, mailboxH *handler.MailboxHandler) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/v1/tenants", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			tenantH.Create(w, r)
			return
		}
		http.NotFound(w, r)
	})

	mux.HandleFunc("/v1/tenants/", func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path

		switch {
		case hasSeg(p, "mailboxes") && lastIs(p, "mailboxes"):
			postOnly(w, r, mailboxH.Create)
		case hasSeg(p, "mailboxes"):
			getOnly(w, r, mailboxH.Get)
		case hasSeg(p, "heartbeat"):
			postOnly(w, r, agentH.Heartbeat)
		case hasSeg(p, "agents") && !lastIs(p, "agents"):
			getOnly(w, r, agentH.Get)
		case lastIs(p, "agents"):
			if r.Method == http.MethodPost {
				agentH.Register(w, r)
			} else {
				agentH.List(w, r)
			}
		case hasSfx(p, "/start"):
			postOnly(w, r, taskH.Start)
		case hasSfx(p, "/complete"):
			postOnly(w, r, taskH.Complete)
		case hasSfx(p, "/fail"):
			postOnly(w, r, taskH.Fail)
		case hasSfx(p, "/cancel"):
			postOnly(w, r, taskH.Cancel)
		case hasSeg(p, "tasks") && !lastIs(p, "tasks"):
			getOnly(w, r, taskH.Get)
		case lastIs(p, "tasks"):
			postOnly(w, r, taskH.Create)
		default:
			if r.Method == http.MethodGet {
				tenantH.Get(w, r)
			} else {
				http.NotFound(w, r)
			}
		}
	})

	return mux
}

func postOnly(w http.ResponseWriter, r *http.Request, fn http.HandlerFunc) {
	if r.Method == http.MethodPost {
		fn(w, r)
	} else {
		http.NotFound(w, r)
	}
}

func getOnly(w http.ResponseWriter, r *http.Request, fn http.HandlerFunc) {
	if r.Method == http.MethodGet {
		fn(w, r)
	} else {
		http.NotFound(w, r)
	}
}

func hasSeg(path, seg string) bool {
	for _, s := range strings.Split(path, "/") {
		if s == seg {
			return true
		}
	}
	return false
}

func lastIs(path, seg string) bool {
	parts := strings.Split(strings.TrimRight(path, "/"), "/")
	return len(parts) > 0 && parts[len(parts)-1] == seg
}

func hasSfx(path, s string) bool {
	return strings.HasSuffix(strings.TrimRight(path, "/"), s)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func cleanDB(db *sql.DB) {
	tables := []string{
		"schema_migrations",
		"outbox_events",
		"audit_event_projection",
		"approvals",
		"policy_rules",
		"budgets",
		"task_attempts",
		"task_errors",
		"events",
		"tasks",
		"mailboxes",
		"agent_capabilities",
		"agents",
		"tenants",
	}
	for _, t := range tables {
		db.Exec("DROP TABLE IF EXISTS " + t + " CASCADE")
	}
}
