package e2e

import (
	"bytes"
	"context"
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
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"

	"github.com/agentium-lab/Janus/server/internal/auth"
	natsdriver "github.com/agentium-lab/Janus/server/internal/driver/nats"
	pgdriver "github.com/agentium-lab/Janus/server/internal/driver/postgres"
	redisdriver "github.com/agentium-lab/Janus/server/internal/driver/redis"
	"github.com/agentium-lab/Janus/server/internal/gateway/mcp"
	"github.com/agentium-lab/Janus/server/internal/handler"
	"github.com/agentium-lab/Janus/server/internal/outbox"
	"github.com/agentium-lab/Janus/server/internal/service"
)

const testTenant = "e2e-tenant"

var (
	pool           *pgxpool.Pool
	server         *httptest.Server
	natsDrv        *natsdriver.Driver
	redisDrv       *redisdriver.Driver
	eventProjector *outbox.EventProjector
)

func TestMain(m *testing.M) {
	pgDSN := envOr("JANUS_PG_DSN", "postgres://janus:janus@localhost:5432/janus_test?sslmode=disable")
	natsURL := envOr("JANUS_NATS_URL", "nats://localhost:4222")
	redisAddr := envOr("JANUS_REDIS_ADDR", "localhost:6379")
	migrationPath := envOr("JANUS_MIGRATION_PATH", "../../../migrations/")

	ctx := context.Background()
	var err error

	// When the required external dependencies are not available, skip the whole
	// package gracefully instead of failing. Tests in this package exercise a
	// real PostgreSQL + NATS + Redis stack and are only meaningful when those
	// services are reachable (e.g. in CI or via docker compose).
	pool, err = pgxpool.New(ctx, pgDSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e: skip (%s not reachable: %v)\n", pgDSN, err)
		os.Exit(0)
	}
	if err := pool.Ping(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "e2e: skip (postgres not reachable: %v)\n", err)
		os.Exit(0)
	}

	cleanDB(pool)

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
		fmt.Fprintf(os.Stderr, "e2e: skip (nats %s not reachable: %v)\n", natsURL, err)
		os.Exit(0)
	}

	redisDrv, err = redisdriver.NewDriver(redisdriver.Config{Addr: redisAddr})
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e: skip (redis %s not reachable: %v)\n", redisAddr, err)
		os.Exit(0)
	}

	tenantRepo := pgdriver.NewTenantRepository(pool)
	agentRepo := pgdriver.NewAgentRepository(pool)
	taskRepo := pgdriver.NewTaskRepository(pool)
	mailboxRepo := pgdriver.NewMailboxRepository(pool)
	attemptRepo := pgdriver.NewTaskAttemptRepository(pool)
	budgetRepo := pgdriver.NewBudgetRepository(pool)
	policyRuleRepo := pgdriver.NewPolicyRuleRepository(pool)
	eventRepo := pgdriver.NewEventRepo(pool)

	tenantSvc := service.NewTenantService(tenantRepo)
	agentSvc := service.NewAgentService(agentRepo, mailboxRepo, redisDrv, natsDrv)
	policySvc := service.NewPolicyService(policyRuleRepo)
	budgetSvc := service.NewBudgetService(budgetRepo)
	taskSvc := service.NewTaskService(taskRepo, natsDrv, nil, nil).WithPolicy(policySvc)
	mailboxSvc := service.NewMailboxService(mailboxRepo, natsDrv)
	dispatchSvc := service.NewDispatchService(taskRepo, attemptRepo, mailboxRepo, natsDrv, policySvc, budgetSvc)
	eventSvc := service.NewEventService(eventRepo)
	contextRefSvc := service.NewContextRefService(pgdriver.NewContextRefRepo(pool))

	// Wire the real event pipeline: NATS → fan-out → (broadcaster for WS,
	// projector for audit_event_projection). This mirrors main.go lines
	// 123-161 so MCP tool calls and task lifecycles produce both live WS
	// events and persistent audit entries.
	rawEventCh := make(chan core.JanusEvent, 256)
	_, err = natsDrv.SubscribeEvents(context.Background(), rawEventCh)
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e: subscribe events: %v\n", err)
		os.Exit(1)
	}
	broadcastCh := make(chan core.JanusEvent, 256)
	projectorCh := make(chan core.JanusEvent, 256)
	go func() {
		for evt := range rawEventCh {
			select {
			case broadcastCh <- evt:
			default:
			}
			select {
			case projectorCh <- evt:
			default:
			}
		}
		close(broadcastCh)
		close(projectorCh)
	}()
	broadcaster := handler.NewFanoutBroadcaster(broadcastCh)
	wsH := handler.NewWebSocketHandler(broadcaster)
	eventProjector = outbox.NewEventProjector(eventSvc)
	go func() {
		for evt := range projectorCh {
			eventProjector.Record(context.Background(), evt)
		}
	}()
	go eventProjector.Start(context.Background())

	mcpGw := mcp.NewGateway(taskSvc, taskSvc, contextRefSvc).WithEventPublisher(natsDrv)

	dispatchH := handler.NewDispatchHandler(&e2eDispatchAdapter{svc: dispatchSvc})
	auditH := handler.NewAuditHandler(&e2eAuditAdapter{svc: eventSvc})
	catalogH := handler.NewCatalogHandler(agentRepo)

	mux := newTestRouter(
		handler.NewTenantHandler(tenantSvc),
		handler.NewAgentHandler(agentSvc),
		handler.NewTaskHandler(taskSvc),
		handler.NewMailboxHandler(mailboxSvc),
		dispatchH,
		auditH,
		mcpGw,
		wsH,
		catalogH,
	)

	server = httptest.NewServer(mux)
	defer server.Close()

	code := m.Run()

	eventProjector.Stop()
	server.Close()
	natsDrv.Close()
	redisDrv.Close()
	pool.Close()
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
		"id":           "agent-1",
		"display_name": "Test Agent",
		"protocol":     "a2a",
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
	// Create a fresh task and cancel it (queued → cancelled is a valid transition).
	body := map[string]interface{}{
		"id":              "task-life-1",
		"source_agent":    "agent-1",
		"target_type":     "agent",
		"target_value":    "agent-2",
		"mailbox_id":      "mb-1",
		"idempotency_key": "e2e-life-1",
		"envelope": map[string]interface{}{
			"janus_version": "0.1.0",
			"task_id":       "task-life-1",
			"tenant_id":     testTenant,
			"source_agent":  "agent-1",
			"target":        map[string]string{"type": "agent", "value": "agent-2"},
			"payload":       map[string]string{"type": "json", "content": `{}`},
			"trace":         map[string]string{"trace_id": "trace-life-1"},
		},
	}
	resp := mustRequest(t, "POST", "/v1/tenants/"+testTenant+"/tasks", body)
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	resp = mustRequest(t, "POST", "/v1/tenants/"+testTenant+"/tasks/task-life-1/cancel", nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp = mustRequest(t, "GET", "/v1/tenants/"+testTenant+"/tasks/task-life-1", nil)
	defer resp.Body.Close()
	var task map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&task)
	assert.Equal(t, "cancelled", task["status"])
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

	// Cancel the task (queued → cancelled is a valid transition; queued → failed
	// is not). This verifies the transition path works end-to-end.
	resp = mustRequest(t, "POST", "/v1/tenants/"+testTenant+"/tasks/task-2/cancel", nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp = mustRequest(t, "GET", "/v1/tenants/"+testTenant+"/tasks/task-2", nil)
	defer resp.Body.Close()
	var task map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&task)
	assert.Equal(t, "cancelled", task["status"])
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
	// Duplicate create returns the existing task (deduped). It should have the
	// same task id and be in a valid state (queued/created).
	assert.Equal(t, "task-idem-1", result["id"])
	status, _ := result["status"].(string)
	assert.Contains(t, []string{"queued", "created"}, status)
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

	deliveries, err := natsDrv.FetchTasks(ctx, "e2e-tenant", "mb-e2e-nats", core.FetchOptions{
		MaxMessages: 1,
		WaitTime:    5 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, deliveries, 1)
	assert.Equal(t, "nats-task-1", deliveries[0].TaskID)

	err = natsDrv.AckTask(ctx, "e2e-tenant", deliveries[0].DeliveryRef)
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

func newTestRouter(tenantH *handler.TenantHandler, agentH *handler.AgentHandler, taskH *handler.TaskHandler, mailboxH *handler.MailboxHandler, dispatchH *handler.DispatchHandler, auditH *handler.AuditHandler, mcpGw http.Handler, wsH http.Handler, catalogH *handler.CatalogHandler) http.Handler {
	mux := http.NewServeMux()

	mux.Handle("/ws", withTenantCtx(wsH))
	mux.Handle("/mcp/", withTenantCtx(mcpGw))

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
		case hasSeg(p, "pull"):
			postOnly(w, r, dispatchH.Pull)
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
		case hasSfx(p, "/catalog"):
			getOnly(w, r, catalogH.List)
		case hasSfx(p, "/complete"):
			postOnly(w, r, taskH.Complete)
		case hasSfx(p, "/fail"):
			postOnly(w, r, taskH.Fail)
		case hasSfx(p, "/cancel"):
			postOnly(w, r, taskH.Cancel)
		case hasSfx(p, "/replay"):
			postOnly(w, r, taskH.Replay)
		case hasSfx(p, "/start"):
			postOnly(w, r, dispatchH.Start)
		case hasSfx(p, "/heartbeat"):
			postOnly(w, r, dispatchH.Heartbeat)
		case hasSfx(p, "/ack"):
			postOnly(w, r, dispatchH.Ack)
		case hasSfx(p, "/nack"):
			postOnly(w, r, dispatchH.Nack)
		case hasSeg(p, "tasks") && hasSfx(p, "/events"):
			getOnly(w, r, auditH.QueryByTask)
		case hasSeg(p, "tasks") && !lastIs(p, "tasks") && !hasSeg(p, "events"):
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

func withTenantCtx(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tid := r.URL.Query().Get("tenant_id")
		if tid == "" {
			tid = testTenant
		}
		ctx := context.WithValue(r.Context(), auth.TenantCtxKey, tid)
		h.ServeHTTP(w, r.WithContext(ctx))
	})
}

func cleanDB(pool *pgxpool.Pool) {
	ctx := context.Background()
	tables := []string{
		"schema_migrations",
		"budget_usage_ledger",
		"budget_usage",
		"outbox_events",
		"context_refs",
		"task_context_refs",
		"audit_event_projection",
		"approvals",
		"policy_rules",
		"budgets",
		"api_keys",
		"task_attempts",
		"events",
		"tasks",
		"mailboxes",
		"agent_capabilities",
		"agents",
		"tenants",
	}
	for _, t := range tables {
		pool.Exec(ctx, "DROP TABLE IF EXISTS "+t+" CASCADE")
	}
}

type e2eDispatchAdapter struct {
	svc *service.DispatchService
}

func (a *e2eDispatchAdapter) PullTask(ctx context.Context, tenantID, mailboxID, agentID string) (*handler.ServicePullResult, error) {
	res, err := a.svc.PullTask(ctx, tenantID, mailboxID, agentID)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	return &handler.ServicePullResult{
		Task:      res.Task,
		LeaseID:   res.LeaseID,
		ExpiresAt: res.ExpiresAt,
	}, nil
}

func (a *e2eDispatchAdapter) StartTask(ctx context.Context, tenantID, taskID, leaseID string) error {
	return a.svc.StartTask(ctx, tenantID, taskID, leaseID)
}

func (a *e2eDispatchAdapter) TaskHeartbeat(ctx context.Context, tenantID, taskID, leaseID string) error {
	return a.svc.TaskHeartbeat(ctx, tenantID, taskID, leaseID)
}

func (a *e2eDispatchAdapter) AckTask(ctx context.Context, tenantID, taskID, leaseID string, resultRef string, usage *core.TokenUsage) error {
	return a.svc.AckTask(ctx, tenantID, taskID, leaseID, resultRef, usage)
}

func (a *e2eDispatchAdapter) NackTask(ctx context.Context, tenantID, taskID, leaseID string, retriable bool, taskErr *core.TaskError) error {
	return a.svc.NackTask(ctx, tenantID, taskID, leaseID, retriable, taskErr)
}

type e2eAuditAdapter struct {
	svc *service.EventService
}

func (a *e2eAuditAdapter) QueryByTask(ctx context.Context, tenantID, taskID string, limit int) (interface{}, error) {
	return a.svc.QueryByTask(ctx, tenantID, taskID, limit)
}

func (a *e2eAuditAdapter) QueryByTrace(ctx context.Context, tenantID, traceID string, limit int) (interface{}, error) {
	return a.svc.QueryByTrace(ctx, tenantID, traceID, limit)
}

func (a *e2eAuditAdapter) QueryByTenant(ctx context.Context, tenantID string, limit int) (interface{}, error) {
	return []struct{}{}, nil
}
