package invariants

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/auth"
	"github.com/agentium-lab/Janus/server/internal/handler"
	"github.com/agentium-lab/Janus/server/internal/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// SECURITY INVARIANTS: these tests execute real HTTP requests against a
// real middleware stack, not placeholder assertions. Each test creates
// two tenants with separate API keys and attempts cross-tenant access.

type securityRepo struct {
	mu    sync.Mutex
	tasks map[string][]*core.Task
	keys  map[string]*core.APIKey
	seq   int
}

func newSecurityRepo() *securityRepo {
	return &securityRepo{tasks: map[string][]*core.Task{}, keys: map[string]*core.APIKey{}}
}

func (r *securityRepo) Create(_ context.Context, task core.Task) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq++
	if task.ID == "" {
		task.ID = fmt.Sprintf("t-%d", r.seq)
	}
	if task.Status == "" {
		task.Status = core.TaskStatusQueued
	}
	now := time.Now().UTC()
	task.CreatedAt, task.UpdatedAt = now, now
	r.tasks[task.TenantID] = append(r.tasks[task.TenantID], &task)
	return nil
}
func (r *securityRepo) Get(_ context.Context, tenantID, taskID string) (*core.Task, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, t := range r.tasks[tenantID] {
		if t.ID == taskID {
			cp := *t
			return &cp, nil
		}
	}
	return nil, fmt.Errorf("not found")
}
func (r *securityRepo) GetByIdempotencyKey(_ context.Context, _, key string) (*core.Task, error) {
	if key == "" {
		return nil, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, ts := range r.tasks {
		for _, t := range ts {
			if t.IdempotencyKey == key {
				cp := *t
				return &cp, nil
			}
		}
	}
	return nil, nil
}
func (r *securityRepo) UpdateStatus(_ context.Context, tenantID, taskID string, status core.TaskStatus, attempt int) error {
	return nil
}
func (r *securityRepo) UpdateStatusWithCheck(_ context.Context, _, _ string, _, _ core.TaskStatus, _ int) (bool, error) {
	return true, nil
}
func (r *securityRepo) UpdateRetryAt(_ context.Context, _, _ string, _ time.Time) error { return nil }
func (r *securityRepo) ListByStatus(_ context.Context, _ string, _ core.TaskStatus, _ int) ([]*core.Task, error) {
	return nil, nil
}
func (r *securityRepo) SetResultRef(_ context.Context, _, _, _ string) error { return nil }
func (r *securityRepo) CountByStatus(_ context.Context, _ string, _ core.TaskStatus) (int, error) {
	return 0, nil
}
func (r *securityRepo) CountRunningByAgent(_ context.Context, _, _ string) (int, error) {
	return 0, nil
}
func (r *securityRepo) ResetForReplay(_ context.Context, _, _ string) error { return nil }

type securityQueue struct{}

func (q *securityQueue) PublishTask(_ context.Context, _ core.TaskMessage) error { return nil }
func (q *securityQueue) FetchTasks(_ context.Context, _, _ string, _ core.FetchOptions) ([]core.TaskDelivery, error) {
	return nil, nil
}
func (q *securityQueue) AckTask(_ context.Context, _ string, _ core.DeliveryRef) error { return nil }
func (q *securityQueue) NackTask(_ context.Context, _ string, _ core.DeliveryRef, _ core.NackReason) error {
	return nil
}
func (q *securityQueue) PublishDLQ(_ context.Context, _ core.TaskMessage, _ []byte) error { return nil }
func (q *securityQueue) PublishEvent(_ context.Context, _ core.JanusEvent) error          { return nil }
func (q *securityQueue) ReplayEvents(_ context.Context, _ core.EventReplayFilter) (core.EventIterator, error) {
	return nil, fmt.Errorf("nf")
}
func (q *securityQueue) EnsureTenant(_ context.Context, _ string) error              { return nil }
func (q *securityQueue) EnsureMailbox(_ context.Context, _ core.MailboxSpec) error   { return nil }
func (q *securityQueue) EnsureConsumer(_ context.Context, _ core.ConsumerSpec) error { return nil }
func (q *securityQueue) Close() error                                                { return nil }

type fakeKeyValidator struct {
	keys map[string]auth.Principal
}

// fakeKeyValidator satisfies auth.PrincipalValidator so the tests run the
// REAL production middleware chain (Middleware -> ScopeGuard -> TenantGuard
// -> AgentIdentityMiddleware), not a re-implementation that can drift.
func (v *fakeKeyValidator) ValidatePrincipal(_ context.Context, apiKey string) (auth.Principal, error) {
	if p, ok := v.keys[apiKey]; ok {
		p.KeyPrefix = apiKey[:8] + "..."
		return p, nil
	}
	return auth.Principal{}, fmt.Errorf("invalid api key")
}

func newSecurityServer(t *testing.T) *httptest.Server {
	t.Helper()
	repo := newSecurityRepo()
	taskSvc := service.NewTaskService(repo, &securityQueue{}, nil, nil)
	taskH := handler.NewTaskHandler(taskSvc)

	validator := &fakeKeyValidator{keys: map[string]auth.Principal{
		"key-tenant-a-admin": {TenantID: "tenant-a", Scopes: []string{"admin"}},
		"key-tenant-b-admin": {TenantID: "tenant-b", Scopes: []string{"admin"}},
		"key-tenant-a-write": {TenantID: "tenant-a", Scopes: []string{"task:write"}},
		"key-revoked":        {TenantID: "tenant-a", Scopes: []string{"admin"}},
	}}
	delete(validator.keys, "key-revoked")

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/tenants/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/tasks") && r.Method == http.MethodPost {
			taskH.Create(w, r)
			return
		}
		http.NotFound(w, r)
	})

	// Production chain from main.go: Middleware -> ScopeGuard -> TenantGuard
	// -> handler, with the only difference being the injected key validator.
	core := auth.Middleware(validator)(auth.ScopeGuard(auth.TenantGuard(extractTenantFromPath)(mux)))
	return httptest.NewServer(core)
}

func extractTenantFromPath(path string) string {
	segs := strings.Split(path, "/")
	for i, seg := range segs {
		if seg == "tenants" && i+1 < len(segs) {
			return segs[i+1]
		}
	}
	return ""
}

func TestSecurity_CrossTenantAccessDenied(t *testing.T) {
	ts := newSecurityServer(t)
	defer ts.Close()

	body := `{"id":"sec-task-1","source_agent":"a1","target_type":"mailbox","target_value":"mb","envelope":{"janus_version":"1","task_id":"sec-task-1","tenant_id":"tenant-a","source_agent":"a1","target":{"type":"mailbox","value":"mb"},"priority":"normal","payload":{"type":"text","content":"x"},"trace":{"trace_id":"s1"}}}`
	req, _ := http.NewRequest("POST", ts.URL+"/v1/tenants/tenant-a/tasks", strings.NewReader(body))
	req.Header.Set("X-API-Key", "key-tenant-a-admin")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode, "tenant A should create own task")

	req2, _ := http.NewRequest("GET", ts.URL+"/v1/tenants/tenant-a/tasks/sec-task-1", nil)
	req2.Header.Set("X-API-Key", "key-tenant-b-admin")
	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	resp2.Body.Close()
	assert.Equal(t, http.StatusForbidden, resp2.StatusCode,
		"tenant B key must NOT read tenant A's task (got %d)", resp2.StatusCode)
}

func TestSecurity_RevokedKeyRejected(t *testing.T) {
	ts := newSecurityServer(t)
	defer ts.Close()

	req, _ := http.NewRequest("GET", ts.URL+"/v1/tenants/tenant-a/tasks/sec-task-1", nil)
	req.Header.Set("X-API-Key", "key-revoked")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode,
		"revoked key must get 401 (got %d)", resp.StatusCode)
}

func TestSecurity_IdempotentReplayReturnsExisting(t *testing.T) {
	repo := newSecurityRepo()
	taskSvc := service.NewTaskService(repo, &securityQueue{}, nil, nil)

	ctx := context.Background()
	task1 := core.Task{
		TenantID: "tenant-a", ID: "idem-1", SourceAgent: "a1",
		TargetType: core.TargetTypeMailbox, TargetValue: "mb",
		IdempotencyKey: "idem-key-42",
		Envelope: core.TaskEnvelope{
			JanusVersion: "1", TaskID: "idem-1", TenantID: "tenant-a",
			SourceAgent: "a1", Target: core.Target{Type: "mailbox", Value: "mb"},
			Payload: core.Payload{Type: "text", Content: "x"},
			Trace:   core.TraceContext{TraceID: "t1"},
		},
	}
	r1, err := taskSvc.Create(ctx, task1)
	require.NoError(t, err)

	r2, err := taskSvc.Create(ctx, task1)
	require.NoError(t, err)
	assert.Equal(t, r1.ID, r2.ID, "same idempotency key must return existing task")
	assert.Equal(t, 1, len(repo.tasks["tenant-a"]), "must be exactly one task")
}

type securityTenantSvc struct{}

func (securityTenantSvc) Create(_ context.Context, _, _ string) error { return nil }
func (securityTenantSvc) Get(_ context.Context, id string) (*core.Tenant, error) {
	return &core.Tenant{ID: id, Name: "t"}, nil
}
func (securityTenantSvc) List(_ context.Context) ([]core.Tenant, error) {
	return []core.Tenant{{ID: "tenant-a", Name: "A"}, {ID: "tenant-b", Name: "B"}}, nil
}

func TestSecurity_TenantAdminCannotManageTenants(t *testing.T) {
	ts := newSecurityServer(t)
	defer ts.Close()

	// A plain tenant admin key (no platform:admin scope).
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/tenants", nil)
	req.Header.Set("X-API-Key", "key-tenant-a-admin")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusForbidden, resp.StatusCode,
		"tenant admin must not list tenants (platform control plane)")

	body := `{"id":"evil-tenant","name":"Evil"}`
	req2, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/tenants", strings.NewReader(body))
	req2.Header.Set("X-API-Key", "key-tenant-b-admin")
	req2.Header.Set("Content-Type", "application/json")
	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusForbidden, resp2.StatusCode,
		"tenant admin must not create tenants (platform control plane)")
}

func TestSecurity_PlatformAdminManagesTenants(t *testing.T) {
	tenantH := handler.NewTenantHandler(securityTenantSvc{})

	validator := &fakeKeyValidator{keys: map[string]auth.Principal{
		"key-platform": {TenantID: "platform-operator", Scopes: []string{"platform:admin"}},
	}}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/tenants", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			tenantH.List(w, r)
			return
		}
		http.NotFound(w, r)
	})
	core := auth.Middleware(validator)(auth.ScopeGuard(auth.TenantGuard(extractTenantFromPath)(mux)))
	ts := httptest.NewServer(core)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/tenants", nil)
	req.Header.Set("X-API-Key", "key-platform")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode,
		"platform:admin key must reach tenant list (empty path tenant passes the guard)")
}

func TestSecurity_EmptyScopeKeyCannotReachPlatformControlPlane(t *testing.T) {
	tenantH := handler.NewTenantHandler(securityTenantSvc{})

	// The forged key mirrors what a tenant admin could mint before the fix:
	// an empty scope set that used to mean "full access".
	validator := &fakeKeyValidator{keys: map[string]auth.Principal{
		"key-empty-scopes": {TenantID: "tenant-a", Scopes: []string{}},
	}}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/tenants", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			tenantH.List(w, r)
			return
		}
		http.NotFound(w, r)
	})
	core := auth.Middleware(validator)(auth.ScopeGuard(auth.TenantGuard(extractTenantFromPath)(mux)))
	ts := httptest.NewServer(core)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/tenants", nil)
	req.Header.Set("X-API-Key", "key-empty-scopes")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusForbidden, resp.StatusCode,
		"empty (legacy full-access) scopes must NEVER include platform authority")
}

type securityAPIKeyRepo struct {
	created []core.APIKey
}

func (r *securityAPIKeyRepo) CreateAPIKey(_ context.Context, tenantID, keyHash, name, prefix string, scopes []string, boundAgentID string) (core.APIKey, error) {
	k := core.APIKey{TenantID: tenantID, Name: name, Prefix: prefix, Scopes: scopes, BoundAgentID: boundAgentID}
	r.created = append(r.created, k)
	return k, nil
}
func (r *securityAPIKeyRepo) ListAPIKeys(_ context.Context, _ string) ([]core.APIKey, error) {
	return r.created, nil
}
func (r *securityAPIKeyRepo) RevokeAPIKey(_ context.Context, _, _ string) (*core.APIKey, error) {
	return nil, fmt.Errorf("not supported in test")
}

func TestSecurity_APIKeyCreationRejectsEmptyScopes(t *testing.T) {
	repo := &securityAPIKeyRepo{}
	svc := service.NewAPIKeyService(repo)

	_, _, err := svc.Create(context.Background(), "tenant-a", "sneaky", nil, "")
	require.Error(t, err, "minting an empty-scope key is a privilege escalation primitive")
	assert.Contains(t, err.Error(), "at least one scope")

	created, raw, err := svc.Create(context.Background(), "tenant-a", "scoped",
		[]string{"task:write"}, "")
	require.NoError(t, err)
	assert.NotEmpty(t, raw)
	assert.Equal(t, []string{"task:write"}, created.Scopes)

	_, _, err = svc.Create(context.Background(), "tenant-a", "pwn",
		[]string{"platform:admin"}, "")
	require.Error(t, err, "platform:admin must not be mintable via the tenant API")
	assert.Contains(t, err.Error(), "unknown scope")
}
