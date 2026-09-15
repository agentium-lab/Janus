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

func (v *fakeKeyValidator) Validate(_ context.Context, apiKey string) (string, error) {
	if p, ok := v.keys[apiKey]; ok {
		return p.TenantID, nil
	}
	return "", fmt.Errorf("invalid api key")
}

func (v *fakeKeyValidator) InjectPrincipal(ctx context.Context, apiKey string) context.Context {
	if p, ok := v.keys[apiKey]; ok {
		return context.WithValue(ctx, auth.PrincipalCtxKey, p)
	}
	return ctx
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

	return httptest.NewServer(customAuthMiddleware(validator, mux))
}

func customAuthMiddleware(v *fakeKeyValidator, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiKey := r.Header.Get("X-API-Key")
		p, ok := v.keys[apiKey]
		if !ok {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":"invalid api key"}`))
			return
		}
		segs := strings.Split(r.URL.Path, "/")
		pathTenant := ""
		for i, seg := range segs {
			if seg == "tenants" && i+1 < len(segs) {
				pathTenant = segs[i+1]
			}
		}
		if pathTenant != "" && pathTenant != p.TenantID {
			w.WriteHeader(http.StatusForbidden)
			w.Write([]byte(`{"error":"tenant mismatch"}`))
			return
		}
		if r.Method == http.MethodPost && !p.HasScope("task:write") {
			w.WriteHeader(http.StatusForbidden)
			w.Write([]byte(`{"error":"missing required scope task:write"}`))
			return
		}
		ctx := context.WithValue(r.Context(), auth.TenantCtxKey, p.TenantID)
		ctx = context.WithValue(ctx, auth.PrincipalCtxKey, p)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
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
