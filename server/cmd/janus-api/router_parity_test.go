package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/auth"
	"github.com/agentium-lab/Janus/server/internal/handler"
	"github.com/agentium-lab/Janus/server/internal/service"
)

// Parity suite for the tenant-subtree route table: every canonical
// (method, path) shape is pinned to the handler it dispatches to (via a
// marker body produced by a fake service) and the scope it demands
// (exercised through the real auth.ScopeGuard). Undefined nesting shapes
// are pinned by the negative tests at the bottom of this file.

type parityRoute struct {
	name   string
	method string
	path   string
	body   string
	scope  string
	code   int
	marker string
	header string // Content-Type substring, for the SSE route
}

func lackingScopeFor(scope string) string {
	if scope == auth.ScopeTaskWrite {
		return auth.ScopeAuditRead
	}
	return auth.ScopeTaskWrite
}

var parityRoutes = []parityRoute{
	// tenant
	{"tenant get", http.MethodGet, "/v1/tenants/acme", "", auth.ScopeTaskRead, 200, "MARKER-TENANT-GET", ""},
	// agents
	{"agent register", http.MethodPost, "/v1/tenants/acme/agents", `{"id":"MARKER-AGENT-REG"}`, auth.ScopeTaskWrite, 201, "MARKER-AGENT-REG", ""},
	{"agent list", http.MethodGet, "/v1/tenants/acme/agents", "", auth.ScopeTaskRead, 200, "MARKER-AGENT-LIST", ""},
	{"agent get", http.MethodGet, "/v1/tenants/acme/agents/a1", "", auth.ScopeTaskRead, 200, "MARKER-AGENT-GET", ""},
	{"agent heartbeat", http.MethodPost, "/v1/tenants/acme/agents/a1/heartbeat", "", auth.ScopeTaskWrite, 200, `"status":"ok"`, ""},
	// mailboxes
	{"mailbox create", http.MethodPost, "/v1/tenants/acme/mailboxes", `{"id":"MARKER-MAILBOX-CREATE","agent_id":"a1"}`, auth.ScopeTaskWrite, 201, "MARKER-MAILBOX-CREATE", ""},
	{"mailbox get", http.MethodGet, "/v1/tenants/acme/mailboxes/mb-1", "", auth.ScopeTaskRead, 200, "MARKER-MAILBOX-GET", ""},
	{"mailbox update", http.MethodPatch, "/v1/tenants/acme/mailboxes/mb-1", `{"max_concurrency":2}`, auth.ScopeTaskWrite, 200, `"status":"updated"`, ""},
	{"mailbox pause", http.MethodPost, "/v1/tenants/acme/mailboxes/mb-1/pause", "", auth.ScopeTaskWrite, 200, "MARKER-MAILBOX-GET", ""},
	{"mailbox resume", http.MethodPost, "/v1/tenants/acme/mailboxes/mb-1/resume", "", auth.ScopeTaskWrite, 200, "MARKER-MAILBOX-GET", ""},
	// dispatch
	{"pull", http.MethodPost, "/v1/tenants/acme/mailboxes/mb-1/pull", `{"agent_id":"a1"}`, auth.ScopeTaskWrite, 204, "", ""},
	// dlq
	{"dlq query", http.MethodGet, "/v1/tenants/acme/dlq", "", auth.ScopeTaskRead, 200, "MARKER-DLQ-QUERY", ""},
	{"dlq replay", http.MethodPost, "/v1/tenants/acme/dlq/t1/replay", "", auth.ScopeAdmin, 200, "MARKER-DLQ-REPLAY", ""},
	{"dlq discard", http.MethodPost, "/v1/tenants/acme/dlq/t1/discard", "", auth.ScopeAdmin, 200, `"status":"discarded"`, ""},
	// tasks
	{"task create", http.MethodPost, "/v1/tenants/acme/tasks", `{"id":"MARKER-TASK-CREATE","source_agent":"a1","target_type":"mailbox","target_value":"mb-1"}`, auth.ScopeTaskWrite, 201, "MARKER-TASK-CREATE", ""},
	{"task get", http.MethodGet, "/v1/tenants/acme/tasks/t1", "", auth.ScopeTaskRead, 200, "MARKER-TASK-GET", ""},
	{"task start", http.MethodPost, "/v1/tenants/acme/tasks/t1/start", `{"lease_id":"l1"}`, auth.ScopeTaskWrite, 200, `"status":"running"`, ""},
	{"task heartbeat", http.MethodPost, "/v1/tenants/acme/tasks/t1/heartbeat", `{"lease_id":"l1"}`, auth.ScopeTaskWrite, 200, `"status":"ok"`, ""},
	{"task ack", http.MethodPost, "/v1/tenants/acme/tasks/t1/ack", `{"lease_id":"l1"}`, auth.ScopeTaskWrite, 200, `"status":"completed"`, ""},
	{"task nack", http.MethodPost, "/v1/tenants/acme/tasks/t1/nack", `{"lease_id":"l1"}`, auth.ScopeTaskWrite, 200, `"status":"nacked"`, ""},
	{"task cancel", http.MethodPost, "/v1/tenants/acme/tasks/t1/cancel", "", auth.ScopeTaskWrite, 200, `"status":"cancelled"`, ""},
	{"task block", http.MethodPost, "/v1/tenants/acme/tasks/t1/block", `{"reason":"r"}`, auth.ScopeTaskWrite, 200, `"status":"blocked"`, ""},
	{"task unblock", http.MethodPost, "/v1/tenants/acme/tasks/t1/unblock", "", auth.ScopeTaskWrite, 200, `"status":"queued"`, ""},
	{"task replay", http.MethodPost, "/v1/tenants/acme/tasks/t1/replay", "", auth.ScopeTaskWrite, 200, "MARKER-TASK-REPLAY", ""},
	{"task complete", http.MethodPost, "/v1/tenants/acme/tasks/t1/complete", "", auth.ScopeAdmin, 200, `"status":"completed"`, ""},
	{"task fail", http.MethodPost, "/v1/tenants/acme/tasks/t1/fail", `{"code":"E","message":"m"}`, auth.ScopeAdmin, 200, `"status":"failed"`, ""},
	{"task progress", http.MethodPost, "/v1/tenants/acme/tasks/t1/progress", `{"message":"m","agent_id":"a1"}`, auth.ScopeTaskWrite, 202, `"status":"accepted"`, ""},
	{"task stream", http.MethodGet, "/v1/tenants/acme/tasks/t1/stream", "", auth.ScopeTaskRead, 200, "", "text/event-stream"},
	// audit
	{"task events", http.MethodGet, "/v1/tenants/acme/tasks/t1/events", "", auth.ScopeTaskRead, 200, "MARKER-AUDIT-TASK", ""},
	{"task traces", http.MethodGet, "/v1/tenants/acme/tasks/t1/traces", "", auth.ScopeAuditRead, 200, "MARKER-AUDIT-TRACE", ""},
	{"trace query", http.MethodGet, "/v1/tenants/acme/traces/tr-1", "", auth.ScopeAuditRead, 200, "MARKER-AUDIT-TRACE", ""},
	{"tenant events", http.MethodGet, "/v1/tenants/acme/events", "", auth.ScopeTaskRead, 200, "MARKER-AUDIT-TENANT", ""},
	{"audit replay", http.MethodPost, "/v1/tenants/acme/audit/replay", "", auth.ScopeAdmin, 400, "from and to query parameters required", ""},
	{"outbox retry-dead", http.MethodPost, "/v1/tenants/acme/outbox/retry-dead", "", auth.ScopeAdmin, 503, "UNAVAILABLE", ""},
	{"catalog", http.MethodGet, "/v1/tenants/acme/catalog", "", auth.ScopeTaskRead, 200, "MARKER-CATALOG", ""},
	// governance
	{"policy create", http.MethodPost, "/v1/tenants/acme/policy-rules", `{"condition":{},"action":{}}`, auth.ScopeAdmin, 201, "MARKER-POLICY", ""},
	{"policy list", http.MethodGet, "/v1/tenants/acme/policy-rules", "", auth.ScopeAdmin, 200, "MARKER-POLICY-LIST", ""},
	{"policy from template", http.MethodPost, "/v1/tenants/acme/policy-rules/templates", `{"template":"allow_agent_capability","agent_id":"a1","capability":"c1"}`, auth.ScopeAdmin, 201, "MARKER-POLICY", ""},
	{"budget upsert", http.MethodPost, "/v1/tenants/acme/budgets", `{"scope_type":"tenant","rpm":1}`, auth.ScopeAdmin, 200, "MARKER-BUDGET-UPSERT", ""},
	{"budget list", http.MethodGet, "/v1/tenants/acme/budgets", "", auth.ScopeAdmin, 200, "MARKER-BUDGET-LIST", ""},
	{"budget get scoped", http.MethodGet, "/v1/tenants/acme/budgets/mailbox/mb-1", "", auth.ScopeAdmin, 200, "MARKER-BUDGET-GET", ""},
	{"budget get one-segment", http.MethodGet, "/v1/tenants/acme/budgets/mailbox", "", auth.ScopeAdmin, 200, "MARKER-BUDGET-GET", ""},
	{"api key create", http.MethodPost, "/v1/tenants/acme/api-keys", `{"name":"k"}`, auth.ScopeAdmin, 201, "MARKER-APIKEY-RAW", ""},
	{"api key list", http.MethodGet, "/v1/tenants/acme/api-keys", "", auth.ScopeAdmin, 200, "MARKER-APIKEY-LIST", ""},
	{"api key revoke", http.MethodPost, "/v1/tenants/acme/api-keys/key-1/revoke", "", auth.ScopeAdmin, 200, "MARKER-APIKEY-REVOKED", ""},
	// approvals
	{"approval request", http.MethodPost, "/v1/tenants/acme/approvals", `{"task_id":"t1"}`, auth.ScopeTaskWrite, 201, "MARKER-APPROVAL-REQUEST", ""},
	{"approval list pending", http.MethodGet, "/v1/tenants/acme/approvals", "", auth.ScopeTaskRead, 200, "MARKER-APPROVAL-PENDING", ""},
	{"approval get", http.MethodGet, "/v1/tenants/acme/approvals/ap-1", "", auth.ScopeTaskRead, 200, "MARKER-APPROVAL-GET", ""},
	{"approval approve", http.MethodPost, "/v1/tenants/acme/approvals/ap-1/approve", `{"approver":"x"}`, auth.ScopeAdmin, 200, `"status":"approved"`, ""},
	{"approval reject", http.MethodPost, "/v1/tenants/acme/approvals/ap-1/reject", `{"approver":"x"}`, auth.ScopeAdmin, 200, `"status":"rejected"`, ""},
	// context refs
	{"context ref get", http.MethodGet, "/v1/tenants/acme/context-refs/ref-1", "", auth.ScopeTaskRead, 200, "MARKER-CTXREF-GET", ""},
	{"context ref attach", http.MethodPost, "/v1/tenants/acme/context-refs", `{"type":"document","uri":"s3://b/f"}`, auth.ScopeTaskWrite, 200, `"type":"document"`, ""},
	{"context ref detach", http.MethodDelete, "/v1/tenants/acme/context-refs/ref-1", "", auth.ScopeTaskWrite, 204, "", ""},
	{"context refs by task", http.MethodGet, "/v1/tenants/acme/tasks/t1/context-refs", "", auth.ScopeTaskRead, 200, "MARKER-CTXREF-LIST", ""},
}

// Marker fakes for every service the route table reaches. Service fakes
// embed their interface so only routed methods need bodies; each returns a
// marker that surfaces in the response body.

func (fakeAuditSvc) QueryByTask(context.Context, string, string, int) (interface{}, error) {
	return map[string]string{"marker": "MARKER-AUDIT-TASK"}, nil
}

func (fakeAuditSvc) QueryByTenant(context.Context, string, int) (interface{}, error) {
	return map[string]string{"marker": "MARKER-AUDIT-TENANT"}, nil
}

func (fakeMailboxSvc) Pause(context.Context, string, string) error  { return nil }
func (fakeMailboxSvc) Resume(context.Context, string, string) error { return nil }
func (fakeMailboxSvc) UpdateConfig(context.Context, string, string, int, int, int, int) error {
	return nil
}

func (fakeBudgetSvc) Get(context.Context, string, string, string) (*core.BudgetSpec, error) {
	return &core.BudgetSpec{ScopeID: "MARKER-BUDGET-GET"}, nil
}

func (fakeDLQSvc) ReplayDLQ(context.Context, string, string) (*core.Task, error) {
	return &core.Task{ID: "MARKER-DLQ-REPLAY"}, nil
}

func (fakeDLQSvc) DiscardDLQ(context.Context, string, string) error { return nil }

func (fakeDispatchSvc) StartTaskAs(context.Context, string, string, string, string) error {
	return nil
}

func (fakeDispatchSvc) TaskHeartbeatAs(context.Context, string, string, string, string) error {
	return nil
}

func (fakeDispatchSvc) AckTaskAs(context.Context, string, string, string, string, string, *core.TokenUsage) error {
	return nil
}

func (fakeDispatchSvc) NackTaskAs(context.Context, string, string, string, string, bool, *core.TaskError) error {
	return nil
}

type fakeAgentSvc struct{ handler.AgentService }

func (fakeAgentSvc) Register(context.Context, core.Agent) error { return nil }
func (fakeAgentSvc) Get(context.Context, string, string) (*core.Agent, error) {
	return &core.Agent{ID: "MARKER-AGENT-GET"}, nil
}
func (fakeAgentSvc) Heartbeat(context.Context, string, string) error { return nil }
func (fakeAgentSvc) List(context.Context, string) ([]*core.Agent, error) {
	return []*core.Agent{{ID: "MARKER-AGENT-LIST"}}, nil
}

type fakeTaskSvc struct{ handler.TaskService }

func (fakeTaskSvc) Create(_ context.Context, task core.Task) (*core.Task, error) {
	return &core.Task{ID: task.ID, Status: core.TaskStatusCreated}, nil
}
func (fakeTaskSvc) Get(context.Context, string, string) (*core.Task, error) {
	return &core.Task{ID: "MARKER-TASK-GET"}, nil
}
func (fakeTaskSvc) Cancel(context.Context, string, string) error        { return nil }
func (fakeTaskSvc) Block(context.Context, string, string, string) error { return nil }
func (fakeTaskSvc) Unblock(context.Context, string, string) error       { return nil }
func (fakeTaskSvc) Replay(context.Context, string, string) (*core.Task, error) {
	return &core.Task{ID: "MARKER-TASK-REPLAY"}, nil
}

// recordingTaskSvc records the arguments its methods are called with so
// tests can assert on what actually reached the service layer.
type recordingTaskSvc struct {
	fakeTaskSvc
	mu      sync.Mutex
	created []core.Task
	cancels [][2]string
}

func (r *recordingTaskSvc) Create(_ context.Context, task core.Task) (*core.Task, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.created = append(r.created, task)
	return &core.Task{ID: task.ID, Status: core.TaskStatusCreated}, nil
}

func (r *recordingTaskSvc) Cancel(_ context.Context, tenantID, taskID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cancels = append(r.cancels, [2]string{tenantID, taskID})
	return nil
}
func (fakeTaskSvc) Complete(context.Context, string, string) error { return nil }
func (fakeTaskSvc) Fail(context.Context, string, string, *core.TaskError) error {
	return nil
}

type fakeApprovalSvc struct{ handler.ApprovalService }

func (fakeApprovalSvc) RequestApproval(_ context.Context, ap core.Approval) (*core.Approval, error) {
	return &core.Approval{ID: "MARKER-APPROVAL-REQUEST", TenantID: ap.TenantID}, nil
}
func (fakeApprovalSvc) Approve(context.Context, string, string, string, string) error {
	return nil
}
func (fakeApprovalSvc) Reject(context.Context, string, string, string, string) error { return nil }
func (fakeApprovalSvc) Get(context.Context, string, string) (*core.Approval, error) {
	return &core.Approval{ID: "MARKER-APPROVAL-GET"}, nil
}
func (fakeApprovalSvc) ListPending(context.Context, string, int) ([]*core.Approval, error) {
	return []*core.Approval{{ID: "MARKER-APPROVAL-PENDING"}}, nil
}

type fakeAPIKeySvc struct{ handler.APIKeyService }

func (fakeAPIKeySvc) Create(context.Context, string, string, []string, string) (core.APIKey, string, error) {
	return core.APIKey{ID: "MARKER-APIKEY-CREATE"}, "MARKER-APIKEY-RAW", nil
}
func (fakeAPIKeySvc) List(context.Context, string) ([]core.APIKey, error) {
	return []core.APIKey{{ID: "MARKER-APIKEY-LIST"}}, nil
}
func (fakeAPIKeySvc) Revoke(context.Context, string, string) (*core.APIKey, error) {
	return &core.APIKey{ID: "MARKER-APIKEY-REVOKED"}, nil
}

type fakePolicySvc struct{ handler.PolicyRuleAdminService }

func (fakePolicySvc) Create(context.Context, string, core.PolicyRule) (core.PolicyRule, error) {
	return core.PolicyRule{ID: "MARKER-POLICY"}, nil
}
func (fakePolicySvc) List(context.Context, string) ([]*core.PolicyRule, error) {
	return []*core.PolicyRule{{ID: "MARKER-POLICY-LIST"}}, nil
}

type fakeProgressSvc struct{ handler.ProgressService }

func (fakeProgressSvc) ReportProgress(context.Context, string, string, string, core.TaskProgress) (*core.JanusEvent, error) {
	return &core.JanusEvent{EventID: "MARKER-PROGRESS"}, nil
}

type nopPublisher struct{}

func (nopPublisher) Publish(core.JanusEvent) {}

// closedBroadcaster hands SSE subscribers an already-closed channel so the
// stream handler sets headers and returns immediately.
type closedBroadcaster struct{}

func (closedBroadcaster) Subscribe(string) <-chan core.JanusEvent {
	ch := make(chan core.JanusEvent)
	close(ch)
	return ch
}
func (closedBroadcaster) Unsubscribe(string, <-chan core.JanusEvent) {}

type fakeContextRefRepo struct{}

func (fakeContextRefRepo) Insert(context.Context, core.ContextRef) error { return nil }
func (fakeContextRefRepo) Get(context.Context, string, string) (*core.ContextRef, error) {
	return &core.ContextRef{ID: "MARKER-CTXREF-GET"}, nil
}
func (fakeContextRefRepo) ListByTask(context.Context, string, string) ([]*core.ContextRef, error) {
	return []*core.ContextRef{{ID: "MARKER-CTXREF-LIST"}}, nil
}
func (fakeContextRefRepo) Delete(context.Context, string, string) error             { return nil }
func (fakeContextRefRepo) BindToTask(context.Context, string, string, string) error { return nil }
func (fakeContextRefRepo) UnbindFromTask(context.Context, string, string, string) error {
	return nil
}

// countingDispatchSvc records whether the dispatch Pull handler ever reached
// the service — the "no side effects" half of the negative tests.
type countingDispatchSvc struct {
	fakeDispatchSvc
	pulls int32
}

func (c *countingDispatchSvc) PullTask(context.Context, string, string, string) (*handler.ServicePullResult, error) {
	atomic.AddInt32(&c.pulls, 1)
	return nil, nil
}

func newParityRouter(dispatchSvc handler.DispatchService) http.Handler {
	return newParityRouterWith(dispatchSvc, fakeTaskSvc{})
}

// newParityRouterWith substitutes the task service (e.g. a recording
// fake) while keeping every other binding identical to the parity router.
func newParityRouterWith(dispatchSvc handler.DispatchService, taskSvc handler.TaskService) http.Handler {
	stub := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	return newRouter(
		handler.NewTenantHandler(fakeTenantSvc{}),
		handler.NewAgentHandler(fakeAgentSvc{}),
		handler.NewTaskHandler(taskSvc),
		handler.NewMailboxHandler(fakeMailboxSvc{}),
		handler.NewDispatchHandler(dispatchSvc),
		handler.NewAuditHandler(fakeAuditSvc{}),
		handler.NewApprovalHandler(fakeApprovalSvc{}),
		handler.NewContextRefHandler(service.NewContextRefService(fakeContextRefRepo{})),
		nil, // wsH: /ws is not part of the tenant route table
		handler.NewSSEHandler(closedBroadcaster{}),
		handler.NewProgressHandler(fakeProgressSvc{}, nopPublisher{}),
		stub,
		stub,
		stub,
		handler.NewDLQHandler(fakeDLQSvc{}),
		handler.NewCatalogHandler(fakeCatalogStore{}),
		handler.NewAPIKeyHandler(fakeAPIKeySvc{}),
		handler.NewPolicyRuleHandler(fakePolicySvc{}),
		handler.NewBudgetHandler(fakeBudgetSvc{}),
	)
}

func parityDo(t *testing.T, rt http.Handler, tc parityRoute, principal *auth.Principal) *httptest.ResponseRecorder {
	t.Helper()
	var body *strings.Reader
	if tc.body == "" {
		body = strings.NewReader("")
	} else {
		body = strings.NewReader(tc.body)
	}
	req := httptest.NewRequest(tc.method, tc.path, body)
	if principal != nil {
		ctx := context.WithValue(req.Context(), auth.PrincipalCtxKey, *principal)
		req = req.WithContext(ctx)
	}
	rec := httptest.NewRecorder()
	rt.ServeHTTP(rec, req)
	return rec
}

func assertParityResponse(t *testing.T, tc parityRoute, rec *httptest.ResponseRecorder, wantCode int, wantScope string) {
	t.Helper()
	if rec.Code != wantCode {
		t.Fatalf("%s %s (%s): want code %d, got %d body=%s",
			tc.method, tc.path, tc.name, wantCode, rec.Code, rec.Body.String())
	}
	if wantScope != "" && !strings.Contains(rec.Body.String(), wantScope) {
		t.Fatalf("%s %s (%s): want body to contain %q, got %s", tc.method, tc.path, tc.name, wantScope, rec.Body.String())
	}
}

func assertDispatchMarker(t *testing.T, tc parityRoute, rec *httptest.ResponseRecorder) {
	t.Helper()
	if tc.marker != "" && !strings.Contains(rec.Body.String(), tc.marker) {
		t.Fatalf("%s %s (%s): dispatched to the wrong handler: want marker %q, got body=%s",
			tc.method, tc.path, tc.name, tc.marker, rec.Body.String())
	}
	if tc.header != "" && !strings.Contains(rec.Header().Get("Content-Type"), tc.header) {
		t.Fatalf("%s %s (%s): want Content-Type %q, got %q", tc.method, tc.path, tc.name, tc.header, rec.Header().Get("Content-Type"))
	}
}

// TestRouteTableParity_Dispatch pins, for every route-table entry, the
// handler a canonical request dispatches to.
func TestRouteTableParity_Dispatch(t *testing.T) {
	rt := newParityRouter(fakeDispatchSvc{})
	for _, tc := range parityRoutes {
		t.Run(tc.name, func(t *testing.T) {
			rec := parityDo(t, rt, tc, nil)
			assertParityResponse(t, tc, rec, tc.code, tc.marker)
			assertDispatchMarker(t, tc, rec)
		})
	}
}

// TestRouteTableParity_Scope pins, for every route-table entry, that a
// principal holding exactly the entry's scope reaches the handler and a
// principal lacking it is refused with 403 naming the scope.
func TestRouteTableParity_Scope(t *testing.T) {
	guarded := auth.ScopeGuard(newParityRouter(fakeDispatchSvc{}))
	for _, tc := range parityRoutes {
		t.Run(tc.name, func(t *testing.T) {
			holding := auth.Principal{TenantID: "acme", Scopes: []string{tc.scope}}
			rec := parityDo(t, guarded, tc, &holding)
			assertParityResponse(t, tc, rec, tc.code, tc.marker)
			assertDispatchMarker(t, tc, rec)

			lacking := auth.Principal{TenantID: "acme", Scopes: []string{lackingScopeFor(tc.scope)}}
			recDeny := parityDo(t, guarded, tc, &lacking)
			if recDeny.Code != http.StatusForbidden {
				t.Fatalf("%s %s (%s): principal lacking %q: want 403, got %d body=%s",
					tc.method, tc.path, tc.name, tc.scope, recDeny.Code, recDeny.Body.String())
			}
			if !strings.Contains(recDeny.Body.String(), tc.scope) {
				t.Fatalf("%s %s (%s): 403 body must name the required scope %q, got %s",
					tc.method, tc.path, tc.name, tc.scope, recDeny.Body.String())
			}
		})
	}
}

// TestRouteTableParity_TrailingSlash pins the tolerated trailing-slash forms
// that real suites send (POST /tasks/ and /mailboxes/, GET /tenants/{t}/) and
// the bare /v1/tenants/ 404.
func TestRouteTableParity_TrailingSlash(t *testing.T) {
	rt := newParityRouter(fakeDispatchSvc{})
	cases := []parityRoute{
		{"tasks create trailing", http.MethodPost, "/v1/tenants/acme/tasks/", `{"id":"MARKER-TASK-CREATE","source_agent":"a1","target_type":"mailbox","target_value":"mb-1"}`, auth.ScopeTaskWrite, 201, "MARKER-TASK-CREATE", ""},
		{"mailboxes create trailing", http.MethodPost, "/v1/tenants/acme/mailboxes/", `{"id":"MARKER-MAILBOX-CREATE","agent_id":"a1"}`, auth.ScopeTaskWrite, 201, "MARKER-MAILBOX-CREATE", ""},
		{"tenant get trailing", http.MethodGet, "/v1/tenants/acme/", "", auth.ScopeTaskRead, 200, "MARKER-TENANT-GET", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := parityDo(t, rt, tc, nil)
			assertParityResponse(t, tc, rec, tc.code, tc.marker)
			assertDispatchMarker(t, tc, rec)
		})
	}
	rec := doReq(t, http.MethodGet, "/v1/tenants/")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("bare /v1/tenants/: want 404, got %d", rec.Code)
	}
}

// N1: undefined nesting under the tenant subtree is rejected with 404 and
// never reaches the dispatch service — no task may be claimed.
func TestRouteTable_UndefinedNesting_404_NoClaim(t *testing.T) {
	svc := &countingDispatchSvc{}
	rt := newParityRouter(svc)
	rec := parityDo(t, rt, parityRoute{method: http.MethodPost, path: "/v1/tenants/t/traces/x/mailboxes/mb/pull", body: `{"agent_id":"a1"}`}, nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("undefined nesting pull: want 404, got %d body=%s", rec.Code, rec.Body.String())
	}
	if n := atomic.LoadInt32(&svc.pulls); n != 0 {
		t.Fatalf("undefined nesting pull: dispatch service must not be called, got %d pulls", n)
	}
}

// N2: an audit:read-only principal can never reach Pull — the canonical
// shape is 403, undefined shapes are 404 without a claim.
func TestRouteTable_AuditReadOnly_CannotPull(t *testing.T) {
	svc := &countingDispatchSvc{}
	guarded := auth.ScopeGuard(newParityRouter(svc))
	auditOnly := auth.Principal{TenantID: "acme", Scopes: []string{auth.ScopeAuditRead}}

	rec := parityDo(t, guarded, parityRoute{method: http.MethodPost, path: "/v1/tenants/acme/mailboxes/mb/pull", body: `{"agent_id":"a1"}`}, &auditOnly)
	if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), auth.ScopeTaskWrite) {
		t.Fatalf("audit:read-only on canonical pull: want 403 naming %q, got %d body=%s", auth.ScopeTaskWrite, rec.Code, rec.Body.String())
	}

	for _, path := range []string{
		"/v1/tenants/acme/traces/x/mailboxes/mb/pull",
		"/v1/tenants/acme/tasks/t1/traces/mailboxes/mb/pull",
		"/v1/tenants/acme/mailboxes/mb/pull/traces",
	} {
		rec := parityDo(t, guarded, parityRoute{method: http.MethodPost, path: path, body: `{"agent_id":"a1"}`}, &auditOnly)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("audit:read-only on undefined shape %s: want 404, got %d body=%s", path, rec.Code, rec.Body.String())
		}
	}
	if n := atomic.LoadInt32(&svc.pulls); n != 0 {
		t.Fatalf("audit:read-only pull probing: dispatch service must not be called, got %d pulls", n)
	}
}

// routeTableWildcards materializes a table pattern into the concrete path
// used by parityRoutes, so the expectations table and auth.TenantRoutes can
// be compared entry by entry.
var routeTableWildcards = map[string]string{
	"{tenantID}":   "acme",
	"{agentID}":    "a1",
	"{mailboxID}":  "mb-1",
	"{taskID}":     "t1",
	"{approvalID}": "ap-1",
	"{keyID}":      "key-1",
	"{refID}":      "ref-1",
	"{traceID}":    "tr-1",
	"{scopeType}":  "mailbox",
	"{scopeID}":    "mb-1",
}

func materializeRoutePattern(pattern string) string {
	p := pattern
	for name, val := range routeTableWildcards {
		p = strings.ReplaceAll(p, name, val)
	}
	return p
}

// TestRouteTable_MatchesAuthTenantRoutes pins the contract that the parity
// expectations and auth.TenantRoutes are the same table: every route entry
// has a pinned expectation with the same scope, and every expectation
// corresponds to a registered entry. A dropped table row or an edited scope
// column fails here.
func TestRouteTable_MatchesAuthTenantRoutes(t *testing.T) {
	byKey := make(map[string]parityRoute, len(parityRoutes))
	for _, tc := range parityRoutes {
		byKey[tc.method+" "+tc.path] = tc
	}
	if len(byKey) != len(parityRoutes) {
		t.Fatalf("parity expectations contain duplicate method+path entries")
	}
	seen := make(map[string]bool, len(auth.TenantRoutes))
	for _, rt := range auth.TenantRoutes {
		key := rt.Method + " " + materializeRoutePattern(rt.Pattern)
		seen[key] = true
		tc, ok := byKey[key]
		if !ok {
			t.Errorf("auth.TenantRoutes entry %s %s has no parity expectation", rt.Method, rt.Pattern)
			continue
		}
		if tc.scope != rt.Scope {
			t.Errorf("%s %s: table scope %q != pinned scope %q", rt.Method, rt.Pattern, rt.Scope, tc.scope)
		}
	}
	for key := range byKey {
		if !seen[key] {
			t.Errorf("parity expectation %s has no auth.TenantRoutes entry", key)
		}
	}
}

// TestRouteTable_ContextRefsLegacySuffixes pins that the legacy loose
// suffix forms (…/attach, …/{refID}/detach, …/list) never dispatch: the
// attach suffix collides with the GET ref pattern and is refused 405, the
// others match no pattern and 404. The canonical shapes live in
// parityRoutes above.
func TestRouteTable_ContextRefsLegacySuffixes(t *testing.T) {
	rt := newParityRouter(fakeDispatchSvc{})
	cases := []struct {
		path     string
		wantCode int
	}{
		{"/v1/tenants/acme/context-refs/attach", http.StatusMethodNotAllowed},
		{"/v1/tenants/acme/context-refs/ref-1/detach", http.StatusNotFound},
		{"/v1/tenants/acme/tasks/t1/context-refs/list", http.StatusNotFound},
	}
	for _, tc := range cases {
		rec := parityDo(t, rt, parityRoute{method: http.MethodPost, path: tc.path}, nil)
		if rec.Code != tc.wantCode {
			t.Fatalf("legacy context-ref suffix shape %s: want %d, got %d", tc.path, tc.wantCode, rec.Code)
		}
		if strings.Contains(rec.Body.String(), "ctxref_") || strings.Contains(rec.Body.String(), "MARKER-CTXREF") {
			t.Fatalf("legacy suffix %s must not reach a context-ref handler, body=%s", tc.path, rec.Body.String())
		}
	}
}

// TestRouteTable_UndefinedShapes_404 pins the rejection of every nesting
// shape the legacy segment switch used to accept loosely. Pure shape
// mismatches 404; a defined shape with an undefined method is refused 405
// by net/http ServeMux.
func TestRouteTable_UndefinedShapes_404(t *testing.T) {
	svc := &countingDispatchSvc{}
	rt := newParityRouter(svc)
	shapes := []struct {
		parityRoute
		wantCode int
	}{
		{parityRoute{method: http.MethodPost, path: "/v1/tenants/acme/mailboxes/mb-1/pull/extra", body: `{"agent_id":"a1"}`}, http.StatusNotFound},
		{parityRoute{method: http.MethodGet, path: "/v1/tenants/acme/tasks/t1/traces/x"}, http.StatusNotFound},
		{parityRoute{method: http.MethodPost, path: "/v1/tenants/acme/tasks/t1/complete/x"}, http.StatusNotFound},
		{parityRoute{method: http.MethodGet, path: "/v1/tenants/acme/traces/tr-1/events"}, http.StatusNotFound},
		{parityRoute{method: http.MethodPost, path: "/v1/tenants/acme/agents/a1/mailboxes", body: `{"id":"m"}`}, http.StatusNotFound},
		{parityRoute{method: http.MethodPost, path: "/v1/tenants/acme/tasks/t1/approvals/ap-1/approve", body: `{"approver":"x"}`}, http.StatusNotFound},
		{parityRoute{method: http.MethodPost, path: "/v1/tenants/acme/audit/replay/x"}, http.StatusNotFound},
		{parityRoute{method: http.MethodPost, path: "/v1/tenants/acme/outbox/retry-dead/x"}, http.StatusNotFound},
		{parityRoute{method: http.MethodPost, path: "/v1/tenants/acme/dlq/t1/replay/x"}, http.StatusNotFound},
		{parityRoute{method: http.MethodPost, path: "/v1/tenants/acme/policy-rules/from-template", body: `{"template":"allow_agent_capability","agent_id":"a1","capability":"c1"}`}, http.StatusNotFound},
		{parityRoute{method: http.MethodGet, path: "/v1/tenants/acme/unknown/deep/path"}, http.StatusNotFound},
		{parityRoute{method: http.MethodPost, path: "/v1/tenants/acme"}, http.StatusMethodNotAllowed},
		{parityRoute{method: http.MethodPost, path: "/v1/tenants/acme/mailboxes/mb-1/pause/x"}, http.StatusNotFound},
	}
	for _, tc := range shapes {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			rec := parityDo(t, rt, tc.parityRoute, nil)
			if rec.Code != tc.wantCode {
				t.Fatalf("undefined shape: want %d, got %d body=%s", tc.wantCode, rec.Code, rec.Body.String())
			}
		})
	}
	if n := atomic.LoadInt32(&svc.pulls); n != 0 {
		t.Fatalf("undefined shapes must never claim: dispatch service called %d times", n)
	}
}

// R1: a tenant literally named "tasks" must not corrupt task-ID parsing.
// POST /v1/tenants/tasks/tasks/t1/cancel must reach the task service with
// tenantID="tasks" and taskID="t1" — asserted on recorded arguments, never
// on response markers.
func TestRouteTable_TenantNamedTasks_CancelUsesPathValues(t *testing.T) {
	svc := &recordingTaskSvc{}
	rt := newParityRouterWith(fakeDispatchSvc{}, svc)
	rec := parityDo(t, rt, parityRoute{method: http.MethodPost, path: "/v1/tenants/tasks/tasks/t1/cancel"}, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("cancel under tenant %q: want 200, got %d body=%s", "tasks", rec.Code, rec.Body.String())
	}
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if len(svc.cancels) != 1 {
		t.Fatalf("cancel: service called %d times, want exactly 1", len(svc.cancels))
	}
	if gotTenant, gotTask := svc.cancels[0][0], svc.cancels[0][1]; gotTenant != "tasks" || gotTask != "t1" {
		t.Fatalf("cancel reached service with tenantID=%q taskID=%q, want tenantID=%q taskID=%q", gotTenant, gotTask, "tasks", "t1")
	}
}

// R2: envelope policy.target_team_id must survive into the built
// core.PolicyContext (routing consumes it as a selection constraint) —
// asserted on the recorded task, never on response markers.
func TestRouteTable_TaskCreate_EnvelopePolicyTargetTeamID(t *testing.T) {
	svc := &recordingTaskSvc{}
	rt := newParityRouterWith(fakeDispatchSvc{}, svc)
	rec := parityDo(t, rt, parityRoute{
		method: http.MethodPost,
		path:   "/v1/tenants/acme/tasks",
		body:   `{"id":"tt-1","envelope":{"policy":{"target_team_id":"team-x"}}}`,
	}, nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("task create: want 201, got %d body=%s", rec.Code, rec.Body.String())
	}
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if len(svc.created) != 1 {
		t.Fatalf("task create: service called %d times, want exactly 1", len(svc.created))
	}
	policy := svc.created[0].Envelope.Policy
	if policy == nil {
		t.Fatal("task create reached service with nil envelope policy")
	}
	if policy.TargetTeamID != "team-x" {
		t.Fatalf("task create reached service with policy.TargetTeamID=%q, want %q", policy.TargetTeamID, "team-x")
	}
}
