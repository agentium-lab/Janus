package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/auth"
	"github.com/agentium-lab/Janus/server/internal/handler"
)

type ga01TaskSvc struct {
	handler.TaskService
	called    bool
	getTenant string
	getTask   string
}

func (f *ga01TaskSvc) Get(_ context.Context, tenantID, taskID string) (*core.Task, error) {
	f.called = true
	f.getTenant, f.getTask = tenantID, taskID
	return &core.Task{ID: taskID, TenantID: tenantID, Status: core.TaskStatusCompleted}, nil
}

func newGuardTestRouter(svc *ga01TaskSvc) http.Handler {
	stub := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	return newRouter(
		nil, // tenantH
		nil, // agentH
		handler.NewTaskHandler(svc),
		nil, // mailboxH
		nil, // dispatchH
		nil, // auditH
		nil, // approvalH
		nil, // contextRefH
		nil, // wsH
		nil, // sseH
		nil, // progressH
		stub, stub, stub, // a2a / acp / mcp gateways
		nil, // dlqH
		nil, // catalogH
		nil, // apiKeyH
		nil, // policyH
		nil, // budgetH
	)
}

// GA-01: the tenant the guard authorizes and the tenant the handler acts on
// must be the SAME string, derived the same way. ServeMux matches the
// escaped path and PathValue returns the unescaped segment, so
// /v1/tenants/acme%2Fother/tasks/t1 is tenant "acme/other" to the handler —
// the guard must not authorize it as "acme".
func TestRouter_GuardAndHandlerShareOneTenantDerivation(t *testing.T) {
	svc := &ga01TaskSvc{}
	h := newGuardTestRouter(svc)

	do := func(principal auth.Principal, rawPath string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, rawPath, nil)
		req = req.WithContext(auth.WithPrincipal(req.Context(), principal))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	acmeReader := auth.Principal{TenantID: "acme", Scopes: []string{"task:read"}}

	rec := do(acmeReader, "/v1/tenants/acme%2Fother/tasks/t1")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("acme reading the acme/other tenant's task via %%2F: want 403, got %d body=%s", rec.Code, rec.Body.String())
	}
	if svc.called {
		t.Fatalf("task service was called (tenant=%q) despite the tenant mismatch", svc.getTenant)
	}

	svc.called = false
	rec = do(auth.Principal{TenantID: "acme/other", Scopes: []string{"task:read"}}, "/v1/tenants/acme%2Fother/tasks/t1")
	if rec.Code != http.StatusOK {
		t.Fatalf("slash tenant reading its own task: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if !svc.called || svc.getTenant != "acme/other" || svc.getTask != "t1" {
		t.Fatalf("handler must act on the same tenant it matched: called=%v tenant=%q task=%q", svc.called, svc.getTenant, svc.getTask)
	}

	svc.called = false
	rec = do(acmeReader, "/v1/tenants/acme/tasks/t1")
	if rec.Code != http.StatusOK || !svc.called || svc.getTenant != "acme" || svc.getTask != "t1" {
		t.Fatalf("plain same-tenant read broken: code=%d called=%v tenant=%q task=%q", rec.Code, svc.called, svc.getTenant, svc.getTask)
	}
}

// The pre-routing extractor must mirror mux semantics on encoded slashes:
// split the ESCAPED path, unescape the tenant segment.
func TestExtractTenantFromPath_EscapedSegment(t *testing.T) {
	cases := map[string]string{
		"/v1/tenants/acme%2Fother/tasks/t1": "acme/other",
		"/v1/tenants/acme/tasks/t1":         "acme",
		"/v1/tenants/a%20b/tasks":           "a b",
		"/v1/tenants":                       "",
	}
	for path, want := range cases {
		if got := extractTenantFromPath(path); got != want {
			t.Fatalf("extractTenantFromPath(%q) = %q, want %q", path, got, want)
		}
	}
}
