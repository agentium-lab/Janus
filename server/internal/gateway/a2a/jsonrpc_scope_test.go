package a2a

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/auth"
)

type stubStatusSvc struct {
	gotTaskID string
}

func (s *stubStatusSvc) Get(_ context.Context, _, taskID string) (*core.Task, error) {
	s.gotTaskID = taskID
	return &core.Task{ID: taskID, Status: core.TaskStatusQueued}, nil
}

// GA-01 scope mapping on the multiplexed /a2a/jsonrpc route, driven through
// the REAL ScopeGuard middleware chained in front of the gateway:
//
//	reader  tasks/get OK; task/send + tasks/cancel 403 at the handler check
//	writer  task/send + tasks/cancel pass the handler check; tasks/get 403
//	        (strict per-method scope — write does not imply read here, same
//	        as the REST surface where GET tasks needs task:read)
//	both    everything allowed
//
// The outer guard itself must admit BOTH key kinds (any-of scopeAnyRoutes):
// a write-only key 403'd at the outer guard before its own methods could
// pass was the R7 P2 regression.
func TestA2A_JSONRPC_ScopeGuardAnyOf_HandlerStrictPerMethod(t *testing.T) {
	status := &stubStatusSvc{}
	gw := auth.ScopeGuard(NewGatewayWithStatus(&spyRegistrar{}, nilTaskSvc{}, status))

	post := func(principal auth.Principal, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/a2a/jsonrpc", strings.NewReader(body))
		ctx := auth.WithPrincipal(req.Context(), principal)
		ctx = context.WithValue(ctx, auth.TenantCtxKey, principal.TenantID)
		req = req.WithContext(ctx)
		rec := httptest.NewRecorder()
		gw.ServeHTTP(rec, req)
		return rec
	}
	const getBody = `{"jsonrpc":"2.0","id":1,"method":"tasks/get","params":{"task_id":"t1"}}`
	const sendBody = `{"jsonrpc":"2.0","id":2,"method":"task/send","params":{"message":{}}}`
	const cancelBody = `{"jsonrpc":"2.0","id":3,"method":"tasks/cancel","params":{"task_id":"t1"}}`

	reader := auth.Principal{TenantID: "acme", Scopes: []string{auth.ScopeTaskRead}}
	writer := auth.Principal{TenantID: "acme", Scopes: []string{auth.ScopeTaskWrite}}
	both := auth.Principal{TenantID: "acme", Scopes: []string{auth.ScopeTaskRead, auth.ScopeTaskWrite}}

	// Given a read-only key: read RPC passes the handler check and reaches
	// the status service; both mutating RPCs are 403'd by the handler.
	rec := post(reader, getBody)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"task_id":"t1"`) || status.gotTaskID != "t1" {
		t.Fatalf("reader tasks/get: want 200 reaching the service, got %d body=%s", rec.Code, rec.Body.String())
	}
	for _, body := range []string{sendBody, cancelBody} {
		if rec := post(reader, body); rec.Code != http.StatusForbidden {
			t.Fatalf("reader mutating rpc %s: want 403, got %d body=%s", body, rec.Code, rec.Body.String())
		}
	}

	// Given a write-only key: mutating RPCs pass the outer guard AND the
	// handler check (the send reaches the task service — the regression
	// this matrix pins); the read RPC is 403'd by the strict handler check.
	rec = post(writer, sendBody)
	if rec.Code != http.StatusOK {
		t.Fatalf("writer task/send: want 200 (outer guard must admit write-only keys), got %d body=%s", rec.Code, rec.Body.String())
	}
	if rec := post(writer, cancelBody); rec.Code != http.StatusOK {
		t.Fatalf("writer tasks/cancel: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if rec := post(writer, getBody); rec.Code != http.StatusForbidden {
		t.Fatalf("writer tasks/get: want strict 403 (write does not imply read), got %d body=%s", rec.Code, rec.Body.String())
	}

	// Given both scopes: everything is allowed.
	if rec := post(both, getBody); rec.Code != http.StatusOK {
		t.Fatalf("both tasks/get: want 200, got %d", rec.Code)
	}
	if rec := post(both, sendBody); rec.Code != http.StatusOK {
		t.Fatalf("both task/send: want 200, got %d", rec.Code)
	}

	// Given a key with NEITHER scope: the outer guard itself rejects.
	neither := auth.Principal{TenantID: "acme", Scopes: []string{"policy:read"}}
	if rec := post(neither, getBody); rec.Code != http.StatusForbidden {
		t.Fatalf("no-scope key: want outer-guard 403, got %d", rec.Code)
	}
	if rec := post(neither, sendBody); rec.Code != http.StatusForbidden {
		t.Fatalf("no-scope key mutating: want outer-guard 403, got %d", rec.Code)
	}
}
