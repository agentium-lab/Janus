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

// GA-01 scope mapping: the multiplexed /a2a/jsonrpc route is gated at the
// task:read floor so a read-only key can use the tasks/get JSON-RPC (a read
// operation that arrives as POST); mutating methods re-check task:write in
// the handler and get 403 with zero service calls.
func TestA2A_JSONRPC_ReadOnlyKeyCanGetButNotSend(t *testing.T) {
	status := &stubStatusSvc{}
	gw := NewGatewayWithStatus(&spyRegistrar{}, nilTaskSvc{}, status)

	mk := func(principal auth.Principal, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/a2a/jsonrpc", strings.NewReader(body))
		ctx := auth.WithPrincipal(req.Context(), principal)
		ctx = context.WithValue(ctx, auth.TenantCtxKey, principal.TenantID)
		req = req.WithContext(ctx)
		rec := httptest.NewRecorder()
		gw.ServeHTTP(rec, req)
		return rec
	}

	reader := auth.Principal{TenantID: "acme", Scopes: []string{auth.ScopeTaskRead}}

	// Given a read-only key calling tasks/get, the read RPC succeeds.
	rec := mk(reader, `{"jsonrpc":"2.0","id":1,"method":"tasks/get","params":{"task_id":"t1"}}`)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"task_id":"t1"`) || status.gotTaskID != "t1" {
		t.Fatalf("read-only tasks/get: want 200 reaching the status service, got %d body=%s", rec.Code, rec.Body.String())
	}

	// Given the same key calling task/send, the write scope re-check 403s
	// before any service call.
	rec = mk(reader, `{"jsonrpc":"2.0","id":2,"method":"task/send","params":{"message":{}}}`)
	if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), "task:write") {
		t.Fatalf("read-only task/send: want 403 task:write, got %d body=%s", rec.Code, rec.Body.String())
	}

	rec = mk(reader, `{"jsonrpc":"2.0","id":3,"method":"tasks/cancel","params":{"task_id":"t1"}}`)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("read-only tasks/cancel: want 403, got %d", rec.Code)
	}

	// Given a task:write key, mutating methods pass the re-check.
	writer := auth.Principal{TenantID: "acme", Scopes: []string{auth.ScopeTaskWrite}}
	rec = mk(writer, `{"jsonrpc":"2.0","id":4,"method":"tasks/get","params":{"task_id":"t2"}}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("task:write tasks/get should still read: got %d body=%s", rec.Code, rec.Body.String())
	}
}
