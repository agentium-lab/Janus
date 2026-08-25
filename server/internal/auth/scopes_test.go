package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHasScope(t *testing.T) {
	full := Principal{}
	narrow := Principal{Scopes: []string{ScopeTaskRead}}
	admin := Principal{Scopes: []string{ScopeAdmin}}

	if !full.HasScope(ScopeTaskWrite) {
		t.Fatal("empty scopes must grant full access for backward compatibility")
	}
	if !narrow.HasScope(ScopeTaskRead) || narrow.HasScope(ScopeTaskWrite) {
		t.Fatal("narrow principal must allow task:read only")
	}
	if !admin.HasScope(ScopeTaskWrite) || !admin.HasScope(ScopeAuditRead) {
		t.Fatal("admin scope must imply every scope")
	}
}

func TestRequiredScope(t *testing.T) {
	cases := []struct {
		name     string
		method   string
		path     string
		want     string
		required bool
	}{
		{"approve needs admin", http.MethodPost, "/v1/tenants/acme/tasks/t1/approvals/a1/approve", ScopeAdmin, true},
		{"reject needs admin", http.MethodPost, "/v1/tenants/acme/approvals/a1/reject", ScopeAdmin, true},
		{"approval list stays read", http.MethodGet, "/v1/tenants/acme/approvals", ScopeTaskRead, true},
		{"api-keys always admin", http.MethodPost, "/v1/tenants/acme/api-keys", ScopeAdmin, true},
		{"dlq replay admin", http.MethodPost, "/v1/tenants/acme/dlq/e1/replay", ScopeAdmin, true},
		{"dlq query read", http.MethodGet, "/v1/tenants/acme/dlq", ScopeTaskRead, true},
		{"traces audit read", http.MethodGet, "/v1/tenants/acme/traces/abc/events", ScopeAuditRead, true},
		{"task publish write", http.MethodPost, "/v1/tenants/acme/tasks", ScopeTaskWrite, true},
		{"agent list read", http.MethodGet, "/v1/tenants/acme/agents", ScopeTaskRead, true},
		{"probe exempt", http.MethodGet, "/healthz", "", false},
		{"gateway exempt", http.MethodPost, "/a2a/task/send", "", false},
		{"ws exempt", http.MethodGet, "/ws", "", false},
	}
	for _, tc := range cases {
		got, required := RequiredScope(tc.method, tc.path)
		if required != tc.required || got != tc.want {
			t.Fatalf("%s: got (%q,%v) want (%q,%v)", tc.name, got, required, tc.want, tc.required)
		}
	}
}

func TestScopeGuardRejectsWithoutScope(t *testing.T) {
	handler := ScopeGuard(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/approvals/a1/approve", nil)
	ctx := context.WithValue(req.Context(), PrincipalCtxKey, Principal{TenantID: "acme", Scopes: []string{ScopeTaskRead}})
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req.WithContext(ctx))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("narrow key hitting approve: want 403, got %d", rec.Code)
	}

	reqFull := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/approvals/a1/approve", nil)
	ctxFull := context.WithValue(reqFull.Context(), PrincipalCtxKey, Principal{TenantID: "acme"})
	recFull := httptest.NewRecorder()
	handler.ServeHTTP(recFull, reqFull.WithContext(ctxFull))
	if recFull.Code != http.StatusOK {
		t.Fatalf("full-access key hitting approve: want 200, got %d", recFull.Code)
	}

	reqDev := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/approvals/a1/approve", nil)
	recDev := httptest.NewRecorder()
	handler.ServeHTTP(recDev, reqDev)
	if recDev.Code != http.StatusOK {
		t.Fatalf("dev-mode request without principal: want pass-through 200, got %d", recDev.Code)
	}
}
