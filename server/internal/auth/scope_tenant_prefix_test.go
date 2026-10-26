package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Tenant ids are unvalidated free-form strings (tenant_service.Create checks
// non-empty only), so a tenant can be named like a resource word. Scope rules
// must therefore match only the resource part after "/v1/tenants/<id>/".

func TestRequiredScope_TenantNamedTraces(t *testing.T) {
	scope, ok := RequiredScope(http.MethodPost, "/v1/tenants/traces/mailboxes/mb/pull")
	assert.True(t, ok)
	assert.Equal(t, ScopeTaskWrite, scope)
}

type stubScopeValidator struct{ principal Principal }

func (s *stubScopeValidator) ValidatePrincipal(context.Context, string) (Principal, error) {
	return s.principal, nil
}

func TestScopeGuard_TenantNamedTraces_PullChain(t *testing.T) {
	chain := func(scopes []string) http.Handler {
		return Middleware(&stubScopeValidator{principal: Principal{TenantID: "traces", Scopes: scopes}})(
			ScopeGuard(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			})))
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/traces/mailboxes/mb/pull", nil)
	req.Header.Set("X-API-Key", "janus_test_key")

	rec := httptest.NewRecorder()
	chain([]string{ScopeAuditRead}).ServeHTTP(rec, req)
	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Contains(t, rec.Body.String(), ScopeTaskWrite)

	recPass := httptest.NewRecorder()
	chain([]string{ScopeTaskWrite}).ServeHTTP(recPass, req)
	assert.Equal(t, http.StatusOK, recPass.Code)
}

func TestRequiredScope_TenantResourceWord_Regressions(t *testing.T) {
	cases := []struct {
		name   string
		method string
		path   string
		want   string
	}{
		{"resource traces stays audit:read", http.MethodGet, "/v1/tenants/acme/tasks/t1/traces", ScopeAuditRead},
		{"mailbox named traces pull stays task:write", http.MethodPost, "/v1/tenants/acme/mailboxes/traces/pull", ScopeTaskWrite},
		{"dlq replay stays admin", http.MethodPost, "/v1/tenants/acme/dlq/x/replay", ScopeAdmin},
		{"tenant named audit keeps mailbox pull at task:write", http.MethodPost, "/v1/tenants/audit/mailboxes/mb/pull", ScopeTaskWrite},
		{"tenant named approvals does not escalate task create", http.MethodPost, "/v1/tenants/approvals/tasks", ScopeTaskWrite},
		{"tenant named traces get falls back to task:read", http.MethodGet, "/v1/tenants/traces", ScopeTaskRead},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scope, ok := RequiredScope(tc.method, tc.path)
			assert.True(t, ok)
			assert.Equal(t, tc.want, scope)
		})
	}
}

// Unlike scope resolution, TenantGuard must keep reading the FULL path.
func TestTenantGuard_MismatchedResourceWordTenant(t *testing.T) {
	guard := TenantGuard(func(path string) string {
		parts := strings.Split(path, "/")
		for i, p := range parts {
			if p == "tenants" && i+1 < len(parts) {
				return parts[i+1]
			}
		}
		return ""
	})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/traces/mailboxes/mb/pull", nil)
	ctx := context.WithValue(req.Context(), TenantCtxKey, "acme")
	rec := httptest.NewRecorder()
	guard.ServeHTTP(rec, req.WithContext(ctx))
	assert.Equal(t, http.StatusForbidden, rec.Code)
}
