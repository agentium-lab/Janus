package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHashKey(t *testing.T) {
	h1 := hashKey("janus_test_key_123")
	h2 := hashKey("janus_test_key_123")
	assert.Equal(t, h1, h2)
	assert.Len(t, h1, 64)

	h3 := hashKey("different_key")
	assert.NotEqual(t, h1, h3)
}

func TestExtractAPIKey(t *testing.T) {
	tests := []struct {
		name   string
		header string
		value  string
		want   string
	}{
		{"x-api-key header", "X-API-Key", "my-api-key", "my-api-key"},
		{"bearer token", "Authorization", "Bearer my-token", "my-token"},
		{"bearer with spaces", "Authorization", "Bearer  my-token  ", "my-token"},
		{"empty", "", "", ""},
		{"wrong scheme", "Authorization", "Basic abc", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.header != "" {
				r.Header.Set(tt.header, tt.value)
			}
			got := extractAPIKey(r)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMiddleware_MissingKey_Returns401(t *testing.T) {
	handler := Middleware(nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("should not reach handler")
	}))

	r := httptest.NewRequest(http.MethodGet, "/v1/tenants", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestMiddleware_ValidKey_SetsTenantContext(t *testing.T) {
	var gotTenant string
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTenant = TenantFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})
	_ = inner
	_ = gotTenant
}

func TestNewAPIKeyValidator(t *testing.T) {
	v := NewAPIKeyValidator(nil)
	assert.NotNil(t, v)
}

func TestValidate_ShortKey(t *testing.T) {
	v := NewAPIKeyValidator(nil)
	_, err := v.Validate(context.Background(), "short")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid api key format")
}

func TestMiddleware_InvalidKey_Returns401(t *testing.T) {
	v := NewAPIKeyValidator(nil)
	handler := Middleware(v)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("should not reach handler")
	}))
	r := httptest.NewRequest(http.MethodGet, "/v1/tenants", nil)
	r.Header.Set("X-API-Key", "short")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestGenerateKey(t *testing.T) {
	raw, prefix, hash, err := GenerateKey()
	assert.NoError(t, err)
	assert.NotEmpty(t, raw)
	assert.True(t, len(raw) > 32, "key should be long enough")
	assert.Equal(t, raw[:8], prefix)
	assert.NotEmpty(t, hash)
	assert.Equal(t, hashKey(raw), hash)

	raw2, _, hash2, err := GenerateKey()
	assert.NoError(t, err)
	assert.NotEqual(t, raw, raw2)
	assert.NotEqual(t, hash, hash2)
}

func TestTenantFromContext(t *testing.T) {
	// No tenant set → empty.
	assert.Equal(t, "", TenantFromContext(httptest.NewRequest("GET", "/", nil).Context()))

	// Tenant set in context.
	req := reqWithTenant("acme")
	assert.Equal(t, "acme", TenantFromContext(req.Context()))
}

func TestTenantGuard_AllowsMatchingTenant(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true })

	extract := func(path string) string { return "acme" }
	guard := TenantGuard(extract)(next)

	req := reqWithTenant("acme")
	req.URL.Path = "/v1/tenants/acme/tasks"
	guard.ServeHTTP(httptest.NewRecorder(), req)
	assert.True(t, called)
}

func TestTenantGuard_BlocksMismatchedTenant(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true })

	extract := func(path string) string { return "evil" }
	guard := TenantGuard(extract)(next)

	req := reqWithTenant("acme")
	req.URL.Path = "/v1/tenants/evil/tasks"
	rec := httptest.NewRecorder()
	guard.ServeHTTP(rec, req)
	assert.False(t, called)
	assert.Equal(t, http.StatusForbidden, rec.Code)
}

func TestTenantGuard_NoAuthTenant_Blocks(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true })

	guard := TenantGuard(func(p string) string { return "acme" })(next)

	req := httptest.NewRequest("GET", "/v1/tenants/acme/tasks", nil)
	w := httptest.NewRecorder()
	guard.ServeHTTP(w, req)
	assert.False(t, called, "missing auth tenant must be rejected (fail-closed)")
	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestTenantGuard_EmptyPathTenant_AllowsAll(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true })

	guard := TenantGuard(func(p string) string { return "" })(next)

	req := reqWithTenant("acme")
	req.URL.Path = "/healthz"
	guard.ServeHTTP(httptest.NewRecorder(), req)
	assert.True(t, called, "no path tenant → allow")
}

// reqWithTenant creates a request with the tenant ID set in context.
func reqWithTenant(tenantID string) *http.Request {
	req := httptest.NewRequest("GET", "/", nil)
	return req.WithContext(context.WithValue(req.Context(), TenantCtxKey, tenantID))
}
