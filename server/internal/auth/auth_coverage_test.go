package auth

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─────────────────────────────────────────────────────────────────────────────
// Fake database/sql driver so ValidatePrincipal's SQL paths can be exercised
// without a real Postgres instance. The query text is never parsed; the fake
// returns canned rows (or errors) per test.
// ─────────────────────────────────────────────────────────────────────────────

type fakeAuthRows struct {
	cols []string
	vals [][]driver.Value
	pos  int
}

func (r *fakeAuthRows) Columns() []string { return r.cols }
func (r *fakeAuthRows) Close() error      { return nil }
func (r *fakeAuthRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.vals) {
		return io.EOF
	}
	copy(dest, r.vals[r.pos])
	r.pos++
	return nil
}

type fakeAuthQueryFn func(query string, args []driver.NamedValue) (driver.Rows, error)

type fakeAuthConn struct{ q fakeAuthQueryFn }

func (c *fakeAuthConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare not supported by fake driver")
}
func (c *fakeAuthConn) Close() error              { return nil }
func (c *fakeAuthConn) Begin() (driver.Tx, error) { return nil, errors.New("tx not supported") }
func (c *fakeAuthConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.q(query, args)
}

type fakeAuthDriver struct {
	mu    sync.Mutex
	conns []*fakeAuthConn
	query fakeAuthQueryFn
}

func (d *fakeAuthDriver) Open(string) (driver.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	c := &fakeAuthConn{q: d.query}
	d.conns = append(d.conns, c)
	return c, nil
}

func (d *fakeAuthDriver) setQuery(q fakeAuthQueryFn) {
	d.mu.Lock()
	d.query = q
	d.mu.Unlock()
}

var (
	fakeAuthDriverOnce sync.Once
	fakeAuthDriverRef  *fakeAuthDriver
)

func newFakeAuthDB(t *testing.T, q fakeAuthQueryFn) *sql.DB {
	t.Helper()
	fakeAuthDriverOnce.Do(func() {
		fakeAuthDriverRef = &fakeAuthDriver{}
		sql.Register("janus-auth-fake", fakeAuthDriverRef)
	})
	fakeAuthDriverRef.setQuery(q)
	db, err := sql.Open("janus-auth-fake", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func singleRowQuery(tenantID, scopes string) fakeAuthQueryFn {
	return func(query string, args []driver.NamedValue) (driver.Rows, error) {
		return &fakeAuthRows{
			cols: []string{"tenant_id", "scopes"},
			vals: [][]driver.Value{{tenantID, scopes}},
		}, nil
	}
}

const covTestKey = "janus_cov_test_key_0123456789"

func TestValidatePrincipal_Success_WithScopes(t *testing.T) {
	db := newFakeAuthDB(t, singleRowQuery("acme", "task:read,task:write"))
	v := NewAPIKeyValidator(db)

	p, err := v.ValidatePrincipal(context.Background(), covTestKey)
	require.NoError(t, err)
	assert.Equal(t, "acme", p.TenantID)
	assert.Equal(t, []string{"task:read", "task:write"}, p.Scopes)
	assert.Equal(t, covTestKey[:8]+"...", p.KeyPrefix)
}

func TestValidatePrincipal_Success_EmptyScopes(t *testing.T) {
	db := newFakeAuthDB(t, singleRowQuery("acme", ""))
	v := NewAPIKeyValidator(db)

	p, err := v.ValidatePrincipal(context.Background(), covTestKey)
	require.NoError(t, err)
	assert.Equal(t, "acme", p.TenantID)
	assert.Empty(t, p.Scopes, "empty scope column must yield nil Scopes (full access)")
}

func TestValidatePrincipal_NotFound(t *testing.T) {
	db := newFakeAuthDB(t, func(string, []driver.NamedValue) (driver.Rows, error) {
		return &fakeAuthRows{cols: []string{"tenant_id", "scopes"}}, nil
	})
	v := NewAPIKeyValidator(db)

	_, err := v.ValidatePrincipal(context.Background(), covTestKey)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid api key")
}

func TestValidatePrincipal_LookupError(t *testing.T) {
	db := newFakeAuthDB(t, func(string, []driver.NamedValue) (driver.Rows, error) {
		return nil, errors.New("connection refused")
	})
	v := NewAPIKeyValidator(db)

	_, err := v.ValidatePrincipal(context.Background(), covTestKey)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "lookup api key")
	assert.Contains(t, err.Error(), "connection refused", "driver error is %w-wrapped")
}

func TestValidatePrincipal_QueryArgs(t *testing.T) {
	var gotArgs []driver.Value
	db := newFakeAuthDB(t, func(query string, args []driver.NamedValue) (driver.Rows, error) {
		assert.Contains(t, query, "revoked_at IS NULL", "revoked keys must be filtered in SQL")
		for _, a := range args {
			gotArgs = append(gotArgs, a.Value)
		}
		return &fakeAuthRows{
			cols: []string{"tenant_id", "scopes"},
			vals: [][]driver.Value{{"acme", ""}},
		}, nil
	})
	v := NewAPIKeyValidator(db)

	_, err := v.ValidatePrincipal(context.Background(), covTestKey)
	require.NoError(t, err)
	require.Len(t, gotArgs, 2)
	assert.Equal(t, covTestKey[:8], gotArgs[0], "prefix arg must be first 8 chars")
	assert.Equal(t, hashKey(covTestKey), gotArgs[1], "hash arg must be sha256 hex")
}

func TestValidate_Success_ReturnsTenant(t *testing.T) {
	db := newFakeAuthDB(t, singleRowQuery("tenant-x", ""))
	v := NewAPIKeyValidator(db)

	tenant, err := v.Validate(context.Background(), covTestKey)
	require.NoError(t, err)
	assert.Equal(t, "tenant-x", tenant)
}

func TestInjectPrincipal(t *testing.T) {
	db := newFakeAuthDB(t, singleRowQuery("acme", "task:read"))
	v := NewAPIKeyValidator(db)

	ctx := context.Background()
	out := v.InjectPrincipal(ctx, covTestKey)
	p, ok := PrincipalFromContext(out)
	require.True(t, ok)
	assert.Equal(t, "acme", p.TenantID)
	assert.Equal(t, covTestKey[:8]+"...", p.KeyPrefix)

	bad := v.InjectPrincipal(ctx, "short")
	_, ok = PrincipalFromContext(bad)
	assert.False(t, ok, "failed validation must leave the context untouched")
}

func TestValidatorHasScope(t *testing.T) {
	db := newFakeAuthDB(t, singleRowQuery("acme", "task:read"))
	v := NewAPIKeyValidator(db)

	assert.False(t, v.HasScope(context.Background(), ScopeTaskRead))

	ctx := v.InjectPrincipal(context.Background(), covTestKey)
	assert.True(t, v.HasScope(ctx, ScopeTaskRead))
	assert.False(t, v.HasScope(ctx, ScopeTaskWrite))
}

func TestPrincipalFromContext_Missing(t *testing.T) {
	_, ok := PrincipalFromContext(context.Background())
	assert.False(t, ok)
}

func TestActingUserFromContext(t *testing.T) {
	assert.Empty(t, ActingUserFromContext(context.Background()))

	ctx := context.WithValue(context.Background(), ActingUserCtxKey, "alice")
	assert.Equal(t, "alice", ActingUserFromContext(ctx))
}

func TestMiddleware_ValidKey_InjectsFullContext(t *testing.T) {
	db := newFakeAuthDB(t, singleRowQuery("acme", "task:read"))
	v := NewAPIKeyValidator(db)

	var (
		gotTenant  string
		gotPrefix  string
		gotActor   string
		gotScopes  []string
		reached    bool
	)
	handler := Middleware(v)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		gotTenant = TenantFromContext(r.Context())
		gotPrefix, _ = r.Context().Value(APIKeyCtxKey).(string)
		gotActor = ActingUserFromContext(r.Context())
		if p, ok := PrincipalFromContext(r.Context()); ok {
			gotScopes = p.Scopes
		}
		w.WriteHeader(http.StatusOK)
	}))

	r := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/tasks", nil)
	r.Header.Set("X-API-Key", covTestKey)
	r.Header.Set(ActingUserHeader, "  alice  ")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	assert.True(t, reached)
	assert.Equal(t, "acme", gotTenant)
	assert.Equal(t, covTestKey[:8]+"...", gotPrefix)
	assert.Equal(t, "alice", gotActor, "acting user header must be trimmed")
	assert.Equal(t, []string{"task:read"}, gotScopes)
}

func TestMiddleware_ValidKey_Bearer_NoActingUser(t *testing.T) {
	db := newFakeAuthDB(t, singleRowQuery("acme", ""))
	v := NewAPIKeyValidator(db)

	var gotTenant, gotActor string
	handler := Middleware(v)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTenant = TenantFromContext(r.Context())
		gotActor = ActingUserFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	r := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/tasks", nil)
	r.Header.Set("Authorization", "Bearer "+covTestKey)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "acme", gotTenant)
	assert.Empty(t, gotActor, "no acting user header → empty")
}

func TestMiddleware_WhitespaceOnlyKey_TreatedAsMissing(t *testing.T) {
	v := NewAPIKeyValidator(nil)
	handler := Middleware(v)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("should not reach handler")
	}))
	r := httptest.NewRequest(http.MethodGet, "/v1/tenants", nil)
	r.Header.Set("X-API-Key", "   ")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestRequiredScope_TenantManagementAndExemptPaths(t *testing.T) {
	scope, ok := RequiredScope(http.MethodDelete, "/v1/tenants")
	assert.True(t, ok)
	assert.Equal(t, ScopeAdmin, scope, "tenant management is control-plane")

	scope, ok = RequiredScope(http.MethodPost, "/a2a/message:send")
	assert.True(t, ok)
	assert.Equal(t, ScopeTaskWrite, scope)

	_, ok = RequiredScope(http.MethodGet, "/metrics")
	assert.False(t, ok)

	scope, ok = RequiredScope(http.MethodHead, "/v1/tenants/acme/tasks")
	assert.True(t, ok)
	assert.Equal(t, ScopeTaskRead, scope)

	scope, ok = RequiredScope(http.MethodPost, "/v1/tenants/acme/approvals/a1")
	assert.True(t, ok)
	assert.Equal(t, ScopeTaskWrite, scope, "no /approve|/reject suffix → plain write")

	scope, ok = RequiredScope(http.MethodPost, "/v1/tenants/acme/dlq/e1/discard")
	assert.True(t, ok)
	assert.Equal(t, ScopeAdmin, scope)
}

func TestHasScope_ExactMatch(t *testing.T) {
	p := Principal{Scopes: []string{ScopeTaskWrite, ScopeAuditRead}}
	assert.True(t, p.HasScope(ScopeTaskWrite))
	assert.True(t, p.HasScope(ScopeAuditRead))
	assert.False(t, p.HasScope(ScopeTaskRead))
	assert.False(t, p.HasScope("nonsense"))
}

func TestTenantGuard_MethodAgnostic(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true })
	guard := TenantGuard(func(string) string { return "acme" })(next)

	req := reqWithTenant("acme")
	req.Method = http.MethodDelete
	req.URL.Path = "/v1/tenants/acme/tasks/t1"
	rec := httptest.NewRecorder()
	guard.ServeHTTP(rec, req)
	assert.True(t, called)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestScopeGuard_SetsJSONContentType(t *testing.T) {
	handler := ScopeGuard(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/api-keys", nil)
	ctx := context.WithValue(req.Context(), PrincipalCtxKey, Principal{Scopes: []string{ScopeTaskRead}})
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req.WithContext(ctx))
	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Contains(t, rec.Body.String(), "missing required scope")
}

func TestExtractAPIKey_TrimsXAPIKey(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("X-API-Key", "  padded-key  ")
	assert.Equal(t, "padded-key", extractAPIKey(r))

	r.Header.Set("Authorization", "Bearer other")
	assert.Equal(t, "padded-key", extractAPIKey(r), "X-API-Key wins over Authorization")
}

func TestGenerateKey_DistinctAndWellFormed(t *testing.T) {
	seen := make(map[string]struct{}, 200)
	for i := 0; i < 200; i++ {
		raw, prefix, hash, err := GenerateKey()
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(raw, "janus_"), "raw key must carry the janus_ prefix")
		require.Equal(t, raw[:8], prefix)
		require.Len(t, hash, 64, "sha256 hex digest")
		require.NotContains(t, seen, raw, "keys must not repeat")
		seen[raw] = struct{}{}
	}
}
