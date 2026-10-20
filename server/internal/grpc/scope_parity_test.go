package grpc

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/agentium-lab/Janus/server/internal/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// principalValidator replays the production *auth.APIKeyValidator behavior
// the AuthInterceptor depends on: Validate (tenant), InjectPrincipal
// (principal into ctx) and HasScope (scope check against that principal).
type principalValidator struct {
	p auth.Principal
}

func (v *principalValidator) Validate(context.Context, string) (string, error) {
	return v.p.TenantID, nil
}

func (v *principalValidator) InjectPrincipal(ctx context.Context, _ string) context.Context {
	return context.WithValue(ctx, auth.PrincipalCtxKey, v.p)
}

func (v *principalValidator) HasScope(ctx context.Context, scope string) bool {
	p, ok := auth.PrincipalFromContext(ctx)
	if !ok {
		return false
	}
	return p.HasScope(scope)
}

func grpcScopeVerdict(t *testing.T, p auth.Principal, fullMethod string) (allowed bool, code codes.Code) {
	t.Helper()
	info := &grpc.UnaryServerInfo{FullMethod: fullMethod}
	_, err := AuthInterceptor(&principalValidator{p: p})(ctxWithKey("janus_parity_key"), &bareReq{}, info,
		func(context.Context, interface{}) (interface{}, error) { return nil, nil })
	if err == nil {
		return true, codes.OK
	}
	return false, status.Code(err)
}

func httpScopeVerdict(t *testing.T, p auth.Principal, method, path string) (allowed bool, statusCode int) {
	t.Helper()
	reached := false
	handler := auth.ScopeGuard(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached = true }))
	req := httptest.NewRequest(method, path, nil)
	ctx := context.WithValue(req.Context(), auth.PrincipalCtxKey, p)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req.WithContext(ctx))
	return reached && rec.Code == http.StatusOK, rec.Code
}

// TestCrossProtocolScopeParity asserts the SAME principal gets the SAME
// verdict on HTTP (auth.ScopeGuard driven by auth.RequiredScope) and gRPC
// (AuthInterceptor driven by auth.RequiredScopeGRPC) for every surface the
// parity task names. The expected matrix encodes the intended policy:
// task:write-only keys lose DLQ replay/discard, GetTrace and WS subscribe;
// task:read keys may subscribe to WS but not the audit/admin surfaces;
// admin and legacy empty-scope keys keep data-plane access everywhere.
func TestCrossProtocolScopeParity(t *testing.T) {
	surfaces := []struct {
		name       string
		httpMethod string
		httpPath   string
		grpcMethod string
	}{
		{"DLQ replay", http.MethodPost, "/v1/tenants/acme/dlq/e1/replay", "/janus.v1.DLQService/ReplayDLQ"},
		{"DLQ discard", http.MethodPost, "/v1/tenants/acme/dlq/e1/discard", "/janus.v1.DLQService/DiscardDLQ"},
		{"GetTrace", http.MethodGet, "/v1/tenants/acme/traces/tr-1", "/janus.v1.AuditService/GetTrace"},
		// The WS surface has no gRPC twin; its row pins the HTTP-side rule
		// (the WS handler enforces the same scope — see ws_handler_test.go).
		{"WS subscribe", http.MethodGet, "/ws", ""},
	}
	principals := []struct {
		name     string
		p        auth.Principal
		expected map[string]bool
	}{
		{
			name: "task:write only",
			p:    auth.Principal{TenantID: "acme", Scopes: []string{auth.ScopeTaskWrite}},
			expected: map[string]bool{
				"DLQ replay": false, "DLQ discard": false, "GetTrace": false, "WS subscribe": false,
			},
		},
		{
			name: "task:read only",
			p:    auth.Principal{TenantID: "acme", Scopes: []string{auth.ScopeTaskRead}},
			expected: map[string]bool{
				"DLQ replay": false, "DLQ discard": false, "GetTrace": false, "WS subscribe": true,
			},
		},
		{
			name: "admin",
			p:    auth.Principal{TenantID: "acme", Scopes: []string{auth.ScopeAdmin}},
			expected: map[string]bool{
				"DLQ replay": true, "DLQ discard": true, "GetTrace": true, "WS subscribe": true,
			},
		},
		{
			name: "legacy empty scopes",
			p:    auth.Principal{TenantID: "acme"},
			expected: map[string]bool{
				"DLQ replay": true, "DLQ discard": true, "GetTrace": true, "WS subscribe": true,
			},
		},
	}

	for _, s := range surfaces {
		for _, pr := range principals {
			t.Run(s.name+"/"+pr.name, func(t *testing.T) {
				httpAllowed, httpCode := httpScopeVerdict(t, pr.p, s.httpMethod, s.httpPath)
				require.Equal(t, pr.expected[s.name], httpAllowed,
					"HTTP verdict mismatch (status %d)", httpCode)

				if s.grpcMethod == "" {
					return
				}
				grpcAllowed, code := grpcScopeVerdict(t, pr.p, s.grpcMethod)
				assert.Equal(t, httpAllowed, grpcAllowed,
					"same principal must get the same verdict on both protocols (grpc code %v)", code)
				if !pr.expected[s.name] {
					assert.Equal(t, codes.PermissionDenied, code,
						"denied gRPC calls must fail with PermissionDenied")
				}
			})
		}
	}
}

// TestCrossProtocolDenialSurfaces are the regression pins for the three
// gaps this change closes: gRPC DLQ replay/discard used to accept plain
// task:write and GetTrace used to accept plain task:read, while HTTP
// already required admin and audit:read respectively.
func TestCrossProtocolDenialSurfaces(t *testing.T) {
	writer := auth.Principal{TenantID: "acme", Scopes: []string{auth.ScopeTaskWrite}}
	reader := auth.Principal{TenantID: "acme", Scopes: []string{auth.ScopeTaskRead}}

	for _, m := range []string{
		"/janus.v1.DLQService/ReplayDLQ",
		"/janus.v1.DLQService/DiscardDLQ",
	} {
		allowed, code := grpcScopeVerdict(t, writer, m)
		assert.False(t, allowed, "%s must not accept a task:write-only key", m)
		assert.Equal(t, codes.PermissionDenied, code)
	}

	allowed, code := grpcScopeVerdict(t, reader, "/janus.v1.AuditService/GetTrace")
	assert.False(t, allowed, "GetTrace must not accept a task:read-only key")
	assert.Equal(t, codes.PermissionDenied, code)
}
