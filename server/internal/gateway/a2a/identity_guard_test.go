package a2a

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/agentium-lab/Janus/server/internal/auth"
	"github.com/stretchr/testify/assert"
)

// The sixth review (P0): identity binding covered only four entrypoints —
// the legacy A2A routes accepted any source_agent from a query parameter
// while the key was bound to a different agent. These tests drive each
// guarded entrypoint with a spoofed identity.

func withBoundPrincipal(r *http.Request, boundAgent string) *http.Request {
	ctx := context.WithValue(r.Context(), auth.TenantCtxKey, "acme")
	ctx = context.WithValue(ctx, auth.PrincipalCtxKey,
		auth.Principal{TenantID: "acme", BoundAgentID: boundAgent})
	return r.WithContext(ctx)
}

func TestLegacyA2A_TaskSend_ImpersonationRejected(t *testing.T) {
	gw := NewGateway(&mockAgentRegistrar{}, &mockTaskCreator{})
	req := withBoundPrincipal(
		httptest.NewRequest(http.MethodPost, "/a2a/task/send?source_agent=victim", strings.NewReader(`{"message":{"role":"user","parts":[]}}`)),
		"real-agent")
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Contains(t, w.Body.String(), "real-agent")
}

func TestLegacyA2A_JSONRPC_ImpersonationRejected(t *testing.T) {
	gw := NewGateway(&mockAgentRegistrar{}, &mockTaskCreator{})
	req := withBoundPrincipal(
		httptest.NewRequest(http.MethodPost, "/a2a/jsonrpc?source_agent=victim", strings.NewReader(`{"jsonrpc":"2.0","method":"message/send","id":1,"params":{}}`)),
		"real-agent")
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestLegacyA2A_TaskSend_OwnIdentityAllowed(t *testing.T) {
	gw := NewGateway(&mockAgentRegistrar{}, &mockTaskCreator{})
	req := withBoundPrincipal(
		httptest.NewRequest(http.MethodPost, "/a2a/task/send?source_agent=real-agent", strings.NewReader(`{"message":{"role":"user","parts":[]}}`)),
		"real-agent")
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestA2AV1_MessageSend_ImpersonationRejected(t *testing.T) {
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, &mockStatusGetter{})
	body := `{"message":{"role":"ROLE_USER","parts":[{"text":"hi"}]},"metadata":{"source_agent":"victim"}}`
	req := withBoundPrincipal(httptest.NewRequest(http.MethodPost, "/a2a/message:send", strings.NewReader(body)), "real-agent")
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Contains(t, w.Body.String(), "PERMISSION_DENIED")
}
