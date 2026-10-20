package a2a

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/server/internal/auth"
)

func v1StreamRequest(body string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/a2a/message:stream", strings.NewReader(body))
	r.Header.Set("A2A-Version", "1.0")
	return r.WithContext(context.WithValue(r.Context(), auth.TenantCtxKey, "acme"))
}

func TestV1StreamMessage_WithoutSubscriber(t *testing.T) {
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, &mockStatusGetter{})
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, v1StreamRequest(`{"message":{"role":"ROLE_USER","parts":[{"text":"hi"}]}}`))
	assert.Equal(t, http.StatusNotImplemented, w.Code)
}

func TestV1StreamMessage_InvalidJSON(t *testing.T) {
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, &mockStatusGetter{}).WithEventSubscriber(newMockSubscriber())
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, v1StreamRequest(`{broken`))
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "INVALID_ARGUMENT")
}

func TestV1StreamMessage_BoundPrincipalMismatch(t *testing.T) {
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, &mockStatusGetter{}).WithEventSubscriber(newMockSubscriber())
	r := httptest.NewRequest(http.MethodPost, "/a2a/message:stream?source_agent=someone-else", strings.NewReader(`{"message":{"role":"ROLE_USER","parts":[{"text":"hi"}]}}`))
	r.Header.Set("A2A-Version", "1.0")
	r = r.WithContext(context.WithValue(r.Context(), auth.TenantCtxKey, "acme"))
	r = r.WithContext(context.WithValue(r.Context(), auth.PrincipalCtxKey, auth.Principal{BoundAgentID: "a1"}))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, r)
	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestV1StreamMessage_TerminalRecheckClosesStream(t *testing.T) {
	tc := &mockTaskCreator{}
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, tc, &mockStatusGetter{task: completedTask("t-now")}).
		WithEventSubscriber(newMockSubscriber())
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, v1StreamRequest(`{"message":{"role":"ROLE_USER","parts":[{"text":"hi"}],"messageId":"m1"}}`))
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "text/event-stream", w.Header().Get("Content-Type"))
	body := w.Body.String()
	assert.Contains(t, body, `"state":"TASK_STATE_COMPLETED"`, "terminal status must close the stream")
}
