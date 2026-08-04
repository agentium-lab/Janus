package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func withAuthCtx(r *http.Request) *http.Request {
	t := r.URL.Query().Get("tenant_id")
	if t == "" {
		t = "acme"
	}
	return r.WithContext(context.WithValue(r.Context(), auth.TenantCtxKey, t))
}

type mockTaskCreator struct{ created *core.Task; err error }
func (m *mockTaskCreator) Create(_ context.Context, task core.Task) (*core.Task, error) {
	if m.err != nil { return nil, m.err }
	m.created = &task
	return &task, nil
}

type mockStatusGetter struct{ task *core.Task; err error }
func (m *mockStatusGetter) Get(_ context.Context, _, _ string) (*core.Task, error) { return m.task, m.err }

type mockResourceRegistrar struct{ ref *core.ContextRef; err error }
func (m *mockResourceRegistrar) Attach(_ context.Context, _, _, _, _, _ string, _ []string) (*core.ContextRef, error) {
	return m.ref, m.err
}

func TestMCP_ToolCall(t *testing.T) {
	taskSvc := &mockTaskCreator{}
	gw := NewGateway(taskSvc, nil, nil)

	body := `{"call_id":"call-1","tool_name":"search","arguments":"query","target":"web_search"}`
	req := httptest.NewRequest("POST", "/mcp/tools/call?tenant_id=acme", strings.NewReader(body))
	req = withAuthCtx(req)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusCreated, w.Code)
	var resp map[string]string
	json.NewDecoder(w.Body).Decode(&resp)
	assert.Equal(t, "call-1", resp["call_id"])
	require.NotNil(t, taskSvc.created)
	assert.Equal(t, "mcp_tool_call", taskSvc.created.Envelope.Payload.Type)
	assert.Equal(t, core.TargetTypeCapability, taskSvc.created.TargetType)
}

func TestMCP_ToolCallNoTarget_400(t *testing.T) {
	taskSvc := &mockTaskCreator{}
	gw := NewGateway(taskSvc, nil, nil)

	body := `{"call_id":"call-2","tool_name":"summarize","arguments":"text"}`
	req := httptest.NewRequest("POST", "/mcp/tools/call?tenant_id=acme", strings.NewReader(body))
	req = withAuthCtx(req)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Nil(t, taskSvc.created)
}

func TestMCP_Resource(t *testing.T) {
	resourceSvc := &mockResourceRegistrar{ref: &core.ContextRef{ID: "ref-1", URI: "file://x"}}
	gw := NewGateway(nil, nil, resourceSvc)

	body := `{"uri":"file:///data","hash":"abc123","classification":"internal"}`
	req := httptest.NewRequest("POST", "/mcp/resources?tenant_id=acme", strings.NewReader(body))
	req = withAuthCtx(req)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusCreated, w.Code)
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	assert.Equal(t, "ref-1", resp["context_ref_id"])
}

func TestMCP_NotFound(t *testing.T) {
	gw := NewGateway(nil, nil, nil)
	req := httptest.NewRequest("GET", "/mcp/unknown", nil)
	req = withAuthCtx(req)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestMCP_InvalidJSON(t *testing.T) {
	gw := NewGateway(&mockTaskCreator{}, nil, nil)
	req := httptest.NewRequest("POST", "/mcp/tools/call", strings.NewReader("not json"))
	req = withAuthCtx(req)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestMCP_ToolCallStatus(t *testing.T) {
	statusSvc := &mockStatusGetter{task: &core.Task{ID: "call-1", Status: core.TaskStatusCompleted, ResultRef: "s3://result"}}
	gw := NewGateway(nil, statusSvc, nil)

	req := httptest.NewRequest("GET", "/mcp/tools/calls/call-1/status?tenant_id=acme", nil)
	req = withAuthCtx(req)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestMCP_ToolCallStatus_MissingID(t *testing.T) {
	gw := NewGateway(nil, &mockStatusGetter{}, nil)
	req := httptest.NewRequest("GET", "/mcp/tools/calls//status?tenant_id=acme", nil)
	req = withAuthCtx(req)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.NotEqual(t, http.StatusOK, w.Code)
}

func TestMCP_ToolCallStatus_NotFound(t *testing.T) {
	statusSvc := &mockStatusGetter{err: fmt.Errorf("not found")}
	gw := NewGateway(nil, statusSvc, nil)

	req := httptest.NewRequest("GET", "/mcp/tools/calls/missing/status?tenant_id=acme", nil)
	req = withAuthCtx(req)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestMCP_ResourceNoService(t *testing.T) {
	gw := NewGateway(nil, nil, nil)
	body := `{"uri":"file:///data"}`
	req := httptest.NewRequest("POST", "/mcp/resources?tenant_id=acme", strings.NewReader(body))
	req = withAuthCtx(req)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}

func TestMCP_ResourceInvalidJSON(t *testing.T) {
	gw := NewGateway(nil, nil, &mockResourceRegistrar{})
	req := httptest.NewRequest("POST", "/mcp/resources", strings.NewReader("bad"))
	req = withAuthCtx(req)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestMCP_ResourceError(t *testing.T) {
	resourceSvc := &mockResourceRegistrar{err: fmt.Errorf("storage error")}
	gw := NewGateway(nil, nil, resourceSvc)
	body := `{"uri":"file:///data"}`
	req := httptest.NewRequest("POST", "/mcp/resources?tenant_id=acme", strings.NewReader(body))
	req = withAuthCtx(req)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestMCP_ToolCallCreateError(t *testing.T) {
	taskSvc := &mockTaskCreator{err: fmt.Errorf("db down")}
	gw := NewGateway(taskSvc, nil, nil)
	body := `{"call_id":"c1","tool_name":"x","target":"cap-x"}`
	req := httptest.NewRequest("POST", "/mcp/tools/call?tenant_id=acme", strings.NewReader(body))
	req = withAuthCtx(req)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestMCP_ToolCallAutoID(t *testing.T) {
	taskSvc := &mockTaskCreator{}
	gw := NewGateway(taskSvc, nil, nil)
	body := `{"tool_name":"search","target":"search-cap"}`
	req := httptest.NewRequest("POST", "/mcp/tools/call?tenant_id=acme", strings.NewReader(body))
	req = withAuthCtx(req)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusCreated, w.Code)
	require.NotNil(t, taskSvc.created)
	assert.Contains(t, taskSvc.created.ID, "mcp_call_")
}
