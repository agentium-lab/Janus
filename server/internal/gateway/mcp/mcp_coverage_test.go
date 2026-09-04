package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/agentium-lab/Janus/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type covEventPublisher struct {
	mu     sync.Mutex
	events []core.JanusEvent
	err    error
}

func (p *covEventPublisher) PublishEvent(_ context.Context, event core.JanusEvent) error {
	p.mu.Lock()
	p.events = append(p.events, event)
	p.mu.Unlock()
	return p.err
}

func (p *covEventPublisher) collected() []core.JanusEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]core.JanusEvent(nil), p.events...)
}

func doJSONRPC(t *testing.T, gw *Gateway, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/mcp?tenant_id=acme", strings.NewReader(body))
	req = withAuthCtx(req)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	return w
}

func decodeRPC(t *testing.T, w *httptest.ResponseRecorder) (id interface{}, result map[string]interface{}, rpcErr map[string]interface{}) {
	t.Helper()
	var resp struct {
		ID     *interface{}     `json:"id"`
		Result *json.RawMessage `json:"result"`
		Error  *json.RawMessage `json:"error"`
	}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	if resp.ID != nil {
		id = *resp.ID
	}
	if resp.Result != nil {
		require.NoError(t, json.Unmarshal(*resp.Result, &result))
	}
	if resp.Error != nil {
		require.NoError(t, json.Unmarshal(*resp.Error, &rpcErr))
	}
	return id, result, rpcErr
}

func TestCov_SanitizeMsg(t *testing.T) {
	assert.Equal(t, "internal error", sanitizeMsg("pq: connection refused at pg pool"))
	assert.Equal(t, "internal error", sanitizeMsg(strings.Repeat("x", 201)))
	assert.Equal(t, "short ok", sanitizeMsg("short ok"))
	assert.Equal(t, "internal error", sanitizeMsg("Context Deadline Exceeded"))
}

func TestCov_ReadJSONLimit_NilBody(t *testing.T) {
	gw := NewGateway(&mockTaskCreator{}, nil, nil)
	req := httptest.NewRequest(http.MethodPost, "/mcp/tools/call?tenant_id=acme", nil)
	req = withAuthCtx(req)
	req.Body = nil
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "INVALID_ARGUMENT", resp["code"])
}

func TestCov_WithEventPublisher(t *testing.T) {
	gw := NewGateway(&mockTaskCreator{}, nil, nil)
	pub := &covEventPublisher{}
	returned := gw.WithEventPublisher(pub)
	require.Same(t, gw, returned)
	require.NotNil(t, gw.eventPub)
}

func TestCov_ToolCall_MissingTenant_403(t *testing.T) {
	gw := NewGateway(&mockTaskCreator{}, nil, nil)
	req := httptest.NewRequest(http.MethodPost, "/mcp/tools/call", strings.NewReader(`{"target":"cap"}`))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "TENANT_REQUIRED", resp["code"])
}

func TestCov_ToolCallStatus_MissingTenant_403(t *testing.T) {
	gw := NewGateway(nil, &mockStatusGetter{}, nil)
	req := httptest.NewRequest(http.MethodGet, "/mcp/tools/calls/call-9/status", nil)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestCov_Resource_MissingTenant_403(t *testing.T) {
	gw := NewGateway(nil, nil, &mockResourceRegistrar{})
	req := httptest.NewRequest(http.MethodPost, "/mcp/resources", strings.NewReader(`{"uri":"file:///d"}`))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestCov_ToolCall_CreateError_Sanitized(t *testing.T) {
	gw := NewGateway(&mockTaskCreator{err: fmt.Errorf("dial tcp 10.0.0.1:5432: connection refused")}, nil, nil)
	body := `{"call_id":"c-leak","tool_name":"t","target":"cap"}`
	req := httptest.NewRequest(http.MethodPost, "/mcp/tools/call?tenant_id=acme&source_agent=cli", strings.NewReader(body))
	req = withAuthCtx(req)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "internal error", resp["error"])
	assert.Equal(t, "INTERNAL", resp["code"])
}

func TestCov_JSONRPC_Initialize_DefaultVersion(t *testing.T) {
	gw := NewGateway(&mockTaskCreator{}, nil, nil)
	w := doJSONRPC(t, gw, `{"jsonrpc":"2.0","method":"initialize","id":1}`)
	assert.Equal(t, http.StatusOK, w.Code)
	id, result, rpcErr := decodeRPC(t, w)
	require.Nil(t, rpcErr)
	assert.Equal(t, float64(1), id)
	require.NotNil(t, result)
	assert.Equal(t, "2024-11-05", result["protocolVersion"])
	info, ok := result["serverInfo"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "janus-mcp", info["name"])
}

func TestCov_JSONRPC_Initialize_EchoesClientVersion(t *testing.T) {
	gw := NewGateway(&mockTaskCreator{}, nil, nil)
	w := doJSONRPC(t, gw, `{"jsonrpc":"2.0","method":"initialize","params":{"protocolVersion":"2025-06-18"},"id":"abc"}`)
	assert.Equal(t, http.StatusOK, w.Code)
	_, result, rpcErr := decodeRPC(t, w)
	require.Nil(t, rpcErr)
	assert.Equal(t, "2025-06-18", result["protocolVersion"])
}

func TestCov_JSONRPC_InitializedNotification_202(t *testing.T) {
	gw := NewGateway(&mockTaskCreator{}, nil, nil)
	w := doJSONRPC(t, gw, `{"jsonrpc":"2.0","method":"notifications/initialized"}`)
	assert.Equal(t, http.StatusAccepted, w.Code)
	assert.Empty(t, w.Body.String())
}

func TestCov_JSONRPC_Ping(t *testing.T) {
	gw := NewGateway(&mockTaskCreator{}, nil, nil)
	w := doJSONRPC(t, gw, `{"jsonrpc":"2.0","method":"ping","id":7}`)
	assert.Equal(t, http.StatusOK, w.Code)
	id, result, rpcErr := decodeRPC(t, w)
	require.Nil(t, rpcErr)
	assert.Equal(t, float64(7), id)
	require.NotNil(t, result)
	assert.Empty(t, result)
}

func TestCov_JSONRPC_ToolsList(t *testing.T) {
	gw := NewGateway(&mockTaskCreator{}, nil, nil)
	w := doJSONRPC(t, gw, `{"jsonrpc":"2.0","method":"tools/list","id":8}`)
	assert.Equal(t, http.StatusOK, w.Code)
	_, result, rpcErr := decodeRPC(t, w)
	require.Nil(t, rpcErr)
	tools, ok := result["tools"].([]interface{})
	require.True(t, ok)
	assert.Empty(t, tools)
}

func TestCov_JSONRPC_ResourcesList(t *testing.T) {
	gw := NewGateway(&mockTaskCreator{}, nil, nil)
	w := doJSONRPC(t, gw, `{"jsonrpc":"2.0","method":"resources/list","id":9}`)
	assert.Equal(t, http.StatusOK, w.Code)
	_, result, rpcErr := decodeRPC(t, w)
	require.Nil(t, rpcErr)
	res, ok := result["resources"].([]interface{})
	require.True(t, ok)
	assert.Empty(t, res)
}

func TestCov_JSONRPC_ToolsCall_Success_WithEvents(t *testing.T) {
	taskSvc := &mockTaskCreator{}
	pub := &covEventPublisher{}
	gw := NewGateway(taskSvc, nil, nil).WithEventPublisher(pub)

	w := doJSONRPC(t, gw, `{"jsonrpc":"2.0","method":"tools/call","params":{"name":"web_search","arguments":"q=janus"},"id":10}`)
	assert.Equal(t, http.StatusOK, w.Code)

	_, result, rpcErr := decodeRPC(t, w)
	require.Nil(t, rpcErr)
	require.NotNil(t, result)
	assert.Equal(t, false, result["isError"])
	content, ok := result["content"].([]interface{})
	require.True(t, ok)
	require.Len(t, content, 1)
	text, ok := content[0].(map[string]interface{})
	require.True(t, ok)
	assert.Contains(t, text["text"], "tool call accepted: task_id=mcp_")
	assert.Equal(t, "text", text["type"])

	require.NotNil(t, taskSvc.created)
	assert.Equal(t, "web_search", taskSvc.created.TargetValue)
	assert.Equal(t, "mcp-client", taskSvc.created.SourceAgent)
	assert.Equal(t, "mcp_tool_call", taskSvc.created.Envelope.Payload.Type)

	events := pub.collected()
	require.Len(t, events, 1)
	assert.Equal(t, core.EventToolInvocationStarted, events[0].EventType)
	assert.Equal(t, taskSvc.created.ID, events[0].TaskID)
	var pl struct {
		ToolName string `json:"tool_name"`
	}
	require.NoError(t, json.Unmarshal(events[0].Payload, &pl))
	assert.Equal(t, "web_search", pl.ToolName)
}

func TestCov_ToolCall_Success_WithEvents(t *testing.T) {
	taskSvc := &mockTaskCreator{}
	pub := &covEventPublisher{}
	gw := NewGateway(taskSvc, nil, nil).WithEventPublisher(pub)

	body := `{"call_id":"call-ev","tool_name":"search","target":"web_search"}`
	req := httptest.NewRequest(http.MethodPost, "/mcp/tools/call?tenant_id=acme", strings.NewReader(body))
	req = withAuthCtx(req)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusCreated, w.Code)
	events := pub.collected()
	require.Len(t, events, 2)
	assert.Equal(t, core.EventToolInvocationRequested, events[0].EventType)
	assert.Equal(t, core.EventToolInvocationStarted, events[1].EventType)
	assert.Equal(t, "call-ev", events[0].TaskID)
}

func TestCov_JSONRPC_ToolsCall_InvalidParams_MissingName(t *testing.T) {
	gw := NewGateway(&mockTaskCreator{}, nil, nil)
	w := doJSONRPC(t, gw, `{"jsonrpc":"2.0","method":"tools/call","params":{"arguments":"q"},"id":11}`)
	assert.Equal(t, http.StatusOK, w.Code)
	id, _, rpcErr := decodeRPC(t, w)
	require.NotNil(t, rpcErr)
	assert.Equal(t, float64(-32602), rpcErr["code"])
	assert.Equal(t, float64(11), id)
}

func TestCov_JSONRPC_ToolsCall_InvalidParams_BadJSON(t *testing.T) {
	gw := NewGateway(&mockTaskCreator{}, nil, nil)
	w := doJSONRPC(t, gw, `{"jsonrpc":"2.0","method":"tools/call","params":42,"id":12}`)
	assert.Equal(t, http.StatusOK, w.Code)
	_, _, rpcErr := decodeRPC(t, w)
	require.NotNil(t, rpcErr)
	assert.Equal(t, float64(-32602), rpcErr["code"])
}

func TestCov_JSONRPC_ToolsCall_CreateError_Sanitized(t *testing.T) {
	gw := NewGateway(&mockTaskCreator{err: fmt.Errorf("pq: duplicate key violates unique constraint")}, nil, nil)
	w := doJSONRPC(t, gw, `{"jsonrpc":"2.0","method":"tools/call","params":{"name":"t"},"id":13}`)
	assert.Equal(t, http.StatusOK, w.Code)
	_, _, rpcErr := decodeRPC(t, w)
	require.NotNil(t, rpcErr)
	assert.Equal(t, float64(-32603), rpcErr["code"])
	assert.Equal(t, "internal error", rpcErr["message"])
}

func TestCov_JSONRPC_MethodNotFound(t *testing.T) {
	gw := NewGateway(&mockTaskCreator{}, nil, nil)
	w := doJSONRPC(t, gw, `{"jsonrpc":"2.0","method":"resources/read","id":14}`)
	assert.Equal(t, http.StatusOK, w.Code)
	_, _, rpcErr := decodeRPC(t, w)
	require.NotNil(t, rpcErr)
	assert.Equal(t, float64(-32601), rpcErr["code"])
	assert.Equal(t, "method not found: resources/read", rpcErr["message"])
}

func TestCov_JSONRPC_InvalidJSON_400(t *testing.T) {
	gw := NewGateway(&mockTaskCreator{}, nil, nil)
	w := doJSONRPC(t, gw, `{not json`)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "INVALID_ARGUMENT", resp["code"])
}

func TestCov_JSONRPC_MissingTenant_403(t *testing.T) {
	gw := NewGateway(&mockTaskCreator{}, nil, nil)
	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","method":"ping","id":1}`))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)
}

// Documents observed behavior: requests missing "jsonrpc" and "id" are still
// dispatched and answered with a null-id response. The gateway performs no
// JSON-RPC 2.0 framing validation (no -32600 invalid-request error).
func TestCov_JSONRPC_MissingIDAndVersion_NoFramingValidation(t *testing.T) {
	gw := NewGateway(&mockTaskCreator{}, nil, nil)
	w := doJSONRPC(t, gw, `{"method":"ping"}`)
	assert.Equal(t, http.StatusOK, w.Code)
	id, result, rpcErr := decodeRPC(t, w)
	require.Nil(t, rpcErr)
	assert.Nil(t, id)
	require.NotNil(t, result)
}

func TestCov_JSONRPC_WrongMethodOnRoot_404(t *testing.T) {
	gw := NewGateway(&mockTaskCreator{}, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/mcp?tenant_id=acme", nil)
	req = withAuthCtx(req)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}
