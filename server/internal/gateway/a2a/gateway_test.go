package a2a

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/agentium-lab/Janus/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockAgentRegistrar struct {
	err error
}

func (m *mockAgentRegistrar) Register(_ context.Context, _ core.Agent) error {
	return m.err
}

type mockTaskCreator struct {
	task *core.Task
	err  error
}

func (m *mockTaskCreator) Create(_ context.Context, task core.Task) (*core.Task, error) {
	if m.err != nil {
		return nil, m.err
	}
	return &task, nil
}

func TestGateway_ServeHTTP_AgentCard(t *testing.T) {
	gw := NewGateway(&mockAgentRegistrar{}, &mockTaskCreator{})
	body := `{"name":"reviewer","url":"http://localhost:8080","capabilities":[{"name":"review"}]}`
	req := httptest.NewRequest(http.MethodPost, "/a2a/agent/card?tenant_id=acme", strings.NewReader(body))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestGateway_ServeHTTP_TaskSend(t *testing.T) {
	gw := NewGateway(&mockAgentRegistrar{}, &mockTaskCreator{})
	body := `{"id":"msg-1","params":{"message":{"role":"user","parts":[{"type":"text","text":"hello"}]}}}`
	req := httptest.NewRequest(http.MethodPost, "/a2a/task/send?tenant_id=acme&source_agent=agent-1&mailbox_id=mb-1", strings.NewReader(body))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestGateway_ServeHTTP_NotFound(t *testing.T) {
	gw := NewGateway(&mockAgentRegistrar{}, &mockTaskCreator{})
	req := httptest.NewRequest(http.MethodGet, "/a2a/unknown", nil)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestGateway_ServeHTTP_WrongMethod(t *testing.T) {
	gw := NewGateway(&mockAgentRegistrar{}, &mockTaskCreator{})
	req := httptest.NewRequest(http.MethodGet, "/a2a/agent/card", nil)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestGateway_HandleAgentCard_DefaultTenant(t *testing.T) {
	gw := NewGateway(&mockAgentRegistrar{}, &mockTaskCreator{})
	body := `{"name":"agent-a","url":"http://localhost"}`
	req := httptest.NewRequest(http.MethodPost, "/a2a/agent/card", strings.NewReader(body))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestGateway_HandleAgentCard_BadJSON(t *testing.T) {
	gw := NewGateway(&mockAgentRegistrar{}, &mockTaskCreator{})
	req := httptest.NewRequest(http.MethodPost, "/a2a/agent/card?tenant_id=acme", strings.NewReader("invalid"))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGateway_HandleAgentCard_RegisterError(t *testing.T) {
	gw := NewGateway(&mockAgentRegistrar{err: fmt.Errorf("db down")}, &mockTaskCreator{})
	body := `{"name":"agent-a","url":"http://localhost"}`
	req := httptest.NewRequest(http.MethodPost, "/a2a/agent/card?tenant_id=acme", strings.NewReader(body))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestGateway_HandleTaskSend_DefaultParams(t *testing.T) {
	gw := NewGateway(&mockAgentRegistrar{}, &mockTaskCreator{})
	body := `{"id":"msg-1","params":{"message":{"role":"user","parts":[{"type":"text","text":"hi"}]}}}`
	req := httptest.NewRequest(http.MethodPost, "/a2a/task/send", strings.NewReader(body))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestGateway_HandleTaskSend_BadJSON(t *testing.T) {
	gw := NewGateway(&mockAgentRegistrar{}, &mockTaskCreator{})
	req := httptest.NewRequest(http.MethodPost, "/a2a/task/send?tenant_id=acme", strings.NewReader("bad"))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGateway_HandleTaskSend_CreateError(t *testing.T) {
	gw := NewGateway(&mockAgentRegistrar{}, &mockTaskCreator{err: fmt.Errorf("task create failed")})
	body := `{"id":"msg-1","params":{"message":{"role":"user","parts":[{"type":"text","text":"hi"}]}}}`
	req := httptest.NewRequest(http.MethodPost, "/a2a/task/send?tenant_id=acme", strings.NewReader(body))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestGateway_NewGateway(t *testing.T) {
	gw := NewGateway(&mockAgentRegistrar{}, &mockTaskCreator{})
	require.NotNil(t, gw)
}

func TestMessageToTask_NoID(t *testing.T) {
	msg := SendMessageRequest{
		Params: SendMessageParams{
			Message: AgentMessage{
				Role: "user",
				Parts: []MessagePart{{Type: "text", Text: "hello"}},
			},
		},
	}
	task := MessageToTask(msg, "acme", "agent-1", "mb-1")
	assert.NotEmpty(t, task.ID)
	assert.Equal(t, "hello", task.Envelope.Payload.Content)
}

func TestMessageToTask_NoContextID(t *testing.T) {
	msg := SendMessageRequest{
		ID: "msg-002",
		Params: SendMessageParams{
			Message: AgentMessage{
				Role: "user",
				Parts: []MessagePart{{Type: "text", Text: "test"}},
			},
		},
	}
	task := MessageToTask(msg, "acme", "agent-1", "mb-1")
	assert.NotEmpty(t, task.Envelope.Trace.TraceID)
}

func TestJanusStatusToA2A_Unknown(t *testing.T) {
	assert.Equal(t, "unknown", JanusStatusToA2A(core.TaskStatus("unknown_status")))
}

func TestGenerateID(t *testing.T) {
	id := generateID()
	assert.Contains(t, id, "task_")
	assert.NotEmpty(t, id)
}

func TestCardToAgent_EmptyCard(t *testing.T) {
	agent := CardToAgent("t1", AgentCard{})
	assert.Equal(t, "t1", agent.TenantID)
	assert.Equal(t, core.AgentStatusOnline, agent.Status)
}

func TestCardToCapabilities_Empty(t *testing.T) {
	caps := CardToCapabilities("t1", "a1", AgentCard{})
	assert.Len(t, caps, 0)
}

func TestMessageToTask_NonTextParts(t *testing.T) {
	msg := SendMessageRequest{
		ID: "msg-003",
		Params: SendMessageParams{
			Message: AgentMessage{
				Role: "user",
				Parts: []MessagePart{
					{Type: "image", Text: ""},
					{Type: "text", Text: "actual content"},
				},
			},
		},
	}
	task := MessageToTask(msg, "t1", "a1", "mb-1")
	assert.Equal(t, "actual content", task.Envelope.Payload.Content)
}

func TestMessageToTask_EmptyParts(t *testing.T) {
	msg := SendMessageRequest{
		ID: "msg-004",
		Params: SendMessageParams{
			Message: AgentMessage{
				Role: "user",
			},
		},
	}
	task := MessageToTask(msg, "t1", "a1", "mb-1")
	assert.Equal(t, "", task.Envelope.Payload.Content)
}

type mockStatusGetter struct {
	task *core.Task
	err  error
}

func (m *mockStatusGetter) Get(_ context.Context, _, _ string) (*core.Task, error) {
	return m.task, m.err
}

func TestGateway_TaskStatus(t *testing.T) {
	statusSvc := &mockStatusGetter{task: &core.Task{ID: "task-1", Status: core.TaskStatusCompleted}}
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, statusSvc)

	req := httptest.NewRequest(http.MethodGet, "/a2a/task/task-1/status?tenant_id=acme", nil)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestGateway_TaskStatus_NotFound(t *testing.T) {
	statusSvc := &mockStatusGetter{err: fmt.Errorf("not found")}
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, statusSvc)

	req := httptest.NewRequest(http.MethodGet, "/a2a/task/missing/status?tenant_id=acme", nil)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestGateway_TaskStatus_NoStatusSvc(t *testing.T) {
	gw := NewGateway(&mockAgentRegistrar{}, &mockTaskCreator{})

	req := httptest.NewRequest(http.MethodGet, "/a2a/task/task-1/status?tenant_id=acme", nil)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}

func TestGateway_JSONRPC_TaskSend(t *testing.T) {
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, nil)

	body := `{"jsonrpc":"2.0","method":"task/send","params":{"id":"msg-1","params":{"message":{"role":"user","parts":[{"type":"text","text":"rpc hello"}]}}},"id":1}`
	req := httptest.NewRequest(http.MethodPost, "/a2a/jsonrpc?tenant_id=acme&source_agent=agent-1&mailbox_id=mb-1", strings.NewReader(body))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestGateway_JSONRPC_TaskGet(t *testing.T) {
	statusSvc := &mockStatusGetter{task: &core.Task{ID: "task-1", Status: core.TaskStatusRunning}}
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, statusSvc)

	body := `{"jsonrpc":"2.0","method":"task/get","params":{"task_id":"task-1"},"id":2}`
	req := httptest.NewRequest(http.MethodPost, "/a2a/jsonrpc?tenant_id=acme", strings.NewReader(body))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestGateway_JSONRPC_MethodNotFound(t *testing.T) {
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, nil)

	body := `{"jsonrpc":"2.0","method":"unknown/method","params":{},"id":3}`
	req := httptest.NewRequest(http.MethodPost, "/a2a/jsonrpc?tenant_id=acme", strings.NewReader(body))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp jsonRPCResponse
	json.NewDecoder(w.Body).Decode(&resp)
	assert.NotNil(t, resp.Error)
}

func TestGateway_JSONRPC_InvalidJSON(t *testing.T) {
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, nil)

	req := httptest.NewRequest(http.MethodPost, "/a2a/jsonrpc?tenant_id=acme", strings.NewReader("bad json"))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGateway_JSONRPC_InvalidParams(t *testing.T) {
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, nil)

	body := `{"jsonrpc":"2.0","method":"task/send","params":"not an object","id":4}`
	req := httptest.NewRequest(http.MethodPost, "/a2a/jsonrpc?tenant_id=acme", strings.NewReader(body))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp jsonRPCResponse
	json.NewDecoder(w.Body).Decode(&resp)
	assert.NotNil(t, resp.Error)
}

func TestGateway_NewGatewayWithStatus(t *testing.T) {
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, &mockStatusGetter{})
	require.NotNil(t, gw)
	require.NotNil(t, gw.statusSvc)
}
