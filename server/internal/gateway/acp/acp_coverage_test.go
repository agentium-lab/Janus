package acp

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

type covAgentRegistrar struct {
	agent core.Agent
	err   error
}

func (m *covAgentRegistrar) Register(_ context.Context, agent core.Agent) error {
	if m.err != nil {
		return m.err
	}
	m.agent = agent
	return nil
}

func TestCov_SanitizeMsg(t *testing.T) {
	assert.Equal(t, "internal error", sanitizeMsg("sql: insert failed"))
	assert.Equal(t, "internal error", sanitizeMsg(strings.Repeat("y", 201)))
	assert.Equal(t, "plain failure", sanitizeMsg("plain failure"))
	assert.Equal(t, "internal error", sanitizeMsg("Dial TCP timeout"))
}

func TestCov_Manifest_MapsSkills_ToCapabilities(t *testing.T) {
	agentSvc := &covAgentRegistrar{}
	gw := NewGateway(agentSvc, nil, nil)

	body := `{"agent_id":"agent-9","name":"Helper","skills":[{"name":"summarize","description":"Summarizes text"}],"endpoint":"http://agent"}`
	req := httptest.NewRequest(http.MethodPost, "/acp/agent/manifest?tenant_id=acme", strings.NewReader(body))
	req = withAuthCtx(req)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "agent-9", agentSvc.agent.ID)
	assert.Equal(t, "acme", agentSvc.agent.TenantID)
	assert.Equal(t, core.AgentProtocol("acp"), agentSvc.agent.Protocol)
	assert.Equal(t, core.AgentStatusOnline, agentSvc.agent.Status)
	require.Len(t, agentSvc.agent.Capabilities, 1)
	assert.Equal(t, "summarize", agentSvc.agent.Capabilities[0].Capability)
	assert.Equal(t, "Summarizes text", agentSvc.agent.Capabilities[0].Description)
}

func TestCov_Manifest_RegisterError_Sanitized(t *testing.T) {
	gw := NewGateway(&mockAgentRegistrar{err: fmt.Errorf("sql: unique violation on agents")}, nil, nil)
	body := `{"agent_id":"agent-x","endpoint":"http://agent"}`
	req := httptest.NewRequest(http.MethodPost, "/acp/agent/manifest?tenant_id=acme", strings.NewReader(body))
	req = withAuthCtx(req)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "internal error", resp["error"])
	assert.Equal(t, "INTERNAL", resp["code"])
}

func TestCov_Manifest_MissingTenant_403(t *testing.T) {
	gw := NewGateway(&mockAgentRegistrar{}, nil, nil)
	req := httptest.NewRequest(http.MethodPost, "/acp/agent/manifest", strings.NewReader(`{"agent_id":"a"}`))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "TENANT_REQUIRED", resp["code"])
}

func TestCov_Manifest_NilBody_400(t *testing.T) {
	gw := NewGateway(&mockAgentRegistrar{}, nil, nil)
	req := httptest.NewRequest(http.MethodPost, "/acp/agent/manifest?tenant_id=acme", nil)
	req = withAuthCtx(req)
	req.Body = nil
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestCov_Run_AutoRunID_AndDefaults(t *testing.T) {
	taskSvc := &mockTaskCreator{}
	gw := NewGateway(nil, taskSvc, nil)

	body := `{"target":"agent-1","input":"payload"}`
	req := httptest.NewRequest(http.MethodPost, "/acp/runs?tenant_id=acme", strings.NewReader(body))
	req = withAuthCtx(req)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusCreated, w.Code)
	var resp map[string]string
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Contains(t, resp["run_id"], "run_")

	require.NotNil(t, taskSvc.createdTask)
	assert.Contains(t, taskSvc.createdTask.ID, "run_")
	assert.Equal(t, core.TargetTypeCapability, taskSvc.createdTask.TargetType)
	assert.Equal(t, "unknown", taskSvc.createdTask.SourceAgent)
	assert.Equal(t, "acp_run", taskSvc.createdTask.Envelope.Payload.Type)
	assert.Equal(t, "payload", taskSvc.createdTask.Envelope.Payload.Content)
	assert.Contains(t, taskSvc.createdTask.Envelope.Trace.TraceID, "acp-run_")
}

func TestCov_Run_InvalidJSON_400(t *testing.T) {
	gw := NewGateway(nil, &mockTaskCreator{}, nil)
	req := httptest.NewRequest(http.MethodPost, "/acp/runs?tenant_id=acme", strings.NewReader("}{"))
	req = withAuthCtx(req)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "INVALID_ARGUMENT", resp["code"])
}

func TestCov_Run_CreateError_Sanitized(t *testing.T) {
	gw := NewGateway(nil, &mockTaskCreator{err: fmt.Errorf("redis: connection refused")}, nil)
	body := `{"run_id":"run-err","target":"agent-1"}`
	req := httptest.NewRequest(http.MethodPost, "/acp/runs?tenant_id=acme", strings.NewReader(body))
	req = withAuthCtx(req)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "internal error", resp["error"])
	assert.Equal(t, "INTERNAL", resp["code"])
}

func TestCov_Run_MissingTenant_403(t *testing.T) {
	gw := NewGateway(nil, &mockTaskCreator{}, nil)
	req := httptest.NewRequest(http.MethodPost, "/acp/runs", strings.NewReader(`{"target":"agent-1"}`))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestCov_ListRuns_MissingRunID_400(t *testing.T) {
	gw := NewGateway(nil, nil, &mockStatusGetter{})
	req := httptest.NewRequest(http.MethodGet, "/acp/runs?tenant_id=acme", nil)
	req = withAuthCtx(req)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "INVALID_ARGUMENT", resp["code"])
}

func TestCov_ListRuns_NotFound_404_Sanitized(t *testing.T) {
	gw := NewGateway(nil, nil, &mockStatusGetter{err: fmt.Errorf("no rows in result set for run-404")})
	req := httptest.NewRequest(http.MethodGet, "/acp/runs?run_id=run-404&tenant_id=acme", nil)
	req = withAuthCtx(req)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "internal error", resp["error"])
	assert.Equal(t, "NOT_FOUND", resp["code"])
}

func TestCov_ListRuns_MissingTenant_403(t *testing.T) {
	gw := NewGateway(nil, nil, &mockStatusGetter{})
	req := httptest.NewRequest(http.MethodGet, "/acp/runs?run_id=run-1", nil)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestCov_Run_MethodMismatch_404(t *testing.T) {
	gw := NewGateway(nil, nil, nil)
	req := httptest.NewRequest(http.MethodPut, "/acp/runs?tenant_id=acme", nil)
	req = withAuthCtx(req)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}
