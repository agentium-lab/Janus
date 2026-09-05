package acp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

type mockAgentRegistrar struct{ err error }

func (m *mockAgentRegistrar) Register(_ context.Context, _ core.Agent) error { return m.err }

type mockTaskCreator struct {
	createdTask *core.Task
	err         error
}

func (m *mockTaskCreator) Create(_ context.Context, task core.Task) (*core.Task, error) {
	if m.err != nil {
		return nil, m.err
	}
	m.createdTask = &task
	return &task, nil
}

type mockStatusGetter struct {
	task *core.Task
	err  error
}

func (m *mockStatusGetter) Get(_ context.Context, _, _ string) (*core.Task, error) {
	return m.task, m.err
}

func TestACP_Manifest(t *testing.T) {
	agentSvc := &mockAgentRegistrar{}
	gw := NewGateway(agentSvc, nil, nil)

	body := `{"agent_id":"agent-1","name":"Bot","skills":[{"name":"code_review","description":"Reviews code"}],"endpoint":"http://localhost"}`
	req := httptest.NewRequest("POST", "/acp/agent/manifest?tenant_id=acme", strReader(body))
	req = withAuthCtx(req)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]string
	json.NewDecoder(w.Body).Decode(&resp)
	assert.Equal(t, "registered", resp["status"])
}

func TestACP_Run(t *testing.T) {
	taskSvc := &mockTaskCreator{}
	gw := NewGateway(nil, taskSvc, nil)

	body := `{"run_id":"run-1","target_type":"agent","target":"agent-1","input":"hello"}`
	req := httptest.NewRequest("POST", "/acp/runs?tenant_id=acme&source_agent=bot", strReader(body))
	req = withAuthCtx(req)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusCreated, w.Code)
	var resp map[string]string
	json.NewDecoder(w.Body).Decode(&resp)
	assert.Equal(t, "run-1", resp["run_id"])
	require.NotNil(t, taskSvc.createdTask)
	assert.Equal(t, core.TaskStatusCreated, taskSvc.createdTask.Status)
}

func TestACP_RunStatus(t *testing.T) {
	statusSvc := &mockStatusGetter{task: &core.Task{ID: "run-1", Status: core.TaskStatusCompleted, ResultRef: "s3://result"}}
	gw := NewGateway(nil, nil, statusSvc)

	req := httptest.NewRequest("GET", "/acp/runs?run_id=run-1&tenant_id=acme", nil)
	req = withAuthCtx(req)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	assert.Equal(t, "run-1", resp["run_id"])
	assert.Equal(t, "completed", resp["status"])
}

func TestACP_RunNoTarget_400(t *testing.T) {
	taskSvc := &mockTaskCreator{}
	gw := NewGateway(nil, taskSvc, nil)

	body := `{"run_id":"run-2","input":"do something"}`
	req := httptest.NewRequest("POST", "/acp/runs?tenant_id=acme", strReader(body))
	req = withAuthCtx(req)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Nil(t, taskSvc.createdTask)
}

func TestACP_NotFound(t *testing.T) {
	gw := NewGateway(nil, nil, nil)
	req := httptest.NewRequest("GET", "/acp/unknown", nil)
	req = withAuthCtx(req)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestACP_InvalidJSON(t *testing.T) {
	gw := NewGateway(&mockAgentRegistrar{}, nil, nil)
	req := httptest.NewRequest("POST", "/acp/agent/manifest", strReader("not json"))
	req = withAuthCtx(req)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func strReader(s string) *strReaderImpl {
	return &strReaderImpl{s: s}
}

type strReaderImpl struct {
	s   string
	pos int
}

func (r *strReaderImpl) Read(p []byte) (int, error) {
	if r.pos >= len(r.s) {
		return 0, errEOF
	}
	n := copy(p, r.s[r.pos:])
	r.pos += n
	return n, nil
}

var errEOF = errEOFType{}

type errEOFType struct{}

func (errEOFType) Error() string { return "EOF" }
