package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

type mockTenantService struct {
	tenants map[string]*core.Tenant
	err     error
}

func (m *mockTenantService) Create(_ context.Context, id, name string) error {
	if m.err != nil {
		return m.err
	}
	if m.tenants == nil {
		m.tenants = make(map[string]*core.Tenant)
	}
	m.tenants[id] = &core.Tenant{ID: id, Name: name}
	return nil
}

func (m *mockTenantService) Get(_ context.Context, id string) (*core.Tenant, error) {
	if m.err != nil {
		return nil, m.err
	}
	t, ok := m.tenants[id]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return t, nil
}

func TestTenantHandler_Create(t *testing.T) {
	svc := &mockTenantService{}
	h := NewTenantHandler(svc)

	body, _ := json.Marshal(map[string]string{"id": "acme", "name": "Acme Corp"})
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants", bytes.NewReader(body))
	w := httptest.NewRecorder()

	h.Create(w, req)
	assert.Equal(t, http.StatusCreated, w.Code)
}

func TestTenantHandler_CreateBadBody(t *testing.T) {
	h := NewTenantHandler(&mockTenantService{})
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants", bytes.NewReader([]byte("invalid")))
	w := httptest.NewRecorder()

	h.Create(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestTenantHandler_CreateServiceError(t *testing.T) {
	h := NewTenantHandler(&mockTenantService{err: fmt.Errorf("conflict")})
	body, _ := json.Marshal(map[string]string{"id": "acme", "name": "Acme"})
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants", bytes.NewReader(body))
	w := httptest.NewRecorder()

	h.Create(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestTenantHandler_Get(t *testing.T) {
	svc := &mockTenantService{tenants: map[string]*core.Tenant{
		"acme": {ID: "acme", Name: "Acme Corp"},
	}}
	h := NewTenantHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme", nil)
	w := httptest.NewRecorder()
	h.Get(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestTenantHandler_GetNotFound(t *testing.T) {
	h := NewTenantHandler(&mockTenantService{})
	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/nonexistent", nil)
	w := httptest.NewRecorder()
	h.Get(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestTenantHandler_GetNoTenant(t *testing.T) {
	h := NewTenantHandler(&mockTenantService{})
	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/", nil)
	w := httptest.NewRecorder()
	h.Get(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

type mockAgentService struct {
	agents map[string]*core.Agent
	err    error
}

func (m *mockAgentService) Register(_ context.Context, a core.Agent) error {
	if m.err != nil {
		return m.err
	}
	if m.agents == nil {
		m.agents = make(map[string]*core.Agent)
	}
	m.agents[a.TenantID+":"+a.ID] = &a
	return nil
}

func (m *mockAgentService) Get(_ context.Context, tenantID, agentID string) (*core.Agent, error) {
	if m.err != nil {
		return nil, m.err
	}
	a, ok := m.agents[tenantID+":"+agentID]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return a, nil
}

func (m *mockAgentService) Heartbeat(_ context.Context, tenantID, agentID string) error {
	return m.err
}

func (m *mockAgentService) UpdateStatus(_ context.Context, tenantID, agentID string, status core.AgentStatus) error {
	return m.err
}

func (m *mockAgentService) List(_ context.Context, tenantID string) ([]*core.Agent, error) {
	if m.err != nil {
		return nil, m.err
	}
	var result []*core.Agent
	for _, a := range m.agents {
		if a.TenantID == tenantID {
			result = append(result, a)
		}
	}
	return result, nil
}

func (m *mockAgentService) ListByStatus(_ context.Context, tenantID string, status core.AgentStatus) ([]*core.Agent, error) {
	return nil, nil
}

func TestAgentHandler_Register(t *testing.T) {
	svc := &mockAgentService{}
	h := NewAgentHandler(svc)

	body, _ := json.Marshal(map[string]interface{}{
		"id": "agent-1", "display_name": "Agent 1", "protocol": "a2a", "max_concurrency": 2,
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/agents", bytes.NewReader(body))
	w := httptest.NewRecorder()

	h.Register(w, req)
	assert.Equal(t, http.StatusCreated, w.Code)
}

func TestAgentHandler_RegisterBadBody(t *testing.T) {
	h := NewAgentHandler(&mockAgentService{})
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/agents", bytes.NewReader([]byte("bad")))
	w := httptest.NewRecorder()
	h.Register(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestAgentHandler_RegisterError(t *testing.T) {
	h := NewAgentHandler(&mockAgentService{err: fmt.Errorf("dup")})
	body, _ := json.Marshal(map[string]string{"id": "a1", "display_name": "A1", "protocol": "a2a"})
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/agents", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.Register(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestAgentHandler_Get(t *testing.T) {
	svc := &mockAgentService{agents: map[string]*core.Agent{
		"acme:agent-1": {ID: "agent-1", TenantID: "acme", DisplayName: "Agent 1"},
	}}
	h := NewAgentHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/agents/agent-1", nil)
	w := httptest.NewRecorder()
	h.Get(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestAgentHandler_GetNotFound(t *testing.T) {
	h := NewAgentHandler(&mockAgentService{})
	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/agents/nonexistent", nil)
	w := httptest.NewRecorder()
	h.Get(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestAgentHandler_Heartbeat(t *testing.T) {
	svc := &mockAgentService{}
	h := NewAgentHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/agents/agent-1/heartbeat", nil)
	w := httptest.NewRecorder()
	h.Heartbeat(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestAgentHandler_HeartbeatError(t *testing.T) {
	h := NewAgentHandler(&mockAgentService{err: fmt.Errorf("redis down")})
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/agents/agent-1/heartbeat", nil)
	w := httptest.NewRecorder()
	h.Heartbeat(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestAgentHandler_List(t *testing.T) {
	svc := &mockAgentService{agents: map[string]*core.Agent{
		"acme:a1": {ID: "a1", TenantID: "acme"},
	}}
	h := NewAgentHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/agents", nil)
	w := httptest.NewRecorder()
	h.List(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestAgentHandler_ListError(t *testing.T) {
	h := NewAgentHandler(&mockAgentService{err: fmt.Errorf("db down")})
	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/agents", nil)
	w := httptest.NewRecorder()
	h.List(w, req)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

type mockTaskService struct {
	tasks map[string]*core.Task
	err   error
}

func (m *mockTaskService) Create(_ context.Context, task core.Task) (*core.Task, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.tasks == nil {
		m.tasks = make(map[string]*core.Task)
	}
	m.tasks[task.TenantID+":"+task.ID] = &task
	return &task, nil
}

func (m *mockTaskService) Get(_ context.Context, tenantID, taskID string) (*core.Task, error) {
	if m.err != nil {
		return nil, m.err
	}
	t, ok := m.tasks[tenantID+":"+taskID]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return t, nil
}

func (m *mockTaskService) Start(_ context.Context, tenantID, taskID string) error  { return m.err }
func (m *mockTaskService) Complete(_ context.Context, tenantID, taskID string) error { return m.err }
func (m *mockTaskService) Fail(_ context.Context, tenantID, taskID string, taskErr *core.TaskError) error {
	return m.err
}
func (m *mockTaskService) Cancel(_ context.Context, tenantID, taskID string) error { return m.err }
func (m *mockTaskService) Block(_ context.Context, tenantID, taskID, reason string) error {
	return m.err
}
func (m *mockTaskService) Unblock(_ context.Context, tenantID, taskID string) error {
	return m.err
}
func (m *mockTaskService) Replay(_ context.Context, tenantID, taskID string) (*core.Task, error) {
	if m.err != nil {
		return nil, m.err
	}
	return &core.Task{TenantID: tenantID, ID: taskID, Status: core.TaskStatusQueued}, nil
}
func (m *mockTaskService) ListByStatus(_ context.Context, tenantID string, status core.TaskStatus, limit int) ([]*core.Task, error) {
	return nil, nil
}

func TestTaskHandler_Create(t *testing.T) {
	svc := &mockTaskService{}
	h := NewTaskHandler(svc)

	body, _ := json.Marshal(map[string]interface{}{
		"id": "task-1", "source_agent": "agent-a",
		"target_type": "capability", "target_value": "review",
		"envelope": map[string]interface{}{
			"janus_version": "0.1", "task_id": "task-1", "tenant_id": "acme",
			"source_agent": "agent-a",
			"target":       map[string]string{"type": "capability", "value": "review"},
			"payload":      map[string]string{"type": "review", "content": "x"},
			"trace":        map[string]string{"trace_id": "t1"},
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.Create(w, req)
	assert.Equal(t, http.StatusCreated, w.Code)
}

func TestTaskHandler_CreateWithFullEnvelope(t *testing.T) {
	svc := &mockTaskService{}
	h := NewTaskHandler(svc)

	futureDeadline := time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339)
	body, _ := json.Marshal(map[string]interface{}{
		"id": "task-full", "source_agent": "agent-a",
		"target_type": "capability", "target_value": "review",
		"deadline": futureDeadline,
		"envelope": map[string]interface{}{
			"janus_version": "0.1", "task_id": "task-full", "tenant_id": "acme",
			"source_agent": "agent-a",
			"target":       map[string]string{"type": "capability", "value": "review"},
			"payload":      map[string]string{"type": "review", "content": "x"},
			"trace":        map[string]string{"trace_id": "t1"},
			"budget": map[string]interface{}{
				"max_tokens":  1000,
				"max_cost_usd": 0.5,
			},
			"policy": map[string]interface{}{
				"data_classification":      "confidential",
				"requires_human_approval": true,
				"allowed_tools":           []string{"search"},
			},
			"context_refs": []map[string]string{
				{"type": "document", "uri": "file:///a.txt", "hash": "abc123", "classification": "internal"},
			},
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.Create(w, req)
	assert.Equal(t, http.StatusCreated, w.Code)

	created := svc.tasks["acme:task-full"]
	require.NotNil(t, created)
	assert.NotNil(t, created.Deadline)
	assert.NotNil(t, created.Envelope.Budget)
	assert.Equal(t, 1000, created.Envelope.Budget.MaxTokens)
	assert.NotNil(t, created.Envelope.Policy)
	assert.True(t, created.Envelope.Policy.RequiresHumanApproval)
	assert.Len(t, created.Envelope.ContextRefs, 1)
	assert.Equal(t, "document", created.Envelope.ContextRefs[0].Type)
}

func TestTaskHandler_CreateBadBody(t *testing.T) {
	h := NewTaskHandler(&mockTaskService{})
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks", bytes.NewReader([]byte("bad")))
	w := httptest.NewRecorder()
	h.Create(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestTaskHandler_CreateError(t *testing.T) {
	h := NewTaskHandler(&mockTaskService{err: fmt.Errorf("fail")})
	body, _ := json.Marshal(map[string]string{
		"id": "t1", "source_agent": "a", "target_type": "cap", "target_value": "r",
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.Create(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestTaskHandler_Get(t *testing.T) {
	svc := &mockTaskService{tasks: map[string]*core.Task{
		"acme:task-1": {ID: "task-1", TenantID: "acme", Status: core.TaskStatusCreated},
	}}
	h := NewTaskHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/tasks/task-1", nil)
	w := httptest.NewRecorder()
	h.Get(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestTaskHandler_GetNotFound(t *testing.T) {
	h := NewTaskHandler(&mockTaskService{})
	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/tasks/nonexistent", nil)
	w := httptest.NewRecorder()
	h.Get(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestTaskHandler_Start(t *testing.T) {
	h := NewTaskHandler(&mockTaskService{})
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/start", nil)
	w := httptest.NewRecorder()
	h.Start(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestTaskHandler_Complete(t *testing.T) {
	h := NewTaskHandler(&mockTaskService{})
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/complete", nil)
	w := httptest.NewRecorder()
	h.Complete(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestTaskHandler_Fail(t *testing.T) {
	h := NewTaskHandler(&mockTaskService{})
	body, _ := json.Marshal(map[string]string{"code": "ERR", "message": "failed"})
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/fail", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.Fail(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestTaskHandler_Cancel(t *testing.T) {
	h := NewTaskHandler(&mockTaskService{})
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/cancel", nil)
	w := httptest.NewRecorder()
	h.Cancel(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestTaskHandler_StartError(t *testing.T) {
	h := NewTaskHandler(&mockTaskService{err: fmt.Errorf("fail")})
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/start", nil)
	w := httptest.NewRecorder()
	h.Start(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestTaskHandler_CompleteError(t *testing.T) {
	h := NewTaskHandler(&mockTaskService{err: fmt.Errorf("already completed")})
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/complete", nil)
	w := httptest.NewRecorder()
	h.Complete(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestTaskHandler_CancelError(t *testing.T) {
	h := NewTaskHandler(&mockTaskService{err: fmt.Errorf("not cancellable")})
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/cancel", nil)
	w := httptest.NewRecorder()
	h.Cancel(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

type mockMailboxService struct {
	mailboxes map[string]*core.Mailbox
	err       error
}

func (m *mockMailboxService) Create(_ context.Context, mb core.Mailbox) error {
	if m.err != nil {
		return m.err
	}
	if m.mailboxes == nil {
		m.mailboxes = make(map[string]*core.Mailbox)
	}
	m.mailboxes[mb.TenantID+":"+mb.ID] = &mb
	return nil
}

func (m *mockMailboxService) Get(_ context.Context, tenantID, mailboxID string) (*core.Mailbox, error) {
	if m.err != nil {
		return nil, m.err
	}
	mb, ok := m.mailboxes[tenantID+":"+mailboxID]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return mb, nil
}

func (m *mockMailboxService) ListByAgent(_ context.Context, tenantID, agentID string) ([]*core.Mailbox, error) {
	return nil, nil
}

func (m *mockMailboxService) UpdateConfig(_ context.Context, tenantID, mailboxID string, maxConcurrency, ackWaitSeconds, maxDeliver, retentionSeconds int) error {
	return m.err
}

func (m *mockMailboxService) Pause(_ context.Context, tenantID, mailboxID string) error {
	return m.err
}

func (m *mockMailboxService) Resume(_ context.Context, tenantID, mailboxID string) error {
	return m.err
}

func TestMailboxHandler_Create(t *testing.T) {
	svc := &mockMailboxService{}
	h := NewMailboxHandler(svc)

	body, _ := json.Marshal(map[string]interface{}{
		"id": "reviewer_default", "agent_id": "agent-1", "max_concurrency": 2,
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/mailboxes", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.Create(w, req)
	assert.Equal(t, http.StatusCreated, w.Code)
}

func TestMailboxHandler_CreateBadBody(t *testing.T) {
	h := NewMailboxHandler(&mockMailboxService{})
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/mailboxes", bytes.NewReader([]byte("bad")))
	w := httptest.NewRecorder()
	h.Create(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestMailboxHandler_CreateError(t *testing.T) {
	h := NewMailboxHandler(&mockMailboxService{err: fmt.Errorf("dup")})
	body, _ := json.Marshal(map[string]string{"id": "mb1", "agent_id": "a1"})
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/mailboxes", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.Create(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestMailboxHandler_Get(t *testing.T) {
	svc := &mockMailboxService{mailboxes: map[string]*core.Mailbox{
		"acme:mb1": {ID: "mb1", TenantID: "acme", AgentID: "a1"},
	}}
	h := NewMailboxHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/mailboxes/mb1", nil)
	w := httptest.NewRecorder()
	h.Get(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestMailboxHandler_GetNotFound(t *testing.T) {
	h := NewMailboxHandler(&mockMailboxService{})
	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/mailboxes/nonexistent", nil)
	w := httptest.NewRecorder()
	h.Get(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestHelpers(t *testing.T) {
	assert.Equal(t, "acme", tenantIDFromPath("/v1/tenants/acme/agents"))
	assert.Equal(t, "", tenantIDFromPath("/v1/agents"))
	assert.Equal(t, "agent-1", lastSegment("/v1/tenants/acme/agents/agent-1"))
	assert.Equal(t, "agent-1", agentIDFromHeartbeatPath("/v1/tenants/acme/agents/agent-1/heartbeat"))
}
