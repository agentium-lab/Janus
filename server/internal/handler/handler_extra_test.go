package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/service"
	"github.com/stretchr/testify/assert"
)

// --- Helpers ---

func TestHelpers_IsDuplicateKeyError(t *testing.T) {
	assert.True(t, isDuplicateKeyError(fmt.Errorf("duplicate key value violates unique constraint")))
	assert.True(t, isDuplicateKeyError(fmt.Errorf("ERROR: 23505")))
	assert.False(t, isDuplicateKeyError(fmt.Errorf("some other error")))
	assert.False(t, isDuplicateKeyError(nil))
}

// Tenant IDs come from the router's wildcards (r.PathValue), so the
// parsing edges are pinned by dispatching through a ServeMux registered
// with the real pattern — including a tenant literally named "tasks".
func TestPathValue_TenantID_ThroughRealPattern(t *testing.T) {
	svc := &mockTenantService{tenants: map[string]*core.Tenant{
		"x":     {ID: "x"},
		"tasks": {ID: "tasks"},
	}}
	h := NewTenantHandler(svc)
	for _, path := range []string{"/v1/tenants/x", "/v1/tenants/tasks"} {
		w := dispatchViaMux("GET /v1/tenants/{tenantID}", h.Get, httptest.NewRequest(http.MethodGet, path, nil))
		assert.Equal(t, http.StatusOK, w.Code, path)
	}
	// No tenant wildcard shape: the real router 404s before any handler runs.
	w := dispatchViaMux("GET /v1/tenants/{tenantID}", h.Get, httptest.NewRequest(http.MethodGet, "/v1/agents", nil))
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestHelpers_ReadJSON_NilBody(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/test", nil)
	err := readJSON(req, &struct{}{})
	assert.Error(t, err)
}

// Resource IDs (agent, approval, task, ref) likewise come from the
// router's wildcards; each subtest pins the handler receiving the IDs the
// pattern captured — a wrong capture misses the seeded key and fails.
func TestPathValue_ResourceIDs_ThroughRealPatterns(t *testing.T) {
	t.Run("agent get", func(t *testing.T) {
		svc := &mockAgentService{agents: map[string]*core.Agent{"acme:a1": {ID: "a1"}}}
		h := NewAgentHandler(svc)
		w := dispatchViaMux("GET /v1/tenants/{tenantID}/agents/{agentID}", h.Get,
			httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/agents/a1", nil))
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "a1")
	})
	t.Run("agent heartbeat", func(t *testing.T) {
		h := NewAgentHandler(&mockAgentService{})
		w := dispatchViaMux("POST /v1/tenants/{tenantID}/agents/{agentID}/heartbeat", h.Heartbeat,
			httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/agents/a1/heartbeat", nil))
		assert.Equal(t, http.StatusOK, w.Code)
	})
	t.Run("approval get", func(t *testing.T) {
		svc := &mockApprovalSvcExtra{approvals: map[string]*core.Approval{
			"acme:appr-1": {ID: "appr-1"},
		}}
		h := NewApprovalHandler(svc)
		w := dispatchViaMux("GET /v1/tenants/{tenantID}/approvals/{approvalID}", h.Get,
			httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/approvals/appr-1", nil))
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "appr-1")
	})
	t.Run("context refs by task", func(t *testing.T) {
		h := NewContextRefHandler(service.NewContextRefService(&mockContextRefRepo{}))
		w := dispatchViaMux("GET /v1/tenants/{tenantID}/tasks/{taskID}/context-refs", h.ListByTask,
			httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/tasks/task-1/context-refs", nil))
		assert.Equal(t, http.StatusOK, w.Code)
	})
	t.Run("context ref get", func(t *testing.T) {
		h := NewContextRefHandler(service.NewContextRefService(&mockContextRefRepo{refs: map[string]*core.ContextRef{
			"ref-1": {ID: "ref-1"},
		}}))
		w := dispatchViaMux("GET /v1/tenants/{tenantID}/context-refs/{refID}", h.Get,
			httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/context-refs/ref-1", nil))
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "ref-1")
	})
}

// --- Approval Handler ---

type mockApprovalSvcExtra struct {
	approvals map[string]*core.Approval
	err       error
}

func (m *mockApprovalSvcExtra) RequestApproval(_ context.Context, approval core.Approval) (*core.Approval, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.approvals == nil {
		m.approvals = make(map[string]*core.Approval)
	}
	result := &core.Approval{TenantID: approval.TenantID, TaskID: approval.TaskID, RequestedBy: approval.RequestedBy}
	m.approvals[approval.TenantID+":"+approval.TaskID] = result
	return result, nil
}

func (m *mockApprovalSvcExtra) Approve(_ context.Context, tenantID, approvalID, approver, reason string) error {
	return m.err
}

func (m *mockApprovalSvcExtra) Reject(_ context.Context, tenantID, approvalID, approver, reason string) error {
	return m.err
}

func (m *mockApprovalSvcExtra) Get(_ context.Context, tenantID, approvalID string) (*core.Approval, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.approvals[tenantID+":"+approvalID], nil
}

func (m *mockApprovalSvcExtra) ListPending(_ context.Context, tenantID string, limit int) ([]*core.Approval, error) {
	return nil, nil
}

func TestApprovalHandler_Request(t *testing.T) {
	svc := &mockApprovalSvcExtra{}
	h := NewApprovalHandler(svc)
	body, _ := json.Marshal(map[string]string{"tenant_id": "acme", "task_id": "task-1", "requested_by": "admin"})
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/approvals", strings.NewReader(string(body)))
	w := httptest.NewRecorder()
	h.Request(w, req)
	assert.Equal(t, http.StatusCreated, w.Code)
}

func TestApprovalHandler_RequestBadBody(t *testing.T) {
	svc := &mockApprovalSvcExtra{}
	h := NewApprovalHandler(svc)
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/approvals", strings.NewReader("bad"))
	w := httptest.NewRecorder()
	h.Request(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestApprovalHandler_RequestError(t *testing.T) {
	svc := &mockApprovalSvcExtra{err: fmt.Errorf("conflict")}
	h := NewApprovalHandler(svc)
	body, _ := json.Marshal(map[string]string{"tenant_id": "acme", "task_id": "task-1"})
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/approvals", strings.NewReader(string(body)))
	w := httptest.NewRecorder()
	h.Request(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestApprovalHandler_Approve(t *testing.T) {
	svc := &mockApprovalSvcExtra{}
	h := NewApprovalHandler(svc)
	body, _ := json.Marshal(map[string]string{"approver": "admin", "reason": "ok"})
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/approvals/appr-1/approve", strings.NewReader(string(body)))
	w := httptest.NewRecorder()
	h.Approve(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestApprovalHandler_ApproveBadBody(t *testing.T) {
	svc := &mockApprovalSvcExtra{}
	h := NewApprovalHandler(svc)
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/approvals/appr-1/approve", strings.NewReader("bad"))
	w := httptest.NewRecorder()
	h.Approve(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestApprovalHandler_ApproveError(t *testing.T) {
	svc := &mockApprovalSvcExtra{err: fmt.Errorf("already processed")}
	h := NewApprovalHandler(svc)
	body, _ := json.Marshal(map[string]string{"approver": "admin", "reason": "ok"})
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/approvals/appr-1/approve", strings.NewReader(string(body)))
	w := httptest.NewRecorder()
	h.Approve(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestApprovalHandler_Reject(t *testing.T) {
	svc := &mockApprovalSvcExtra{}
	h := NewApprovalHandler(svc)
	body, _ := json.Marshal(map[string]string{"reason": "denied"})
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/approvals/appr-1/reject", strings.NewReader(string(body)))
	w := httptest.NewRecorder()
	h.Reject(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestApprovalHandler_RejectBadBody(t *testing.T) {
	svc := &mockApprovalSvcExtra{}
	h := NewApprovalHandler(svc)
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/approvals/appr-1/reject", strings.NewReader("bad"))
	w := httptest.NewRecorder()
	h.Reject(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestApprovalHandler_RejectError(t *testing.T) {
	svc := &mockApprovalSvcExtra{err: fmt.Errorf("db error")}
	h := NewApprovalHandler(svc)
	body, _ := json.Marshal(map[string]string{"reason": "denied"})
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/approvals/appr-1/reject", strings.NewReader(string(body)))
	w := httptest.NewRecorder()
	h.Reject(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// --- Context Ref Handler ---

type mockContextRefRepo struct {
	refs map[string]*core.ContextRef
	err  error
}

func (m *mockContextRefRepo) Insert(_ context.Context, ref core.ContextRef) error {
	if m.err != nil {
		return m.err
	}
	if m.refs == nil {
		m.refs = make(map[string]*core.ContextRef)
	}
	m.refs[ref.ID] = &ref
	return nil
}

func (m *mockContextRefRepo) Get(_ context.Context, tenantID, id string) (*core.ContextRef, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.refs != nil {
		if r, ok := m.refs[id]; ok {
			return r, nil
		}
	}
	return nil, fmt.Errorf("not found")
}

func (m *mockContextRefRepo) ListByTask(_ context.Context, tenantID, taskID string) ([]*core.ContextRef, error) {
	if m.err != nil {
		return nil, m.err
	}
	return []*core.ContextRef{}, nil
}

func (m *mockContextRefRepo) Delete(_ context.Context, tenantID, id string) error {
	return m.err
}

func (m *mockContextRefRepo) BindToTask(_ context.Context, _, _, _ string) error {
	return m.err
}

func (m *mockContextRefRepo) UnbindFromTask(_ context.Context, _, _, _ string) error {
	return m.err
}

func TestContextRefHandler_Attach(t *testing.T) {
	svc := service.NewContextRefService(&mockContextRefRepo{})
	h := NewContextRefHandler(svc)
	body, _ := json.Marshal(map[string]interface{}{
		"type": "file", "uri": "s3://bucket/f.txt", "access_scope": []string{"agent-1"},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/context-refs", strings.NewReader(string(body)))
	w := dispatchViaMux("POST /v1/tenants/{tenantID}/context-refs", h.Attach, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestContextRefHandler_Attach_NoTenant(t *testing.T) {
	svc := service.NewContextRefService(&mockContextRefRepo{})
	h := NewContextRefHandler(svc)
	body, _ := json.Marshal(map[string]string{"type": "file", "uri": "s3://b/f"})
	req := httptest.NewRequest(http.MethodPost, "/v1/context-refs", strings.NewReader(string(body)))
	w := httptest.NewRecorder()
	h.Attach(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestContextRefHandler_Attach_BadBody(t *testing.T) {
	svc := service.NewContextRefService(&mockContextRefRepo{})
	h := NewContextRefHandler(svc)
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/context-refs", strings.NewReader("bad"))
	w := httptest.NewRecorder()
	h.Attach(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestContextRefHandler_Attach_Error(t *testing.T) {
	svc := service.NewContextRefService(&mockContextRefRepo{err: fmt.Errorf("db error")})
	h := NewContextRefHandler(svc)
	body, _ := json.Marshal(map[string]string{"type": "file", "uri": "s3://b/f"})
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/context-refs", strings.NewReader(string(body)))
	w := dispatchViaMux("POST /v1/tenants/{tenantID}/context-refs", h.Attach, req)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestContextRefHandler_Get(t *testing.T) {
	svc := service.NewContextRefService(&mockContextRefRepo{refs: map[string]*core.ContextRef{
		"ref-1": {ID: "ref-1", TenantID: "acme", Type: "file"},
	}})
	h := NewContextRefHandler(svc)
	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/context-refs/ref-1", nil)
	w := dispatchViaMux("GET /v1/tenants/{tenantID}/context-refs/{refID}", h.Get, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestContextRefHandler_Get_NotFound(t *testing.T) {
	svc := service.NewContextRefService(&mockContextRefRepo{})
	h := NewContextRefHandler(svc)
	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/context-refs/ref-999", nil)
	w := httptest.NewRecorder()
	h.Get(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestContextRefHandler_ListByTask(t *testing.T) {
	svc := service.NewContextRefService(&mockContextRefRepo{})
	h := NewContextRefHandler(svc)
	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/tasks/task-1/context-refs", nil)
	w := httptest.NewRecorder()
	h.ListByTask(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestContextRefHandler_ListByTask_Error(t *testing.T) {
	svc := service.NewContextRefService(&mockContextRefRepo{err: fmt.Errorf("db error")})
	h := NewContextRefHandler(svc)
	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/tasks/task-1/context-refs", nil)
	w := httptest.NewRecorder()
	h.ListByTask(w, req)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestContextRefHandler_Detach(t *testing.T) {
	svc := service.NewContextRefService(&mockContextRefRepo{})
	h := NewContextRefHandler(svc)
	req := httptest.NewRequest(http.MethodDelete, "/v1/tenants/acme/context-refs/ref-1", nil)
	w := httptest.NewRecorder()
	h.Detach(w, req)
	assert.Equal(t, http.StatusNoContent, w.Code)
}

func TestContextRefHandler_Detach_Error(t *testing.T) {
	svc := service.NewContextRefService(&mockContextRefRepo{err: fmt.Errorf("not found")})
	h := NewContextRefHandler(svc)
	req := httptest.NewRequest(http.MethodDelete, "/v1/tenants/acme/context-refs/ref-1", nil)
	w := httptest.NewRecorder()
	h.Detach(w, req)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

// --- Mailbox Handler additional tests ---

func TestMailboxHandler_Create_DefaultConcurrency(t *testing.T) {
	svc := &mockMailboxService{}
	h := NewMailboxHandler(svc)
	body, _ := json.Marshal(map[string]interface{}{
		"id": "mb-1", "agent_id": "agent-1",
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/mailboxes", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.Create(w, req)
	assert.Equal(t, http.StatusCreated, w.Code)
}

// --- Agent Handler edge cases ---

func TestAgentHandler_Register_DuplicateKey(t *testing.T) {
	svc := &mockAgentService{err: fmt.Errorf("duplicate key value violates unique constraint")}
	h := NewAgentHandler(svc)
	body, _ := json.Marshal(map[string]string{"id": "a1", "display_name": "A1", "protocol": "a2a"})
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/agents", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.Register(w, req)
	assert.Equal(t, http.StatusConflict, w.Code)
}

func TestAgentHandler_Register_DefaultConcurrency(t *testing.T) {
	svc := &mockAgentService{}
	h := NewAgentHandler(svc)
	body, _ := json.Marshal(map[string]string{"id": "a1", "display_name": "A1", "protocol": "a2a"})
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/agents", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.Register(w, req)
	assert.Equal(t, http.StatusCreated, w.Code)
}

func TestSanitizeError_RedactsInternalLeaks(t *testing.T) {
	cases := []struct {
		name   string
		msg    string
		status int
		want   string
	}{
		{"sql driver error on 5xx", "pq: connection refused", 500, "internal server error"},
		{"sql driver error on 4xx", "duplicate key value violates", 400, "bad request"},
		{"DSN leak on 5xx", "postgres://user:pass@host:5432/db?sslmode=disable", 500, "internal server error"},
		{"DSN leak on 4xx", "host=db port=5432 user=u", 404, "bad request"},
		{"runtime error on 5xx", "runtime error: invalid memory address", 500, "internal server error"},
		{"generic 5xx becomes internal", "operation took too long", 500, "internal server error"},
		{"safe message on 4xx passes through", "tenant id is required", 400, "tenant id is required"},
		{"not found safe on 4xx", "not found", 404, "not found"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, sanitizeError(c.msg, c.status))
		})
	}
}

func TestReadJSONWithLimit_RejectsOversizedBody(t *testing.T) {
	big := strings.Repeat("a", 1024)
	req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(big))
	err := readJSONWithLimit(req, &struct{}{}, 16)
	assert.Error(t, err)
}
