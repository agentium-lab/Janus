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

func TestHelpers_TenantIDFromPath_Edges(t *testing.T) {
	assert.Equal(t, "", tenantIDFromPath("/v1/agents"))
	assert.Equal(t, "", tenantIDFromPath("/tenants"))
	assert.Equal(t, "x", tenantIDFromPath("/v1/tenants/x"))
}

func TestHelpers_ReadJSON_NilBody(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/test", nil)
	err := readJSON(req, &struct{}{})
	assert.Error(t, err)
}

func TestPathSegmentByMarker(t *testing.T) {
	assert.Equal(t, "acme", pathSegmentByMarker("/v1/tenants/acme/tasks", "tenants"))
	assert.Equal(t, "", pathSegmentByMarker("/v1/foo/bar", "tenants"))
	assert.Equal(t, "", pathSegmentByMarker("", "tenants"))
}

func TestPathSegmentBefore(t *testing.T) {
	assert.Equal(t, "task-1", pathSegmentBefore("/v1/tenants/acme/tasks/task-1/context-refs", "context-refs"))
	assert.Equal(t, "v1", pathSegmentBefore("/v1/context-refs", "context-refs"))
}

func TestLastPathSegment(t *testing.T) {
	assert.Equal(t, "ref-1", lastPathSegment("/v1/tenants/acme/context-refs/ref-1"))
	assert.Equal(t, "", lastPathSegment(""))
}

func TestAgentIDFromHeartbeatPath(t *testing.T) {
	assert.Equal(t, "a1", agentIDFromHeartbeatPath("/v1/tenants/acme/agents/a1/heartbeat"))
	assert.Equal(t, "", agentIDFromHeartbeatPath("/v1/tenants/acme"))
}

func TestExtractTenantAndApproval(t *testing.T) {
	tid, aid := extractTenantAndApproval("/v1/tenants/acme/approvals/appr-1/approve")
	assert.Equal(t, "acme", tid)
	assert.Equal(t, "appr-1", aid)

	tid, aid = extractTenantAndApproval("/v1/tenants")
	assert.Equal(t, "", tid)
	assert.Equal(t, "", aid)
}

func TestStringsSplit(t *testing.T) {
	assert.Nil(t, stringsSplit("", "/"))
	assert.Equal(t, []string{"", "v1", "tenants"}, stringsSplit("/v1/tenants", "/"))
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
	w := httptest.NewRecorder()
	h.Attach(w, req)
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
	w := httptest.NewRecorder()
	h.Attach(w, req)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestContextRefHandler_Get(t *testing.T) {
	svc := service.NewContextRefService(&mockContextRefRepo{refs: map[string]*core.ContextRef{
		"ref-1": {ID: "ref-1", TenantID: "acme", Type: "file"},
	}})
	h := NewContextRefHandler(svc)
	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/context-refs/ref-1", nil)
	w := httptest.NewRecorder()
	h.Get(w, req)
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
