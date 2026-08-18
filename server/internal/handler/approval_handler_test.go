package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

type approvalHandlerSvcMock struct {
	requestResult *core.Approval
	requestErr    error
	approveErr    error
	rejectErr     error
	getResult     *core.Approval
	getErr        error
	listResult    []*core.Approval
	listErr       error

	approveCalled bool
	rejectCalled  bool
	lastApprover  string
	lastReason    string
}

func (m *approvalHandlerSvcMock) RequestApproval(_ context.Context, approval core.Approval) (*core.Approval, error) {
	if m.requestErr != nil {
		return nil, m.requestErr
	}
	if m.requestResult != nil {
		return m.requestResult, nil
	}
	return &approval, nil
}

func (m *approvalHandlerSvcMock) Approve(_ context.Context, _, _, approver, reason string) error {
	m.approveCalled = true
	m.lastApprover = approver
	m.lastReason = reason
	return m.approveErr
}

func (m *approvalHandlerSvcMock) Reject(_ context.Context, _, _, approver, reason string) error {
	m.rejectCalled = true
	m.lastApprover = approver
	m.lastReason = reason
	return m.rejectErr
}

func (m *approvalHandlerSvcMock) Get(_ context.Context, _, _ string) (*core.Approval, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	if m.getResult != nil {
		return m.getResult, nil
	}
	return &core.Approval{Status: "pending"}, nil
}

func (m *approvalHandlerSvcMock) ListPending(_ context.Context, _ string, _ int) ([]*core.Approval, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	return m.listResult, nil
}

func TestApprovalHandler_Request_HappyPath(t *testing.T) {
	svc := &approvalHandlerSvcMock{
		requestResult: &core.Approval{ID: "appr-1", TenantID: "acme", TaskID: "task-1", Status: "pending"},
	}
	h := NewApprovalHandler(svc)

	body := `{"task_id":"task-1","requested_by":"alice"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/approvals", strings.NewReader(body))
	w := httptest.NewRecorder()

	h.Request(w, req)

	require.Equal(t, http.StatusCreated, w.Code)
	var got core.Approval
	require.NoError(t, json.NewDecoder(w.Body).Decode(&got))
	assert.Equal(t, "appr-1", got.ID)
}

func TestApprovalHandler_Request_BadJSON(t *testing.T) {
	svc := &approvalHandlerSvcMock{}
	h := NewApprovalHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/approvals", strings.NewReader("{not json"))
	w := httptest.NewRecorder()

	h.Request(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	var env errorEnvelope
	require.NoError(t, json.NewDecoder(w.Body).Decode(&env))
	assert.Equal(t, "INVALID_ARGUMENT", env.Code)
}

func TestApprovalHandler_Request_ServiceError(t *testing.T) {
	svc := &approvalHandlerSvcMock{requestErr: errors.New("conflict")}
	h := NewApprovalHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/approvals",
		strings.NewReader(`{"task_id":"task-1","requested_by":"alice"}`))
	w := httptest.NewRecorder()

	h.Request(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestApprovalHandler_Approve_HappyPath(t *testing.T) {
	svc := &approvalHandlerSvcMock{}
	h := NewApprovalHandler(svc)

	body := `{"approver":"bob","reason":"ok"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/approvals/appr-1/approve", strings.NewReader(body))
	w := httptest.NewRecorder()

	h.Approve(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.True(t, svc.approveCalled)
	assert.Equal(t, "bob", svc.lastApprover)
	assert.Equal(t, "ok", svc.lastReason)
}

func TestApprovalHandler_Approve_ServiceError(t *testing.T) {
	svc := &approvalHandlerSvcMock{approveErr: errors.New("already decided")}
	h := NewApprovalHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/approvals/appr-1/approve",
		strings.NewReader(`{"approver":"bob","reason":"ok"}`))
	w := httptest.NewRecorder()

	h.Approve(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	var env errorEnvelope
	require.NoError(t, json.NewDecoder(w.Body).Decode(&env))
	assert.Equal(t, "INVALID_ARGUMENT", env.Code)
}

func TestApprovalHandler_Reject_HappyPath(t *testing.T) {
	svc := &approvalHandlerSvcMock{}
	h := NewApprovalHandler(svc)

	body := `{"approver":"carol","reason":"denied"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/approvals/appr-1/reject", strings.NewReader(body))
	w := httptest.NewRecorder()

	h.Reject(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.True(t, svc.rejectCalled)
	assert.Equal(t, "carol", svc.lastApprover)
}

func TestApprovalHandler_ListPending_HappyPath(t *testing.T) {
	svc := &approvalHandlerSvcMock{listResult: []*core.Approval{
		{ID: "a1", Status: "pending"},
		{ID: "a2", Status: "pending"},
	}}
	h := NewApprovalHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/approvals", nil)
	w := httptest.NewRecorder()

	h.ListPending(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var got []*core.Approval
	require.NoError(t, json.NewDecoder(w.Body).Decode(&got))
	assert.Len(t, got, 2)
}

func TestApprovalHandler_ListPending_EmptyJSONArray(t *testing.T) {
	svc := &approvalHandlerSvcMock{}
	h := NewApprovalHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/approvals", nil)
	w := httptest.NewRecorder()

	h.ListPending(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "[]", strings.TrimSpace(w.Body.String()))
}

func TestApprovalHandler_ListPending_ErrorEnvelope(t *testing.T) {
	svc := &approvalHandlerSvcMock{listErr: errors.New("db down")}
	h := NewApprovalHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/approvals", nil)
	w := httptest.NewRecorder()

	h.ListPending(w, req)

	require.Equal(t, http.StatusInternalServerError, w.Code)
	var env errorEnvelope
	require.NoError(t, json.NewDecoder(w.Body).Decode(&env))
	assert.Equal(t, "INTERNAL", env.Code)
}

func TestApprovalHandler_Get_HappyPath(t *testing.T) {
	svc := &approvalHandlerSvcMock{getResult: &core.Approval{ID: "appr-1", Status: "pending"}}
	h := NewApprovalHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/approvals/appr-1", nil)
	w := httptest.NewRecorder()

	h.Get(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var got core.Approval
	require.NoError(t, json.NewDecoder(w.Body).Decode(&got))
	assert.Equal(t, "appr-1", got.ID)
}

func TestApprovalHandler_Get_NotFoundEnvelope(t *testing.T) {
	svc := &approvalHandlerSvcMock{getErr: errors.New("not found")}
	h := NewApprovalHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/approvals/missing", nil)
	w := httptest.NewRecorder()

	h.Get(w, req)

	require.Equal(t, http.StatusNotFound, w.Code)
	var env errorEnvelope
	require.NoError(t, json.NewDecoder(w.Body).Decode(&env))
	assert.Equal(t, "NOT_FOUND", env.Code)
}
