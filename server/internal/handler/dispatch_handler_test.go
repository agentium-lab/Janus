package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

type mockDispatchSvc struct {
	pullResult *ServicePullResult
	pullErr    error
	startErr   error
	hbErr      error
	ackErr     error
	nackErr    error
}

func (m *mockDispatchSvc) PullTask(_ context.Context, tenantID, mailboxID, agentID string) (*ServicePullResult, error) {
	if m.pullErr != nil {
		return nil, m.pullErr
	}
	return m.pullResult, nil
}
func (m *mockDispatchSvc) StartTask(_ context.Context, tenantID, taskID, leaseID string) error {
	return m.startErr
}
func (m *mockDispatchSvc) TaskHeartbeat(_ context.Context, tenantID, taskID, leaseID string) error {
	return m.hbErr
}
func (m *mockDispatchSvc) AckTask(_ context.Context, tenantID, taskID, leaseID string, resultRef string, usage *core.TokenUsage) error {
	return m.ackErr
}
func (m *mockDispatchSvc) NackTask(_ context.Context, tenantID, taskID, leaseID string, retriable bool, taskErr *core.TaskError) error {
	return m.nackErr
}

func TestDispatchHandler_Pull_Success(t *testing.T) {
	svc := &mockDispatchSvc{
		pullResult: &ServicePullResult{
			Task:    &core.Task{ID: "task-1", Status: core.TaskStatusClaimed},
			LeaseID: "lease-abc",
		},
	}
	h := NewDispatchHandler(svc)

	body := `{"agent_id":"agent-1"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/mailboxes/mb-1/pull", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.Pull(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	lease, ok := resp["lease"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "lease-abc", lease["lease_id"])
}

func TestDispatchHandler_Pull_NoMessages(t *testing.T) {
	svc := &mockDispatchSvc{}
	h := NewDispatchHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/mailboxes/mb-1/pull", bytes.NewBufferString(`{}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.Pull(w, req)
	assert.Equal(t, http.StatusNoContent, w.Code)
}

func TestDispatchHandler_Pull_Error(t *testing.T) {
	svc := &mockDispatchSvc{pullErr: assert.AnError}
	h := NewDispatchHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/mailboxes/mb-1/pull", bytes.NewBufferString(`{}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.Pull(w, req)
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}

func TestDispatchHandler_Start_Success(t *testing.T) {
	svc := &mockDispatchSvc{}
	h := NewDispatchHandler(svc)

	body := `{"lease_id":"lease-abc"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/start", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.Start(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestDispatchHandler_Start_Error(t *testing.T) {
	svc := &mockDispatchSvc{startErr: assert.AnError}
	h := NewDispatchHandler(svc)

	body := `{"lease_id":"lease-abc"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/start", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.Start(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestDispatchHandler_Start_BadRequest(t *testing.T) {
	svc := &mockDispatchSvc{}
	h := NewDispatchHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/start", nil)
	w := httptest.NewRecorder()

	h.Start(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestDispatchHandler_Heartbeat_Success(t *testing.T) {
	svc := &mockDispatchSvc{}
	h := NewDispatchHandler(svc)

	body := `{"lease_id":"lease-abc"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/heartbeat", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.Heartbeat(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestDispatchHandler_Heartbeat_Error(t *testing.T) {
	svc := &mockDispatchSvc{hbErr: assert.AnError}
	h := NewDispatchHandler(svc)

	body := `{"lease_id":"lease-abc"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/heartbeat", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.Heartbeat(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestDispatchHandler_Ack_Success(t *testing.T) {
	svc := &mockDispatchSvc{}
	h := NewDispatchHandler(svc)

	body := `{"lease_id":"lease-abc","result_ref":"s3://results/1","token_usage":{"prompt_tokens":10000,"completion_tokens":5000,"total_tokens":15000}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/ack", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.Ack(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestDispatchHandler_Ack_Error(t *testing.T) {
	svc := &mockDispatchSvc{ackErr: assert.AnError}
	h := NewDispatchHandler(svc)

	body := `{"lease_id":"lease-abc"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/ack", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.Ack(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestDispatchHandler_Nack_Success(t *testing.T) {
	svc := &mockDispatchSvc{}
	h := NewDispatchHandler(svc)

	body := `{"lease_id":"lease-abc","retriable":true,"error":{"code":"TIMEOUT","message":"agent timed out"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/nack", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.Nack(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestDispatchHandler_Nack_Error(t *testing.T) {
	svc := &mockDispatchSvc{nackErr: assert.AnError}
	h := NewDispatchHandler(svc)

	body := `{"lease_id":"lease-abc"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/nack", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.Nack(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestDispatchHandler_Pull_MissingParams(t *testing.T) {
	svc := &mockDispatchSvc{}
	h := NewDispatchHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants//mailboxes//pull", bytes.NewBufferString(`{}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.Pull(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}
