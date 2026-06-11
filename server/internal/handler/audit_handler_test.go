package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockAuditSvc struct {
	taskEvents  []map[string]string
	traceEvents []map[string]string
	err         error
}

func (m *mockAuditSvc) QueryByTask(_ context.Context, tenantID, taskID string, limit int) (interface{}, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.taskEvents, nil
}

func (m *mockAuditSvc) QueryByTrace(_ context.Context, tenantID, traceID string, limit int) (interface{}, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.traceEvents, nil
}

func (m *mockAuditSvc) QueryByTenant(_ context.Context, tenantID string, limit int) (interface{}, error) {
	if m.err != nil {
		return nil, m.err
	}
	return []struct{}{}, nil
}

func TestAuditHandler_QueryByTask(t *testing.T) {
	svc := &mockAuditSvc{
		taskEvents: []map[string]string{
			{"event_id": "e1", "event_type": "task.created"},
			{"event_id": "e2", "event_type": "task.completed"},
		},
	}
	h := NewAuditHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/tasks/task-1/events?limit=10", nil)
	w := httptest.NewRecorder()
	h.QueryByTask(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	events, ok := resp["events"].([]interface{})
	require.True(t, ok)
	assert.Len(t, events, 2)
}

func TestAuditHandler_QueryByTask_Error(t *testing.T) {
	svc := &mockAuditSvc{err: assert.AnError}
	h := NewAuditHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/tasks/task-1/events", nil)
	w := httptest.NewRecorder()
	h.QueryByTask(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestAuditHandler_QueryByTrace(t *testing.T) {
	svc := &mockAuditSvc{
		traceEvents: []map[string]string{
			{"event_id": "e1", "event_type": "task.created"},
		},
	}
	h := NewAuditHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/traces/trace-abc?limit=20", nil)
	w := httptest.NewRecorder()
	h.QueryByTrace(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	events, ok := resp["events"].([]interface{})
	require.True(t, ok)
	assert.Len(t, events, 1)
}

func TestAuditHandler_QueryByTrace_Error(t *testing.T) {
	svc := &mockAuditSvc{err: assert.AnError}
	h := NewAuditHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/traces/trace-abc", nil)
	w := httptest.NewRecorder()
	h.QueryByTrace(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestAuditHandler_QueryByTenant(t *testing.T) {
	svc := &mockAuditSvc{}
	h := NewAuditHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/events?limit=10", nil)
	w := httptest.NewRecorder()
	h.QueryByTenant(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestAuditHandler_QueryByTenant_Error(t *testing.T) {
	svc := &mockAuditSvc{err: assert.AnError}
	h := NewAuditHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/events", nil)
	w := httptest.NewRecorder()
	h.QueryByTenant(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestAuditHandler_QueryByTenant_DefaultLimit(t *testing.T) {
	svc := &mockAuditSvc{}
	h := NewAuditHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/events", nil)
	w := httptest.NewRecorder()
	h.QueryByTenant(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestTaskHandler_Replay(t *testing.T) {
	svc := &mockTaskService{}
	h := NewTaskHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/replay", nil)
	w := httptest.NewRecorder()
	h.Replay(w, req)

	assert.Equal(t, http.StatusNotImplemented, w.Code)
}
