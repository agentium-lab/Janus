package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

type dlqHandlerSvcMock struct {
	queryResult  []*core.Task
	queryErr     error
	replayResult *core.Task
	replayErr    error
	discardErr   error

	lastMailbox string
	lastLimit   int
	lastTaskID  string
}

func (m *dlqHandlerSvcMock) QueryDLQ(_ context.Context, _ string, mailbox string, limit int) ([]*core.Task, error) {
	m.lastMailbox = mailbox
	m.lastLimit = limit
	if m.queryErr != nil {
		return nil, m.queryErr
	}
	return m.queryResult, nil
}

func (m *dlqHandlerSvcMock) ReplayDLQ(_ context.Context, _ string, taskID string) (*core.Task, error) {
	m.lastTaskID = taskID
	if m.replayErr != nil {
		return nil, m.replayErr
	}
	if m.replayResult != nil {
		return m.replayResult, nil
	}
	return &core.Task{ID: taskID, Status: core.TaskStatusCreated}, nil
}

func (m *dlqHandlerSvcMock) DiscardDLQ(_ context.Context, _ string, taskID string) error {
	m.lastTaskID = taskID
	return m.discardErr
}

func TestDLQHandler_Query_ReturnsTasksEnvelope(t *testing.T) {
	svc := &dlqHandlerSvcMock{queryResult: []*core.Task{
		{ID: "t1", TenantID: "acme", Status: core.TaskStatusDeadLettered, MailboxID: "mb-1"},
		{ID: "t2", TenantID: "acme", Status: core.TaskStatusDeadLettered, MailboxID: "mb-1"},
	}}
	h := NewDLQHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/dlq?mailbox=mb-1", nil)
	w := httptest.NewRecorder()

	h.Query(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var resp struct {
		Tasks []*core.Task `json:"tasks"`
	}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	require.Len(t, resp.Tasks, 2)
	assert.Equal(t, "mb-1", svc.lastMailbox)
}

func TestDLQHandler_Query_EmptyReturnsEmptyArray(t *testing.T) {
	svc := &dlqHandlerSvcMock{}
	h := NewDLQHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/dlq", nil)
	w := httptest.NewRecorder()

	h.Query(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var resp struct {
		Tasks []*core.Task `json:"tasks"`
	}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Empty(t, resp.Tasks)
}

func TestDLQHandler_Query_CustomLimit(t *testing.T) {
	svc := &dlqHandlerSvcMock{}
	h := NewDLQHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/dlq?limit=10", nil)
	w := httptest.NewRecorder()

	h.Query(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, 10, svc.lastLimit)
}

func TestDLQHandler_Query_ErrorEnvelope(t *testing.T) {
	svc := &dlqHandlerSvcMock{queryErr: errors.New("db unavailable")}
	h := NewDLQHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/dlq", nil)
	w := httptest.NewRecorder()

	h.Query(w, req)

	require.Equal(t, http.StatusInternalServerError, w.Code)
	var env errorEnvelope
	require.NoError(t, json.NewDecoder(w.Body).Decode(&env))
	assert.Equal(t, "INTERNAL", env.Code)
}

func TestDLQHandler_Replay_HappyPath(t *testing.T) {
	svc := &dlqHandlerSvcMock{replayResult: &core.Task{ID: "t1", TenantID: "acme", Status: core.TaskStatusCreated}}
	h := NewDLQHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/dlq/t1/replay", nil)
	w := httptest.NewRecorder()

	h.Replay(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var got core.Task
	require.NoError(t, json.NewDecoder(w.Body).Decode(&got))
	assert.Equal(t, "t1", got.ID)
	assert.Equal(t, "t1", svc.lastTaskID)
}

func TestDLQHandler_Replay_ErrorEnvelope(t *testing.T) {
	svc := &dlqHandlerSvcMock{replayErr: errors.New("not dead-lettered")}
	h := NewDLQHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/dlq/t1/replay", nil)
	w := httptest.NewRecorder()

	h.Replay(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	var env errorEnvelope
	require.NoError(t, json.NewDecoder(w.Body).Decode(&env))
	assert.Equal(t, "INVALID_ARGUMENT", env.Code)
}

func TestDLQHandler_Discard_HappyPath(t *testing.T) {
	svc := &dlqHandlerSvcMock{}
	h := NewDLQHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/dlq/t1/discard", nil)
	w := httptest.NewRecorder()

	h.Discard(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var resp map[string]string
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "discarded", resp["status"])
	assert.Equal(t, "t1", svc.lastTaskID)
}

func TestDLQHandler_Discard_ErrorEnvelope(t *testing.T) {
	svc := &dlqHandlerSvcMock{discardErr: errors.New("already discarded")}
	h := NewDLQHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/dlq/t1/discard", nil)
	w := httptest.NewRecorder()

	h.Discard(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	var env errorEnvelope
	require.NoError(t, json.NewDecoder(w.Body).Decode(&env))
	assert.Equal(t, "INVALID_ARGUMENT", env.Code)
}
