package a2a

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/agentium-lab/Janus/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockLister struct {
	tasks   []*core.Task
	nextTok string
	calls   int
}

func (m *mockLister) ListPage(_ context.Context, _ string, pageSize int, pageToken string) ([]*core.Task, string, error) {
	m.calls++
	if pageToken == "end" {
		return nil, "", nil
	}
	start := 0
	if pageToken == "p2" {
		start = pageSize
	}
	end := start + pageSize
	if end > len(m.tasks) {
		end = len(m.tasks)
	}
	return m.tasks[start:end], m.nextTok, nil
}

func TestGatewayV1_ListTasks(t *testing.T) {
	now := time.Now().UTC()
	lister := &mockLister{tasks: []*core.Task{
		{TenantID: "acme", ID: "t1", Status: core.TaskStatusRunning, CreatedAt: now, UpdatedAt: now},
		{TenantID: "acme", ID: "t2", Status: core.TaskStatusCompleted, CreatedAt: now, UpdatedAt: now},
	}, nextTok: "p2"}
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, &mockStatusGetter{}).WithTaskLister(lister)

	req := withAuthCtx(httptest.NewRequest(http.MethodGet, "/a2a/tasks?pageSize=1", nil))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "application/a2a+json")

	var resp struct {
		Tasks         []map[string]interface{} `json:"tasks"`
		NextPageToken string                   `json:"nextPageToken"`
		PageSize      float64                  `json:"pageSize"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Len(t, resp.Tasks, 1)
	assert.Equal(t, float64(1), resp.PageSize)
	assert.Equal(t, "p2", resp.NextPageToken)
}

func TestGatewayV1_ListTasks_NotConfigured(t *testing.T) {
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, &mockStatusGetter{})
	req := withAuthCtx(httptest.NewRequest(http.MethodGet, "/a2a/tasks", nil))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}

func TestGatewayV1_VersionNegotiation(t *testing.T) {
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, &mockStatusGetter{})

	req := withAuthCtx(httptest.NewRequest(http.MethodGet, "/a2a/tasks/t-1", nil))
	req.Header.Set("A2A-Version", "0.3")
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "VERSION_NOT_SUPPORTED")

	req2 := withAuthCtx(httptest.NewRequest(http.MethodGet, "/a2a/tasks/t-1", nil))
	w2 := httptest.NewRecorder()
	gw.ServeHTTP(w2, req2)
	assert.Equal(t, http.StatusNotFound, w2.Code, "empty version header passes version check")

	req3 := withAuthCtx(httptest.NewRequest(http.MethodGet, "/a2a/tasks/t-1", nil))
	req3.Header.Set("A2A-Version", "1.0")
	w3 := httptest.NewRecorder()
	gw.ServeHTTP(w3, req3)
	assert.Equal(t, http.StatusNotFound, w3.Code, "1.0 passes version check (404 = task lookup, not version)")
}

func TestGatewayV1_Cancel_ReturnsTaskObject(t *testing.T) {
	cancelled := completedTask("t-x")
	cancelled.Status = core.TaskStatusCancelled
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, &mockStatusGetter{task: cancelled})

	req := withAuthCtx(httptest.NewRequest(http.MethodPost, "/a2a/tasks/t-x:cancel", nil))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	var resp map[string]interface{}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	taskObj, ok := resp["task"].(map[string]interface{})
	require.True(t, ok, "CancelTask must return the Task object per proto, got: %v", resp)
	assert.Equal(t, "t-x", taskObj["id"])
}

func TestGatewayV1_StreamMessage_TaskIDSemantics(t *testing.T) {
	running := &core.Task{TenantID: "acme", ID: "t-live", Status: core.TaskStatusRunning,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
		Envelope: core.TaskEnvelope{Trace: core.TraceContext{TraceID: "ctx-live"}}}
	otherCtx := &core.Task{TenantID: "acme", ID: "t-live", Status: core.TaskStatusRunning,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
		Envelope: core.TaskEnvelope{Trace: core.TraceContext{TraceID: "ctx-other"}}}

	t.Run("nonexistent_taskId_rejected", func(t *testing.T) {
		gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, &mockStatusGetter{err: fmt.Errorf("no rows")}).WithEventSubscriber(newMockSubscriber())
		body := `{"message":{"role":"ROLE_USER","parts":[{"text":"hi"}],"taskId":"ghost"}}`
		req := withAuthCtx(httptest.NewRequest(http.MethodPost, "/a2a/message:stream", strings.NewReader(body)))
		w := httptest.NewRecorder()
		gw.ServeHTTP(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "INVALID_ARGUMENT")
	})

	t.Run("live_taskId_honest_rejection", func(t *testing.T) {
		gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, &mockStatusGetter{task: running}).WithEventSubscriber(newMockSubscriber())
		body := `{"message":{"role":"ROLE_USER","parts":[{"text":"hi"}],"taskId":"t-live"}}`
		req := withAuthCtx(httptest.NewRequest(http.MethodPost, "/a2a/message:stream", strings.NewReader(body)))
		w := httptest.NewRecorder()
		gw.ServeHTTP(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "continuation is not yet supported")
	})

	t.Run("contextId_mismatch_rejected", func(t *testing.T) {
		gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, &mockStatusGetter{task: otherCtx}).WithEventSubscriber(newMockSubscriber())
		body := `{"message":{"role":"ROLE_USER","parts":[{"text":"hi"}],"taskId":"t-live","contextId":"ctx-live"}}`
		req := withAuthCtx(httptest.NewRequest(http.MethodPost, "/a2a/message:stream", strings.NewReader(body)))
		w := httptest.NewRecorder()
		gw.ServeHTTP(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "contextId does not match")
	})
}
