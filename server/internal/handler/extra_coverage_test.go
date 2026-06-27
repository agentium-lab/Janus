package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/agentium-lab/Janus/core"
)

// --- Mailbox Update ---

func TestMailboxHandler_Update(t *testing.T) {
	svc := &mockMailboxService{
		mailboxes: map[string]*core.Mailbox{
			"acme:mb-1": {ID: "mb-1", TenantID: "acme", AgentID: "a1", MaxConcurrency: 5},
		},
	}
	h := NewMailboxHandler(svc)

	req := httptest.NewRequest(http.MethodPatch, "/v1/tenants/acme/mailboxes/mb-1", bytes.NewBufferString(`{"max_concurrency": 10, "ack_wait_seconds": 60}`))
	w := httptest.NewRecorder()
	h.Update(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]string
	json.NewDecoder(w.Body).Decode(&resp)
	assert.Equal(t, "updated", resp["status"])
}

func TestMailboxHandler_Update_BadBody(t *testing.T) {
	svc := &mockMailboxService{}
	h := NewMailboxHandler(svc)

	req := httptest.NewRequest(http.MethodPatch, "/v1/tenants/acme/mailboxes/mb-1", bytes.NewBufferString("{invalid"))
	w := httptest.NewRecorder()
	h.Update(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestMailboxHandler_Update_NotFound(t *testing.T) {
	svc := &mockMailboxService{err: fmt.Errorf("not found")}
	h := NewMailboxHandler(svc)

	req := httptest.NewRequest(http.MethodPatch, "/v1/tenants/acme/mailboxes/mb-missing", bytes.NewBufferString(`{}`))
	w := httptest.NewRecorder()
	h.Update(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

// --- Task Block / Unblock ---

func TestTaskHandler_Block(t *testing.T) {
	svc := &mockTaskService{}
	h := NewTaskHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/block", bytes.NewBufferString(`{"reason":"manual review"}`))
	w := httptest.NewRecorder()
	h.Block(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]string
	json.NewDecoder(w.Body).Decode(&resp)
	assert.Equal(t, "blocked", resp["status"])
}

func TestTaskHandler_Block_Error(t *testing.T) {
	svc := &mockTaskService{err: fmt.Errorf("task is terminal")}
	h := NewTaskHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/block", nil)
	w := httptest.NewRecorder()
	h.Block(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestTaskHandler_Unblock(t *testing.T) {
	svc := &mockTaskService{}
	h := NewTaskHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/unblock", nil)
	w := httptest.NewRecorder()
	h.Unblock(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]string
	json.NewDecoder(w.Body).Decode(&resp)
	assert.Equal(t, "running", resp["status"])
}

func TestTaskHandler_Unblock_Error(t *testing.T) {
	svc := &mockTaskService{err: fmt.Errorf("not blocked")}
	h := NewTaskHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/unblock", nil)
	w := httptest.NewRecorder()
	h.Unblock(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// --- DLQ edge cases ---

func TestDLQHandler_Query_NoMailbox(t *testing.T) {
	svc := &mockDLQService{}
	h := NewDLQHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/dlq", nil)
	w := httptest.NewRecorder()
	h.Query(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

// --- errorCodeForStatus ---

func TestErrorCodeForStatus_AllMappings(t *testing.T) {
	cases := []struct {
		status int
		code   string
	}{
		{400, "INVALID_ARGUMENT"},
		{401, "UNAUTHENTICATED"},
		{403, "PERMISSION_DENIED"},
		{404, "NOT_FOUND"},
		{409, "CONFLICT"},
		{429, "RESOURCE_EXHAUSTED"},
		{503, "UNAVAILABLE"},
		{500, "INTERNAL"},
		{502, "INTERNAL"},
		{418, "UNKNOWN"},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.code, errorCodeForStatus(tc.status), "status %d", tc.status)
	}
}

// --- Context ref handler: skipped (NewContextRefHandler takes concrete *service.ContextRefService, not interface) ---


// --- DLQServiceAdapter tests ---

type mockDLQTaskRepo struct {
	tasks        map[string]*core.Task
	listResult   []*core.Task
	updateErr    error
	getErr       error
	listErr      error
	updatedCancels int
}

func (m *mockDLQTaskRepo) ListDeadLettered(_ context.Context, tenantID, mailboxID string, limit int) ([]*core.Task, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	return m.listResult, nil
}

func (m *mockDLQTaskRepo) UpdateStatus(_ context.Context, tenantID, taskID string, status core.TaskStatus, attemptIncrement int) error {
	if m.updateErr != nil {
		return m.updateErr
	}
	if t, ok := m.tasks[tenantID+":"+taskID]; ok {
		t.Status = status
	}
	m.updatedCancels++
	return nil
}

func (m *mockDLQTaskRepo) Get(_ context.Context, tenantID, taskID string) (*core.Task, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	if t, ok := m.tasks[tenantID+":"+taskID]; ok {
		return t, nil
	}
	return nil, fmt.Errorf("not found")
}

type mockDLQQueueDriver struct {
	publishTaskErr  error
	publishEventErr error
	publishedTasks  int
	publishedEvents int
}

func (m *mockDLQQueueDriver) PublishTask(_ context.Context, msg core.TaskMessage) error {
	if m.publishTaskErr != nil {
		return m.publishTaskErr
	}
	m.publishedTasks++
	return nil
}

func (m *mockDLQQueueDriver) PublishEvent(_ context.Context, event core.JanusEvent) error {
	if m.publishEventErr != nil {
		return m.publishEventErr
	}
	m.publishedEvents++
	return nil
}

func TestDLQServiceAdapter_QueryDLQ(t *testing.T) {
	repo := &mockDLQTaskRepo{listResult: []*core.Task{
		{ID: "t1", TenantID: "acme", Status: core.TaskStatusDeadLettered},
	}}
	driver := &mockDLQQueueDriver{}
	adapter := NewDLQServiceAdapter(repo, driver)

	tasks, err := adapter.QueryDLQ(context.Background(), "acme", "mb-1", 10)
	assert.NoError(t, err)
	assert.Len(t, tasks, 1)
}

func TestDLQServiceAdapter_QueryDLQ_EmptyTenant(t *testing.T) {
	adapter := NewDLQServiceAdapter(&mockDLQTaskRepo{}, &mockDLQQueueDriver{})
	tasks, err := adapter.QueryDLQ(context.Background(), "", "mb-1", 10)
	assert.NoError(t, err)
	assert.Nil(t, tasks)
}

func TestDLQServiceAdapter_ReplayDLQ(t *testing.T) {
	repo := &mockDLQTaskRepo{tasks: map[string]*core.Task{
		"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusDeadLettered, MailboxID: "mb-1", Priority: core.PriorityNormal},
	}}
	driver := &mockDLQQueueDriver{}
	adapter := NewDLQServiceAdapter(repo, driver)

	task, err := adapter.ReplayDLQ(context.Background(), "acme", "t1")
	assert.NoError(t, err)
	assert.NotNil(t, task)
}

func TestDLQServiceAdapter_ReplayDLQ_NotDeadLettered(t *testing.T) {
	repo := &mockDLQTaskRepo{tasks: map[string]*core.Task{
		"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusCompleted},
	}}
	adapter := NewDLQServiceAdapter(repo, &mockDLQQueueDriver{})

	_, err := adapter.ReplayDLQ(context.Background(), "acme", "t1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not dead_lettered")
}

func TestDLQServiceAdapter_ReplayDLQ_GetError(t *testing.T) {
	repo := &mockDLQTaskRepo{getErr: fmt.Errorf("db error")}
	adapter := NewDLQServiceAdapter(repo, &mockDLQQueueDriver{})

	_, err := adapter.ReplayDLQ(context.Background(), "acme", "t1")
	assert.Error(t, err)
}

func TestDLQServiceAdapter_ReplayDLQ_PublishError(t *testing.T) {
	repo := &mockDLQTaskRepo{tasks: map[string]*core.Task{
		"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusDeadLettered, MailboxID: "mb-1"},
	}}
	driver := &mockDLQQueueDriver{publishTaskErr: fmt.Errorf("nats down")}
	adapter := NewDLQServiceAdapter(repo, driver)

	_, err := adapter.ReplayDLQ(context.Background(), "acme", "t1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nats down")
}

func TestDLQServiceAdapter_ReplayDLQ_NoMailbox(t *testing.T) {
	repo := &mockDLQTaskRepo{tasks: map[string]*core.Task{
		"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusDeadLettered, MailboxID: ""},
	}}
	driver := &mockDLQQueueDriver{}
	adapter := NewDLQServiceAdapter(repo, driver)

	task, err := adapter.ReplayDLQ(context.Background(), "acme", "t1")
	assert.NoError(t, err)
	assert.NotNil(t, task)
}

func TestDLQServiceAdapter_DiscardDLQ(t *testing.T) {
	repo := &mockDLQTaskRepo{tasks: map[string]*core.Task{
		"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusDeadLettered},
	}}
	adapter := NewDLQServiceAdapter(repo, &mockDLQQueueDriver{})

	err := adapter.DiscardDLQ(context.Background(), "acme", "t1")
	assert.NoError(t, err)
	assert.Equal(t, core.TaskStatusCancelled, repo.tasks["acme:t1"].Status)
}

func TestDLQServiceAdapter_DiscardDLQ_UpdateError(t *testing.T) {
	repo := &mockDLQTaskRepo{updateErr: fmt.Errorf("db locked")}
	adapter := NewDLQServiceAdapter(repo, &mockDLQQueueDriver{})

	err := adapter.DiscardDLQ(context.Background(), "acme", "t1")
	assert.Error(t, err)
}

func TestDlqMustMarshal(t *testing.T) {
	result := dlqMustMarshal(map[string]string{"key": "value"})
	assert.Contains(t, string(result), "key")
}

func TestDLQHandler_Query_Error(t *testing.T) {
	svc := &mockDLQServiceErr{}
	h := NewDLQHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/dlq", nil)
	w := httptest.NewRecorder()
	h.Query(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestDLQHandler_Replay_Error(t *testing.T) {
	svc := &mockDLQServiceErr{}
	h := NewDLQHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/dlq/t1/replay", nil)
	w := httptest.NewRecorder()
	h.Replay(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestDLQHandler_Discard_Error(t *testing.T) {
	svc := &mockDLQServiceErr{}
	h := NewDLQHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/dlq/t1/discard", nil)
	w := httptest.NewRecorder()
	h.Discard(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestTenantAndTaskFromDLQPath(t *testing.T) {
	tenant, task := tenantAndTaskFromDLQPath("/v1/tenants/acme/dlq/task-123/replay")
	assert.Equal(t, "acme", tenant)
	assert.Equal(t, "task-123", task)
}

func TestTenantAndTaskFromDLQPath_Malformed(t *testing.T) {
	tenant, task := tenantAndTaskFromDLQPath("/v1/dlq")
	assert.Equal(t, "", tenant)
	assert.Equal(t, "", task)
}

func TestApprovalHandler_ListPending_Error(t *testing.T) {
	svc := &mockApprovalServiceErr{}
	h := NewApprovalHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/approvals", nil)
	w := httptest.NewRecorder()
	h.ListPending(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

// --- Error mocks ---

type mockDLQServiceErr struct{}

func (m *mockDLQServiceErr) QueryDLQ(_ context.Context, _, _ string, _ int) ([]*core.Task, error) {
	return nil, fmt.Errorf("db error")
}
func (m *mockDLQServiceErr) ReplayDLQ(_ context.Context, _, _ string) (*core.Task, error) {
	return nil, fmt.Errorf("replay error")
}
func (m *mockDLQServiceErr) DiscardDLQ(_ context.Context, _, _ string) error {
	return fmt.Errorf("discard error")
}

type mockApprovalServiceErr struct{}

func (m *mockApprovalServiceErr) RequestApproval(_ context.Context, a core.Approval) (*core.Approval, error) {
	return &a, nil
}
func (m *mockApprovalServiceErr) Approve(_ context.Context, _, _, _, _ string) error { return nil }
func (m *mockApprovalServiceErr) Reject(_ context.Context, _, _, _, _ string) error  { return nil }
func (m *mockApprovalServiceErr) Get(_ context.Context, _, _ string) (*core.Approval, error) {
	return nil, fmt.Errorf("not found")
}
func (m *mockApprovalServiceErr) ListPending(_ context.Context, _ string, _ int) ([]*core.Approval, error) {
	return nil, fmt.Errorf("db error")
}

type mockAuditServiceErr struct{}

func (m *mockAuditServiceErr) QueryByTrace(_ context.Context, _, _ string, _ int) (interface{}, error) {
	return nil, fmt.Errorf("db error")
}

type mockDispatchServiceErr struct{}

func (m *mockDispatchServiceErr) PullTask(_ context.Context, _, _ string) (*core.Task, error) {
	return nil, nil
}
func (m *mockDispatchServiceErr) StartTask(_ context.Context, _, _, _ string) error { return nil }
func (m *mockDispatchServiceErr) TaskHeartbeat(_ context.Context, _, _, _ string) error {
	return fmt.Errorf("heartbeat error")
}
func (m *mockDispatchServiceErr) AckTask(_ context.Context, _, _, _ string) error { return nil }
func (m *mockDispatchServiceErr) NackTask(_ context.Context, _, _, _ string, _ bool, _ string) error {
	return nil
}
