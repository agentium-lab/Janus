package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

// --- Mock ApprovalService ---

type mockApprovalService struct {
	approvals map[string]*core.Approval
}

func (m *mockApprovalService) RequestApproval(_ context.Context, approval core.Approval) (*core.Approval, error) {
	if m.approvals == nil {
		m.approvals = make(map[string]*core.Approval)
	}
	m.approvals[approval.ID] = &approval
	return &approval, nil
}
func (m *mockApprovalService) Approve(_ context.Context, tenantID, approvalID, approver, reason string) error {
	return nil
}
func (m *mockApprovalService) Reject(_ context.Context, tenantID, approvalID, approver, reason string) error {
	return nil
}
func (m *mockApprovalService) Get(_ context.Context, tenantID, approvalID string) (*core.Approval, error) {
	if a, ok := m.approvals[approvalID]; ok {
		return a, nil
	}
	return nil, fmt.Errorf("not found")
}
func (m *mockApprovalService) ListPending(_ context.Context, tenantID string, limit int) ([]*core.Approval, error) {
	var result []*core.Approval
	for _, a := range m.approvals {
		if a.Status == "pending" {
			result = append(result, a)
		}
	}
	return result, nil
}

func TestApprovalHandler_ListPending(t *testing.T) {
	svc := &mockApprovalService{approvals: map[string]*core.Approval{
		"ap-1": {ID: "ap-1", TenantID: "acme", TaskID: "task-1", Status: "pending"},
		"ap-2": {ID: "ap-2", TenantID: "acme", TaskID: "task-2", Status: "pending"},
	}}
	h := NewApprovalHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/approvals", nil)
	w := httptest.NewRecorder()
	h.ListPending(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var approvals []*core.Approval
	json.NewDecoder(w.Body).Decode(&approvals)
	assert.Len(t, approvals, 2)
}

func TestApprovalHandler_ListPending_Empty(t *testing.T) {
	svc := &mockApprovalService{}
	h := NewApprovalHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/approvals", nil)
	w := httptest.NewRecorder()
	h.ListPending(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var approvals []*core.Approval
	json.NewDecoder(w.Body).Decode(&approvals)
	assert.Empty(t, approvals)
}

func TestApprovalHandler_Get(t *testing.T) {
	svc := &mockApprovalService{approvals: map[string]*core.Approval{
		"ap-1": {ID: "ap-1", TenantID: "acme", TaskID: "task-1", Status: "pending"},
	}}
	h := NewApprovalHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/approvals/ap-1", nil)
	w := httptest.NewRecorder()
	h.Get(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var approval core.Approval
	json.NewDecoder(w.Body).Decode(&approval)
	assert.Equal(t, "ap-1", approval.ID)
}

func TestApprovalHandler_Get_NotFound(t *testing.T) {
	svc := &mockApprovalService{}
	h := NewApprovalHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/approvals/nonexistent", nil)
	w := httptest.NewRecorder()
	h.Get(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

// --- DLQ handler tests ---

func TestDLQHandler_Query(t *testing.T) {
	svc := &mockDLQService{tasks: []*core.Task{
		{ID: "task-dlq-1", TenantID: "acme", Status: core.TaskStatusDeadLettered},
	}}
	h := NewDLQHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/dlq?mailbox=mb-1", nil)
	w := httptest.NewRecorder()
	h.Query(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp struct {
		Tasks []*core.Task `json:"tasks"`
	}
	json.NewDecoder(w.Body).Decode(&resp)
	require.Len(t, resp.Tasks, 1)
	assert.Equal(t, "task-dlq-1", resp.Tasks[0].ID)
}

func TestDLQHandler_Replay(t *testing.T) {
	svc := &mockDLQService{
		tasks:    []*core.Task{{ID: "task-dlq-1", TenantID: "acme", Status: core.TaskStatusDeadLettered}},
		replayed: &core.Task{ID: "task-dlq-1", TenantID: "acme", Status: core.TaskStatusCreated},
	}
	h := NewDLQHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/dlq/task-dlq-1/replay", nil)
	w := httptest.NewRecorder()
	h.Replay(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var task core.Task
	json.NewDecoder(w.Body).Decode(&task)
	assert.Equal(t, "task-dlq-1", task.ID)
}

func TestDLQHandler_Discard(t *testing.T) {
	svc := &mockDLQService{}
	h := NewDLQHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/dlq/task-dlq-1/discard", nil)
	w := httptest.NewRecorder()
	h.Discard(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

// mockDLQService implements DLQService for handler tests.
type mockDLQService struct {
	tasks    []*core.Task
	replayed *core.Task
}

func (m *mockDLQService) QueryDLQ(_ context.Context, tenantID, mailboxID string, limit int) ([]*core.Task, error) {
	return m.tasks, nil
}
func (m *mockDLQService) ReplayDLQ(_ context.Context, tenantID, taskID string) (*core.Task, error) {
	if m.replayed != nil {
		return m.replayed, nil
	}
	return &core.Task{ID: taskID, Status: core.TaskStatusCreated}, nil
}
func (m *mockDLQService) DiscardDLQ(_ context.Context, tenantID, taskID string) error {
	return nil
}
