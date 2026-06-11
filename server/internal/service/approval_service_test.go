package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/agentium-lab/Janus/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockApprovalRepo struct {
	approvals map[string]*core.Approval
	err       error
}

func (m *mockApprovalRepo) Create(_ context.Context, a core.Approval) error {
	if m.err != nil {
		return m.err
	}
	if m.approvals == nil {
		m.approvals = make(map[string]*core.Approval)
	}
	m.approvals[a.TenantID+":"+a.ID] = &a
	return nil
}

func (m *mockApprovalRepo) Get(_ context.Context, tenantID, approvalID string) (*core.Approval, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.approvals[tenantID+":"+approvalID], nil
}

func (m *mockApprovalRepo) GetPendingByTask(_ context.Context, tenantID, taskID string) (*core.Approval, error) {
	if m.err != nil {
		return nil, m.err
	}
	for _, a := range m.approvals {
		if a.TaskID == taskID && a.TenantID == tenantID && a.Status == "pending" {
			return a, nil
		}
	}
	return nil, fmt.Errorf("not found")
}

func (m *mockApprovalRepo) UpdateDecision(_ context.Context, tenantID, approvalID, decision, approver, reason string) error {
	if m.err != nil {
		return m.err
	}
	a, ok := m.approvals[tenantID+":"+approvalID]
	if !ok {
		return fmt.Errorf("not found")
	}
	a.Status = decision
	a.Decision = decision
	a.Approver = approver
	a.Reason = reason
	return nil
}

func (m *mockApprovalRepo) ListPending(_ context.Context, tenantID string, limit int) ([]*core.Approval, error) {
	if m.err != nil {
		return nil, m.err
	}
	var result []*core.Approval
	for _, a := range m.approvals {
		if a.TenantID == tenantID && a.Status == "pending" {
			result = append(result, a)
		}
	}
	return result, nil
}

func TestApprovalService_RequestApproval(t *testing.T) {
	repo := &mockApprovalRepo{}
	taskSvc := NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil)
	svc := NewApprovalService(repo, taskSvc)

	result, err := svc.RequestApproval(context.Background(), core.Approval{
		TenantID:    "acme",
		TaskID:      "task-1",
		RequestedBy: "agent-1",
	})
	require.NoError(t, err)
	assert.NotEmpty(t, result.ID)
	assert.Equal(t, "pending", result.Status)
	assert.False(t, result.ExpiresAt.IsZero())
}

func TestApprovalService_RequestApproval_MissingFields(t *testing.T) {
	repo := &mockApprovalRepo{}
	taskSvc := NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil)
	svc := NewApprovalService(repo, taskSvc)

	_, err := svc.RequestApproval(context.Background(), core.Approval{
		TenantID: "acme",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "required")
}

func TestApprovalService_Approve(t *testing.T) {
	approvalID := "apr-1"
	repo := &mockApprovalRepo{approvals: map[string]*core.Approval{
		"acme:apr-1": {TenantID: "acme", ID: approvalID, TaskID: "task-1", Status: "pending", ExpiresAt: time.Now().Add(1 * time.Hour)},
	}}
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:task-1": {TenantID: "acme", ID: "task-1", Status: core.TaskStatusApprovalPending},
	}}
	taskSvc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	svc := NewApprovalService(repo, taskSvc)

	err := svc.Approve(context.Background(), "acme", approvalID, "admin", "looks good")
	require.NoError(t, err)
	assert.Equal(t, "approved", repo.approvals["acme:apr-1"].Status)
}

func TestApprovalService_Reject(t *testing.T) {
	approvalID := "apr-2"
	repo := &mockApprovalRepo{approvals: map[string]*core.Approval{
		"acme:apr-2": {TenantID: "acme", ID: approvalID, TaskID: "task-2", Status: "pending", ExpiresAt: time.Now().Add(1 * time.Hour)},
	}}
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:task-2": {TenantID: "acme", ID: "task-2", Status: core.TaskStatusApprovalPending},
	}}
	taskSvc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	svc := NewApprovalService(repo, taskSvc)

	err := svc.Reject(context.Background(), "acme", approvalID, "admin", "security risk")
	require.NoError(t, err)
	assert.Equal(t, "rejected", repo.approvals["acme:apr-2"].Status)
}

func TestApprovalService_Approve_AlreadyDecided(t *testing.T) {
	repo := &mockApprovalRepo{approvals: map[string]*core.Approval{
		"acme:apr-3": {TenantID: "acme", ID: "apr-3", Status: "approved"},
	}}
	taskSvc := NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil)
	svc := NewApprovalService(repo, taskSvc)

	err := svc.Approve(context.Background(), "acme", "apr-3", "admin", "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already decided")
}

func TestApprovalService_Expire(t *testing.T) {
	repo := &mockApprovalRepo{approvals: map[string]*core.Approval{
		"acme:apr-4": {TenantID: "acme", ID: "apr-4", TaskID: "task-4", Status: "pending"},
	}}
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:task-4": {TenantID: "acme", ID: "task-4", Status: core.TaskStatusApprovalPending},
	}}
	taskSvc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	svc := NewApprovalService(repo, taskSvc)

	err := svc.Expire(context.Background(), "acme", "apr-4")
	require.NoError(t, err)
	assert.Equal(t, "expired", repo.approvals["acme:apr-4"].Status)
}

func TestApprovalService_Get(t *testing.T) {
	repo := &mockApprovalRepo{approvals: map[string]*core.Approval{
		"acme:apr-1": {TenantID: "acme", ID: "apr-1", Status: "pending"},
	}}
	svc := NewApprovalService(repo, nil)

	result, err := svc.Get(context.Background(), "acme", "apr-1")
	require.NoError(t, err)
	assert.Equal(t, "pending", result.Status)
}

func TestApprovalService_ListPending(t *testing.T) {
	repo := &mockApprovalRepo{approvals: map[string]*core.Approval{
		"acme:a1": {TenantID: "acme", ID: "a1", Status: "pending"},
		"acme:a2": {TenantID: "acme", ID: "a2", Status: "approved"},
		"acme:a3": {TenantID: "acme", ID: "a3", Status: "pending"},
	}}
	svc := NewApprovalService(repo, nil)

	result, err := svc.ListPending(context.Background(), "acme", 10)
	require.NoError(t, err)
	assert.Len(t, result, 2)
}

func TestApprovalService_ListPending_DefaultLimit(t *testing.T) {
	svc := NewApprovalService(&mockApprovalRepo{}, nil)
	result, err := svc.ListPending(context.Background(), "acme", 0)
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestApprovalService_Approve_Expired(t *testing.T) {
	repo := &mockApprovalRepo{approvals: map[string]*core.Approval{
		"acme:apr-exp": {TenantID: "acme", ID: "apr-exp", TaskID: "task-exp", Status: "pending", ExpiresAt: time.Now().Add(-1 * time.Hour)},
	}}
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:task-exp": {TenantID: "acme", ID: "task-exp", Status: core.TaskStatusApprovalPending},
	}}
	taskSvc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	svc := NewApprovalService(repo, taskSvc)

	err := svc.Approve(context.Background(), "acme", "apr-exp", "admin", "late")
	require.NoError(t, err)
	assert.Equal(t, "expired", repo.approvals["acme:apr-exp"].Status)
}

func TestApprovalService_Reject_AlreadyDecided(t *testing.T) {
	repo := &mockApprovalRepo{approvals: map[string]*core.Approval{
		"acme:apr-5": {TenantID: "acme", ID: "apr-5", Status: "rejected"},
	}}
	svc := NewApprovalService(repo, NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil))

	err := svc.Reject(context.Background(), "acme", "apr-5", "admin", "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already decided")
}

func TestApprovalService_Approve_GetError(t *testing.T) {
	repo := &mockApprovalRepo{err: fmt.Errorf("db down")}
	svc := NewApprovalService(repo, nil)

	err := svc.Approve(context.Background(), "acme", "apr-1", "admin", "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get approval")
}

func TestApprovalService_Reject_GetError(t *testing.T) {
	repo := &mockApprovalRepo{err: fmt.Errorf("db down")}
	svc := NewApprovalService(repo, nil)

	err := svc.Reject(context.Background(), "acme", "apr-1", "admin", "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get approval")
}

func TestApprovalService_Expire_UpdateError(t *testing.T) {
	repo := &mockApprovalRepo{err: fmt.Errorf("db down")}
	svc := NewApprovalService(repo, nil)

	err := svc.Expire(context.Background(), "acme", "apr-1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expire approval")
}

func TestApprovalService_RequestApproval_RepoError(t *testing.T) {
	repo := &mockApprovalRepo{err: fmt.Errorf("db down")}
	svc := NewApprovalService(repo, nil)

	_, err := svc.RequestApproval(context.Background(), core.Approval{
		TenantID: "acme", TaskID: "task-1", RequestedBy: "admin",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "create approval")
}
