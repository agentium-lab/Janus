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
	updateErr error
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
	if m.updateErr != nil {
		return m.updateErr
	}
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
	svc := NewApprovalService(repo, taskSvc, nil)

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
	svc := NewApprovalService(repo, taskSvc, nil)

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
	svc := NewApprovalService(repo, taskSvc, nil)

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
	svc := NewApprovalService(repo, taskSvc, nil)

	err := svc.Reject(context.Background(), "acme", approvalID, "admin", "security risk")
	require.NoError(t, err)
	assert.Equal(t, "rejected", repo.approvals["acme:apr-2"].Status)
}

func TestApprovalService_Approve_AlreadyDecided(t *testing.T) {
	repo := &mockApprovalRepo{approvals: map[string]*core.Approval{
		"acme:apr-3": {TenantID: "acme", ID: "apr-3", Status: "approved"},
	}}
	taskSvc := NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil)
	svc := NewApprovalService(repo, taskSvc, nil)

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
	svc := NewApprovalService(repo, taskSvc, nil)

	err := svc.Expire(context.Background(), "acme", "apr-4")
	require.NoError(t, err)
	assert.Equal(t, "expired", repo.approvals["acme:apr-4"].Status)
}

func TestApprovalService_Get(t *testing.T) {
	repo := &mockApprovalRepo{approvals: map[string]*core.Approval{
		"acme:apr-1": {TenantID: "acme", ID: "apr-1", Status: "pending"},
	}}
	svc := NewApprovalService(repo, nil, nil)

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
	svc := NewApprovalService(repo, nil, nil)

	result, err := svc.ListPending(context.Background(), "acme", 10)
	require.NoError(t, err)
	assert.Len(t, result, 2)
}

func TestApprovalService_ListPending_DefaultLimit(t *testing.T) {
	svc := NewApprovalService(&mockApprovalRepo{}, nil, nil)
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
	svc := NewApprovalService(repo, taskSvc, nil)

	err := svc.Approve(context.Background(), "acme", "apr-exp", "admin", "late")
	require.NoError(t, err)
	assert.Equal(t, "expired", repo.approvals["acme:apr-exp"].Status)
}

func TestApprovalService_Reject_AlreadyDecided(t *testing.T) {
	repo := &mockApprovalRepo{approvals: map[string]*core.Approval{
		"acme:apr-5": {TenantID: "acme", ID: "apr-5", Status: "rejected"},
	}}
	svc := NewApprovalService(repo, NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil), nil)

	err := svc.Reject(context.Background(), "acme", "apr-5", "admin", "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already decided")
}

func TestApprovalService_Approve_GetError(t *testing.T) {
	repo := &mockApprovalRepo{err: fmt.Errorf("db down")}
	svc := NewApprovalService(repo, nil, nil)

	err := svc.Approve(context.Background(), "acme", "apr-1", "admin", "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get approval")
}

func TestApprovalService_Reject_GetError(t *testing.T) {
	repo := &mockApprovalRepo{err: fmt.Errorf("db down")}
	svc := NewApprovalService(repo, nil, nil)

	err := svc.Reject(context.Background(), "acme", "apr-1", "admin", "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get approval")
}

func TestApprovalService_Expire_UpdateError(t *testing.T) {
	repo := &mockApprovalRepo{err: fmt.Errorf("db down")}
	svc := NewApprovalService(repo, nil, nil)

	err := svc.Expire(context.Background(), "acme", "apr-1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "db down")
}

func TestApprovalService_RequestApproval_RepoError(t *testing.T) {
	repo := &mockApprovalRepo{err: fmt.Errorf("db down")}
	svc := NewApprovalService(repo, nil, nil)

	_, err := svc.RequestApproval(context.Background(), core.Approval{
		TenantID: "acme", TaskID: "task-1", RequestedBy: "admin",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "create approval")
}

type mockOutboxWriter struct {
	inserts   int
	insertErr error
}

func (m *mockOutboxWriter) InsertDirect(_ context.Context, _, _, _ string, _ []byte) error {
	m.inserts++
	if m.insertErr != nil {
		return m.insertErr
	}
	return nil
}

func TestApprovalService_Approve_WithOutbox(t *testing.T) {
	repo := &mockApprovalRepo{approvals: map[string]*core.Approval{
		"acme:apr-ob": {TenantID: "acme", ID: "apr-ob", TaskID: "task-ob", Status: "pending", ExpiresAt: time.Now().Add(1 * time.Hour)},
	}}
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:task-ob": {TenantID: "acme", ID: "task-ob", Status: core.TaskStatusApprovalPending, MailboxID: "mb-1"},
	}}
	taskSvc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	svc := NewApprovalService(repo, taskSvc, nil)
	outbox := &mockOutboxWriter{}
	svc.WithOutbox(outbox)

	err := svc.Approve(context.Background(), "acme", "apr-ob", "admin", "looks good")
	require.NoError(t, err)
	assert.Equal(t, 1, outbox.inserts)
}

func TestApprovalService_Approve_WithQueueDriver(t *testing.T) {
	qdrv := &mockQueueDriver{}
	repo := &mockApprovalRepo{approvals: map[string]*core.Approval{
		"acme:apr-qd": {TenantID: "acme", ID: "apr-qd", TaskID: "task-qd", Status: "pending", ExpiresAt: time.Now().Add(1 * time.Hour)},
	}}
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:task-qd": {TenantID: "acme", ID: "task-qd", Status: core.TaskStatusApprovalPending, MailboxID: "mb-1"},
	}}
	taskSvc := NewTaskService(taskRepo, qdrv, nil, nil)
	svc := NewApprovalService(repo, taskSvc, qdrv)

	err := svc.Approve(context.Background(), "acme", "apr-qd", "admin", "ok")
	require.NoError(t, err)
	assert.Equal(t, 1, len(qdrv.publishedTasks))
}

func TestApprovalService_Approve_NoMailbox(t *testing.T) {
	repo := &mockApprovalRepo{approvals: map[string]*core.Approval{
		"acme:apr-nm": {TenantID: "acme", ID: "apr-nm", TaskID: "task-nm", Status: "pending", ExpiresAt: time.Now().Add(1 * time.Hour)},
	}}
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:task-nm": {TenantID: "acme", ID: "task-nm", Status: core.TaskStatusApprovalPending, MailboxID: ""},
	}}
	taskSvc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	svc := NewApprovalService(repo, taskSvc, nil)

	err := svc.Approve(context.Background(), "acme", "apr-nm", "admin", "ok")
	require.NoError(t, err)
}

func TestApprovalService_Approve_TransitionError(t *testing.T) {
	repo := &mockApprovalRepo{approvals: map[string]*core.Approval{
		"acme:apr-te": {TenantID: "acme", ID: "apr-te", TaskID: "task-te", Status: "pending", ExpiresAt: time.Now().Add(1 * time.Hour)},
	}}
	taskRepo := &mockTaskRepo{err: fmt.Errorf("db down")}
	taskSvc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	svc := NewApprovalService(repo, taskSvc, nil)

	err := svc.Approve(context.Background(), "acme", "apr-te", "admin", "ok")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "queue task")
}

func TestApprovalService_Approve_UpdateDecisionError(t *testing.T) {
	repo := &mockApprovalRepo{approvals: map[string]*core.Approval{
		"acme:apr-ud": {TenantID: "acme", ID: "apr-ud", TaskID: "task-ud", Status: "pending", ExpiresAt: time.Now().Add(1 * time.Hour)},
	}, updateErr: fmt.Errorf("db locked")}
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:task-ud": {TenantID: "acme", ID: "task-ud", Status: core.TaskStatusApprovalPending},
	}}
	taskSvc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	svc := NewApprovalService(repo, taskSvc, nil)

	err := svc.Approve(context.Background(), "acme", "apr-ud", "admin", "ok")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "update approval")
}
