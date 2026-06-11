package grpc

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb "github.com/agentium-lab/Janus/proto/gen/janus/v1"
	"github.com/agentium-lab/Janus/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockTaskService struct {
	tasks map[string]*core.Task
	err   error
}

func (m *mockTaskService) Create(_ context.Context, task core.Task) (*core.Task, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.tasks == nil {
		m.tasks = make(map[string]*core.Task)
	}
	m.tasks[task.TenantID+":"+task.ID] = &task
	return &task, nil
}

func (m *mockTaskService) Get(_ context.Context, tenantID, taskID string) (*core.Task, error) {
	if m.err != nil {
		return nil, m.err
	}
	t, ok := m.tasks[tenantID+":"+taskID]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return t, nil
}

func (m *mockTaskService) Cancel(_ context.Context, tenantID, taskID string) error {
	if m.err != nil {
		return m.err
	}
	key := tenantID + ":" + taskID
	if t, ok := m.tasks[key]; ok {
		t.Status = core.TaskStatusCancelled
	}
	return nil
}

func (m *mockTaskService) Replay(_ context.Context, tenantID, taskID string) (*core.Task, error) {
	if m.err != nil {
		return nil, m.err
	}
	key := tenantID + ":" + taskID
	if t, ok := m.tasks[key]; ok {
		t.Status = core.TaskStatusQueued
		return t, nil
	}
	return nil, fmt.Errorf("not found")
}

func makeTestTask(tenantID, id string) *core.Task {
	return &core.Task{
		TenantID:   tenantID,
		ID:         id,
		SourceAgent: "agent-1",
		TargetType: core.TargetTypeAgent,
		TargetValue: "agent-2",
		MailboxID:  "mb-1",
		Status:     core.TaskStatusQueued,
		Priority:   core.PriorityNormal,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
		Envelope: core.TaskEnvelope{
			JanusVersion: "0.4.0",
			TaskID:       id,
			TenantID:     tenantID,
			SourceAgent:  "agent-1",
			Target: core.Target{
				Type:  "agent",
				Value: "agent-2",
			},
			Priority: core.PriorityNormal,
			Payload: core.Payload{
				Type:    "text",
				Content: "hello",
			},
			Trace: core.TraceContext{
				TraceID: "trace-1",
			},
		},
	}
}

func TestTaskServiceServer_CreateTask(t *testing.T) {
	mock := &mockTaskService{}
	s := &TaskServiceServer{svc: mock}

	resp, err := s.CreateTask(context.Background(), &pb.CreateTaskRequest{
		TenantId: "acme",
		Envelope: &pb.TaskEnvelope{
			TaskId:      "task-1",
			TenantId:    "acme",
			SourceAgent: "agent-1",
			Target:      &pb.Target{Type: "agent", Value: "agent-2"},
			Priority:    "normal",
			Payload:     &pb.TaskPayload{Type: "text", Content: "hello"},
			Trace:       &pb.TraceContext{TraceId: "trace-1"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "task-1", resp.Id)
	assert.Equal(t, "acme", resp.TenantId)
}

func TestTaskServiceServer_CreateTask_NoEnvelope(t *testing.T) {
	mock := &mockTaskService{}
	s := &TaskServiceServer{svc: mock}

	_, err := s.CreateTask(context.Background(), &pb.CreateTaskRequest{
		TenantId: "acme",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "envelope")
}

func TestTaskServiceServer_GetTask(t *testing.T) {
	mock := &mockTaskService{tasks: map[string]*core.Task{
		"acme:task-1": makeTestTask("acme", "task-1"),
	}}
	s := &TaskServiceServer{svc: mock}

	resp, err := s.GetTask(context.Background(), &pb.GetTaskRequest{
		TenantId: "acme", TaskId: "task-1",
	})
	require.NoError(t, err)
	assert.Equal(t, "task-1", resp.Id)
	assert.Equal(t, "queued", resp.Status)
}

func TestTaskServiceServer_GetTask_NotFound(t *testing.T) {
	mock := &mockTaskService{}
	s := &TaskServiceServer{svc: mock}

	_, err := s.GetTask(context.Background(), &pb.GetTaskRequest{
		TenantId: "acme", TaskId: "nonexistent",
	})
	assert.Error(t, err)
}

func TestTaskServiceServer_CancelTask(t *testing.T) {
	mock := &mockTaskService{tasks: map[string]*core.Task{
		"acme:task-1": makeTestTask("acme", "task-1"),
	}}
	s := &TaskServiceServer{svc: mock}

	resp, err := s.CancelTask(context.Background(), &pb.CancelTaskRequest{
		TenantId: "acme", TaskId: "task-1", Reason: "user cancel",
	})
	require.NoError(t, err)
	assert.Equal(t, "cancelled", resp.Status)
}

func TestTaskServiceServer_ReplayTask(t *testing.T) {
	mock := &mockTaskService{tasks: map[string]*core.Task{
		"acme:task-1": makeTestTask("acme", "task-1"),
	}}
	s := &TaskServiceServer{svc: mock}

	resp, err := s.ReplayTask(context.Background(), &pb.ReplayTaskRequest{
		TenantId: "acme",
		TaskId:   "task-1",
	})
	require.NoError(t, err)
	assert.Equal(t, "queued", resp.Status)
	assert.Equal(t, "agent-1", resp.SourceAgent)
}

func TestTaskServiceServer_CreateTask_Error(t *testing.T) {
	mock := &mockTaskService{err: assert.AnError}
	s := &TaskServiceServer{svc: mock}

	_, err := s.CreateTask(context.Background(), &pb.CreateTaskRequest{
		TenantId: "acme",
		Envelope: &pb.TaskEnvelope{
			TaskId: "task-1", TenantId: "acme", SourceAgent: "agent-1",
			Target: &pb.Target{Type: "agent", Value: "agent-2"},
		},
	})
	assert.Error(t, err)
}

func TestTaskServiceServer_CancelTask_Error(t *testing.T) {
	mock := &mockTaskService{err: assert.AnError}
	s := &TaskServiceServer{svc: mock}

	_, err := s.CancelTask(context.Background(), &pb.CancelTaskRequest{
		TenantId: "acme", TaskId: "task-1",
	})
	assert.Error(t, err)
}

func TestTaskServiceServer_ReplayTask_Error(t *testing.T) {
	mock := &mockTaskService{err: assert.AnError}
	s := &TaskServiceServer{svc: mock}

	_, err := s.ReplayTask(context.Background(), &pb.ReplayTaskRequest{
		TenantId: "acme", TaskId: "task-1",
	})
	assert.Error(t, err)
}
