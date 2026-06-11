package grpc

import (
	"context"
	"testing"
	"time"

	pb "github.com/agentium-lab/Janus/proto/gen/janus/v1"
	"github.com/agentium-lab/Janus/core"
	svc "github.com/agentium-lab/Janus/server/internal/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockDispatchService struct {
	pullResult *svc.PullResult
	err        error
	started    bool
	acked      bool
	nacked     bool
	heartbeated bool
}

func (m *mockDispatchService) PullTask(_ context.Context, _, _, _ string) (*svc.PullResult, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.pullResult, nil
}

func (m *mockDispatchService) StartTask(_ context.Context, _, _, _ string) error {
	m.started = true
	return m.err
}

func (m *mockDispatchService) TaskHeartbeat(_ context.Context, _, _, _ string) error {
	m.heartbeated = true
	return m.err
}

func (m *mockDispatchService) AckTask(_ context.Context, _, _, _, _ string, _ *core.TokenUsage) error {
	m.acked = true
	return m.err
}

func (m *mockDispatchService) NackTask(_ context.Context, _, _, _ string, _ bool, _ *core.TaskError) error {
	m.nacked = true
	return m.err
}

func TestDispatchServiceServer_PullTask_Empty(t *testing.T) {
	mock := &mockDispatchService{}
	s := &DispatchServiceServer{svc: mock}

	resp, err := s.PullTask(context.Background(), &pb.PullTaskRequest{
		TenantId:  "acme",
		MailboxId: "mb-1",
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Nil(t, resp.Task)
}

func TestDispatchServiceServer_PullTask_WithTask(t *testing.T) {
	task := makeTestTask("acme", "task-1")
	task.Status = core.TaskStatusClaimed
	mock := &mockDispatchService{
		pullResult: &svc.PullResult{
			Task:     task,
			LeaseID:  "lease-1",
			ExpiresAt: time.Now().Add(30 * time.Second),
		},
	}
	s := &DispatchServiceServer{svc: mock}

	resp, err := s.PullTask(context.Background(), &pb.PullTaskRequest{
		TenantId:  "acme",
		MailboxId: "mb-1",
	})
	require.NoError(t, err)
	assert.NotNil(t, resp.Task)
	assert.Equal(t, "task-1", resp.Task.Id)
	assert.Equal(t, "lease-1", resp.Lease.LeaseId)
}

func TestDispatchServiceServer_StartTask(t *testing.T) {
	mock := &mockDispatchService{}
	s := &DispatchServiceServer{svc: mock}

	_, err := s.StartTask(context.Background(), &pb.StartTaskRequest{
		TenantId: "acme", TaskId: "task-1", LeaseId: "lease-1",
	})
	require.NoError(t, err)
	assert.True(t, mock.started)
}

func TestDispatchServiceServer_TaskHeartbeat(t *testing.T) {
	mock := &mockDispatchService{}
	s := &DispatchServiceServer{svc: mock}

	_, err := s.TaskHeartbeat(context.Background(), &pb.TaskHeartbeatRequest{
		TenantId: "acme", TaskId: "task-1", LeaseId: "lease-1",
	})
	require.NoError(t, err)
	assert.True(t, mock.heartbeated)
}

func TestDispatchServiceServer_AckTask(t *testing.T) {
	mock := &mockDispatchService{}
	s := &DispatchServiceServer{svc: mock}

	_, err := s.AckTask(context.Background(), &pb.AckTaskRequest{
		TenantId:  "acme",
		TaskId:    "task-1",
		LeaseId:   "lease-1",
		ResultRef: "s3://results/task-1.json",
		TokenUsage: &pb.TokenUsage{
			PromptTokens:     100,
			CompletionTokens: 50,
			TotalTokens:      150,
		},
	})
	require.NoError(t, err)
	assert.True(t, mock.acked)
}

func TestDispatchServiceServer_AckTask_NoUsage(t *testing.T) {
	mock := &mockDispatchService{}
	s := &DispatchServiceServer{svc: mock}

	_, err := s.AckTask(context.Background(), &pb.AckTaskRequest{
		TenantId: "acme", TaskId: "task-1", LeaseId: "lease-1",
	})
	require.NoError(t, err)
	assert.True(t, mock.acked)
}

func TestDispatchServiceServer_NackTask(t *testing.T) {
	mock := &mockDispatchService{}
	s := &DispatchServiceServer{svc: mock}

	_, err := s.NackTask(context.Background(), &pb.NackTaskRequest{
		TenantId: "acme",
		TaskId:   "task-1",
		LeaseId:  "lease-1",
		Retriable: true,
		Error:    &pb.TaskError{Code: "TIMEOUT", Message: "agent timed out"},
	})
	require.NoError(t, err)
	assert.True(t, mock.nacked)
}

func TestDispatchServiceServer_Error(t *testing.T) {
	mock := &mockDispatchService{err: assert.AnError}
	s := &DispatchServiceServer{svc: mock}

	_, err := s.PullTask(context.Background(), &pb.PullTaskRequest{
		TenantId: "acme", MailboxId: "mb-1",
	})
	assert.Error(t, err)
}
