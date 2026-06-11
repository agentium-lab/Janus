package grpc

import (
	"context"

	pb "github.com/agentium-lab/Janus/proto/gen/janus/v1"
	"github.com/agentium-lab/Janus/core"
	svc "github.com/agentium-lab/Janus/server/internal/service"
)

type taskService interface {
	Create(ctx context.Context, task core.Task) (*core.Task, error)
	Get(ctx context.Context, tenantID, taskID string) (*core.Task, error)
	Cancel(ctx context.Context, tenantID, taskID string) error
	Replay(ctx context.Context, tenantID, taskID string) (*core.Task, error)
}

type TaskServiceServer struct {
	pb.UnimplementedTaskServiceServer
	svc taskService
}

func NewTaskServiceServer(s *svc.TaskService) *TaskServiceServer {
	return &TaskServiceServer{svc: s}
}

func (s *TaskServiceServer) CreateTask(ctx context.Context, req *pb.CreateTaskRequest) (*pb.Task, error) {
	task, err := createTaskReqToCore(req)
	if err != nil {
		return nil, err
	}
	result, err := s.svc.Create(ctx, task)
	if err != nil {
		return nil, err
	}
	return taskToProto(result), nil
}

func (s *TaskServiceServer) GetTask(ctx context.Context, req *pb.GetTaskRequest) (*pb.Task, error) {
	task, err := s.svc.Get(ctx, req.TenantId, req.TaskId)
	if err != nil {
		return nil, err
	}
	return taskToProto(task), nil
}

func (s *TaskServiceServer) CancelTask(ctx context.Context, req *pb.CancelTaskRequest) (*pb.Task, error) {
	if err := s.svc.Cancel(ctx, req.TenantId, req.TaskId); err != nil {
		return nil, err
	}
	task, err := s.svc.Get(ctx, req.TenantId, req.TaskId)
	if err != nil {
		return nil, err
	}
	return taskToProto(task), nil
}

func (s *TaskServiceServer) ReplayTask(ctx context.Context, req *pb.ReplayTaskRequest) (*pb.Task, error) {
	result, err := s.svc.Replay(ctx, req.TenantId, req.TaskId)
	if err != nil {
		return nil, err
	}
	return taskToProto(result), nil
}
