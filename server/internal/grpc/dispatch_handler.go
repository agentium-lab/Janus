package grpc

import (
	"context"

	pb "github.com/agentium-lab/Janus/proto/gen/janus/v1"
	"github.com/agentium-lab/Janus/core"
	svc "github.com/agentium-lab/Janus/server/internal/service"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type dispatchService interface {
	PullTask(ctx context.Context, tenantID, mailboxID, agentID string) (*svc.PullResult, error)
	StartTask(ctx context.Context, tenantID, taskID, leaseID string) error
	TaskHeartbeat(ctx context.Context, tenantID, taskID, leaseID string) error
	AckTask(ctx context.Context, tenantID, taskID, leaseID string, resultRef string, usage *core.TokenUsage) error
	NackTask(ctx context.Context, tenantID, taskID, leaseID string, retriable bool, taskErr *core.TaskError) error
}

type DispatchServiceServer struct {
	pb.UnimplementedDispatchServiceServer
	svc dispatchService
}

func NewDispatchServiceServer(s *svc.DispatchService) *DispatchServiceServer {
	return &DispatchServiceServer{svc: s}
}

func (s *DispatchServiceServer) PullTask(ctx context.Context, req *pb.PullTaskRequest) (*pb.PullTaskResponse, error) {
	result, err := s.svc.PullTask(ctx, req.TenantId, req.MailboxId, "")
	if err != nil {
		return nil, err
	}
	if result == nil || result.Task == nil {
		return &pb.PullTaskResponse{}, nil
	}
	resp := &pb.PullTaskResponse{
		Task: taskToProto(result.Task),
		Lease: &pb.Lease{
			LeaseId:   result.LeaseID,
			ExpiresAt: timestamppb.New(result.ExpiresAt),
		},
	}
	return resp, nil
}

func (s *DispatchServiceServer) StartTask(ctx context.Context, req *pb.StartTaskRequest) (*pb.StartTaskResponse, error) {
	if err := s.svc.StartTask(ctx, req.TenantId, req.TaskId, req.LeaseId); err != nil {
		return nil, err
	}
	return &pb.StartTaskResponse{}, nil
}

func (s *DispatchServiceServer) TaskHeartbeat(ctx context.Context, req *pb.TaskHeartbeatRequest) (*pb.TaskHeartbeatResponse, error) {
	if err := s.svc.TaskHeartbeat(ctx, req.TenantId, req.TaskId, req.LeaseId); err != nil {
		return nil, err
	}
	return &pb.TaskHeartbeatResponse{}, nil
}

func (s *DispatchServiceServer) AckTask(ctx context.Context, req *pb.AckTaskRequest) (*pb.AckTaskResponse, error) {
	var usage *core.TokenUsage
	if req.TokenUsage != nil {
		usage = &core.TokenUsage{
			PromptTokens:     int(req.TokenUsage.PromptTokens),
			CompletionTokens: int(req.TokenUsage.CompletionTokens),
			TotalTokens:      int(req.TokenUsage.TotalTokens),
		}
	}
	if err := s.svc.AckTask(ctx, req.TenantId, req.TaskId, req.LeaseId, req.ResultRef, usage); err != nil {
		return nil, err
	}
	return &pb.AckTaskResponse{}, nil
}

func (s *DispatchServiceServer) NackTask(ctx context.Context, req *pb.NackTaskRequest) (*pb.NackTaskResponse, error) {
	var taskErr *core.TaskError
	if req.Error != nil {
		taskErr = &core.TaskError{
			Code:    req.Error.Code,
			Message: req.Error.Message,
		}
	}
	if err := s.svc.NackTask(ctx, req.TenantId, req.TaskId, req.LeaseId, req.Retriable, taskErr); err != nil {
		return nil, err
	}
	return &pb.NackTaskResponse{}, nil
}
