package grpc

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/agentium-lab/Janus/proto/gen/janus/v1"
	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/handler"
)

type dlqService interface {
	QueryDLQ(ctx context.Context, tenantID, mailboxID string, limit int) ([]*core.Task, error)
	ReplayDLQ(ctx context.Context, tenantID, taskID string) (*core.Task, error)
	DiscardDLQ(ctx context.Context, tenantID, taskID string) error
}

type DLQServiceServer struct {
	pb.UnimplementedDLQServiceServer
	svc dlqService
}

func NewDLQServiceServer(svc *handler.DLQServiceAdapter) *DLQServiceServer {
	return &DLQServiceServer{svc: svc}
}

func (s *DLQServiceServer) QueryDLQ(ctx context.Context, req *pb.DLQQueryRequest) (*pb.DLQQueryResponse, error) {
	limit := int(req.Limit)
	if limit == 0 {
		limit = 50
	}
	tasks, err := s.svc.QueryDLQ(ctx, req.TenantId, req.MailboxId, limit)
	if err != nil {
		return nil, err
	}
	resp := &pb.DLQQueryResponse{}
	for _, t := range tasks {
		resp.Tasks = append(resp.Tasks, taskToProto(t))
	}
	return resp, nil
}

func (s *DLQServiceServer) ReplayDLQ(ctx context.Context, req *pb.DLQActionRequest) (*pb.Task, error) {
	task, err := s.svc.ReplayDLQ(ctx, req.TenantId, req.TaskId)
	if err != nil {
		return nil, err
	}
	return taskToProto(task), nil
}

func (s *DLQServiceServer) DiscardDLQ(ctx context.Context, req *pb.DLQActionRequest) (*pb.DLQActionResponse, error) {
	if req.TenantId == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required")
	}
	if err := s.svc.DiscardDLQ(ctx, req.TenantId, req.TaskId); err != nil {
		return nil, err
	}
	return &pb.DLQActionResponse{Status: "discarded"}, nil
}
