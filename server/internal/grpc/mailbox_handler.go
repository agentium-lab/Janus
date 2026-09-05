package grpc

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agentium-lab/Janus/core"
	pb "github.com/agentium-lab/Janus/proto/gen/janus/v1"
	svc "github.com/agentium-lab/Janus/server/internal/service"
)

type mailboxService interface {
	Create(ctx context.Context, mailbox core.Mailbox) error
	Get(ctx context.Context, tenantID, mailboxID string) (*core.Mailbox, error)
	UpdateConfig(ctx context.Context, tenantID, mailboxID string, maxConcurrency, ackWaitSeconds, maxDeliver, retentionSeconds int) error
	Pause(ctx context.Context, tenantID, mailboxID string) error
	Resume(ctx context.Context, tenantID, mailboxID string) error
}

type MailboxServiceServer struct {
	pb.UnimplementedMailboxServiceServer
	svc mailboxService
}

func NewMailboxServiceServer(s *svc.MailboxService) *MailboxServiceServer {
	return &MailboxServiceServer{svc: s}
}

func (s *MailboxServiceServer) CreateMailbox(ctx context.Context, req *pb.CreateMailboxRequest) (*pb.Mailbox, error) {
	if req.TenantId == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required")
	}
	mb := core.Mailbox{
		TenantID:       req.TenantId,
		ID:             req.Id,
		AgentID:        req.AgentId,
		Status:         "active",
		MaxConcurrency: int(req.MaxConcurrency),
	}
	if err := s.svc.Create(ctx, mb); err != nil {
		return nil, err
	}
	created, err := s.svc.Get(ctx, req.TenantId, req.Id)
	if err != nil {
		return nil, err
	}
	return mailboxToProto(created), nil
}

func (s *MailboxServiceServer) GetMailbox(ctx context.Context, req *pb.GetMailboxRequest) (*pb.Mailbox, error) {
	mb, err := s.svc.Get(ctx, req.TenantId, req.MailboxId)
	if err != nil {
		return nil, err
	}
	return mailboxToProto(mb), nil
}

func (s *MailboxServiceServer) UpdateMailbox(ctx context.Context, req *pb.UpdateMailboxRequest) (*pb.Mailbox, error) {
	if req.TenantId == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required")
	}
	if err := s.svc.UpdateConfig(ctx, req.TenantId, req.MailboxId,
		int(req.MaxConcurrency), int(req.AckWaitSeconds), int(req.MaxDeliver), int(req.RetentionSeconds)); err != nil {
		return nil, err
	}
	mb, err := s.svc.Get(ctx, req.TenantId, req.MailboxId)
	if err != nil {
		return nil, err
	}
	return mailboxToProto(mb), nil
}

func (s *MailboxServiceServer) PauseMailbox(ctx context.Context, req *pb.MailboxActionRequest) (*pb.MailboxActionResponse, error) {
	if err := s.svc.Pause(ctx, req.TenantId, req.MailboxId); err != nil {
		return nil, err
	}
	return &pb.MailboxActionResponse{Status: "paused"}, nil
}

func (s *MailboxServiceServer) ResumeMailbox(ctx context.Context, req *pb.MailboxActionRequest) (*pb.MailboxActionResponse, error) {
	if err := s.svc.Resume(ctx, req.TenantId, req.MailboxId); err != nil {
		return nil, err
	}
	return &pb.MailboxActionResponse{Status: "active"}, nil
}

func mailboxToProto(mb *core.Mailbox) *pb.Mailbox {
	return &pb.Mailbox{
		TenantId:       mb.TenantID,
		Id:             mb.ID,
		AgentId:        mb.AgentID,
		Status:         string(mb.Status),
		MaxConcurrency: int32(mb.MaxConcurrency),
	}
}
