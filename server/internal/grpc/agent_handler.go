package grpc

import (
	"context"

	pb "github.com/agentium-lab/Janus/proto/gen/janus/v1"
	"github.com/agentium-lab/Janus/core"
	svc "github.com/agentium-lab/Janus/server/internal/service"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type agentService interface {
	Register(ctx context.Context, agent core.Agent) error
	Get(ctx context.Context, tenantID, agentID string) (*core.Agent, error)
	UpdateStatus(ctx context.Context, tenantID, agentID string, status core.AgentStatus) error
	Heartbeat(ctx context.Context, tenantID, agentID string) error
	List(ctx context.Context, tenantID string) ([]*core.Agent, error)
}

type AgentServiceServer struct {
	pb.UnimplementedAgentServiceServer
	svc agentService
}

func NewAgentServiceServer(s *svc.AgentService) *AgentServiceServer {
	return &AgentServiceServer{svc: s}
}

func (s *AgentServiceServer) RegisterAgent(ctx context.Context, req *pb.RegisterAgentRequest) (*pb.Agent, error) {
	agent := registerReqToAgent(req)
	if err := s.svc.Register(ctx, agent); err != nil {
		return nil, err
	}
	result, err := s.svc.Get(ctx, req.TenantId, req.Id)
	if err != nil {
		return nil, err
	}
	return agentToProto(result), nil
}

func (s *AgentServiceServer) UpdateAgent(ctx context.Context, req *pb.UpdateAgentRequest) (*pb.Agent, error) {
	if req.Status != nil {
		if err := s.svc.UpdateStatus(ctx, req.TenantId, req.AgentId, core.AgentStatus(req.GetStatus())); err != nil {
			return nil, err
		}
	}
	result, err := s.svc.Get(ctx, req.TenantId, req.AgentId)
	if err != nil {
		return nil, err
	}
	return agentToProto(result), nil
}

func (s *AgentServiceServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if err := s.svc.Heartbeat(ctx, req.TenantId, req.AgentId); err != nil {
		return nil, err
	}
	return &pb.HeartbeatResponse{
		ServerTime: timestamppb.Now(),
	}, nil
}

func (s *AgentServiceServer) ListAgents(ctx context.Context, req *pb.ListAgentsRequest) (*pb.ListAgentsResponse, error) {
	agents, err := s.svc.List(ctx, req.TenantId)
	if err != nil {
		return nil, err
	}
	pbAgents := make([]*pb.Agent, len(agents))
	for i, a := range agents {
		pbAgents[i] = agentToProto(a)
	}
	return &pb.ListAgentsResponse{Agents: pbAgents}, nil
}

func (s *AgentServiceServer) GetAgent(ctx context.Context, req *pb.GetAgentRequest) (*pb.Agent, error) {
	agent, err := s.svc.Get(ctx, req.TenantId, req.AgentId)
	if err != nil {
		return nil, err
	}
	return agentToProto(agent), nil
}
