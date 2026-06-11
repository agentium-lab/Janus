package service

import (
	"context"
	"fmt"

	"github.com/agentium-lab/Janus/core"
)

type AgentService struct {
	agentRepo    AgentRepo
	mailboxRepo  MailboxRepo
	hbDriver     HeartbeatDriver
	queueDriver  QueueDriver
}

func NewAgentService(
	agentRepo AgentRepo,
	mailboxRepo MailboxRepo,
	hbDriver HeartbeatDriver,
	queueDriver QueueDriver,
) *AgentService {
	return &AgentService{
		agentRepo:   agentRepo,
		mailboxRepo: mailboxRepo,
		hbDriver:    hbDriver,
		queueDriver: queueDriver,
	}
}

func (s *AgentService) Register(ctx context.Context, agent core.Agent) error {
	if agent.ID == "" {
		return fmt.Errorf("agent id is required")
	}
	if agent.TenantID == "" {
		return fmt.Errorf("tenant id is required")
	}
	if agent.DisplayName == "" {
		return fmt.Errorf("display name is required")
	}

	if agent.Status == "" {
		agent.Status = core.AgentStatusOffline
	}

	if err := s.agentRepo.Register(ctx, agent); err != nil {
		return fmt.Errorf("register agent: %w", err)
	}

	if err := s.hbDriver.Ping(ctx, agent.TenantID, agent.ID); err != nil {
		return fmt.Errorf("initial heartbeat: %w", err)
	}

	if err := s.agentRepo.UpdateStatus(ctx, agent.TenantID, agent.ID, core.AgentStatusOnline); err != nil {
		return fmt.Errorf("set online: %w", err)
	}

	return nil
}

func (s *AgentService) Get(ctx context.Context, tenantID, agentID string) (*core.Agent, error) {
	if tenantID == "" || agentID == "" {
		return nil, fmt.Errorf("tenant id and agent id are required")
	}
	agent, err := s.agentRepo.Get(ctx, tenantID, agentID)
	if err != nil {
		return nil, fmt.Errorf("get agent: %w", err)
	}
	return agent, nil
}

func (s *AgentService) Heartbeat(ctx context.Context, tenantID, agentID string) error {
	if tenantID == "" || agentID == "" {
		return fmt.Errorf("tenant id and agent id are required")
	}
	if err := s.hbDriver.Ping(ctx, tenantID, agentID); err != nil {
		return fmt.Errorf("heartbeat: %w", err)
	}
	return s.agentRepo.UpdateHeartbeat(ctx, tenantID, agentID)
}

func (s *AgentService) UpdateStatus(ctx context.Context, tenantID, agentID string, status core.AgentStatus) error {
	if tenantID == "" || agentID == "" {
		return fmt.Errorf("tenant id and agent id are required")
	}
	return s.agentRepo.UpdateStatus(ctx, tenantID, agentID, status)
}

func (s *AgentService) List(ctx context.Context, tenantID string) ([]*core.Agent, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("tenant id is required")
	}
	return s.agentRepo.List(ctx, tenantID)
}

func (s *AgentService) ListByStatus(ctx context.Context, tenantID string, status core.AgentStatus) ([]*core.Agent, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("tenant id is required")
	}
	return s.agentRepo.ListByStatus(ctx, tenantID, status)
}

func (s *AgentService) ResolveCapability(ctx context.Context, tenantID, capability string) ([]*core.Agent, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("tenant id is required")
	}
	if capability == "" {
		return nil, fmt.Errorf("capability is required")
	}
	return s.agentRepo.FindByCapability(ctx, tenantID, capability)
}
