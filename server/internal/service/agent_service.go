package service

import (
	"time"

	"log"

	"context"
	"fmt"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/nilguard"
)

type AgentService struct {
	agentRepo   AgentRepo
	mailboxRepo MailboxRepo
	hbDriver    HeartbeatDriver
	queueDriver QueueDriver
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
		hbDriver:    nilguard.Interface(hbDriver),
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

	if len(agent.Capabilities) > 0 {
		if err := s.agentRepo.UpsertCapabilities(ctx, agent); err != nil {
			return fmt.Errorf("upsert capabilities: %w", err)
		}
	}

	// The Redis presence mark is best-effort (mirrors Heartbeat): PG is the
	// source of truth for both registration and liveness, so a Redis blip
	// mid-registration must neither fail the request nor leave the agent
	// stuck unregistered-online-less on a retry conflict.
	if s.hbDriver != nil {
		if err := pingMarkBounded(ctx, s.hbDriver, agent.TenantID, agent.ID); err != nil {
			log.Printf("agent %s/%s: initial redis mark failed (pg registration durable): %v", agent.TenantID, agent.ID, err)
		}
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
	// PG is the durable record and must be updated even when the Redis
	// heartbeat driver is absent (PG-only mode) — otherwise offline detection
	// has no data at all. Redis TTL marking is best-effort on top.
	if err := s.agentRepo.UpdateHeartbeat(ctx, tenantID, agentID); err != nil {
		return fmt.Errorf("update heartbeat: %w", err)
	}
	if s.hbDriver == nil {
		return nil
	}
	// The Redis TTL mark is best-effort on top of the durable PG record.
	// Best-effort must also be BOUNDED: during a broker restart the
	// client's pool can hold half-open connections whose commands hang
	// until the read timeout retries exhaust — far beyond a sane request
	// latency. Cap the mark at 500ms; the PG record is what liveness uses.
	if err := pingMarkBounded(ctx, s.hbDriver, tenantID, agentID); err != nil {
		log.Printf("agent %s/%s: redis heartbeat mark failed (pg record durable): %v", tenantID, agentID, err)
	}
	return nil
}

// pingMarkBounded runs the best-effort redis presence mark under a hard
// 500ms budget (context deadline, not just dial timeout).
func pingMarkBounded(ctx context.Context, hb HeartbeatDriver, tenantID, agentID string) error {
	markCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	return hb.Ping(markCtx, tenantID, agentID)
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
