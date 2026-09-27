package heartbeat

import (
	"context"
	"log"
	"time"

	"github.com/agentium-lab/Janus/core"
)

type Sweeper struct {
	agentStatus AgentStatusUpdater
	interval    time.Duration
	stopCh      chan struct{}
}

type HeartbeatScanner interface {
	ScanExpired(ctx context.Context, tenantID string) ([]string, error)
}

type AgentStatusUpdater interface {
	UpdateStatus(ctx context.Context, tenantID, agentID string, status core.AgentStatus) error
	ListAllByStatus(ctx context.Context, status core.AgentStatus) ([]*core.Agent, error)
}

func NewSweeper(_ HeartbeatScanner, agentStatus AgentStatusUpdater, interval time.Duration) *Sweeper {
	// The scanner parameter is retained for signature compatibility; the
	// sweep decision is PG-based (see staleThreshold).
	return &Sweeper{
		agentStatus: agentStatus,
		interval:    interval,
		stopCh:      make(chan struct{}),
	}
}

func (s *Sweeper) Start(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.sweep(ctx)
		}
	}
}

func (s *Sweeper) Stop() {
	close(s.stopCh)
}

// staleThreshold is how long an agent's durable PG heartbeat may age before
// the sweeper marks it offline. PG is the source of truth: the Redis presence
// keys are a best-effort cache and lose data on flush/restart, so liveness
// must never be decided by them alone (a Redis wipe used to leave dead
// agents online forever because ScanExpired returned nothing).
const staleThreshold = 90 * time.Second

func (s *Sweeper) sweep(ctx context.Context) {
	onlineAgents, err := s.agentStatus.ListAllByStatus(ctx, core.AgentStatusOnline)
	if err != nil {
		return
	}

	if len(onlineAgents) == 0 {
		return
	}

	for _, agent := range onlineAgents {
		stale := agent.LastHeartbeatAt == nil ||
			time.Since(*agent.LastHeartbeatAt) > staleThreshold
		if !stale {
			continue
		}
		if err := s.agentStatus.UpdateStatus(ctx, agent.TenantID, agent.ID, core.AgentStatusOffline); err != nil {
			log.Printf("sweeper: failed to mark agent %s/%s offline: %v", agent.TenantID, agent.ID, err)
		} else {
			log.Printf("sweeper: agent %s/%s marked offline (pg heartbeat stale)", agent.TenantID, agent.ID)
		}
	}
}
