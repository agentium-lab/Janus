package heartbeat

import (
	"context"
	"log"
	"time"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/nilguard"
)

type Sweeper struct {
	hbDriver    HeartbeatScanner
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

func NewSweeper(hbDriver HeartbeatScanner, agentStatus AgentStatusUpdater, interval time.Duration) *Sweeper {
	return &Sweeper{
		hbDriver:    nilguard.Interface(hbDriver),
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

func (s *Sweeper) sweep(ctx context.Context) {
	if s.hbDriver == nil {
		return
	}
	onlineAgents, err := s.agentStatus.ListAllByStatus(ctx, core.AgentStatusOnline)
	if err != nil {
		return
	}

	if len(onlineAgents) == 0 {
		return
	}

	byTenant := make(map[string][]string)
	for _, agent := range onlineAgents {
		byTenant[agent.TenantID] = append(byTenant[agent.TenantID], agent.ID)
	}

	for tenantID, agentIDs := range byTenant {
		expired, err := s.hbDriver.ScanExpired(ctx, tenantID)
		if err != nil {
			continue
		}
		expiredSet := make(map[string]struct{}, len(expired))
		for _, id := range expired {
			expiredSet[id] = struct{}{}
		}
		for _, agentID := range agentIDs {
			if _, ok := expiredSet[agentID]; ok {
				if err := s.agentStatus.UpdateStatus(ctx, tenantID, agentID, core.AgentStatusOffline); err != nil {
					log.Printf("sweeper: failed to mark agent %s/%s offline: %v", tenantID, agentID, err)
				} else {
					log.Printf("sweeper: agent %s/%s marked offline (heartbeat expired)", tenantID, agentID)
				}
			}
		}
	}
}
