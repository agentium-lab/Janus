package heartbeat

import (
	"context"
	"log"
	"time"

	"github.com/agentium-lab/Janus/core"
)

type Sweeper struct {
	agentStatus    AgentStatusUpdater
	interval       time.Duration
	staleThreshold time.Duration
	stopCh         chan struct{}
}

type HeartbeatScanner interface {
	ScanExpired(ctx context.Context, tenantID string) ([]string, error)
}

type AgentStatusUpdater interface {
	UpdateStatus(ctx context.Context, tenantID, agentID string, status core.AgentStatus) error
	ListAllByStatus(ctx context.Context, status core.AgentStatus) ([]*core.Agent, error)
	MarkStaleOnline(ctx context.Context, threshold time.Duration) (int64, error)
}

func NewSweeper(_ HeartbeatScanner, agentStatus AgentStatusUpdater, interval time.Duration) *Sweeper {
	return &Sweeper{
		agentStatus:    agentStatus,
		interval:       interval,
		staleThreshold: defaultStaleThreshold,
		stopCh:         make(chan struct{}),
	}
}

// WithStaleThreshold sets how long an agent's durable PG heartbeat may age
// before the sweeper marks it offline (derive it from the configured
// heartbeat TTL plus a grace window).
func (s *Sweeper) WithStaleThreshold(d time.Duration) *Sweeper {
	if d > 0 {
		s.staleThreshold = d
	}
	return s
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

// defaultStaleThreshold is the fallback heartbeat staleness threshold
// (the configured heartbeat TTL plus a grace window, when not provided).
const defaultStaleThreshold = 90 * time.Second

func (s *Sweeper) sweep(ctx context.Context) {
	// One guarded statement: check AND write are atomic per row, so a
	// heartbeat landing mid-sweep cannot be overwritten with offline.
	n, err := s.agentStatus.MarkStaleOnline(ctx, s.staleThreshold)
	if err != nil {
		log.Printf("sweeper: mark stale online: %v", err)
		return
	}
	if n > 0 {
		log.Printf("sweeper: marked %d agent(s) offline (pg heartbeat stale)", n)
	}
}
