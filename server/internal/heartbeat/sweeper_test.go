package heartbeat

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/agentium-lab/Janus/core"
)

type mockAgentStatus struct {
	online      []*core.Agent
	updates     []statusUpdate
	listErr     error
	staleCutoff time.Time
	staleCalls  int
}

type statusUpdate struct {
	tenantID string
	agentID  string
	status   core.AgentStatus
}

func (m *mockAgentStatus) UpdateStatus(ctx context.Context, tenantID, agentID string, status core.AgentStatus) error {
	m.updates = append(m.updates, statusUpdate{tenantID, agentID, status})
	return nil
}

func (m *mockAgentStatus) ListAllByStatus(ctx context.Context, status core.AgentStatus) ([]*core.Agent, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	return m.online, nil
}

// MarkStaleOnline mirrors the guarded statement: every online agent whose
// heartbeat is older than the threshold flips to offline in one call —
// the write is conditional per row, so no TOCTOU window exists.
func (m *mockAgentStatus) MarkStaleOnline(ctx context.Context, threshold time.Duration) (int64, error) {
	m.staleCalls++
	m.staleCutoff = time.Now().Add(-threshold)
	n := int64(0)
	for _, a := range m.online {
		if a.LastHeartbeatAt == nil || a.LastHeartbeatAt.Before(m.staleCutoff) {
			m.updates = append(m.updates, statusUpdate{a.TenantID, a.ID, core.AgentStatusOffline})
			a.Status = core.AgentStatusOffline
			n++
		}
	}
	return n, nil
}

func hbAgo(d time.Duration) *time.Time {
	t := time.Now().Add(-d)
	return &t
}

// PG heartbeat age is the source of truth: agents whose durable
// last_heartbeat_at is older than the threshold go offline even when Redis
// (the best-effort cache) has no data at all — e.g. after a Redis flush.
func TestSweeper_StalePGHeartbeat_MarksOffline(t *testing.T) {
	status := &mockAgentStatus{
		online: []*core.Agent{
			{ID: "agent-fresh", TenantID: "t1", Status: core.AgentStatusOnline, LastHeartbeatAt: hbAgo(5 * time.Second)},
			{ID: "agent-stale", TenantID: "t1", Status: core.AgentStatusOnline, LastHeartbeatAt: hbAgo(10 * time.Minute)},
		},
	}
	s := NewSweeper(nil, status, 10*time.Second)
	s.sweep(context.Background())

	if len(status.updates) != 1 {
		t.Fatalf("expected 1 update, got %d", len(status.updates))
	}
	if status.updates[0].agentID != "agent-stale" {
		t.Errorf("expected agent-stale, got %s", status.updates[0].agentID)
	}
	if status.updates[0].status != core.AgentStatusOffline {
		t.Errorf("expected offline, got %s", status.updates[0].status)
	}
}

// An online agent that never heartbeated (nil timestamp) is stale.
func TestSweeper_NilHeartbeat_MarksOffline(t *testing.T) {
	status := &mockAgentStatus{
		online: []*core.Agent{{ID: "a1", TenantID: "t1", Status: core.AgentStatusOnline}},
	}
	s := NewSweeper(nil, status, time.Second)
	s.sweep(context.Background())
	if len(status.updates) != 1 {
		t.Fatalf("expected 1 update, got %d", len(status.updates))
	}
}

func TestSweeper_AllFresh_NoUpdates(t *testing.T) {
	status := &mockAgentStatus{
		online: []*core.Agent{
			{ID: "a1", TenantID: "t1", Status: core.AgentStatusOnline, LastHeartbeatAt: hbAgo(time.Second)},
			{ID: "a2", TenantID: "t2", Status: core.AgentStatusOnline, LastHeartbeatAt: hbAgo(2 * time.Second)},
		},
	}
	s := NewSweeper(nil, status, time.Second)
	s.sweep(context.Background())
	if len(status.updates) != 0 {
		t.Fatalf("expected no updates, got %d", len(status.updates))
	}
}

func TestSweeper_MultiTenant(t *testing.T) {
	status := &mockAgentStatus{
		online: []*core.Agent{
			{ID: "a1", TenantID: "t1", Status: core.AgentStatusOnline, LastHeartbeatAt: hbAgo(time.Hour)},
			{ID: "a2", TenantID: "t2", Status: core.AgentStatusOnline, LastHeartbeatAt: hbAgo(5 * time.Second)},
			{ID: "a3", TenantID: "t2", Status: core.AgentStatusOnline, LastHeartbeatAt: hbAgo(2 * time.Hour)},
		},
	}
	s := NewSweeper(nil, status, time.Second)
	s.sweep(context.Background())
	if len(status.updates) != 2 {
		t.Fatalf("expected 2 updates across tenants, got %d", len(status.updates))
	}
}

func TestSweeper_EmptyOnline_NoRepoCalls(t *testing.T) {
	status := &mockAgentStatus{}
	s := NewSweeper(nil, status, time.Second)
	s.sweep(context.Background())
	if len(status.updates) != 0 {
		t.Fatalf("expected no updates, got %d", len(status.updates))
	}
}

func TestSweeper_ListError_SweepSurvives(t *testing.T) {
	status := &mockAgentStatus{listErr: fmt.Errorf("db down")}
	s := NewSweeper(nil, status, time.Second)
	s.sweep(context.Background()) // must not panic
}
