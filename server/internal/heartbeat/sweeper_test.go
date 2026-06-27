package heartbeat

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/agentium-lab/Janus/core"
)

type mockHBScanner struct {
	expired map[string][]string
}

func (m *mockHBScanner) ScanExpired(ctx context.Context, tenantID string) ([]string, error) {
	return m.expired[tenantID], nil
}

type mockAgentStatus struct {
	online  []*core.Agent
	updates []statusUpdate
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
	return m.online, nil
}

func TestSweeper_MarksExpiredAgentsOffline(t *testing.T) {
	scanner := &mockHBScanner{
		expired: map[string][]string{
			"t1": {"agent-2"},
		},
	}
	status := &mockAgentStatus{
		online: []*core.Agent{
			{ID: "agent-1", TenantID: "t1", Status: core.AgentStatusOnline},
			{ID: "agent-2", TenantID: "t1", Status: core.AgentStatusOnline},
		},
	}

	s := NewSweeper(scanner, status, 10*time.Second)
	s.sweep(context.Background())

	if len(status.updates) != 1 {
		t.Fatalf("expected 1 update, got %d", len(status.updates))
	}
	if status.updates[0].agentID != "agent-2" {
		t.Errorf("expected agent-2, got %s", status.updates[0].agentID)
	}
	if status.updates[0].status != core.AgentStatusOffline {
		t.Errorf("expected offline, got %s", status.updates[0].status)
	}
}

func TestSweeper_NoExpired_NoUpdates(t *testing.T) {
	scanner := &mockHBScanner{expired: map[string][]string{}}
	status := &mockAgentStatus{
		online: []*core.Agent{
			{ID: "agent-1", TenantID: "t1", Status: core.AgentStatusOnline},
		},
	}

	s := NewSweeper(scanner, status, 10*time.Second)
	s.sweep(context.Background())

	if len(status.updates) != 0 {
		t.Fatalf("expected 0 updates, got %d", len(status.updates))
	}
}

func TestSweeper_MultiTenant(t *testing.T) {
	scanner := &mockHBScanner{
		expired: map[string][]string{
			"t1": {"a1"},
			"t2": {"b1"},
		},
	}
	status := &mockAgentStatus{
		online: []*core.Agent{
			{ID: "a1", TenantID: "t1", Status: core.AgentStatusOnline},
			{ID: "a2", TenantID: "t1", Status: core.AgentStatusOnline},
			{ID: "b1", TenantID: "t2", Status: core.AgentStatusOnline},
		},
	}

	s := NewSweeper(scanner, status, 10*time.Second)
	s.sweep(context.Background())

	if len(status.updates) != 2 {
		t.Fatalf("expected 2 updates, got %d", len(status.updates))
	}

	updated := map[string]bool{}
	for _, u := range status.updates {
		updated[u.agentID] = true
	}
	if !updated["a1"] || !updated["b1"] {
		t.Errorf("expected a1 and b1 to be marked offline, got %v", updated)
	}
}

func TestSweeper_NoOnlineAgents(t *testing.T) {
	scanner := &mockHBScanner{expired: map[string][]string{"t1": {"a1"}}}
	status := &mockAgentStatus{online: nil}

	s := NewSweeper(scanner, status, 10*time.Second)
	s.sweep(context.Background())

	if len(status.updates) != 0 {
		t.Fatalf("expected 0 updates, got %d", len(status.updates))
	}
}

func TestSweeper_StartStop(t *testing.T) {
	scanner := &mockHBScanner{expired: map[string][]string{}}
	status := &mockAgentStatus{online: nil}

	s := NewSweeper(scanner, status, 50*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.Start(ctx)
	time.Sleep(100 * time.Millisecond)
	s.Stop()
}

func TestSweeper_ExpiredNotOnline(t *testing.T) {
	scanner := &mockHBScanner{
		expired: map[string][]string{
			"t1": {"offline-agent"},
		},
	}
	status := &mockAgentStatus{
		online: []*core.Agent{
			{ID: "agent-1", TenantID: "t1", Status: core.AgentStatusOnline},
		},
	}

	s := NewSweeper(scanner, status, 10*time.Second)
	s.sweep(context.Background())

	if len(status.updates) != 0 {
		t.Fatalf("expected 0 updates (expired agent not online), got %d", len(status.updates))
	}
}

func TestSweeper_ScanExpiredEmptyForTenant(t *testing.T) {
	scanner := &mockHBScanner{
		expired: map[string][]string{
			"t1": {},
		},
	}
	status := &mockAgentStatus{
		online: []*core.Agent{
			{ID: "agent-1", TenantID: "t1", Status: core.AgentStatusOnline},
		},
	}

	s := NewSweeper(scanner, status, 10*time.Second)
	s.sweep(context.Background())

	if len(status.updates) != 0 {
		t.Fatalf("expected 0 updates, got %d", len(status.updates))
	}
}

type mockHBScannerErr struct{}

func (m *mockHBScannerErr) ScanExpired(ctx context.Context, tenantID string) ([]string, error) {
	return nil, fmt.Errorf("scan error")
}

type mockAgentStatusListErr struct{}

func (m *mockAgentStatusListErr) UpdateStatus(ctx context.Context, tenantID, agentID string, status core.AgentStatus) error {
	return nil
}

func (m *mockAgentStatusListErr) ListAllByStatus(ctx context.Context, status core.AgentStatus) ([]*core.Agent, error) {
	return nil, fmt.Errorf("list error")
}

func TestSweeper_ListError_Returns(t *testing.T) {
	scanner := &mockHBScanner{expired: map[string][]string{}}
	status := &mockAgentStatusListErr{}

	s := NewSweeper(scanner, status, 10*time.Second)
	s.sweep(context.Background())
}

func TestSweeper_ScanExpiredError_ContinuesLoop(t *testing.T) {
	scanner := &mockHBScannerErr{}
	status := &mockAgentStatus{
		online: []*core.Agent{
			{ID: "a1", TenantID: "t1", Status: core.AgentStatusOnline},
		},
	}

	s := NewSweeper(scanner, status, 10*time.Second)
	s.sweep(context.Background())

	if len(status.updates) != 0 {
		t.Fatalf("expected 0 updates on scan error, got %d", len(status.updates))
	}
}

type mockAgentStatusUpdateErr struct{}

func (m *mockAgentStatusUpdateErr) UpdateStatus(ctx context.Context, tenantID, agentID string, status core.AgentStatus) error {
	return fmt.Errorf("update error")
}

func (m *mockAgentStatusUpdateErr) ListAllByStatus(ctx context.Context, status core.AgentStatus) ([]*core.Agent, error) {
	return []*core.Agent{
		{ID: "a1", TenantID: "t1", Status: core.AgentStatusOnline},
	}, nil
}

func TestSweeper_UpdateStatusError_LogsAndContinues(t *testing.T) {
	scanner := &mockHBScanner{expired: map[string][]string{"t1": {"a1"}}}
	status := &mockAgentStatusUpdateErr{}

	s := NewSweeper(scanner, status, 10*time.Second)
	s.sweep(context.Background())
}
