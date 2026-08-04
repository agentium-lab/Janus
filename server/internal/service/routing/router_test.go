package routing

import (
	"context"
	"fmt"
	"testing"

	"github.com/agentium-lab/Janus/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockLookup struct {
	candidates    []AgentCandidate
	mailboxes     map[string]bool
	agentMailbox  map[string]string
	groupMap      map[string][]string
	humanMap      map[string][]string
}

func (m *mockLookup) ListOnlineByCapability(_ context.Context, _, _ string) ([]AgentCandidate, error) {
	return m.candidates, nil
}
func (m *mockLookup) GetAgentMailbox(_ context.Context, _, agentID string) (string, error) {
	if mb, ok := m.agentMailbox[agentID]; ok {
		return mb, nil
	}
	return "", fmt.Errorf("not found")
}
func (m *mockLookup) ValidateMailbox(_ context.Context, _, mailboxID string) (bool, error) {
	return m.mailboxes[mailboxID], nil
}
func (m *mockLookup) GetGroupMailboxes(_ context.Context, _, groupID string) ([]string, error) {
	return m.groupMap[groupID], nil
}
func (m *mockLookup) GetHumanMailboxes(_ context.Context, _, humanID string) ([]string, error) {
	return m.humanMap[humanID], nil
}

type mockPolicy struct{ denyAgents map[string]bool }

func (m *mockPolicy) CheckRoute(_ context.Context, _, agentID, _ string) (bool, error) {
	return !m.denyAgents[agentID], nil
}

type mockBudget struct{ denyAgents map[string]bool }

func (m *mockBudget) CheckCapacity(_ context.Context, _, agentID string, _, _ int) (bool, error) {
	return !m.denyAgents[agentID], nil
}

func TestRoute_AgentTarget(t *testing.T) {
	lookup := &mockLookup{agentMailbox: map[string]string{"agent-1": "mb-1"}}
	r := NewRouter(lookup, nil, nil)

	result, err := r.Route(context.Background(), "acme", core.Target{Type: core.TargetTypeAgent, Value: "agent-1"}, core.TaskEnvelope{})
	require.NoError(t, err)
	assert.Equal(t, "mb-1", result.MailboxID)
	assert.Equal(t, "agent-1", result.AgentID)
}

func TestRoute_MailboxActive(t *testing.T) {
	lookup := &mockLookup{mailboxes: map[string]bool{"mb-1": true}}
	r := NewRouter(lookup, nil, nil)

	result, err := r.Route(context.Background(), "acme", core.Target{Type: core.TargetTypeMailbox, Value: "mb-1"}, core.TaskEnvelope{})
	require.NoError(t, err)
	assert.Equal(t, "mb-1", result.MailboxID)
}

func TestRoute_MailboxInactive(t *testing.T) {
	lookup := &mockLookup{mailboxes: map[string]bool{"mb-1": false}}
	r := NewRouter(lookup, nil, nil)

	_, err := r.Route(context.Background(), "acme", core.Target{Type: core.TargetTypeMailbox, Value: "mb-1"}, core.TaskEnvelope{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not active")
}

func TestRoute_CapabilitySingleCandidate(t *testing.T) {
	lookup := &mockLookup{
		candidates: []AgentCandidate{{
			AgentID: "agent-1", MailboxID: "mb-1", Status: "online",
			Capabilities: []core.AgentCapability{{Capability: "code_review"}},
		}},
	}
	r := NewRouter(lookup, nil, nil)

	result, err := r.Route(context.Background(), "acme",
		core.Target{Type: core.TargetTypeCapability, Value: "code_review"},
		core.TaskEnvelope{})
	require.NoError(t, err)
	assert.Equal(t, "agent-1", result.AgentID)
	assert.Equal(t, "mb-1", result.MailboxID)
	assert.Greater(t, result.Score, 0)
}

func TestRoute_CapabilityNoCandidates(t *testing.T) {
	lookup := &mockLookup{candidates: nil}
	r := NewRouter(lookup, nil, nil)

	_, err := r.Route(context.Background(), "acme",
		core.Target{Type: core.TargetTypeCapability, Value: "nonexistent"},
		core.TaskEnvelope{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no online agents")
}

func TestRoute_CapabilityPolicyDeny(t *testing.T) {
	lookup := &mockLookup{
		candidates: []AgentCandidate{{
			AgentID: "agent-1", MailboxID: "mb-1", Status: "online",
			Capabilities: []core.AgentCapability{{Capability: "code_review"}},
		}},
	}
	policy := &mockPolicy{denyAgents: map[string]bool{"agent-1": true}}
	r := NewRouter(lookup, policy, nil)

	_, err := r.Route(context.Background(), "acme",
		core.Target{Type: core.TargetTypeCapability, Value: "code_review"},
		core.TaskEnvelope{})
	assert.Error(t, err)
}

func TestRoute_CapacityExceeded(t *testing.T) {
	lookup := &mockLookup{
		candidates: []AgentCandidate{{
			AgentID: "agent-1", MailboxID: "mb-1", Status: "online",
			MaxConcurrency: 2, RunningCount: 2,
			Capabilities: []core.AgentCapability{{Capability: "code_review"}},
		}},
	}
	r := NewRouter(lookup, nil, nil)

	result, err := r.Route(context.Background(), "acme",
		core.Target{Type: core.TargetTypeCapability, Value: "code_review"},
		core.TaskEnvelope{})
	assert.Error(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.FilteredOut, 1)
	assert.Equal(t, "capacity_exceeded", result.FilteredOut[0].Reason)
}

func TestRoute_CapabilityBacklogTieBreak(t *testing.T) {
	lookup := &mockLookup{
		candidates: []AgentCandidate{
			{AgentID: "agent-1", MailboxID: "mb-1", Status: "online", Backlog: 5,
				Capabilities: []core.AgentCapability{{Capability: "code_review"}}},
			{AgentID: "agent-2", MailboxID: "mb-2", Status: "online", Backlog: 1,
				Capabilities: []core.AgentCapability{{Capability: "code_review"}}},
		},
	}
	r := NewRouter(lookup, nil, nil)

	result, err := r.Route(context.Background(), "acme",
		core.Target{Type: core.TargetTypeCapability, Value: "code_review"},
		core.TaskEnvelope{})
	require.NoError(t, err)
	assert.Equal(t, "agent-2", result.AgentID, "lower backlog should win tie-break")
}

func TestRoute_GroupActiveMailbox(t *testing.T) {
	lookup := &mockLookup{
		groupMap:  map[string][]string{"dev-team": {"mb-1", "mb-2"}},
		mailboxes: map[string]bool{"mb-1": false, "mb-2": true},
	}
	r := NewRouter(lookup, nil, nil)

	result, err := r.Route(context.Background(), "acme",
		core.Target{Type: core.TargetTypeGroup, Value: "dev-team"},
		core.TaskEnvelope{})
	require.NoError(t, err)
	assert.Equal(t, "mb-2", result.MailboxID)
}

func TestRoute_GroupNoMapping(t *testing.T) {
	lookup := &mockLookup{groupMap: nil}
	r := NewRouter(lookup, nil, nil)

	_, err := r.Route(context.Background(), "acme",
		core.Target{Type: core.TargetTypeGroup, Value: "unknown"},
		core.TaskEnvelope{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no group mailbox")
}

func TestRoute_HumanActiveMailbox(t *testing.T) {
	lookup := &mockLookup{
		humanMap:  map[string][]string{"alice": {"mb-alice"}},
		mailboxes: map[string]bool{"mb-alice": true},
	}
	r := NewRouter(lookup, nil, nil)

	_, err := r.Route(context.Background(), "acme",
		core.Target{Type: core.TargetTypeHuman, Value: "alice"},
		core.TaskEnvelope{})
	require.Error(t, err, "human routing should return error")
	assert.Contains(t, err.Error(), "not supported")
}

func TestRoute_UnsupportedTargetType(t *testing.T) {
	r := NewRouter(&mockLookup{}, nil, nil)

	_, err := r.Route(context.Background(), "acme",
		core.Target{Type: "unknown_type", Value: "x"},
		core.TaskEnvelope{})
	assert.Error(t, err)
}

func TestScoreCandidate_ExactMatch(t *testing.T) {
	c := AgentCandidate{
		Capabilities: []core.AgentCapability{{Capability: "code_review"}},
		Backlog:      0,
	}
	score := scoreCandidate(c, "code_review", core.TaskEnvelope{})
	assert.Greater(t, score, 0)
}

func TestScoreCandidate_NoMatch(t *testing.T) {
	c := AgentCandidate{
		Capabilities: []core.AgentCapability{{Capability: "deploy"}},
		Backlog:      0,
	}
	score := scoreCandidate(c, "code_review", core.TaskEnvelope{})
	assert.Equal(t, 0, score)
}

func TestSelectedEvent(t *testing.T) {
	result := &RouterResult{AgentID: "a1", MailboxID: "mb1", Reason: "capability_semantic", Score: 10}
	evt := SelectedEvent("acme", "task-1", result)
	assert.Equal(t, EventRoutingSelected, evt.EventType)
	assert.Equal(t, "acme", evt.TenantID)
	assert.Equal(t, 10, evt.Score)
}

func TestFailedEvent(t *testing.T) {
	filtered := []FilteredCandidate{{AgentID: "a1", Reason: "offline"}}
	evt := FailedEvent("acme", "task-1", "no_candidates", filtered)
	assert.Equal(t, EventRoutingFailed, evt.EventType)
	assert.Contains(t, evt.Reason, "offline")
}
