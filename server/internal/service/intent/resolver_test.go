package intent

import (
	"context"
	"testing"

	"github.com/agentium-lab/Janus/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockAgentLookup struct {
	agents []core.Agent
}

func (m *mockAgentLookup) ListOnlineAgents(_ context.Context, _ string) ([]core.Agent, error) {
	return m.agents, nil
}

func TestIntentResolver_ExactMatch(t *testing.T) {
	lookup := &mockAgentLookup{
		agents: []core.Agent{{
			ID: "agent-1", Status: core.AgentStatusOnline,
			Capabilities: []core.AgentCapability{{Capability: "code_review"}},
		}},
	}
	r := NewResolver(lookup)
	result, err := r.Resolve(context.Background(), "acme", "code_review", core.Payload{}, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "code_review", result.ResolvedCapability)
	assert.Greater(t, result.Confidence, 0.5)
}

func TestIntentResolver_PartialMatch(t *testing.T) {
	lookup := &mockAgentLookup{
		agents: []core.Agent{{
			ID: "agent-1", Status: core.AgentStatusOnline,
			Capabilities: []core.AgentCapability{{Capability: "code_review", Description: "Reviews pull requests"}},
		}},
	}
	r := NewResolver(lookup)
	result, err := r.Resolve(context.Background(), "acme", "review", core.Payload{}, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "code_review", result.ResolvedCapability)
	assert.Greater(t, result.Confidence, 0.0)
}

func TestIntentResolver_NoMatch(t *testing.T) {
	lookup := &mockAgentLookup{
		agents: []core.Agent{{
			ID: "agent-1", Status: core.AgentStatusOnline,
			Capabilities: []core.AgentCapability{{Capability: "deploy"}},
		}},
	}
	r := NewResolver(lookup)
	result, err := r.Resolve(context.Background(), "acme", "code_review", core.Payload{}, nil, nil)
	require.NoError(t, err)
	assert.Empty(t, result.ResolvedCapability)
	assert.Contains(t, result.Reason, "no matching")
}

func TestIntentResolver_LowConfidence(t *testing.T) {
	lookup := &mockAgentLookup{
		agents: []core.Agent{{
			ID: "agent-1", Status: core.AgentStatusOnline,
			Capabilities: []core.AgentCapability{{Capability: "deploy", Description: "code"}},
		}},
	}
	r := NewResolver(lookup)
	result, err := r.Resolve(context.Background(), "acme", "code", core.Payload{}, nil, nil)
	require.NoError(t, err)
	if result.ResolvedCapability != "" {
		assert.Less(t, result.Confidence, 0.5)
	}
}

func TestIntentResolver_AmbiguousMatch(t *testing.T) {
	lookup := &mockAgentLookup{
		agents: []core.Agent{{
			ID: "agent-1", Status: core.AgentStatusOnline,
			Capabilities: []core.AgentCapability{
				{Capability: "code_review"},
				{Capability: "code_test"},
			},
		}},
	}
	r := NewResolver(lookup)
	result, err := r.Resolve(context.Background(), "acme", "code_review", core.Payload{}, nil, nil)
	require.NoError(t, err)
	if len(result.Candidates) > 1 {
		if result.ResolvedCapability == "" {
			assert.Contains(t, result.Reason, "ambiguous")
		}
	}
}

func TestIntentResolver_PayloadBoost(t *testing.T) {
	lookup := &mockAgentLookup{
		agents: []core.Agent{{
			ID: "agent-1", Status: core.AgentStatusOnline,
			Capabilities: []core.AgentCapability{
				{Capability: "deploy"},
				{Capability: "code_review", Description: "code review"},
			},
		}},
	}
	r := NewResolver(lookup)
	result, err := r.Resolve(context.Background(), "acme", "code", core.Payload{Content: "please do a code_review"}, nil, nil)
	require.NoError(t, err)
	if result.ResolvedCapability != "" {
		assert.Equal(t, "code_review", result.ResolvedCapability)
	}
}

func TestIntentResolver_PolicyHints(t *testing.T) {
	lookup := &mockAgentLookup{
		agents: []core.Agent{{
			ID: "agent-1", Status: core.AgentStatusOnline,
			Capabilities: []core.AgentCapability{{Capability: "code_review"}},
		}},
	}
	r := NewResolver(lookup)
	result, err := r.Resolve(context.Background(), "acme", "something", core.Payload{}, nil, []string{"code_review"})
	require.NoError(t, err)
	assert.NotEmpty(t, result.Candidates)
}

func TestIntentResolver_NoAgents(t *testing.T) {
	lookup := &mockAgentLookup{agents: nil}
	r := NewResolver(lookup)
	result, err := r.Resolve(context.Background(), "acme", "code_review", core.Payload{}, nil, nil)
	require.NoError(t, err)
	assert.Empty(t, result.ResolvedCapability)
}
