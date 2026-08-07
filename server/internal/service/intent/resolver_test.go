package intent

import (
	"context"
	"fmt"
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

type fakeLLM struct {
	resp string
	err  error
}

func (f *fakeLLM) Complete(ctx context.Context, systemPrompt, userPrompt string) (string, error) {
	return f.resp, f.err
}

func TestResolve_LLMMatch(t *testing.T) {
	lookup := &mockAgentLookup{
		agents: []core.Agent{
			{ID: "a1", Status: core.AgentStatusOnline, Capabilities: []core.AgentCapability{{Capability: "code_review", Description: "reviews code"}}},
			{ID: "a2", Status: core.AgentStatusOnline, Capabilities: []core.AgentCapability{{Capability: "deploy", Description: "deploys"}}},
		},
	}
	r := NewResolver(lookup).WithLLM(&fakeLLM{resp: "code_review"})
	result, err := r.Resolve(context.Background(), "acme", "review my PR", core.Payload{}, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "code_review", result.ResolvedCapability)
	assert.Equal(t, 1.0, result.Confidence)
	assert.Equal(t, "llm-resolved", result.Reason)
}

func TestResolve_LLMNone_FallbackKeyword(t *testing.T) {
	lookup := &mockAgentLookup{
		agents: []core.Agent{
			{ID: "a1", Status: core.AgentStatusOnline, Capabilities: []core.AgentCapability{{Capability: "code_review", Description: "reviews"}}},
		},
	}
	r := NewResolver(lookup).WithLLM(&fakeLLM{resp: "NONE"})
	result, err := r.Resolve(context.Background(), "acme", "code_review", core.Payload{}, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "code_review", result.ResolvedCapability, "NONE should fall through to keyword")
}

func TestResolve_LLMError_FallbackKeyword(t *testing.T) {
	lookup := &mockAgentLookup{
		agents: []core.Agent{
			{ID: "a1", Status: core.AgentStatusOnline, Capabilities: []core.AgentCapability{{Capability: "code_review", Description: "reviews code"}}},
		},
	}
	r := NewResolver(lookup).WithLLM(&fakeLLM{err: fmt.Errorf("timeout")})
	result, err := r.Resolve(context.Background(), "acme", "code_review", core.Payload{}, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "code_review", result.ResolvedCapability, "LLM error should fall through to keyword")
}
