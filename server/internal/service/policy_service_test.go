package service

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

type mockPolicyRuleRepo struct {
	rules []*core.PolicyRule
	err   error
}

func (m *mockPolicyRuleRepo) Create(_ context.Context, rule core.PolicyRule) error {
	if m.err != nil {
		return m.err
	}
	m.rules = append(m.rules, &rule)
	return nil
}

func (m *mockPolicyRuleRepo) ListActive(_ context.Context, tenantID string) ([]*core.PolicyRule, error) {
	if m.err != nil {
		return nil, m.err
	}
	var active []*core.PolicyRule
	for _, r := range m.rules {
		if r.TenantID == tenantID && r.Status == "active" {
			active = append(active, r)
		}
	}
	return active, nil
}

func TestPolicyService_AllowByDefault(t *testing.T) {
	svc := NewPolicyService(&mockPolicyRuleRepo{})
	ctx := context.Background()

	decision, err := svc.Evaluate(ctx, core.PolicyInput{
		TenantID: "acme",
		Actor:    core.PolicyActor{Type: "agent", ID: "agent-1"},
		Action:   "execute",
		Resource: core.PolicyResource{Type: "capability", Value: "code_review"},
	})
	require.NoError(t, err)
	assert.Equal(t, core.PolicyDecisionAllow, decision.Decision)
	assert.Equal(t, "default_allow", decision.DecisionID)
}

func TestPolicyService_DenyMatch(t *testing.T) {
	repo := &mockPolicyRuleRepo{
		rules: []*core.PolicyRule{
			{
				TenantID: "acme", ID: "deny-external", Name: "Deny External",
				Status: "active", Priority: 100,
				Condition: json.RawMessage(`{"actor.type":"external"}`),
				Action:    json.RawMessage(`{"decision":"deny"}`),
			},
		},
	}
	svc := NewPolicyService(repo)
	ctx := context.Background()

	decision, err := svc.Evaluate(ctx, core.PolicyInput{
		TenantID: "acme",
		Actor:    core.PolicyActor{Type: "external", ID: "user-1"},
		Action:   "execute",
	})
	require.NoError(t, err)
	assert.Equal(t, core.PolicyDecisionDeny, decision.Decision)
	assert.Equal(t, "deny-external", decision.DecisionID)
	assert.Contains(t, decision.MatchedRules, "deny-external")
}

func TestPolicyService_NoMatchAllows(t *testing.T) {
	repo := &mockPolicyRuleRepo{
		rules: []*core.PolicyRule{
			{
				TenantID: "acme", ID: "deny-external", Name: "Deny External",
				Status: "active", Priority: 100,
				Condition: json.RawMessage(`{"actor.type":"external"}`),
				Action:    json.RawMessage(`{"decision":"deny"}`),
			},
		},
	}
	svc := NewPolicyService(repo)
	ctx := context.Background()

	decision, err := svc.Evaluate(ctx, core.PolicyInput{
		TenantID: "acme",
		Actor:    core.PolicyActor{Type: "agent", ID: "agent-1"},
		Action:   "execute",
	})
	require.NoError(t, err)
	assert.Equal(t, core.PolicyDecisionAllow, decision.Decision)
}

func TestPolicyService_ApprovalRequired(t *testing.T) {
	repo := &mockPolicyRuleRepo{
		rules: []*core.PolicyRule{
			{
				TenantID: "acme", ID: "require-approval", Name: "Require Approval",
				Status: "active", Priority: 50,
				Condition: json.RawMessage(`{"resource.type":"sensitive_data"}`),
				Action:    json.RawMessage(`{"decision":"approval_required"}`),
			},
		},
	}
	svc := NewPolicyService(repo)
	ctx := context.Background()

	decision, err := svc.Evaluate(ctx, core.PolicyInput{
		TenantID: "acme",
		Actor:    core.PolicyActor{Type: "agent", ID: "agent-1"},
		Resource: core.PolicyResource{Type: "sensitive_data", Value: "customer_pii"},
	})
	require.NoError(t, err)
	assert.Equal(t, core.PolicyDecisionApprovalRequired, decision.Decision)
}

func TestPolicyService_InvalidConditionIgnored(t *testing.T) {
	repo := &mockPolicyRuleRepo{
		rules: []*core.PolicyRule{
			{
				TenantID: "acme", ID: "bad-rule", Name: "Bad Rule",
				Status: "active", Priority: 100,
				Condition: json.RawMessage(`invalid json`),
				Action:    json.RawMessage(`{"decision":"deny"}`),
			},
		},
	}
	svc := NewPolicyService(repo)
	ctx := context.Background()

	decision, err := svc.Evaluate(ctx, core.PolicyInput{
		TenantID: "acme",
		Actor:    core.PolicyActor{Type: "agent", ID: "agent-1"},
	})
	require.NoError(t, err)
	assert.Equal(t, core.PolicyDecisionAllow, decision.Decision)
}

func TestPolicyService_InvalidActionSkipped(t *testing.T) {
	repo := &mockPolicyRuleRepo{
		rules: []*core.PolicyRule{
			{
				TenantID: "acme", ID: "no-action", Name: "No Action",
				Status: "active", Priority: 100,
				Condition: json.RawMessage(`{"actor.type":"agent"}`),
				Action:    json.RawMessage(`{}`),
			},
		},
	}
	svc := NewPolicyService(repo)
	ctx := context.Background()

	decision, err := svc.Evaluate(ctx, core.PolicyInput{
		TenantID: "acme",
		Actor:    core.PolicyActor{Type: "agent", ID: "agent-1"},
	})
	require.NoError(t, err)
	assert.Equal(t, core.PolicyDecisionAllow, decision.Decision)
}

func TestPolicyService_RepoError(t *testing.T) {
	repo := &mockPolicyRuleRepo{err: fmt.Errorf("db down")}
	svc := NewPolicyService(repo)
	ctx := context.Background()

	_, err := svc.Evaluate(ctx, core.PolicyInput{TenantID: "acme"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "load policy rules")
}

func TestPolicyService_EmptyTenantID(t *testing.T) {
	svc := NewPolicyService(&mockPolicyRuleRepo{})
	ctx := context.Background()

	_, err := svc.Evaluate(ctx, core.PolicyInput{TenantID: ""})
	assert.EqualError(t, err, "tenant id is required")
}

func TestPolicyService_PriorityOrder(t *testing.T) {
	repo := &mockPolicyRuleRepo{
		rules: []*core.PolicyRule{
			{
				TenantID: "acme", ID: "high-prio-deny", Name: "High Prio Deny",
				Status: "active", Priority: 100,
				Condition: json.RawMessage(`{"actor.type":"agent"}`),
				Action:    json.RawMessage(`{"decision":"deny"}`),
			},
			{
				TenantID: "acme", ID: "low-prio-allow", Name: "Low Prio Allow",
				Status: "active", Priority: 200,
				Condition: json.RawMessage(`{"actor.type":"agent"}`),
				Action:    json.RawMessage(`{"decision":"allow"}`),
			},
		},
	}
	svc := NewPolicyService(repo)
	ctx := context.Background()

	decision, err := svc.Evaluate(ctx, core.PolicyInput{
		TenantID: "acme",
		Actor:    core.PolicyActor{Type: "agent", ID: "agent-1"},
	})
	require.NoError(t, err)
	assert.Equal(t, core.PolicyDecisionDeny, decision.Decision)
	assert.Equal(t, "high-prio-deny", decision.DecisionID)
}

func TestPolicyService_InactiveRuleSkipped(t *testing.T) {
	repo := &mockPolicyRuleRepo{
		rules: []*core.PolicyRule{
			{
				TenantID: "acme", ID: "inactive-deny", Name: "Inactive Deny",
				Status: "inactive", Priority: 100,
				Condition: json.RawMessage(`{"actor.type":"agent"}`),
				Action:    json.RawMessage(`{"decision":"deny"}`),
			},
		},
	}
	svc := NewPolicyService(repo)
	ctx := context.Background()

	decision, err := svc.Evaluate(ctx, core.PolicyInput{
		TenantID: "acme",
		Actor:    core.PolicyActor{Type: "agent", ID: "agent-1"},
	})
	require.NoError(t, err)
	assert.Equal(t, core.PolicyDecisionAllow, decision.Decision)
}

func TestPolicyService_MultiConditionMatch(t *testing.T) {
	repo := &mockPolicyRuleRepo{
		rules: []*core.PolicyRule{
			{
				TenantID: "acme", ID: "multi-cond", Name: "Multi Condition",
				Status: "active", Priority: 100,
				Condition: json.RawMessage(`{"actor.type":"external","action":"delete"}`),
				Action:    json.RawMessage(`{"decision":"deny"}`),
			},
		},
	}
	svc := NewPolicyService(repo)
	ctx := context.Background()

	decision, err := svc.Evaluate(ctx, core.PolicyInput{
		TenantID: "acme",
		Actor:    core.PolicyActor{Type: "external", ID: "user-1"},
		Action:   "delete",
	})
	require.NoError(t, err)
	assert.Equal(t, core.PolicyDecisionDeny, decision.Decision)

	decision, err = svc.Evaluate(ctx, core.PolicyInput{
		TenantID: "acme",
		Actor:    core.PolicyActor{Type: "external", ID: "user-1"},
		Action:   "read",
	})
	require.NoError(t, err)
	assert.Equal(t, core.PolicyDecisionAllow, decision.Decision)
}

func TestPolicyService_DifferentTenant(t *testing.T) {
	repo := &mockPolicyRuleRepo{
		rules: []*core.PolicyRule{
			{
				TenantID: "other", ID: "deny-all", Name: "Deny All",
				Status: "active", Priority: 100,
				Condition: json.RawMessage(`{"actor.type":"agent"}`),
				Action:    json.RawMessage(`{"decision":"deny"}`),
			},
		},
	}
	svc := NewPolicyService(repo)
	ctx := context.Background()

	decision, err := svc.Evaluate(ctx, core.PolicyInput{
		TenantID: "acme",
		Actor:    core.PolicyActor{Type: "agent", ID: "agent-1"},
	})
	require.NoError(t, err)
	assert.Equal(t, core.PolicyDecisionAllow, decision.Decision)
}
