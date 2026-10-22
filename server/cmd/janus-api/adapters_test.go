package main

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/service"
	"github.com/agentium-lab/Janus/server/internal/service/routing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// memPolicyRuleRepo is an in-memory service.PolicyRuleRepo so these tests
// run the REAL PolicyService.Evaluate (exact-equality condition matching)
// against stored rules, exactly as the wired broker does.
type memPolicyRuleRepo struct{ rules []*core.PolicyRule }

func (m *memPolicyRuleRepo) Create(_ context.Context, rule core.PolicyRule) error {
	m.rules = append(m.rules, &rule)
	return nil
}

func (m *memPolicyRuleRepo) ListActive(_ context.Context, _ string) ([]*core.PolicyRule, error) {
	return m.rules, nil
}

func storeTemplateRule(t *testing.T, repo *memPolicyRuleRepo, tenantID string, req core.PolicyRuleTemplateRequest) {
	t.Helper()
	rule, err := req.BuildPolicyRule(tenantID)
	require.NoError(t, err)
	require.NoError(t, repo.Create(context.Background(), rule))
}

func storeRawRule(t *testing.T, repo *memPolicyRuleRepo, tenantID, id string, condition, action map[string]string) {
	t.Helper()
	condJSON, err := json.Marshal(condition)
	require.NoError(t, err)
	actionJSON, err := json.Marshal(action)
	require.NoError(t, err)
	require.NoError(t, repo.Create(context.Background(), core.PolicyRule{
		TenantID: tenantID, ID: id, Name: id, Status: "active",
		Condition: condJSON, Action: actionJSON,
	}))
}

// routeTestLookup implements routing.AgentLookup over a static candidate list.
type routeTestLookup struct{ candidates []routing.AgentCandidate }

func (l *routeTestLookup) ListOnlineByCapability(_ context.Context, _, _ string) ([]routing.AgentCandidate, error) {
	return l.candidates, nil
}
func (l *routeTestLookup) GetAgent(_ context.Context, _, agentID string) (*routing.AgentCandidate, error) {
	for _, c := range l.candidates {
		if c.AgentID == agentID {
			return &c, nil
		}
	}
	return nil, nil
}
func (l *routeTestLookup) GetAgentMailbox(_ context.Context, _, agentID string) (string, error) {
	return agentID + ".default", nil
}
func (l *routeTestLookup) ValidateMailbox(_ context.Context, _, _ string) (bool, error) {
	return true, nil
}
func (l *routeTestLookup) GetGroupMailboxes(_ context.Context, _, _ string) ([]string, error) {
	return nil, nil
}
func (l *routeTestLookup) GetHumanMailboxes(_ context.Context, _, _ string) ([]string, error) {
	return nil, nil
}

func confidentialEnvelope() core.TaskEnvelope {
	return core.TaskEnvelope{Policy: &core.PolicyContext{DataClassification: "confidential"}}
}

// A stored deny_agent_data_classification (task.route) rule filters the
// matching routing candidate: CheckRoute evaluates both the execute and
// task.route actions.
func TestPolicyCheckerAdapter_TaskRouteTemplateDenyFiltersCandidate(t *testing.T) {
	repo := &memPolicyRuleRepo{}
	storeTemplateRule(t, repo, "acme", core.PolicyRuleTemplateRequest{
		Template:           core.PolicyTemplateDenyAgentDataClassification,
		AgentID:            "agent-1",
		DataClassification: "confidential",
	})
	checker := policyCheckerAdapter{svc: service.NewPolicyService(repo)}

	allowed, err := checker.CheckRoute(context.Background(), "acme", "agent-1", "confidential", "")
	require.NoError(t, err)
	assert.False(t, allowed, "denied data classification must filter the candidate")

	allowed, err = checker.CheckRoute(context.Background(), "acme", "agent-1", "public", "")
	require.NoError(t, err)
	assert.True(t, allowed, "non-matching classification stays allowed")

	allowed, err = checker.CheckRoute(context.Background(), "acme", "agent-2", "confidential", "")
	require.NoError(t, err)
	assert.True(t, allowed, "non-matching agent stays allowed")
}

// A task.route ALLOW rule keeps the candidate.
func TestPolicyCheckerAdapter_TaskRouteTemplateAllowKeepsCandidate(t *testing.T) {
	repo := &memPolicyRuleRepo{}
	storeTemplateRule(t, repo, "acme", core.PolicyRuleTemplateRequest{
		Template:           core.PolicyTemplateAllowAgentDataClassification,
		AgentID:            "agent-1",
		DataClassification: "confidential",
	})
	checker := policyCheckerAdapter{svc: service.NewPolicyService(repo)}

	allowed, err := checker.CheckRoute(context.Background(), "acme", "agent-1", "confidential", "")
	require.NoError(t, err)
	assert.True(t, allowed)
}

// Hand-written "execute" rules keep filtering exactly as before.
func TestPolicyCheckerAdapter_ExecuteRulesStillFilter(t *testing.T) {
	repo := &memPolicyRuleRepo{}
	storeRawRule(t, repo, "acme", "exec-deny",
		map[string]string{"action": "execute", "resource.type": "agent", "resource.value": "agent-1"},
		map[string]string{"decision": "deny"})
	checker := policyCheckerAdapter{svc: service.NewPolicyService(repo)}

	allowed, err := checker.CheckRoute(context.Background(), "acme", "agent-1", "confidential", "")
	require.NoError(t, err)
	assert.False(t, allowed)

	allowed, err = checker.CheckRoute(context.Background(), "acme", "agent-2", "confidential", "")
	require.NoError(t, err)
	assert.True(t, allowed)
}

// A task.route approval_required rule also filters the candidate.
func TestPolicyCheckerAdapter_TaskRouteApprovalRequiredFilters(t *testing.T) {
	repo := &memPolicyRuleRepo{}
	storeRawRule(t, repo, "acme", "route-approval",
		map[string]string{"action": "task.route", "context.target_agent_id": "agent-1"},
		map[string]string{"decision": "approval_required"})
	checker := policyCheckerAdapter{svc: service.NewPolicyService(repo)}

	allowed, err := checker.CheckRoute(context.Background(), "acme", "agent-1", "confidential", "")
	require.NoError(t, err)
	assert.False(t, allowed)
}

// Through Router.Route: the denied candidate is filtered (policy_denied)
// while the other candidate still wins.
func TestPolicyCheckerAdapter_TaskRouteDenyFiltersCandidateThroughRouter(t *testing.T) {
	repo := &memPolicyRuleRepo{}
	storeTemplateRule(t, repo, "acme", core.PolicyRuleTemplateRequest{
		Template:           core.PolicyTemplateDenyAgentDataClassification,
		AgentID:            "agent-1",
		DataClassification: "confidential",
	})
	router := routing.NewRouter(
		&routeTestLookup{candidates: []routing.AgentCandidate{
			{AgentID: "agent-1", MailboxID: "agent-1.default", Status: "online",
				Capabilities: []core.AgentCapability{{Capability: "code_review"}}},
			{AgentID: "agent-2", MailboxID: "agent-2.default", Status: "online",
				Capabilities: []core.AgentCapability{{Capability: "code_review"}}},
		}},
		policyCheckerAdapter{svc: service.NewPolicyService(repo)},
		nil,
	)

	result, err := router.Route(context.Background(), "acme",
		core.Target{Type: core.TargetTypeCapability, Value: "code_review"},
		confidentialEnvelope(),
	)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "agent-2", result.AgentID)
	require.Len(t, result.FilteredOut, 1)
	assert.Equal(t, "agent-1", result.FilteredOut[0].AgentID)
	assert.Equal(t, "policy_denied", result.FilteredOut[0].Reason)
}

// Through Router.Route: a task.route ALLOW rule keeps the candidate routable.
func TestPolicyCheckerAdapter_TaskRouteAllowKeepsCandidateThroughRouter(t *testing.T) {
	repo := &memPolicyRuleRepo{}
	storeTemplateRule(t, repo, "acme", core.PolicyRuleTemplateRequest{
		Template:           core.PolicyTemplateAllowAgentDataClassification,
		AgentID:            "agent-1",
		DataClassification: "confidential",
	})
	router := routing.NewRouter(
		&routeTestLookup{candidates: []routing.AgentCandidate{
			{AgentID: "agent-1", MailboxID: "agent-1.default", Status: "online",
				Capabilities: []core.AgentCapability{{Capability: "code_review"}}},
		}},
		policyCheckerAdapter{svc: service.NewPolicyService(repo)},
		nil,
	)

	result, err := router.Route(context.Background(), "acme",
		core.Target{Type: core.TargetTypeCapability, Value: "code_review"},
		confidentialEnvelope(),
	)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "agent-1", result.AgentID)
	assert.Empty(t, result.FilteredOut)
}
