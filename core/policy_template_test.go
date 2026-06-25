package core

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildPolicyRule_AllowAgentCapability(t *testing.T) {
	req := PolicyRuleTemplateRequest{
		Template:   PolicyTemplateAllowAgentCapability,
		AgentID:    "agent-1",
		Capability: "code_review",
	}
	rule, err := req.BuildPolicyRule("acme")
	require.NoError(t, err)
	assert.Equal(t, "acme", rule.TenantID)
	assert.Contains(t, rule.ID, "policy-tpl-allow-agent-capability-")
	assert.Equal(t, "active", rule.Status)
	assert.Equal(t, 100, rule.Priority)
	assert.Contains(t, string(rule.Condition), `"task.publish"`)
	assert.Contains(t, string(rule.Condition), `"code_review"`)
	assert.Contains(t, string(rule.Action), `"allow"`)
}

func TestBuildPolicyRule_DenyTeamCapability(t *testing.T) {
	req := PolicyRuleTemplateRequest{
		Template:   PolicyTemplateDenyTeamCapability,
		TeamID:     "team-a",
		Capability: "deploy",
	}
	rule, err := req.BuildPolicyRule("acme")
	require.NoError(t, err)
	assert.Contains(t, string(rule.Condition), `"team-a"`)
	assert.Contains(t, string(rule.Action), `"deny"`)
}

func TestBuildPolicyRule_RequireApprovalCapability(t *testing.T) {
	req := PolicyRuleTemplateRequest{Template: PolicyTemplateRequireApprovalCapability, Capability: "delete_db"}
	rule, err := req.BuildPolicyRule("acme")
	require.NoError(t, err)
	assert.Contains(t, string(rule.Action), `"approval_required"`)
	assert.Contains(t, rule.Name, "Require approval for delete_db")
}

func TestBuildPolicyRule_RequireApprovalTool(t *testing.T) {
	req := PolicyRuleTemplateRequest{Template: PolicyTemplateRequireApprovalTool, Tool: "mcp.exec"}
	rule, err := req.BuildPolicyRule("acme")
	require.NoError(t, err)
	assert.Contains(t, string(rule.Condition), `"tool.invoke"`)
	assert.Contains(t, string(rule.Condition), `"mcp.exec"`)
	assert.Contains(t, string(rule.Action), `"approval_required"`)
}

func TestBuildPolicyRule_AgentDataClassification(t *testing.T) {
	req := PolicyRuleTemplateRequest{
		Template: PolicyTemplateAllowAgentDataClassification,
		AgentID: "agent-1", DataClassification: "confidential",
	}
	rule, err := req.BuildPolicyRule("acme")
	require.NoError(t, err)
	assert.Contains(t, string(rule.Condition), `"task.route"`)
	assert.Contains(t, string(rule.Condition), `"confidential"`)
}

func TestBuildPolicyRule_TeamDataClassification(t *testing.T) {
	req := PolicyRuleTemplateRequest{
		Template: PolicyTemplateDenyTeamDataClassification,
		TeamID: "team-a", DataClassification: "restricted",
	}
	rule, err := req.BuildPolicyRule("acme")
	require.NoError(t, err)
	assert.Contains(t, string(rule.Condition), `"restricted"`)
	assert.Contains(t, string(rule.Action), `"deny"`)
}

func TestBuildPolicyRule_AgentTool(t *testing.T) {
	req := PolicyRuleTemplateRequest{
		Template: PolicyTemplateAllowAgentTool,
		AgentID: "agent-1", Tool: "git.commit",
	}
	rule, err := req.BuildPolicyRule("acme")
	require.NoError(t, err)
	assert.Contains(t, string(rule.Condition), `"tool.invoke"`)
	assert.Contains(t, string(rule.Condition), `"git.commit"`)
}

func TestBuildPolicyRule_TeamTool(t *testing.T) {
	req := PolicyRuleTemplateRequest{
		Template: PolicyTemplateDenyTeamTool,
		TeamID: "team-a", Tool: "destructive_op",
	}
	rule, err := req.BuildPolicyRule("acme")
	require.NoError(t, err)
	assert.Contains(t, string(rule.Action), `"deny"`)
}

func TestBuildPolicyRule_CustomNameAndStatus(t *testing.T) {
	req := PolicyRuleTemplateRequest{
		Template: PolicyTemplateAllowAgentCapability,
		AgentID: "a1", Capability: "x", Name: "my-rule", Status: "paused", Priority: 50,
	}
	rule, err := req.BuildPolicyRule("acme")
	require.NoError(t, err)
	assert.Equal(t, "my-rule", rule.Name)
	assert.Equal(t, "paused", rule.Status)
	assert.Equal(t, 50, rule.Priority)
}

func TestBuildPolicyRule_EmptyTenantID_Error(t *testing.T) {
	req := PolicyRuleTemplateRequest{Template: PolicyTemplateAllowAgentCapability, AgentID: "a", Capability: "x"}
	_, err := req.BuildPolicyRule("  ")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tenant id is required")
}

func TestBuildPolicyRule_UnknownTemplate_Error(t *testing.T) {
	req := PolicyRuleTemplateRequest{Template: "nonexistent_template", AgentID: "a", Capability: "x"}
	_, err := req.BuildPolicyRule("acme")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported policy template")
}

func TestBuildPolicyRule_AgentCapabilityMissingFields(t *testing.T) {
	cases := []struct {
		name     string
		template string
		req      PolicyRuleTemplateRequest
		errMsg   string
	}{
		{"agent cap missing agent", PolicyTemplateAllowAgentCapability, PolicyRuleTemplateRequest{Capability: "x"}, "agent_id and capability are required"},
		{"team cap missing team", PolicyTemplateAllowTeamCapability, PolicyRuleTemplateRequest{Capability: "x"}, "team_id and capability are required"},
		{"approval cap missing cap", PolicyTemplateRequireApprovalCapability, PolicyRuleTemplateRequest{}, "capability is required"},
		{"approval tool missing tool", PolicyTemplateRequireApprovalTool, PolicyRuleTemplateRequest{}, "tool is required"},
		{"agent class missing agent", PolicyTemplateAllowAgentDataClassification, PolicyRuleTemplateRequest{DataClassification: "x"}, "agent_id and data_classification are required"},
		{"team class missing team", PolicyTemplateAllowTeamDataClassification, PolicyRuleTemplateRequest{DataClassification: "x"}, "team_id and data_classification are required"},
		{"agent tool missing agent", PolicyTemplateAllowAgentTool, PolicyRuleTemplateRequest{Tool: "x"}, "agent_id and tool are required"},
		{"team tool missing team", PolicyTemplateAllowTeamTool, PolicyRuleTemplateRequest{Tool: "x"}, "team_id and tool are required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.req.Template = tc.template
			_, err := tc.req.BuildPolicyRule("acme")
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func TestNormalizePolicyTemplate(t *testing.T) {
	assert.Equal(t, "allow_agent_capability", normalizePolicyTemplate("Allow-Agent.Capability"))
	assert.Equal(t, "allow_agent_capability", normalizePolicyTemplate("  ALLOW_AGENT_CAPABILITY  "))
	assert.Equal(t, "", normalizePolicyTemplate(""))
}

func TestBuildPolicyRule_StableID_Deterministic(t *testing.T) {
	req := PolicyRuleTemplateRequest{
		Template: PolicyTemplateAllowAgentCapability, AgentID: "a1", Capability: "x",
	}
	rule1, _ := req.BuildPolicyRule("acme")
	rule2, _ := req.BuildPolicyRule("acme")
	assert.Equal(t, rule1.ID, rule2.ID, "same inputs must produce same stable ID")

	// Different capability → different ID.
	req.Capability = "y"
	rule3, _ := req.BuildPolicyRule("acme")
	assert.NotEqual(t, rule1.ID, rule3.ID)
}

func TestBuildPolicyRule_NormalizesTemplate(t *testing.T) {
	// Input with dashes/dots/case should normalize to the canonical template.
	req := PolicyRuleTemplateRequest{
		Template: "ALLOW-AGENT.CAPABILITY", AgentID: "a1", Capability: "x",
	}
	rule, err := req.BuildPolicyRule("acme")
	require.NoError(t, err)
	assert.Contains(t, rule.ID, "allow-agent-capability-")
}

func TestPolicyTemplateSlug(t *testing.T) {
	assert.Equal(t, "allow-agent-capability", policyTemplateSlug("allow_agent_capability"))
	assert.Equal(t, "custom", policyTemplateSlug(""))
	// Long slug truncated to 48 chars.
	long := strings.Repeat("a", 60)
	slug := policyTemplateSlug(long)
	assert.LessOrEqual(t, len(slug), 48)
}
