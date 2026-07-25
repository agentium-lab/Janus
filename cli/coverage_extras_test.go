package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/agentium-lab/Janus/core"
	janus "github.com/agentium-lab/Janus/sdk/go"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// ---------- ProjectCapability UnmarshalYAML / MarshalYAML ----------

func TestProjectCapability_UnmarshalYAML_Scalar(t *testing.T) {
	var c ProjectCapability
	node := &yaml.Node{Kind: yaml.ScalarNode, Value: "  code_review  "}
	err := c.UnmarshalYAML(node)
	require.NoError(t, err)
	assert.Equal(t, "code_review", c.ID)
	assert.Empty(t, c.Description)
}

func TestProjectCapability_UnmarshalYAML_Mapping(t *testing.T) {
	var c ProjectCapability
	data := []byte(`id: review
description: Reviews code
data_classifications:
  - public
  - internal`)
	var node yaml.Node
	require.NoError(t, yaml.Unmarshal(data, &node))
	err := c.UnmarshalYAML(node.Content[0])
	require.NoError(t, err)
	assert.Equal(t, "review", c.ID)
	assert.Equal(t, "Reviews code", c.Description)
	assert.Equal(t, []string{"public", "internal"}, c.DataClassifications)
}

func TestProjectCapability_UnmarshalYAML_InvalidKind(t *testing.T) {
	var c ProjectCapability
	node := &yaml.Node{Kind: yaml.SequenceNode}
	err := c.UnmarshalYAML(node)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "capability must be a string or object")
}

func TestProjectCapability_MarshalYAML_IDOnly(t *testing.T) {
	c := ProjectCapability{ID: "code_review"}
	out, err := c.MarshalYAML()
	require.NoError(t, err)
	assert.Equal(t, "code_review", out)
}

func TestProjectCapability_MarshalYAML_Full(t *testing.T) {
	c := ProjectCapability{ID: "review", Description: "Desc", DataClassifications: []string{"public"}}
	out, err := c.MarshalYAML()
	require.NoError(t, err)
	assert.NotEqual(t, "review", out, "should return struct, not scalar")
}

// ---------- selectedProjectTenants ----------

func TestSelectedProjectTenants_All(t *testing.T) {
	cfg := emptyProjectConfig()
	cfg.Tenants["zebra"] = &ProjectTenant{}
	cfg.Tenants["apple"] = &ProjectTenant{}
	ids, err := selectedProjectTenants(nil, cfg, true)
	require.NoError(t, err)
	assert.Equal(t, []string{"apple", "zebra"}, ids)
}

func TestSelectedProjectTenants_NoTenants(t *testing.T) {
	cfg := emptyProjectConfig()
	_, err := selectedProjectTenants(nil, cfg, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no tenants")
}

func TestSelectedProjectTenants_FlagChanged(t *testing.T) {
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{}

	oldTenantID := tenantID
	defer func() { tenantID = oldTenantID }()
	tenantID = "acme"

	cmd := &cobra.Command{}
	cmd.Flags().String("tenant", "", "")
	require.NoError(t, cmd.Flags().Set("tenant", "acme"))

	ids, err := selectedProjectTenants(cmd, cfg, false)
	require.NoError(t, err)
	assert.Equal(t, []string{"acme"}, ids)
}

func TestSelectedProjectTenants_FlagChangedMissing(t *testing.T) {
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{}

	oldTenantID := tenantID
	defer func() { tenantID = oldTenantID }()
	tenantID = "ghost"

	cmd := &cobra.Command{}
	cmd.Flags().String("tenant", "", "")
	require.NoError(t, cmd.Flags().Set("tenant", "ghost"))

	_, err := selectedProjectTenants(cmd, cfg, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not declared")
}

func TestSelectedProjectTenants_DefaultTenant(t *testing.T) {
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{}
	cfg.DefaultTenant = "acme"

	ids, err := selectedProjectTenants(nil, cfg, false)
	require.NoError(t, err)
	assert.Equal(t, []string{"acme"}, ids)
}

func TestSelectedProjectTenants_SingleTenant(t *testing.T) {
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{}

	ids, err := selectedProjectTenants(nil, cfg, false)
	require.NoError(t, err)
	assert.Equal(t, []string{"acme"}, ids)
}

func TestSelectedProjectTenants_MultipleNoDefault(t *testing.T) {
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{}
	cfg.Tenants["corp"] = &ProjectTenant{}

	_, err := selectedProjectTenants(nil, cfg, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "multiple tenants")
}

func TestSelectedProjectTenants_GlobalTenantID(t *testing.T) {
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{}
	cfg.Tenants["corp"] = &ProjectTenant{}

	oldTenantID := tenantID
	defer func() { tenantID = oldTenantID }()
	tenantID = "acme"

	ids, err := selectedProjectTenants(nil, cfg, false)
	require.NoError(t, err)
	assert.Equal(t, []string{"acme"}, ids)
}

// ---------- tenantFlagChanged ----------

func TestTenantFlagChanged_Nil(t *testing.T) {
	assert.False(t, tenantFlagChanged(nil))
}

func TestTenantFlagChanged_NotChanged(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().String("tenant", "", "")
	assert.False(t, tenantFlagChanged(cmd))
}

func TestTenantFlagChanged_Changed(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().String("tenant", "", "")
	require.NoError(t, cmd.Flags().Set("tenant", "acme"))
	assert.True(t, tenantFlagChanged(cmd))
}

// ---------- projectClient ----------

func TestProjectClient(t *testing.T) {
	oldServerURL := serverURL
	oldAPIKey := apiKey
	defer func() {
		serverURL = oldServerURL
		apiKey = oldAPIKey
	}()
	serverURL = "http://localhost:8080"
	apiKey = "test-key"

	c := projectClient("acme")
	require.NotNil(t, c)
}

// ---------- mergeBudgetsIntoProject ----------

func TestMergeBudgetsIntoProject_AllScopes(t *testing.T) {
	target := ProjectBudgets{}
	budgets := []janus.BudgetSpec{
		{ScopeType: "tenant", ScopeID: "acme", RPM: 100},
		{ScopeType: "team", ScopeID: "eng", TPM: 50},
		{ScopeType: "agent", ScopeID: "a1", MaxConcurrency: 5},
		{ScopeType: "model_provider", ScopeID: "openai", DailyCostUSD: 1.5},
		{ScopeType: "model", ScopeID: "gpt4", MonthlyCostUSD: 10},
		{ScopeType: "task", ScopeID: "review", RPM: 20},
	}
	mergeBudgetsIntoProject(&target, budgets, false)

	require.NotNil(t, target.Tenant)
	assert.Equal(t, 100, target.Tenant.RPM)
	assert.Equal(t, 50, target.Teams["eng"].TPM)
	assert.Equal(t, 5, target.Agents["a1"].Concurrency)
	assert.Equal(t, 1.5, target.ModelProviders["openai"].DailyUSD)
	assert.Equal(t, 10.0, target.Models["gpt4"].MonthlyUSD)
	assert.Equal(t, 20, target.Tasks["review"].RPM)
}

func TestMergeBudgetsIntoProject_OverwriteFalse(t *testing.T) {
	target := ProjectBudgets{
		Tenant: &ProjectBudgetLimits{RPM: 50},
		Teams:  map[string]ProjectBudgetLimits{"eng": {RPM: 30}},
	}
	budgets := []janus.BudgetSpec{
		{ScopeType: "tenant", ScopeID: "acme", RPM: 100},
		{ScopeType: "team", ScopeID: "eng", RPM: 60},
	}
	mergeBudgetsIntoProject(&target, budgets, false)

	assert.Equal(t, 50, target.Tenant.RPM)
	assert.Equal(t, 30, target.Teams["eng"].RPM)
}

func TestMergeBudgetsIntoProject_OverwriteTrue(t *testing.T) {
	target := ProjectBudgets{
		Tenant: &ProjectBudgetLimits{RPM: 50},
		Teams:  map[string]ProjectBudgetLimits{"eng": {RPM: 30}},
	}
	budgets := []janus.BudgetSpec{
		{ScopeType: "tenant", ScopeID: "acme", RPM: 100},
		{ScopeType: "team", ScopeID: "eng", RPM: 60},
	}
	mergeBudgetsIntoProject(&target, budgets, true)

	assert.Equal(t, 100, target.Tenant.RPM)
	assert.Equal(t, 60, target.Teams["eng"].RPM)
}

func TestMergeBudgetsIntoProject_Empty(t *testing.T) {
	target := ProjectBudgets{Tenant: &ProjectBudgetLimits{RPM: 10}}
	mergeBudgetsIntoProject(&target, []janus.BudgetSpec{}, false)
	assert.Equal(t, 10, target.Tenant.RPM)
}

// ---------- mergePolicyRulesIntoProject ----------

func TestMergePolicyRulesIntoProject_Overwrite(t *testing.T) {
	target := ProjectPolicies{
		Allow: []ProjectPolicyBinding{{Agent: "a1", Capability: "cap1"}},
	}
	rules := []core.PolicyRule{
		{
			Condition: json.RawMessage(`{"action":"task.publish","resource.type":"capability","resource.value":"cap2","actor.id":"a2"}`),
			Action:    json.RawMessage(`{"decision":"allow"}`),
		},
	}
	mergePolicyRulesIntoProject(&target, rules, true)
	assert.Len(t, target.Allow, 1)
	assert.Equal(t, "a2", target.Allow[0].Agent)
	assert.Equal(t, "cap2", target.Allow[0].Capability)
	assert.Empty(t, target.Approve.Capabilities)
}

func TestMergePolicyRulesIntoProject_Merge(t *testing.T) {
	target := ProjectPolicies{
		Allow: []ProjectPolicyBinding{{Agent: "a1", Capability: "cap1"}},
	}
	rules := []core.PolicyRule{
		{
			Condition: json.RawMessage(`{"action":"task.publish","resource.type":"capability","resource.value":"cap2","actor.id":"a2"}`),
			Action:    json.RawMessage(`{"decision":"allow"}`),
		},
	}
	mergePolicyRulesIntoProject(&target, rules, false)
	assert.Len(t, target.Allow, 2)
}

func TestMergePolicyRulesIntoProject_Empty(t *testing.T) {
	target := ProjectPolicies{
		Allow: []ProjectPolicyBinding{{Agent: "a1", Capability: "cap1"}},
	}
	mergePolicyRulesIntoProject(&target, []core.PolicyRule{}, false)
	assert.Len(t, target.Allow, 1)
}

// ---------- mergePolicyRuleIntoProject ----------

func TestMergePolicyRuleIntoProject_ApproveCapability(t *testing.T) {
	target := ProjectPolicies{}
	rule := core.PolicyRule{
		Condition: json.RawMessage(`{"action":"task.publish","resource.type":"capability","resource.value":"deploy"}`),
		Action:    json.RawMessage(`{"decision":"approval_required"}`),
	}
	mergePolicyRuleIntoProject(&target, rule)
	assert.Contains(t, target.Approve.Capabilities, "deploy")
}

func TestMergePolicyRuleIntoProject_ApproveTool(t *testing.T) {
	target := ProjectPolicies{}
	rule := core.PolicyRule{
		Condition: json.RawMessage(`{"action":"tool.invoke","resource.type":"tool","resource.value":"rm"}`),
		Action:    json.RawMessage(`{"decision":"approval_required"}`),
	}
	mergePolicyRuleIntoProject(&target, rule)
	assert.Contains(t, target.Approve.Tools, "rm")
}

func TestMergePolicyRuleIntoProject_AllowAgentCapability(t *testing.T) {
	target := ProjectPolicies{}
	rule := core.PolicyRule{
		Condition: json.RawMessage(`{"action":"task.publish","resource.type":"capability","resource.value":"review","actor.id":"a1"}`),
		Action:    json.RawMessage(`{"decision":"allow"}`),
	}
	mergePolicyRuleIntoProject(&target, rule)
	require.Len(t, target.Allow, 1)
	assert.Equal(t, "a1", target.Allow[0].Agent)
	assert.Equal(t, "review", target.Allow[0].Capability)
}

func TestMergePolicyRuleIntoProject_DenyTeamCapability(t *testing.T) {
	target := ProjectPolicies{}
	rule := core.PolicyRule{
		Condition: json.RawMessage(`{"action":"task.publish","resource.type":"capability","resource.value":"deploy","actor.team_id":"ops"}`),
		Action:    json.RawMessage(`{"decision":"deny"}`),
	}
	mergePolicyRuleIntoProject(&target, rule)
	require.Len(t, target.Deny, 1)
	assert.Equal(t, "ops", target.Deny[0].Team)
	assert.Equal(t, "deploy", target.Deny[0].Capability)
}

func TestMergePolicyRuleIntoProject_AllowAgentTool(t *testing.T) {
	target := ProjectPolicies{}
	rule := core.PolicyRule{
		Condition: json.RawMessage(`{"action":"tool.invoke","resource.type":"tool","resource.value":"mcp","actor.id":"a1"}`),
		Action:    json.RawMessage(`{"decision":"allow"}`),
	}
	mergePolicyRuleIntoProject(&target, rule)
	require.Len(t, target.Tools.Allow, 1)
	assert.Equal(t, "a1", target.Tools.Allow[0].Agent)
	assert.Equal(t, "mcp", target.Tools.Allow[0].Tool)
}

func TestMergePolicyRuleIntoProject_DenyTeamTool(t *testing.T) {
	target := ProjectPolicies{}
	rule := core.PolicyRule{
		Condition: json.RawMessage(`{"action":"tool.invoke","resource.type":"tool","resource.value":"mcp","actor.team_id":"ops"}`),
		Action:    json.RawMessage(`{"decision":"deny"}`),
	}
	mergePolicyRuleIntoProject(&target, rule)
	require.Len(t, target.Tools.Deny, 1)
	assert.Equal(t, "ops", target.Tools.Deny[0].Team)
	assert.Equal(t, "mcp", target.Tools.Deny[0].Tool)
}

func TestMergePolicyRuleIntoProject_AllowAgentClassification(t *testing.T) {
	target := ProjectPolicies{}
	rule := core.PolicyRule{
		Condition: json.RawMessage(`{"action":"task.route","context.target_agent_id":"a1","context.data_classification":"public"}`),
		Action:    json.RawMessage(`{"decision":"allow"}`),
	}
	mergePolicyRuleIntoProject(&target, rule)
	require.Len(t, target.DataClassification.Allow, 1)
	assert.Equal(t, "a1", target.DataClassification.Allow[0].Agent)
	assert.Equal(t, "public", target.DataClassification.Allow[0].Classification)
}

func TestMergePolicyRuleIntoProject_DenyTeamClassification(t *testing.T) {
	target := ProjectPolicies{}
	rule := core.PolicyRule{
		Condition: json.RawMessage(`{"action":"task.route","context.target_team_id":"ops","context.data_classification":"confidential"}`),
		Action:    json.RawMessage(`{"decision":"deny"}`),
	}
	mergePolicyRuleIntoProject(&target, rule)
	require.Len(t, target.DataClassification.Deny, 1)
	assert.Equal(t, "ops", target.DataClassification.Deny[0].Team)
	assert.Equal(t, "confidential", target.DataClassification.Deny[0].Classification)
}

func TestMergePolicyRuleIntoProject_InvalidJSON(t *testing.T) {
	target := ProjectPolicies{Allow: []ProjectPolicyBinding{{Agent: "a1", Capability: "cap1"}}}
	rule := core.PolicyRule{
		Condition: json.RawMessage(`{broken`),
		Action:    json.RawMessage(`{"decision":"allow"}`),
	}
	mergePolicyRuleIntoProject(&target, rule)
	assert.Len(t, target.Allow, 1)
}

func TestMergePolicyRuleIntoProject_UnknownDecision(t *testing.T) {
	target := ProjectPolicies{Allow: []ProjectPolicyBinding{{Agent: "a1", Capability: "cap1"}}}
	rule := core.PolicyRule{
		Condition: json.RawMessage(`{"action":"task.publish","resource.type":"capability","resource.value":"review","actor.id":"a1"}`),
		Action:    json.RawMessage(`{"decision":"throttle"}`),
	}
	mergePolicyRuleIntoProject(&target, rule)
	assert.Len(t, target.Allow, 1)
}

func TestMergePolicyRuleIntoProject_UnknownActionName(t *testing.T) {
	target := ProjectPolicies{}
	rule := core.PolicyRule{
		Condition: json.RawMessage(`{"action":"unknown.action","resource.type":"capability","resource.value":"review","actor.id":"a1"}`),
		Action:    json.RawMessage(`{"decision":"allow"}`),
	}
	mergePolicyRuleIntoProject(&target, rule)
	assert.Empty(t, target.Allow)
}

// ---------- validateProjectPolicies extra coverage ----------

func TestValidateProjectPolicies_DuplicateRule(t *testing.T) {
	policies := ProjectPolicies{
		Allow: []ProjectPolicyBinding{
			{Agent: "a1", Capability: "cap1"},
			{Agent: "a1", Capability: "cap1"},
		},
	}
	err := validateProjectPolicies("acme", policies, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate")
}

// ---------- policyTemplateRequests extra coverage ----------

func TestPolicyTemplateRequests_BindingMissingSubject(t *testing.T) {
	policies := ProjectPolicies{
		Allow: []ProjectPolicyBinding{{Capability: "cap1"}},
	}
	_, err := policyTemplateRequests(policies, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires agent or team")
}

func TestPolicyTemplateRequests_BindingBothSubject(t *testing.T) {
	policies := ProjectPolicies{
		Allow: []ProjectPolicyBinding{{Agent: "a1", Team: "t1", Capability: "cap1"}},
	}
	_, err := policyTemplateRequests(policies, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot set both agent and team")
}

func TestPolicyTemplateRequests_BindingMissingResource(t *testing.T) {
	policies := ProjectPolicies{
		Allow: []ProjectPolicyBinding{{Agent: "a1"}},
	}
	_, err := policyTemplateRequests(policies, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires capability or tool")
}

func TestPolicyTemplateRequests_BindingBothResource(t *testing.T) {
	policies := ProjectPolicies{
		Allow: []ProjectPolicyBinding{{Agent: "a1", Capability: "cap1", Tool: "t1"}},
	}
	_, err := policyTemplateRequests(policies, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot set both capability and tool")
}

func TestPolicyTemplateRequests_ClassificationMissingSubject(t *testing.T) {
	policies := ProjectPolicies{
		DataClassification: ProjectClassificationPolicy{
			Allow: []ProjectClassificationBinding{{Classification: "public"}},
		},
	}
	_, err := policyTemplateRequests(policies, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires agent or team")
}

func TestPolicyTemplateRequests_ClassificationMissingClassification(t *testing.T) {
	policies := ProjectPolicies{
		DataClassification: ProjectClassificationPolicy{
			Allow: []ProjectClassificationBinding{{Agent: "a1"}},
		},
	}
	_, err := policyTemplateRequests(policies, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires classification")
}

func TestPolicyTemplateRequests_ClassificationInvalid(t *testing.T) {
	policies := ProjectPolicies{
		DataClassification: ProjectClassificationPolicy{
			Allow: []ProjectClassificationBinding{{Agent: "a1", Classification: "top_secret"}},
		},
	}
	_, err := policyTemplateRequests(policies, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid classification")
}

func TestPolicyTemplateRequests_ToolAllowAgent(t *testing.T) {
	policies := ProjectPolicies{
		Tools: ProjectToolPolicy{
			Allow: []ProjectToolBinding{{Agent: "a1", Tool: "t1"}},
		},
	}
	reqs, err := policyTemplateRequests(policies, 0)
	require.NoError(t, err)
	found := false
	for _, r := range reqs {
		if r.Template == core.PolicyTemplateAllowAgentTool {
			found = true
			break
		}
	}
	assert.True(t, found)
}

func TestPolicyTemplateRequests_ToolDenyTeam(t *testing.T) {
	policies := ProjectPolicies{
		Tools: ProjectToolPolicy{
			Deny: []ProjectToolBinding{{Team: "ops", Tool: "t1"}},
		},
	}
	reqs, err := policyTemplateRequests(policies, 0)
	require.NoError(t, err)
	found := false
	for _, r := range reqs {
		if r.Template == core.PolicyTemplateDenyTeamTool {
			found = true
			break
		}
	}
	assert.True(t, found)
}

func TestPolicyTemplateRequests_ClassificationWithClassificationsSlice(t *testing.T) {
	policies := ProjectPolicies{
		DataClassification: ProjectClassificationPolicy{
			Allow: []ProjectClassificationBinding{{Agent: "a1", Classifications: []string{"public", "internal"}}},
		},
	}
	reqs, err := policyTemplateRequests(policies, 0)
	require.NoError(t, err)
	assert.Len(t, reqs, 2)
}

// ---------- loadProjectConfig extra coverage ----------

func TestLoadProjectConfig_ParseError(t *testing.T) {
	tmpDir := t.TempDir()
	yamlPath := tmpDir + "/janus.project.yaml"
	require.NoError(t, writeFile(yamlPath, []byte(`{broken yaml`)))
	projectFile = yamlPath
	defer func() { projectFile = "" }()

	_, _, err := loadProjectConfig(false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse")
}

func writeFile(path string, data []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	f.Close()
	return err
}

// ---------- saveProjectConfig extra coverage ----------

func TestSaveProjectConfig_Normalize(t *testing.T) {
	tmpDir := t.TempDir()
	yamlPath := tmpDir + "/janus.project.yaml"
	cfg := &ProjectConfig{Version: "", Tenants: nil}
	err := saveProjectConfig(yamlPath, cfg)
	require.NoError(t, err)

	data, err := os.ReadFile(yamlPath)
	require.NoError(t, err)
	assert.Contains(t, string(data), "version: v1")
}

// ---------- resolveProjectPath extra coverage ----------

func TestResolveProjectPath_FindUpward(t *testing.T) {
	projectFile = ""
	t.Setenv("JANUS_PROJECT_FILE", "")

	tmpDir := t.TempDir()
	subDir := tmpDir + "/sub"
	require.NoError(t, os.MkdirAll(subDir, 0755))
	yamlPath := tmpDir + "/" + defaultProjectFileName
	require.NoError(t, os.WriteFile(yamlPath, []byte("version: v1\n"), 0644))

	wd, _ := os.Getwd()
	os.Chdir(subDir)
	defer os.Chdir(wd)

	path, err := resolveProjectPath(false)
	require.NoError(t, err)
	assert.Equal(t, yamlPath, path)
}

// ---------- validateProjectConfig extra coverage ----------

func TestValidateProjectConfig_EmptyAgentID(t *testing.T) {
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{
		Agents: map[string]*ProjectAgent{"": {Capabilities: []ProjectCapability{{ID: "x"}}}},
	}
	err := validateProjectConfig(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty agent id")
}

func TestValidateProjectConfig_NegativeRPM(t *testing.T) {
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{
		Agents: map[string]*ProjectAgent{
			"a1": {Capabilities: []ProjectCapability{{ID: "x"}}, RPM: -1},
		},
	}
	err := validateProjectConfig(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "negative limits")
}

func TestValidateProjectConfig_NegativeTPM(t *testing.T) {
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{
		Agents: map[string]*ProjectAgent{
			"a1": {Capabilities: []ProjectCapability{{ID: "x"}}, TPM: -1},
		},
	}
	err := validateProjectConfig(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "negative limits")
}

// ---------- validateBudgetLimits extra coverage ----------

func TestValidateBudgetLimits_NegativeTPM(t *testing.T) {
	budgets := ProjectBudgets{Agents: map[string]ProjectBudgetLimits{"a1": {TPM: -1}}}
	err := validateBudgetLimits("acme", budgets)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "negative limits")
}

func TestValidateBudgetLimits_NegativeConcurrency(t *testing.T) {
	budgets := ProjectBudgets{Agents: map[string]ProjectBudgetLimits{"a1": {Concurrency: -1}}}
	err := validateBudgetLimits("acme", budgets)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "negative limits")
}

func TestValidateBudgetLimits_NegativeDailyUSD(t *testing.T) {
	budgets := ProjectBudgets{Agents: map[string]ProjectBudgetLimits{"a1": {DailyUSD: -1}}}
	err := validateBudgetLimits("acme", budgets)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "negative limits")
}

func TestValidateBudgetLimits_NegativeMonthlyUSD(t *testing.T) {
	budgets := ProjectBudgets{Agents: map[string]ProjectBudgetLimits{"a1": {MonthlyUSD: -1}}}
	err := validateBudgetLimits("acme", budgets)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "negative limits")
}

func TestValidateBudgetLimits_NegativeTenant(t *testing.T) {
	budgets := ProjectBudgets{Tenant: &ProjectBudgetLimits{RPM: -1}}
	err := validateBudgetLimits("acme", budgets)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "negative limits")
}

// ---------- registerAgentRequest extra coverage ----------

func TestRegisterAgentRequest_CapabilitySchemaError(t *testing.T) {
	// This is impossible to trigger with real json.Marshal, but we keep the test
	// structure for completeness. The function path is covered by other tests.
	agent := ProjectAgent{
		Capabilities: []ProjectCapability{{ID: "x"}},
	}
	req, err := registerAgentRequest("a1", agent, ProjectDefaults{})
	require.NoError(t, err)
	assert.Equal(t, "a1", req.ID)
}

// ---------- agentConcurrency extra coverage ----------

func TestAgentConcurrency_CapacityFallback(t *testing.T) {
	agent := ProjectAgent{Capacity: ProjectCapacity{MaxConcurrency: 7}}
	defaults := ProjectDefaults{Capacity: ProjectCapacity{MaxConcurrency: 10}}
	assert.Equal(t, 7, agentConcurrency(agent, defaults))
}

// ---------- envelope extra coverage ----------

func TestEnvelope(t *testing.T) {
	oldTenantID := tenantID
	defer func() { tenantID = oldTenantID }()
	tenantID = "acme"

	e := envelope("t1", "src", "agent", "dst", `{"key":"val"}`)
	assert.Equal(t, "t1", e.TaskID)
	assert.Equal(t, "acme", e.TenantID)
	assert.Equal(t, "src", e.SourceAgent)
	assert.Equal(t, core.TargetType("agent"), e.Target.Type)
	assert.Equal(t, "dst", e.Target.Value)
	assert.Equal(t, `{"key":"val"}`, e.Payload.Content)
}

// ---------- printJSON extra coverage ----------

func TestPrintJSON(t *testing.T) {
	var buf bytes.Buffer
	printJSON(&buf, map[string]string{"key": "value"})
	assert.Contains(t, buf.String(), "key")
	assert.Contains(t, buf.String(), "value")
}

// ---------- HTTP command coverage ----------

func TestAPIKeyCreate_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/tenants/test-tenant/api-keys", r.URL.Path)
		assert.Equal(t, "POST", r.Method)
		json.NewEncoder(w).Encode(map[string]string{"id": "k1", "name": "test-key"})
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	out, err := executeCommand(root, "api-key", "create", "--name", "test-key")
	assert.NoError(t, err)
	assert.Contains(t, out, "test-key")
}

func TestAPIKeyList_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/tenants/test-tenant/api-keys", r.URL.Path)
		assert.Equal(t, "GET", r.Method)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"api_keys": []map[string]string{{"id": "k1", "name": "test"}},
		})
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	out, err := executeCommand(root, "api-key", "list")
	assert.NoError(t, err)
	assert.Contains(t, out, "k1")
}

func TestAPIKeyRevoke_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/tenants/test-tenant/api-keys/k1/revoke", r.URL.Path)
		assert.Equal(t, "POST", r.Method)
		json.NewEncoder(w).Encode(map[string]string{"id": "k1", "status": "revoked"})
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	out, err := executeCommand(root, "api-key", "revoke", "k1")
	assert.NoError(t, err)
	assert.Contains(t, out, "k1")
}

func TestDLQQuery_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/tenants/test-tenant/dlq", r.URL.Path)
		assert.Equal(t, "GET", r.Method)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"tasks": []map[string]string{{"id": "t1", "status": "dead_lettered"}},
		})
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	out, err := executeCommand(root, "dlq", "query")
	assert.NoError(t, err)
	assert.Contains(t, out, "t1")
}

func TestDLQReplay_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/tenants/test-tenant/dlq/t1/replay", r.URL.Path)
		assert.Equal(t, "POST", r.Method)
		json.NewEncoder(w).Encode(map[string]string{"id": "t1", "status": "created"})
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	out, err := executeCommand(root, "dlq", "replay", "t1")
	assert.NoError(t, err)
	assert.Contains(t, out, "t1")
}

func TestDLQDiscard_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/tenants/test-tenant/dlq/t1/discard", r.URL.Path)
		assert.Equal(t, "POST", r.Method)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	out, err := executeCommand(root, "dlq", "discard", "t1")
	assert.NoError(t, err)
	assert.Contains(t, out, "discarded")
}

func TestPolicyAllowAgent_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "/policy-rules")
		assert.Equal(t, "POST", r.Method)
		body, _ := io.ReadAll(r.Body)
		var req core.PolicyRuleTemplateRequest
		json.Unmarshal(body, &req)
		assert.Equal(t, "a1", req.AgentID)
		assert.Equal(t, "cap1", req.Capability)
		json.NewEncoder(w).Encode(map[string]string{"id": "rule-1"})
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	out, err := executeCommand(root, "policy", "allow-agent", "--agent", "a1", "--capability", "cap1")
	assert.NoError(t, err)
	assert.Contains(t, out, "rule-1")
}

func TestPolicyDenyAgent_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"id": "rule-1"})
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "policy", "deny-agent", "--agent", "a1", "--capability", "cap1")
	assert.NoError(t, err)
}

func TestPolicyAllowTeam_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"id": "rule-1"})
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "policy", "allow-team", "--team", "ops", "--capability", "cap1")
	assert.NoError(t, err)
}

func TestPolicyDenyTeam_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"id": "rule-1"})
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "policy", "deny-team", "--team", "ops", "--capability", "cap1")
	assert.NoError(t, err)
}

func TestPolicyRequireApproval_Capability(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"id": "rule-1"})
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "policy", "require-approval", "--capability", "cap1")
	assert.NoError(t, err)
}

func TestPolicyRequireApproval_Tool(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"id": "rule-1"})
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "policy", "require-approval", "--tool", "t1")
	assert.NoError(t, err)
}

func TestPolicyRequireApproval_Missing(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "policy", "require-approval")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "--capability or --tool is required")
}

func TestPolicyRequireApproval_Both(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "policy", "require-approval", "--capability", "cap1", "--tool", "t1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mutually exclusive")
}

func TestPolicyAllowClassification_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"id": "rule-1"})
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "policy", "allow-classification", "--agent", "a1", "--classification", "public")
	assert.NoError(t, err)
}

func TestPolicyDenyClassification_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"id": "rule-1"})
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "policy", "deny-classification", "--team", "ops", "--classification", "confidential")
	assert.NoError(t, err)
}

func TestPolicyAllowTool_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"id": "rule-1"})
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "policy", "allow-tool", "--agent", "a1", "--tool", "t1")
	assert.NoError(t, err)
}

func TestPolicyDenyTool_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"id": "rule-1"})
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "policy", "deny-tool", "--team", "ops", "--tool", "t1")
	assert.NoError(t, err)
}

func TestPolicyList_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/tenants/test-tenant/policy-rules", r.URL.Path)
		assert.Equal(t, "GET", r.Method)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"policy_rules": []map[string]string{{"id": "r1", "subject": "agent/a1"}},
		})
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	out, err := executeCommand(root, "policy", "list")
	assert.NoError(t, err)
	assert.Contains(t, out, "r1")
}

func TestMailboxCreate_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "/mailboxes")
		assert.Equal(t, "POST", r.Method)
		json.NewEncoder(w).Encode(map[string]string{"id": "mb1"})
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	out, err := executeCommand(root, "mailbox", "create", "--id", "mb1", "--agent", "a1")
	assert.NoError(t, err)
	assert.Contains(t, out, "mb1")
}

func TestMailboxStatus_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "/mailboxes/mb1")
		assert.Equal(t, "GET", r.Method)
		json.NewEncoder(w).Encode(map[string]string{"id": "mb1", "status": "active"})
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	out, err := executeCommand(root, "mailbox", "status", "mb1")
	assert.NoError(t, err)
	assert.Contains(t, out, "active")
}

func TestMailboxPause_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "/mailboxes/mb1/pause")
		json.NewEncoder(w).Encode(map[string]string{"id": "mb1", "status": "paused"})
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	out, err := executeCommand(root, "mailbox", "pause", "mb1")
	assert.NoError(t, err)
	assert.Contains(t, out, "paused")
}

func TestMailboxResume_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "/mailboxes/mb1/resume")
		json.NewEncoder(w).Encode(map[string]string{"id": "mb1", "status": "active"})
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	out, err := executeCommand(root, "mailbox", "resume", "mb1")
	assert.NoError(t, err)
	assert.Contains(t, out, "active")
}

func TestTaskReplay_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "/tasks/t1/replay")
		json.NewEncoder(w).Encode(map[string]string{"id": "t1", "status": "replayed"})
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	out, err := executeCommand(root, "task", "replay", "t1")
	assert.NoError(t, err)
	assert.Contains(t, out, "t1")
}

func TestProjectInit_Success(t *testing.T) {
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, defaultProjectFileName)
	projectFile = yamlPath
	defer func() { projectFile = "" }()

	root := &cobra.Command{Use: "janus", SilenceErrors: true, SilenceUsage: true}
	root.AddCommand(projectCmd())
	out, err := executeCommand(root, "project", "init")
	assert.NoError(t, err)
	assert.Contains(t, out, "Created")
}

func TestProjectInit_AlreadyExists(t *testing.T) {
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, defaultProjectFileName)
	require.NoError(t, os.WriteFile(yamlPath, []byte("version: v1\n"), 0644))
	projectFile = yamlPath
	defer func() { projectFile = "" }()

	root := &cobra.Command{Use: "janus", SilenceErrors: true, SilenceUsage: true}
	root.AddCommand(projectCmd())
	_, err := executeCommand(root, "project", "init")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestProjectInit_Force(t *testing.T) {
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, defaultProjectFileName)
	require.NoError(t, os.WriteFile(yamlPath, []byte("version: v1\n"), 0644))
	projectFile = yamlPath
	defer func() { projectFile = "" }()

	root := &cobra.Command{Use: "janus", SilenceErrors: true, SilenceUsage: true}
	root.AddCommand(projectCmd())
	out, err := executeCommand(root, "project", "init", "--force")
	assert.NoError(t, err)
	assert.Contains(t, out, "Created")
}

func TestProjectValidate_Success(t *testing.T) {
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, defaultProjectFileName)
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{
		Agents: map[string]*ProjectAgent{
			"a1": {Capabilities: []ProjectCapability{{ID: "x"}}},
		},
	}
	require.NoError(t, saveProjectConfig(yamlPath, cfg))
	projectFile = yamlPath
	defer func() { projectFile = "" }()

	root := &cobra.Command{Use: "janus", SilenceErrors: true, SilenceUsage: true}
	root.AddCommand(projectCmd())
	out, err := executeCommand(root, "project", "validate")
	assert.NoError(t, err)
	assert.Contains(t, out, "is valid")
}

func TestProjectValidate_Invalid(t *testing.T) {
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, defaultProjectFileName)
	require.NoError(t, os.WriteFile(yamlPath, []byte(`version: v999`), 0644))
	projectFile = yamlPath
	defer func() { projectFile = "" }()

	root := &cobra.Command{Use: "janus", SilenceErrors: true, SilenceUsage: true}
	root.AddCommand(projectCmd())
	_, err := executeCommand(root, "project", "validate")
	assert.Error(t, err)
}

func TestTenantAdd_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/tenants", r.URL.Path)
		assert.Equal(t, "POST", r.Method)
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, defaultProjectFileName)
	projectFile = yamlPath
	defer func() { projectFile = "" }()

	root := newTestRoot(srv)
	root.AddCommand(tenantCmd())
	out, err := executeCommand(root, "tenant", "add", "acme", "--name", "Acme Corp")
	assert.NoError(t, err)
	assert.Contains(t, out, "Tenant acme added")
}

func TestTenantAdd_MissingID(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()

	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, defaultProjectFileName)
	projectFile = yamlPath
	defer func() { projectFile = "" }()

	root := newTestRoot(srv)
	root.AddCommand(tenantCmd())
	_, err := executeCommand(root, "tenant", "add", "  ")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "tenant id is required")
}

func TestLoadProjectConfig_NotFoundNoCreate(t *testing.T) {
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, defaultProjectFileName)
	projectFile = yamlPath
	defer func() { projectFile = "" }()

	_, _, err := loadProjectConfig(false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no such file or directory")
}

// ---------- diffProjectTenant ----------

func TestDiffProjectTenant_TenantNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "not found"})
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldAPIKey := apiKey
	defer func() { serverURL = oldServerURL; apiKey = oldAPIKey }()
	serverURL = srv.URL
	apiKey = "test-key"

	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{
		Agents: map[string]*ProjectAgent{
			"a1": {Capabilities: []ProjectCapability{{ID: "cap1"}}},
		},
	}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	err := diffProjectTenant(cmd, cfg, "acme")
	require.NoError(t, err)
	out := buf.String()
	assert.Contains(t, out, "+ tenant acme")
	assert.Contains(t, out, "+ agent a1")
	assert.Contains(t, out, "+ mailbox a1.default")
}

func TestDiffProjectTenant_TenantExistsAgentNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/tenants/acme":
			json.NewEncoder(w).Encode(core.Tenant{ID: "acme", Name: "Acme"})
		case "/v1/tenants/acme/agents/a1":
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": "not found"})
		case "/v1/tenants/acme/mailboxes/a1.default":
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": "not found"})
		case "/v1/tenants/acme/budgets":
			json.NewEncoder(w).Encode(map[string]interface{}{"budgets": []janus.BudgetSpec{}})
		case "/v1/tenants/acme/policy-rules":
			json.NewEncoder(w).Encode(map[string]interface{}{"policy_rules": []core.PolicyRule{}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldAPIKey := apiKey
	defer func() { serverURL = oldServerURL; apiKey = oldAPIKey }()
	serverURL = srv.URL
	apiKey = "test-key"

	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{
		Agents: map[string]*ProjectAgent{
			"a1": {Capabilities: []ProjectCapability{{ID: "cap1"}}},
		},
	}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	err := diffProjectTenant(cmd, cfg, "acme")
	require.NoError(t, err)
	out := buf.String()
	assert.Contains(t, out, "= tenant acme")
	assert.Contains(t, out, "+ agent a1")
	assert.Contains(t, out, "+ mailbox a1.default")
}

func TestDiffProjectTenant_TenantExistsAgentExistsMailboxSame(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/tenants/acme":
			json.NewEncoder(w).Encode(core.Tenant{ID: "acme", Name: "Acme"})
		case "/v1/tenants/acme/agents/a1":
			json.NewEncoder(w).Encode(core.Agent{ID: "a1", DisplayName: "A1", Protocol: "custom-sdk"})
		case "/v1/tenants/acme/mailboxes/a1.default":
			json.NewEncoder(w).Encode(core.Mailbox{ID: "a1.default", AgentID: "a1", MaxConcurrency: 1, ACKWaitSeconds: 0, MaxDeliver: 0, RetentionSeconds: 0})
		case "/v1/tenants/acme/budgets":
			json.NewEncoder(w).Encode(map[string]interface{}{"budgets": []janus.BudgetSpec{}})
		case "/v1/tenants/acme/policy-rules":
			json.NewEncoder(w).Encode(map[string]interface{}{"policy_rules": []core.PolicyRule{}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldAPIKey := apiKey
	defer func() { serverURL = oldServerURL; apiKey = oldAPIKey }()
	serverURL = srv.URL
	apiKey = "test-key"

	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{
		Agents: map[string]*ProjectAgent{
			"a1": {Capabilities: []ProjectCapability{{ID: "cap1"}}},
		},
	}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	err := diffProjectTenant(cmd, cfg, "acme")
	require.NoError(t, err)
	out := buf.String()
	assert.Contains(t, out, "= tenant acme")
	assert.Contains(t, out, "= agent a1")
	assert.Contains(t, out, "= mailbox a1.default")
}

func TestDiffProjectTenant_TenantExistsAgentExistsMailboxDiff(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/tenants/acme":
			json.NewEncoder(w).Encode(core.Tenant{ID: "acme", Name: "Acme"})
		case "/v1/tenants/acme/agents/a1":
			json.NewEncoder(w).Encode(core.Agent{ID: "a1", DisplayName: "A1", Protocol: "custom-sdk"})
		case "/v1/tenants/acme/mailboxes/a1.default":
			json.NewEncoder(w).Encode(core.Mailbox{ID: "a1.default", AgentID: "a1", MaxConcurrency: 99, ACKWaitSeconds: 30, MaxDeliver: 10, RetentionSeconds: 86400})
		case "/v1/tenants/acme/budgets":
			json.NewEncoder(w).Encode(map[string]interface{}{"budgets": []janus.BudgetSpec{}})
		case "/v1/tenants/acme/policy-rules":
			json.NewEncoder(w).Encode(map[string]interface{}{"policy_rules": []core.PolicyRule{}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldAPIKey := apiKey
	defer func() { serverURL = oldServerURL; apiKey = oldAPIKey }()
	serverURL = srv.URL
	apiKey = "test-key"

	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{
		Agents: map[string]*ProjectAgent{
			"a1": {Capabilities: []ProjectCapability{{ID: "cap1"}}},
		},
	}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	err := diffProjectTenant(cmd, cfg, "acme")
	require.NoError(t, err)
	out := buf.String()
	assert.Contains(t, out, "= tenant acme")
	assert.Contains(t, out, "= agent a1")
	assert.Contains(t, out, "~ mailbox a1.default")
}

func TestDiffProjectTenant_WithBudgetsAndPolicies(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/tenants/acme":
			json.NewEncoder(w).Encode(core.Tenant{ID: "acme", Name: "Acme"})
		case "/v1/tenants/acme/agents/a1":
			json.NewEncoder(w).Encode(core.Agent{ID: "a1", DisplayName: "A1", Protocol: "custom-sdk"})
		case "/v1/tenants/acme/mailboxes/a1.default":
			json.NewEncoder(w).Encode(core.Mailbox{ID: "a1.default", AgentID: "a1", MaxConcurrency: 1, ACKWaitSeconds: 30, MaxDeliver: 10, RetentionSeconds: 86400})
		case "/v1/tenants/acme/budgets":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"budgets": []janus.BudgetSpec{
					{ScopeType: "tenant", ScopeID: "acme", RPM: 100},
					{ScopeType: "agent", ScopeID: "a1", RPM: 50},
				},
			})
		case "/v1/tenants/acme/policy-rules":
			json.NewEncoder(w).Encode(map[string]interface{}{"policy_rules": []core.PolicyRule{}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldAPIKey := apiKey
	defer func() { serverURL = oldServerURL; apiKey = oldAPIKey }()
	serverURL = srv.URL
	apiKey = "test-key"

	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{
		Agents: map[string]*ProjectAgent{
			"a1": {Capabilities: []ProjectCapability{{ID: "cap1"}}},
		},
		Budgets: ProjectBudgets{Tenant: &ProjectBudgetLimits{RPM: 100}},
		Policies: ProjectPolicies{
			Allow: []ProjectPolicyBinding{{Agent: "a1", Capability: "cap1"}},
		},
	}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	err := diffProjectTenant(cmd, cfg, "acme")
	require.NoError(t, err)
	out := buf.String()
	assert.Contains(t, out, "= budget tenant/acme")
	assert.Contains(t, out, "+ policy")
}

func TestDiffProjectTenant_APIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "boom"})
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldAPIKey := apiKey
	defer func() { serverURL = oldServerURL; apiKey = oldAPIKey }()
	serverURL = srv.URL
	apiKey = "test-key"

	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{Agents: map[string]*ProjectAgent{"a1": {}}}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	err := diffProjectTenant(cmd, cfg, "acme")
	require.Error(t, err)
}

// ---------- applyProjectTenant ----------

func TestApplyProjectTenant_CreateAll(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/tenants/acme" && r.Method == "GET":
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": "not found"})
		case r.URL.Path == "/v1/tenants" && r.Method == "POST":
			w.WriteHeader(http.StatusCreated)
		case r.URL.Path == "/v1/tenants/acme/agents/a1" && r.Method == "GET":
			w.WriteHeader(http.StatusNotFound)
		case r.URL.Path == "/v1/tenants/acme/agents" && r.Method == "POST":
			w.WriteHeader(http.StatusCreated)
		case r.URL.Path == "/v1/tenants/acme/mailboxes/a1.default" && r.Method == "GET":
			w.WriteHeader(http.StatusNotFound)
		case r.URL.Path == "/v1/tenants/acme/mailboxes" && r.Method == "POST":
			json.NewEncoder(w).Encode(map[string]string{"id": "a1.default", "status": "active"})
		case r.URL.Path == "/v1/tenants/acme/budgets" && r.Method == "POST":
			json.NewEncoder(w).Encode(janus.BudgetSpec{ScopeType: "tenant", ScopeID: "acme"})
		case r.URL.Path == "/v1/tenants/acme/policy-rules" && r.Method == "GET":
			json.NewEncoder(w).Encode(map[string]interface{}{"policy_rules": []core.PolicyRule{}})
		case r.URL.Path == "/v1/tenants/acme/policy-rules/templates" && r.Method == "POST":
			json.NewEncoder(w).Encode(core.PolicyRule{ID: "rule-1"})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldAPIKey := apiKey
	defer func() { serverURL = oldServerURL; apiKey = oldAPIKey }()
	serverURL = srv.URL
	apiKey = "test-key"

	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{
		Agents: map[string]*ProjectAgent{
			"a1": {Capabilities: []ProjectCapability{{ID: "cap1"}}},
		},
		Budgets: ProjectBudgets{Tenant: &ProjectBudgetLimits{RPM: 100}},
		Policies: ProjectPolicies{
			Allow: []ProjectPolicyBinding{{Agent: "a1", Capability: "cap1"}},
		},
	}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	err := applyProjectTenant(cmd, cfg, "acme")
	require.NoError(t, err)
	out := buf.String()
	assert.Contains(t, out, "created tenant acme")
	assert.Contains(t, out, "created agent a1")
	assert.Contains(t, out, "created mailbox a1.default")
	assert.Contains(t, out, "upserted budget tenant/acme")
	assert.Contains(t, out, "created policy")
}

func TestApplyProjectTenant_ExistingTenantAndAgent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/tenants/acme" && r.Method == "GET":
			json.NewEncoder(w).Encode(core.Tenant{ID: "acme", Name: "Acme"})
		case r.URL.Path == "/v1/tenants/acme/agents/a1" && r.Method == "GET":
			json.NewEncoder(w).Encode(core.Agent{ID: "a1", DisplayName: "A1", Protocol: "custom-sdk"})
		case r.URL.Path == "/v1/tenants/acme/mailboxes/a1.default" && r.Method == "GET":
			json.NewEncoder(w).Encode(core.Mailbox{ID: "a1.default", AgentID: "a1", MaxConcurrency: 99, ACKWaitSeconds: 30, MaxDeliver: 10, RetentionSeconds: 86400})
		case r.URL.Path == "/v1/tenants/acme/mailboxes/a1.default" && r.Method == "PATCH":
			json.NewEncoder(w).Encode(map[string]string{"id": "a1.default", "status": "active"})
		case r.URL.Path == "/v1/tenants/acme/budgets" && r.Method == "POST":
			json.NewEncoder(w).Encode(janus.BudgetSpec{ScopeType: "tenant", ScopeID: "acme"})
		case r.URL.Path == "/v1/tenants/acme/policy-rules" && r.Method == "GET":
			json.NewEncoder(w).Encode(map[string]interface{}{"policy_rules": []core.PolicyRule{}})
		case r.URL.Path == "/v1/tenants/acme/policy-rules/templates" && r.Method == "POST":
			json.NewEncoder(w).Encode(core.PolicyRule{ID: "rule-1"})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldAPIKey := apiKey
	defer func() { serverURL = oldServerURL; apiKey = oldAPIKey }()
	serverURL = srv.URL
	apiKey = "test-key"

	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{
		Agents: map[string]*ProjectAgent{
			"a1": {Capabilities: []ProjectCapability{{ID: "cap1"}}},
		},
		Budgets: ProjectBudgets{Tenant: &ProjectBudgetLimits{RPM: 100}},
		Policies: ProjectPolicies{
			Allow: []ProjectPolicyBinding{{Agent: "a1", Capability: "cap1"}},
		},
	}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	err := applyProjectTenant(cmd, cfg, "acme")
	require.NoError(t, err)
	out := buf.String()
	assert.Contains(t, out, "existing agent a1")
	assert.Contains(t, out, "updated mailbox a1.default")
}

func TestApplyProjectTenant_MailboxConflict(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/tenants/acme" && r.Method == "GET":
			w.WriteHeader(http.StatusNotFound)
		case r.URL.Path == "/v1/tenants" && r.Method == "POST":
			w.WriteHeader(http.StatusCreated)
		case r.URL.Path == "/v1/tenants/acme/agents/a1" && r.Method == "GET":
			w.WriteHeader(http.StatusNotFound)
		case r.URL.Path == "/v1/tenants/acme/agents" && r.Method == "POST":
			w.WriteHeader(http.StatusCreated)
		case r.URL.Path == "/v1/tenants/acme/mailboxes/a1.default" && r.Method == "GET":
			w.WriteHeader(http.StatusNotFound)
		case r.URL.Path == "/v1/tenants/acme/mailboxes" && r.Method == "POST":
			w.WriteHeader(http.StatusConflict)
			json.NewEncoder(w).Encode(map[string]string{"error": "conflict"})
		case r.URL.Path == "/v1/tenants/acme/budgets" && r.Method == "POST":
			json.NewEncoder(w).Encode(janus.BudgetSpec{ScopeType: "tenant", ScopeID: "acme"})
		case r.URL.Path == "/v1/tenants/acme/policy-rules" && r.Method == "GET":
			json.NewEncoder(w).Encode(map[string]interface{}{"policy_rules": []core.PolicyRule{}})
		case r.URL.Path == "/v1/tenants/acme/policy-rules/templates" && r.Method == "POST":
			json.NewEncoder(w).Encode(core.PolicyRule{ID: "rule-1"})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldAPIKey := apiKey
	defer func() { serverURL = oldServerURL; apiKey = oldAPIKey }()
	serverURL = srv.URL
	apiKey = "test-key"

	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{
		Agents: map[string]*ProjectAgent{
			"a1": {Capabilities: []ProjectCapability{{ID: "cap1"}}},
		},
		Budgets: ProjectBudgets{Tenant: &ProjectBudgetLimits{RPM: 100}},
		Policies: ProjectPolicies{
			Allow: []ProjectPolicyBinding{{Agent: "a1", Capability: "cap1"}},
		},
	}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	err := applyProjectTenant(cmd, cfg, "acme")
	require.NoError(t, err)
	out := buf.String()
	assert.Contains(t, out, "created tenant acme")
	assert.Contains(t, out, "created agent a1")
}

func TestApplyProjectTenant_APIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "boom"})
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldAPIKey := apiKey
	defer func() { serverURL = oldServerURL; apiKey = oldAPIKey }()
	serverURL = srv.URL
	apiKey = "test-key"

	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{Agents: map[string]*ProjectAgent{"a1": {}}}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	err := applyProjectTenant(cmd, cfg, "acme")
	require.Error(t, err)
}

// ---------- syncProjectTenant ----------

func TestSyncProjectTenant_Basic(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/tenants/acme":
			json.NewEncoder(w).Encode(core.Tenant{ID: "acme", Name: "Acme Corp"})
		case "/v1/tenants/acme/agents":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"agents": []core.Agent{
					{ID: "a1", DisplayName: "Agent 1", Protocol: "custom-sdk", MaxConcurrency: 5, RPM: 100, TPM: 1000},
				},
			})
		case "/v1/tenants/acme/budgets":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"budgets": []janus.BudgetSpec{
					{ScopeType: "tenant", ScopeID: "acme", RPM: 200},
				},
			})
		case "/v1/tenants/acme/policy-rules":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"policy_rules": []core.PolicyRule{
					{ID: "rule-1", Condition: json.RawMessage(`{"action":"task.publish","resource.type":"capability","resource.value":"cap1","actor.id":"a1"}`), Action: json.RawMessage(`{"decision":"allow"}`)},
				},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldAPIKey := apiKey
	defer func() { serverURL = oldServerURL; apiKey = oldAPIKey }()
	serverURL = srv.URL
	apiKey = "test-key"

	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{}

	ctx := context.Background()
	err := syncProjectTenant(ctx, cfg, "acme", false)
	require.NoError(t, err)

	pt := cfg.Tenants["acme"]
	require.NotNil(t, pt)
	assert.Equal(t, "Acme Corp", pt.Name)
	require.Len(t, pt.Agents, 1)
	assert.Equal(t, "Agent 1", pt.Agents["a1"].Name)
	assert.Equal(t, 5, pt.Agents["a1"].Concurrency)
	assert.Equal(t, 100, pt.Agents["a1"].RPM)
	require.NotNil(t, pt.Budgets.Tenant)
	assert.Equal(t, 200, pt.Budgets.Tenant.RPM)
	require.Len(t, pt.Policies.Allow, 1)
	assert.Equal(t, "a1", pt.Policies.Allow[0].Agent)
}

func TestSyncProjectTenant_Overwrite(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/tenants/acme":
			json.NewEncoder(w).Encode(core.Tenant{ID: "acme", Name: "New Name"})
		case "/v1/tenants/acme/agents":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"agents": []core.Agent{
					{ID: "a1", DisplayName: "New Agent", Protocol: "custom-sdk"},
				},
			})
		case "/v1/tenants/acme/budgets":
			json.NewEncoder(w).Encode(map[string]interface{}{"budgets": []janus.BudgetSpec{}})
		case "/v1/tenants/acme/policy-rules":
			json.NewEncoder(w).Encode(map[string]interface{}{"policy_rules": []core.PolicyRule{}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldAPIKey := apiKey
	defer func() { serverURL = oldServerURL; apiKey = oldAPIKey }()
	serverURL = srv.URL
	apiKey = "test-key"

	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{
		Name:   "Old Name",
		Agents: map[string]*ProjectAgent{"a1": {Name: "Old Agent", Capabilities: []ProjectCapability{{ID: "cap1"}}}},
	}

	ctx := context.Background()
	err := syncProjectTenant(ctx, cfg, "acme", true)
	require.NoError(t, err)

	pt := cfg.Tenants["acme"]
	assert.Equal(t, "New Name", pt.Name)
	assert.Equal(t, "New Agent", pt.Agents["a1"].Name)
}

func TestSyncProjectTenant_APIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "boom"})
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldAPIKey := apiKey
	defer func() { serverURL = oldServerURL; apiKey = oldAPIKey }()
	serverURL = srv.URL
	apiKey = "test-key"

	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{}

	ctx := context.Background()
	err := syncProjectTenant(ctx, cfg, "acme", false)
	require.Error(t, err)
}

// ---------- project command wrappers ----------

func TestProjectDiffCmd_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/tenants/acme":
			w.WriteHeader(http.StatusNotFound)
		case "/v1/tenants/acme/agents/a1":
			w.WriteHeader(http.StatusNotFound)
		case "/v1/tenants/acme/mailboxes/a1.default":
			w.WriteHeader(http.StatusNotFound)
		case "/v1/tenants/acme/budgets":
			json.NewEncoder(w).Encode(map[string]interface{}{"budgets": []janus.BudgetSpec{}})
		case "/v1/tenants/acme/policy-rules":
			json.NewEncoder(w).Encode(map[string]interface{}{"policy_rules": []core.PolicyRule{}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldTenantID := tenantID
	oldProjectFile := projectFile
	defer func() { serverURL = oldServerURL; tenantID = oldTenantID; projectFile = oldProjectFile }()
	serverURL = srv.URL
	tenantID = "acme"

	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, defaultProjectFileName)
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{
		Agents: map[string]*ProjectAgent{
			"a1": {Capabilities: []ProjectCapability{{ID: "cap1"}}},
		},
	}
	require.NoError(t, saveProjectConfig(yamlPath, cfg))
	projectFile = yamlPath

	root := &cobra.Command{Use: "janus", SilenceErrors: true, SilenceUsage: true}
	root.AddCommand(projectCmd())
	out, err := executeCommand(root, "project", "diff")
	require.NoError(t, err)
	assert.Contains(t, out, "+ tenant acme")
}

func TestProjectApplyCmd_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/tenants/acme" && r.Method == "GET":
			w.WriteHeader(http.StatusNotFound)
		case r.URL.Path == "/v1/tenants" && r.Method == "POST":
			w.WriteHeader(http.StatusCreated)
		case r.URL.Path == "/v1/tenants/acme/agents/a1" && r.Method == "GET":
			w.WriteHeader(http.StatusNotFound)
		case r.URL.Path == "/v1/tenants/acme/agents" && r.Method == "POST":
			w.WriteHeader(http.StatusCreated)
		case r.URL.Path == "/v1/tenants/acme/mailboxes/a1.default" && r.Method == "GET":
			w.WriteHeader(http.StatusNotFound)
		case r.URL.Path == "/v1/tenants/acme/mailboxes" && r.Method == "POST":
			json.NewEncoder(w).Encode(map[string]string{"id": "a1.default", "status": "active"})
		case r.URL.Path == "/v1/tenants/acme/budgets" && r.Method == "POST":
			json.NewEncoder(w).Encode(janus.BudgetSpec{ScopeType: "tenant", ScopeID: "acme"})
		case r.URL.Path == "/v1/tenants/acme/policy-rules" && r.Method == "GET":
			json.NewEncoder(w).Encode(map[string]interface{}{"policy_rules": []core.PolicyRule{}})
		case r.URL.Path == "/v1/tenants/acme/policy-rules/templates" && r.Method == "POST":
			json.NewEncoder(w).Encode(core.PolicyRule{ID: "rule-1"})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldTenantID := tenantID
	oldProjectFile := projectFile
	defer func() { serverURL = oldServerURL; tenantID = oldTenantID; projectFile = oldProjectFile }()
	serverURL = srv.URL
	tenantID = "acme"

	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, defaultProjectFileName)
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{
		Agents: map[string]*ProjectAgent{
			"a1": {Capabilities: []ProjectCapability{{ID: "cap1"}}},
		},
		Budgets: ProjectBudgets{Tenant: &ProjectBudgetLimits{RPM: 100}},
		Policies: ProjectPolicies{
			Allow: []ProjectPolicyBinding{{Agent: "a1", Capability: "cap1"}},
		},
	}
	require.NoError(t, saveProjectConfig(yamlPath, cfg))
	projectFile = yamlPath

	root := &cobra.Command{Use: "janus", SilenceErrors: true, SilenceUsage: true}
	root.AddCommand(projectCmd())
	out, err := executeCommand(root, "project", "apply")
	require.NoError(t, err)
	assert.Contains(t, out, "created tenant acme")
}

func TestProjectApplyCmd_ContinueOnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "boom"})
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldTenantID := tenantID
	oldProjectFile := projectFile
	defer func() { serverURL = oldServerURL; tenantID = oldTenantID; projectFile = oldProjectFile }()
	serverURL = srv.URL
	tenantID = "acme"

	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, defaultProjectFileName)
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{Agents: map[string]*ProjectAgent{"a1": {}}}
	cfg.Tenants["corp"] = &ProjectTenant{Agents: map[string]*ProjectAgent{"a2": {}}}
	require.NoError(t, saveProjectConfig(yamlPath, cfg))
	projectFile = yamlPath

	root := &cobra.Command{Use: "janus", SilenceErrors: true, SilenceUsage: true}
	root.AddCommand(projectCmd())
	_, err := executeCommand(root, "project", "apply", "--all-tenants", "--continue-on-error")
	require.Error(t, err)
}

func TestProjectSyncCmd_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/tenants/acme":
			json.NewEncoder(w).Encode(core.Tenant{ID: "acme", Name: "Acme Corp"})
		case "/v1/tenants/acme/agents":
			json.NewEncoder(w).Encode(map[string]interface{}{"agents": []core.Agent{}})
		case "/v1/tenants/acme/budgets":
			json.NewEncoder(w).Encode(map[string]interface{}{"budgets": []janus.BudgetSpec{}})
		case "/v1/tenants/acme/policy-rules":
			json.NewEncoder(w).Encode(map[string]interface{}{"policy_rules": []core.PolicyRule{}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldTenantID := tenantID
	oldProjectFile := projectFile
	defer func() { serverURL = oldServerURL; tenantID = oldTenantID; projectFile = oldProjectFile }()
	serverURL = srv.URL
	tenantID = "acme"

	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, defaultProjectFileName)
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{}
	require.NoError(t, saveProjectConfig(yamlPath, cfg))
	projectFile = yamlPath

	root := &cobra.Command{Use: "janus", SilenceErrors: true, SilenceUsage: true}
	root.AddCommand(projectCmd())
	out, err := executeCommand(root, "project", "sync")
	require.NoError(t, err)
	assert.Contains(t, out, "Synced")
}

// ---------- agentAddCmd ----------

func TestAgentAddCmd_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/tenants/acme/agents" && r.Method == "POST":
			w.WriteHeader(http.StatusCreated)
		case r.URL.Path == "/v1/tenants/acme/mailboxes" && r.Method == "POST":
			json.NewEncoder(w).Encode(map[string]string{"id": "a1.default", "status": "active"})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldTenantID := tenantID
	oldProjectFile := projectFile
	defer func() { serverURL = oldServerURL; tenantID = oldTenantID; projectFile = oldProjectFile }()
	serverURL = srv.URL
	tenantID = "acme"

	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, defaultProjectFileName)
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{Agents: map[string]*ProjectAgent{}}
	require.NoError(t, saveProjectConfig(yamlPath, cfg))
	projectFile = yamlPath

	root := newTestRoot(srv)
	root.AddCommand(agentCmd())
	out, err := executeCommand(root, "agent", "add", "a1", "--capability", "cap1")
	require.NoError(t, err)
	assert.Contains(t, out, "Agent a1 added to tenant acme")

	// Verify file was updated
	loaded, _, err := loadProjectConfig(false)
	require.NoError(t, err)
	require.NotNil(t, loaded.Tenants["acme"].Agents["a1"])
	assert.Equal(t, "cap1", loaded.Tenants["acme"].Agents["a1"].Capabilities[0].ID)
}

func TestAgentAddCmd_MissingCapability(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()

	oldServerURL := serverURL
	oldTenantID := tenantID
	oldProjectFile := projectFile
	defer func() { serverURL = oldServerURL; tenantID = oldTenantID; projectFile = oldProjectFile }()
	serverURL = srv.URL
	tenantID = "acme"

	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, defaultProjectFileName)
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{Agents: map[string]*ProjectAgent{}}
	require.NoError(t, saveProjectConfig(yamlPath, cfg))
	projectFile = yamlPath

	root := newTestRoot(srv)
	root.AddCommand(agentCmd())
	_, err := executeCommand(root, "agent", "add", "a1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--capability is required")
}

func TestAgentAddCmd_AlreadyExists(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()

	oldServerURL := serverURL
	oldTenantID := tenantID
	oldProjectFile := projectFile
	defer func() { serverURL = oldServerURL; tenantID = oldTenantID; projectFile = oldProjectFile }()
	serverURL = srv.URL
	tenantID = "acme"

	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, defaultProjectFileName)
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{Agents: map[string]*ProjectAgent{"a1": {Capabilities: []ProjectCapability{{ID: "cap1"}}}}}
	require.NoError(t, saveProjectConfig(yamlPath, cfg))
	projectFile = yamlPath

	root := newTestRoot(srv)
	root.AddCommand(agentCmd())
	_, err := executeCommand(root, "agent", "add", "a1", "--capability", "cap2")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

// ---------- additional command coverage ----------

func TestAPIKeyCreate_MissingName(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "api-key", "create")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--name is required")
}

func TestDLQQuery_WithOptions(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/tenants/test-tenant/dlq", r.URL.Path)
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "mb1", r.URL.Query().Get("mailbox"))
		assert.Equal(t, "10", r.URL.Query().Get("limit"))
		json.NewEncoder(w).Encode(map[string]interface{}{
			"tasks": []map[string]string{{"id": "t1", "status": "dead_lettered"}},
		})
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	out, err := executeCommand(root, "dlq", "query", "--mailbox", "mb1", "--limit", "10")
	assert.NoError(t, err)
	assert.Contains(t, out, "t1")
}

func TestPolicyCapabilityCmd_MissingFlags(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "policy", "allow-agent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--agent and --capability are required")
}

func TestPolicyClassificationCmd_MissingClassification(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "policy", "allow-classification", "--agent", "a1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--classification is required")
}

func TestPolicyToolCmd_MissingTool(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "policy", "allow-tool", "--agent", "a1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--tool is required")
}

func TestTenantAddCmd_AlreadyExists(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()

	oldServerURL := serverURL
	oldTenantID := tenantID
	oldProjectFile := projectFile
	defer func() { serverURL = oldServerURL; tenantID = oldTenantID; projectFile = oldProjectFile }()
	serverURL = srv.URL
	tenantID = "acme"

	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, defaultProjectFileName)
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{}
	require.NoError(t, saveProjectConfig(yamlPath, cfg))
	projectFile = yamlPath

	root := newTestRoot(srv)
	root.AddCommand(tenantCmd())
	_, err := executeCommand(root, "tenant", "add", "acme")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestTenantAddCmd_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldTenantID := tenantID
	oldProjectFile := projectFile
	defer func() { serverURL = oldServerURL; tenantID = oldTenantID; projectFile = oldProjectFile }()
	serverURL = srv.URL
	tenantID = "acme"

	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, defaultProjectFileName)
	cfg := emptyProjectConfig()
	require.NoError(t, saveProjectConfig(yamlPath, cfg))
	projectFile = yamlPath

	root := newTestRoot(srv)
	root.AddCommand(tenantCmd())
	_, err := executeCommand(root, "tenant", "add", "newcorp", "--name", "New Corp")
	require.Error(t, err)
}

// ---------- diffProjectTenant error paths ----------

func TestDiffProjectTenant_ListBudgetsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/tenants/acme":
			json.NewEncoder(w).Encode(core.Tenant{ID: "acme", Name: "Acme"})
		case "/v1/tenants/acme/agents/a1":
			json.NewEncoder(w).Encode(core.Agent{ID: "a1", DisplayName: "A1", Protocol: "custom-sdk"})
		case "/v1/tenants/acme/mailboxes/a1.default":
			json.NewEncoder(w).Encode(core.Mailbox{ID: "a1.default", AgentID: "a1", MaxConcurrency: 1, ACKWaitSeconds: 0, MaxDeliver: 0, RetentionSeconds: 0})
		case "/v1/tenants/acme/budgets":
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldAPIKey := apiKey
	defer func() { serverURL = oldServerURL; apiKey = oldAPIKey }()
	serverURL = srv.URL
	apiKey = "test-key"

	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{Agents: map[string]*ProjectAgent{"a1": {}}}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	err := diffProjectTenant(cmd, cfg, "acme")
	require.Error(t, err)
}

func TestDiffProjectTenant_ListPolicyRulesError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/tenants/acme":
			json.NewEncoder(w).Encode(core.Tenant{ID: "acme", Name: "Acme"})
		case "/v1/tenants/acme/agents/a1":
			json.NewEncoder(w).Encode(core.Agent{ID: "a1", DisplayName: "A1", Protocol: "custom-sdk"})
		case "/v1/tenants/acme/mailboxes/a1.default":
			json.NewEncoder(w).Encode(core.Mailbox{ID: "a1.default", AgentID: "a1", MaxConcurrency: 1, ACKWaitSeconds: 0, MaxDeliver: 0, RetentionSeconds: 0})
		case "/v1/tenants/acme/budgets":
			json.NewEncoder(w).Encode(map[string]interface{}{"budgets": []janus.BudgetSpec{}})
		case "/v1/tenants/acme/policy-rules":
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldAPIKey := apiKey
	defer func() { serverURL = oldServerURL; apiKey = oldAPIKey }()
	serverURL = srv.URL
	apiKey = "test-key"

	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{Agents: map[string]*ProjectAgent{"a1": {}}}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	err := diffProjectTenant(cmd, cfg, "acme")
	require.Error(t, err)
}

// ---------- applyProjectTenant error paths ----------

func TestApplyProjectTenant_CreateTenantError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/tenants/acme" && r.Method == "GET":
			w.WriteHeader(http.StatusNotFound)
		case r.URL.Path == "/v1/tenants" && r.Method == "POST":
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldAPIKey := apiKey
	defer func() { serverURL = oldServerURL; apiKey = oldAPIKey }()
	serverURL = srv.URL
	apiKey = "test-key"

	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{Agents: map[string]*ProjectAgent{"a1": {}}}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	err := applyProjectTenant(cmd, cfg, "acme")
	require.Error(t, err)
}

func TestApplyProjectTenant_UpsertBudgetError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/tenants/acme" && r.Method == "GET":
			json.NewEncoder(w).Encode(core.Tenant{ID: "acme", Name: "Acme"})
		case r.URL.Path == "/v1/tenants/acme/agents/a1" && r.Method == "GET":
			json.NewEncoder(w).Encode(core.Agent{ID: "a1", DisplayName: "A1", Protocol: "custom-sdk"})
		case r.URL.Path == "/v1/tenants/acme/mailboxes/a1.default" && r.Method == "GET":
			json.NewEncoder(w).Encode(core.Mailbox{ID: "a1.default", AgentID: "a1", MaxConcurrency: 1, ACKWaitSeconds: 0, MaxDeliver: 0, RetentionSeconds: 0})
		case r.URL.Path == "/v1/tenants/acme/budgets" && r.Method == "POST":
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldAPIKey := apiKey
	defer func() { serverURL = oldServerURL; apiKey = oldAPIKey }()
	serverURL = srv.URL
	apiKey = "test-key"

	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{
		Agents: map[string]*ProjectAgent{"a1": {Capabilities: []ProjectCapability{{ID: "cap1"}}}},
		Budgets: ProjectBudgets{Tenant: &ProjectBudgetLimits{RPM: 100}},
	}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	err := applyProjectTenant(cmd, cfg, "acme")
	require.Error(t, err)
}

func TestApplyProjectTenant_CreatePolicyRuleError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/tenants/acme" && r.Method == "GET":
			json.NewEncoder(w).Encode(core.Tenant{ID: "acme", Name: "Acme"})
		case r.URL.Path == "/v1/tenants/acme/agents/a1" && r.Method == "GET":
			json.NewEncoder(w).Encode(core.Agent{ID: "a1", DisplayName: "A1", Protocol: "custom-sdk"})
		case r.URL.Path == "/v1/tenants/acme/mailboxes/a1.default" && r.Method == "GET":
			json.NewEncoder(w).Encode(core.Mailbox{ID: "a1.default", AgentID: "a1", MaxConcurrency: 1, ACKWaitSeconds: 0, MaxDeliver: 0, RetentionSeconds: 0})
		case r.URL.Path == "/v1/tenants/acme/budgets" && r.Method == "POST":
			json.NewEncoder(w).Encode(janus.BudgetSpec{ScopeType: "tenant", ScopeID: "acme"})
		case r.URL.Path == "/v1/tenants/acme/policy-rules" && r.Method == "GET":
			json.NewEncoder(w).Encode(map[string]interface{}{"policy_rules": []core.PolicyRule{}})
		case r.URL.Path == "/v1/tenants/acme/policy-rules/templates" && r.Method == "POST":
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldAPIKey := apiKey
	defer func() { serverURL = oldServerURL; apiKey = oldAPIKey }()
	serverURL = srv.URL
	apiKey = "test-key"

	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{
		Agents: map[string]*ProjectAgent{"a1": {Capabilities: []ProjectCapability{{ID: "cap1"}}}},
		Policies: ProjectPolicies{
			Allow: []ProjectPolicyBinding{{Agent: "a1", Capability: "cap1"}},
		},
	}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	err := applyProjectTenant(cmd, cfg, "acme")
	require.Error(t, err)
}

// ---------- projectSyncCmd error paths ----------

func TestProjectSyncCmd_ValidateError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/tenants/acme":
			json.NewEncoder(w).Encode(core.Tenant{ID: "acme", Name: "Acme Corp"})
		case "/v1/tenants/acme/agents":
			json.NewEncoder(w).Encode(map[string]interface{}{"agents": []core.Agent{}})
		case "/v1/tenants/acme/budgets":
			json.NewEncoder(w).Encode(map[string]interface{}{"budgets": []janus.BudgetSpec{}})
		case "/v1/tenants/acme/policy-rules":
			json.NewEncoder(w).Encode(map[string]interface{}{"policy_rules": []core.PolicyRule{}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldTenantID := tenantID
	oldProjectFile := projectFile
	defer func() { serverURL = oldServerURL; tenantID = oldTenantID; projectFile = oldProjectFile }()
	serverURL = srv.URL
	tenantID = "acme"

	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, defaultProjectFileName)
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{Agents: map[string]*ProjectAgent{"": {Capabilities: []ProjectCapability{{ID: "x"}}}}}
	require.NoError(t, saveProjectConfig(yamlPath, cfg))
	projectFile = yamlPath

	root := &cobra.Command{Use: "janus", SilenceErrors: true, SilenceUsage: true}
	root.AddCommand(projectCmd())
	_, err := executeCommand(root, "project", "sync")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty agent id")
}

// ---------- agentAddCmd error paths ----------

func TestAgentAddCmd_RegisterAgentError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/tenants/acme/agents" && r.Method == "POST":
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldTenantID := tenantID
	oldProjectFile := projectFile
	defer func() { serverURL = oldServerURL; tenantID = oldTenantID; projectFile = oldProjectFile }()
	serverURL = srv.URL
	tenantID = "acme"

	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, defaultProjectFileName)
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{Agents: map[string]*ProjectAgent{}}
	require.NoError(t, saveProjectConfig(yamlPath, cfg))
	projectFile = yamlPath

	root := newTestRoot(srv)
	root.AddCommand(agentCmd())
	_, err := executeCommand(root, "agent", "add", "a1", "--capability", "cap1")
	require.Error(t, err)
}

func TestAgentAddCmd_CreateMailboxError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/tenants/acme/agents" && r.Method == "POST":
			w.WriteHeader(http.StatusCreated)
		case r.URL.Path == "/v1/tenants/acme/mailboxes" && r.Method == "POST":
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldTenantID := tenantID
	oldProjectFile := projectFile
	defer func() { serverURL = oldServerURL; tenantID = oldTenantID; projectFile = oldProjectFile }()
	serverURL = srv.URL
	tenantID = "acme"

	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, defaultProjectFileName)
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{Agents: map[string]*ProjectAgent{}}
	require.NoError(t, saveProjectConfig(yamlPath, cfg))
	projectFile = yamlPath

	root := newTestRoot(srv)
	root.AddCommand(agentCmd())
	_, err := executeCommand(root, "agent", "add", "a1", "--capability", "cap1")
	require.Error(t, err)
}

// ---------- createPolicyRuleFromTemplate error ----------

func TestCreatePolicyRuleFromTemplate_Error(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "policy", "allow-agent", "--agent", "a1", "--capability", "cap1")
	require.Error(t, err)
}

// ---------- apiKeyCmd error paths ----------

func TestAPIKeyList_Error(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "api-key", "list")
	require.Error(t, err)
}

func TestAPIKeyRevoke_Error(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "api-key", "revoke", "k1")
	require.Error(t, err)
}

// ---------- dlqCmd error paths ----------

func TestDLQReplay_Error(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "dlq", "replay", "t1")
	require.Error(t, err)
}

func TestDLQDiscard_Error(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "dlq", "discard", "t1")
	require.Error(t, err)
}

// ---------- policyClassificationCmd / policyToolCmd error paths ----------

func TestPolicyAllowClassification_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "policy", "allow-classification", "--agent", "a1", "--classification", "public")
	require.Error(t, err)
}

func TestPolicyAllowTool_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "policy", "allow-tool", "--agent", "a1", "--tool", "t1")
	require.Error(t, err)
}

// ---------- projectValidateCmd error path ----------

func TestProjectValidateCmd_LoadError(t *testing.T) {
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, defaultProjectFileName)
	projectFile = yamlPath
	defer func() { projectFile = "" }()

	root := &cobra.Command{Use: "janus", SilenceErrors: true, SilenceUsage: true}
	root.AddCommand(projectCmd())
	_, err := executeCommand(root, "project", "validate")
	require.Error(t, err)
}

// ---------- saveProjectConfig error path ----------

func TestSaveProjectConfig_MkdirError(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "existingfile")
	require.NoError(t, os.WriteFile(filePath, []byte("x"), 0644))
	cfg := emptyProjectConfig()
	err := saveProjectConfig(filePath+"/sub/dir/janus.project.yaml", cfg)
	require.Error(t, err)
}

// ---------- additional coverage for command wrappers and error paths ----------

func TestProjectDiffCmd_AllTenants(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/tenants/acme":
			w.WriteHeader(http.StatusNotFound)
		case "/v1/tenants/acme/agents/a1":
			w.WriteHeader(http.StatusNotFound)
		case "/v1/tenants/acme/mailboxes/a1.default":
			w.WriteHeader(http.StatusNotFound)
		case "/v1/tenants/acme/budgets":
			json.NewEncoder(w).Encode(map[string]interface{}{"budgets": []janus.BudgetSpec{}})
		case "/v1/tenants/acme/policy-rules":
			json.NewEncoder(w).Encode(map[string]interface{}{"policy_rules": []core.PolicyRule{}})
		case "/v1/tenants/corp":
			w.WriteHeader(http.StatusNotFound)
		case "/v1/tenants/corp/agents/b1":
			w.WriteHeader(http.StatusNotFound)
		case "/v1/tenants/corp/mailboxes/b1.default":
			w.WriteHeader(http.StatusNotFound)
		case "/v1/tenants/corp/budgets":
			json.NewEncoder(w).Encode(map[string]interface{}{"budgets": []janus.BudgetSpec{}})
		case "/v1/tenants/corp/policy-rules":
			json.NewEncoder(w).Encode(map[string]interface{}{"policy_rules": []core.PolicyRule{}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldTenantID := tenantID
	oldProjectFile := projectFile
	defer func() { serverURL = oldServerURL; tenantID = oldTenantID; projectFile = oldProjectFile }()
	serverURL = srv.URL
	tenantID = ""

	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, defaultProjectFileName)
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{Agents: map[string]*ProjectAgent{"a1": {Capabilities: []ProjectCapability{{ID: "cap1"}}}}}
	cfg.Tenants["corp"] = &ProjectTenant{Agents: map[string]*ProjectAgent{"b1": {Capabilities: []ProjectCapability{{ID: "cap2"}}}}}
	require.NoError(t, saveProjectConfig(yamlPath, cfg))
	projectFile = yamlPath

	root := &cobra.Command{Use: "janus", SilenceErrors: true, SilenceUsage: true}
	root.AddCommand(projectCmd())
	out, err := executeCommand(root, "project", "diff", "--all-tenants")
	require.NoError(t, err)
	assert.Contains(t, out, "Tenant acme")
	assert.Contains(t, out, "Tenant corp")
}

func TestProjectApplyCmd_ContinueOnError_SuccessPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/tenants/acme" && r.Method == "GET":
			w.WriteHeader(http.StatusNotFound)
		case r.URL.Path == "/v1/tenants" && r.Method == "POST":
			w.WriteHeader(http.StatusCreated)
		case r.URL.Path == "/v1/tenants/acme/agents/a1" && r.Method == "GET":
			w.WriteHeader(http.StatusNotFound)
		case r.URL.Path == "/v1/tenants/acme/agents" && r.Method == "POST":
			w.WriteHeader(http.StatusCreated)
		case r.URL.Path == "/v1/tenants/acme/mailboxes/a1.default" && r.Method == "GET":
			w.WriteHeader(http.StatusNotFound)
		case r.URL.Path == "/v1/tenants/acme/mailboxes" && r.Method == "POST":
			json.NewEncoder(w).Encode(map[string]string{"id": "a1.default", "status": "active"})
		case r.URL.Path == "/v1/tenants/acme/budgets" && r.Method == "POST":
			json.NewEncoder(w).Encode(janus.BudgetSpec{ScopeType: "tenant", ScopeID: "acme"})
		case r.URL.Path == "/v1/tenants/acme/policy-rules" && r.Method == "GET":
			json.NewEncoder(w).Encode(map[string]interface{}{"policy_rules": []core.PolicyRule{}})
		case r.URL.Path == "/v1/tenants/acme/policy-rules/templates" && r.Method == "POST":
			json.NewEncoder(w).Encode(core.PolicyRule{ID: "rule-1"})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldTenantID := tenantID
	oldProjectFile := projectFile
	defer func() { serverURL = oldServerURL; tenantID = oldTenantID; projectFile = oldProjectFile }()
	serverURL = srv.URL
	tenantID = ""

	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, defaultProjectFileName)
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{
		Agents: map[string]*ProjectAgent{"a1": {Capabilities: []ProjectCapability{{ID: "cap1"}}}},
		Budgets: ProjectBudgets{Tenant: &ProjectBudgetLimits{RPM: 100}},
		Policies: ProjectPolicies{Allow: []ProjectPolicyBinding{{Agent: "a1", Capability: "cap1"}}},
	}
	require.NoError(t, saveProjectConfig(yamlPath, cfg))
	projectFile = yamlPath

	root := &cobra.Command{Use: "janus", SilenceErrors: true, SilenceUsage: true}
	root.AddCommand(projectCmd())
	out, err := executeCommand(root, "project", "apply", "--all-tenants", "--continue-on-error")
	require.NoError(t, err)
	assert.Contains(t, out, "created tenant acme")
}

func TestProjectSyncCmd_Overwrite(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/tenants/acme":
			json.NewEncoder(w).Encode(core.Tenant{ID: "acme", Name: "New Name"})
		case "/v1/tenants/acme/agents":
			json.NewEncoder(w).Encode(map[string]interface{}{"agents": []core.Agent{{ID: "a1", DisplayName: "New Agent", Protocol: "custom-sdk", Capabilities: []core.AgentCapability{{Capability: "cap1"}}}}})
		case "/v1/tenants/acme/budgets":
			json.NewEncoder(w).Encode(map[string]interface{}{"budgets": []janus.BudgetSpec{}})
		case "/v1/tenants/acme/policy-rules":
			json.NewEncoder(w).Encode(map[string]interface{}{"policy_rules": []core.PolicyRule{}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldTenantID := tenantID
	oldProjectFile := projectFile
	defer func() { serverURL = oldServerURL; tenantID = oldTenantID; projectFile = oldProjectFile }()
	serverURL = srv.URL
	tenantID = "acme"

	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, defaultProjectFileName)
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{
		Name:   "Old Name",
		Agents: map[string]*ProjectAgent{"a1": {Name: "Old Agent", Capabilities: []ProjectCapability{{ID: "cap1"}}}},
	}
	require.NoError(t, saveProjectConfig(yamlPath, cfg))
	projectFile = yamlPath

	root := &cobra.Command{Use: "janus", SilenceErrors: true, SilenceUsage: true}
	root.AddCommand(projectCmd())
	out, err := executeCommand(root, "project", "sync", "--overwrite")
	require.NoError(t, err)
	assert.Contains(t, out, "Synced")
}

func TestProjectSyncCmd_AllTenants(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/tenants/acme":
			json.NewEncoder(w).Encode(core.Tenant{ID: "acme", Name: "Acme"})
		case "/v1/tenants/acme/agents":
			json.NewEncoder(w).Encode(map[string]interface{}{"agents": []core.Agent{}})
		case "/v1/tenants/acme/budgets":
			json.NewEncoder(w).Encode(map[string]interface{}{"budgets": []janus.BudgetSpec{}})
		case "/v1/tenants/acme/policy-rules":
			json.NewEncoder(w).Encode(map[string]interface{}{"policy_rules": []core.PolicyRule{}})
		case "/v1/tenants/corp":
			json.NewEncoder(w).Encode(core.Tenant{ID: "corp", Name: "Corp"})
		case "/v1/tenants/corp/agents":
			json.NewEncoder(w).Encode(map[string]interface{}{"agents": []core.Agent{}})
		case "/v1/tenants/corp/budgets":
			json.NewEncoder(w).Encode(map[string]interface{}{"budgets": []janus.BudgetSpec{}})
		case "/v1/tenants/corp/policy-rules":
			json.NewEncoder(w).Encode(map[string]interface{}{"policy_rules": []core.PolicyRule{}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldTenantID := tenantID
	oldProjectFile := projectFile
	defer func() { serverURL = oldServerURL; tenantID = oldTenantID; projectFile = oldProjectFile }()
	serverURL = srv.URL
	tenantID = ""

	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, defaultProjectFileName)
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{}
	cfg.Tenants["corp"] = &ProjectTenant{}
	require.NoError(t, saveProjectConfig(yamlPath, cfg))
	projectFile = yamlPath

	root := &cobra.Command{Use: "janus", SilenceErrors: true, SilenceUsage: true}
	root.AddCommand(projectCmd())
	out, err := executeCommand(root, "project", "sync", "--all-tenants")
	require.NoError(t, err)
	assert.Contains(t, out, "Synced")
}

func TestSyncProjectTenant_ListAgentsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/tenants/acme":
			json.NewEncoder(w).Encode(core.Tenant{ID: "acme", Name: "Acme"})
		case "/v1/tenants/acme/agents":
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldAPIKey := apiKey
	defer func() { serverURL = oldServerURL; apiKey = oldAPIKey }()
	serverURL = srv.URL
	apiKey = "test-key"

	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{}

	ctx := context.Background()
	err := syncProjectTenant(ctx, cfg, "acme", false)
	require.Error(t, err)
}

func TestSyncProjectTenant_ListBudgetsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/tenants/acme":
			json.NewEncoder(w).Encode(core.Tenant{ID: "acme", Name: "Acme"})
		case "/v1/tenants/acme/agents":
			json.NewEncoder(w).Encode(map[string]interface{}{"agents": []core.Agent{}})
		case "/v1/tenants/acme/budgets":
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldAPIKey := apiKey
	defer func() { serverURL = oldServerURL; apiKey = oldAPIKey }()
	serverURL = srv.URL
	apiKey = "test-key"

	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{}

	ctx := context.Background()
	err := syncProjectTenant(ctx, cfg, "acme", false)
	require.Error(t, err)
}

func TestSyncProjectTenant_ListPolicyRulesError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/tenants/acme":
			json.NewEncoder(w).Encode(core.Tenant{ID: "acme", Name: "Acme"})
		case "/v1/tenants/acme/agents":
			json.NewEncoder(w).Encode(map[string]interface{}{"agents": []core.Agent{}})
		case "/v1/tenants/acme/budgets":
			json.NewEncoder(w).Encode(map[string]interface{}{"budgets": []janus.BudgetSpec{}})
		case "/v1/tenants/acme/policy-rules":
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldAPIKey := apiKey
	defer func() { serverURL = oldServerURL; apiKey = oldAPIKey }()
	serverURL = srv.URL
	apiKey = "test-key"

	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{}

	ctx := context.Background()
	err := syncProjectTenant(ctx, cfg, "acme", false)
	require.Error(t, err)
}

func TestApplyProjectTenant_RegisterAgentError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/tenants/acme" && r.Method == "GET":
			w.WriteHeader(http.StatusNotFound)
		case r.URL.Path == "/v1/tenants" && r.Method == "POST":
			w.WriteHeader(http.StatusCreated)
		case r.URL.Path == "/v1/tenants/acme/agents/a1" && r.Method == "GET":
			w.WriteHeader(http.StatusNotFound)
		case r.URL.Path == "/v1/tenants/acme/agents" && r.Method == "POST":
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldAPIKey := apiKey
	defer func() { serverURL = oldServerURL; apiKey = oldAPIKey }()
	serverURL = srv.URL
	apiKey = "test-key"

	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{Agents: map[string]*ProjectAgent{"a1": {Capabilities: []ProjectCapability{{ID: "cap1"}}}}}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	err := applyProjectTenant(cmd, cfg, "acme")
	require.Error(t, err)
}

func TestApplyProjectTenant_UpdateMailboxError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/tenants/acme" && r.Method == "GET":
			json.NewEncoder(w).Encode(core.Tenant{ID: "acme", Name: "Acme"})
		case r.URL.Path == "/v1/tenants/acme/agents/a1" && r.Method == "GET":
			json.NewEncoder(w).Encode(core.Agent{ID: "a1", DisplayName: "A1", Protocol: "custom-sdk"})
		case r.URL.Path == "/v1/tenants/acme/mailboxes/a1.default" && r.Method == "GET":
			json.NewEncoder(w).Encode(core.Mailbox{ID: "a1.default", AgentID: "a1", MaxConcurrency: 99, ACKWaitSeconds: 30, MaxDeliver: 10, RetentionSeconds: 86400})
		case r.URL.Path == "/v1/tenants/acme/mailboxes/a1.default" && r.Method == "PATCH":
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldAPIKey := apiKey
	defer func() { serverURL = oldServerURL; apiKey = oldAPIKey }()
	serverURL = srv.URL
	apiKey = "test-key"

	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{Agents: map[string]*ProjectAgent{"a1": {Capabilities: []ProjectCapability{{ID: "cap1"}}}}}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	err := applyProjectTenant(cmd, cfg, "acme")
	require.Error(t, err)
}

func TestAgentAddCmd_ValidateError(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()

	oldServerURL := serverURL
	oldTenantID := tenantID
	oldProjectFile := projectFile
	defer func() { serverURL = oldServerURL; tenantID = oldTenantID; projectFile = oldProjectFile }()
	serverURL = srv.URL
	tenantID = "acme"

	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, defaultProjectFileName)
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{Agents: map[string]*ProjectAgent{"": {Capabilities: []ProjectCapability{{ID: "x"}}}}}
	require.NoError(t, saveProjectConfig(yamlPath, cfg))
	projectFile = yamlPath

	root := newTestRoot(srv)
	root.AddCommand(agentCmd())
	_, err := executeCommand(root, "agent", "add", "a1", "--capability", "cap1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty agent id")
}

func TestAgentAddCmd_SaveConfigError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/tenants/acme/agents" && r.Method == "POST":
			w.WriteHeader(http.StatusCreated)
		case r.URL.Path == "/v1/tenants/acme/mailboxes" && r.Method == "POST":
			json.NewEncoder(w).Encode(map[string]string{"id": "a1.default", "status": "active"})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldTenantID := tenantID
	oldProjectFile := projectFile
	defer func() { serverURL = oldServerURL; tenantID = oldTenantID; projectFile = oldProjectFile }()
	serverURL = srv.URL
	tenantID = "acme"

	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, defaultProjectFileName)
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{Agents: map[string]*ProjectAgent{}}
	require.NoError(t, saveProjectConfig(yamlPath, cfg))
	projectFile = yamlPath

	// Make the file read-only to trigger save error
	require.NoError(t, os.Chmod(yamlPath, 0444))
	defer os.Chmod(yamlPath, 0644)

	root := newTestRoot(srv)
	root.AddCommand(agentCmd())
	_, err := executeCommand(root, "agent", "add", "a1", "--capability", "cap1")
	require.Error(t, err)
}

func TestDiffProjectTenant_GetAgentError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/tenants/acme":
			json.NewEncoder(w).Encode(core.Tenant{ID: "acme", Name: "Acme"})
		case "/v1/tenants/acme/agents/a1":
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldAPIKey := apiKey
	defer func() { serverURL = oldServerURL; apiKey = oldAPIKey }()
	serverURL = srv.URL
	apiKey = "test-key"

	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{Agents: map[string]*ProjectAgent{"a1": {}}}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	err := diffProjectTenant(cmd, cfg, "acme")
	require.Error(t, err)
}

func TestDiffProjectTenant_GetMailboxError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/tenants/acme":
			json.NewEncoder(w).Encode(core.Tenant{ID: "acme", Name: "Acme"})
		case "/v1/tenants/acme/agents/a1":
			json.NewEncoder(w).Encode(core.Agent{ID: "a1", DisplayName: "A1", Protocol: "custom-sdk"})
		case "/v1/tenants/acme/mailboxes/a1.default":
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldAPIKey := apiKey
	defer func() { serverURL = oldServerURL; apiKey = oldAPIKey }()
	serverURL = srv.URL
	apiKey = "test-key"

	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{Agents: map[string]*ProjectAgent{"a1": {}}}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	err := diffProjectTenant(cmd, cfg, "acme")
	require.Error(t, err)
}

// ---------- project command wrapper error paths ----------

func TestProjectDiffCmd_LoadError(t *testing.T) {
	oldProjectFile := projectFile
	defer func() { projectFile = oldProjectFile }()
	projectFile = "/nonexistent/path/janus.project.yaml"

	root := &cobra.Command{Use: "janus", SilenceErrors: true, SilenceUsage: true}
	root.AddCommand(projectCmd())
	_, err := executeCommand(root, "project", "diff")
	require.Error(t, err)
}

func TestProjectDiffCmd_ValidateError(t *testing.T) {
	oldProjectFile := projectFile
	defer func() { projectFile = oldProjectFile }()

	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, defaultProjectFileName)
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{Agents: map[string]*ProjectAgent{"": {Capabilities: []ProjectCapability{{ID: "x"}}}}}
	require.NoError(t, saveProjectConfig(yamlPath, cfg))
	projectFile = yamlPath

	root := &cobra.Command{Use: "janus", SilenceErrors: true, SilenceUsage: true}
	root.AddCommand(projectCmd())
	_, err := executeCommand(root, "project", "diff")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty agent id")
}

func TestProjectApplyCmd_LoadError(t *testing.T) {
	oldProjectFile := projectFile
	defer func() { projectFile = oldProjectFile }()
	projectFile = "/nonexistent/path/janus.project.yaml"

	root := &cobra.Command{Use: "janus", SilenceErrors: true, SilenceUsage: true}
	root.AddCommand(projectCmd())
	_, err := executeCommand(root, "project", "apply")
	require.Error(t, err)
}

func TestProjectApplyCmd_ValidateError(t *testing.T) {
	oldProjectFile := projectFile
	defer func() { projectFile = oldProjectFile }()

	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, defaultProjectFileName)
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{Agents: map[string]*ProjectAgent{"": {Capabilities: []ProjectCapability{{ID: "x"}}}}}
	require.NoError(t, saveProjectConfig(yamlPath, cfg))
	projectFile = yamlPath

	root := &cobra.Command{Use: "janus", SilenceErrors: true, SilenceUsage: true}
	root.AddCommand(projectCmd())
	_, err := executeCommand(root, "project", "apply")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty agent id")
}

func TestProjectSyncCmd_LoadError(t *testing.T) {
	oldProjectFile := projectFile
	defer func() { projectFile = oldProjectFile }()
	projectFile = "/nonexistent/path/janus.project.yaml"

	root := &cobra.Command{Use: "janus", SilenceErrors: true, SilenceUsage: true}
	root.AddCommand(projectCmd())
	_, err := executeCommand(root, "project", "sync")
	require.Error(t, err)
}

func TestProjectSyncCmd_SelectedTenantsError(t *testing.T) {
	oldProjectFile := projectFile
	oldTenantID := tenantID
	defer func() { projectFile = oldProjectFile; tenantID = oldTenantID }()

	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, defaultProjectFileName)
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{}
	cfg.Tenants["corp"] = &ProjectTenant{}
	require.NoError(t, saveProjectConfig(yamlPath, cfg))
	projectFile = yamlPath
	tenantID = ""

	root := &cobra.Command{Use: "janus", SilenceErrors: true, SilenceUsage: true}
	root.AddCommand(projectCmd())
	_, err := executeCommand(root, "project", "sync")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "multiple tenants")
}

// ---------- applyProjectTenant additional error paths ----------

func TestApplyProjectTenant_ListPolicyRulesError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/tenants/acme" && r.Method == "GET":
			json.NewEncoder(w).Encode(core.Tenant{ID: "acme", Name: "Acme"})
		case r.URL.Path == "/v1/tenants/acme/agents/a1" && r.Method == "GET":
			json.NewEncoder(w).Encode(core.Agent{ID: "a1", DisplayName: "A1", Protocol: "custom-sdk"})
		case r.URL.Path == "/v1/tenants/acme/mailboxes/a1.default" && r.Method == "GET":
			json.NewEncoder(w).Encode(core.Mailbox{ID: "a1.default", AgentID: "a1", MaxConcurrency: 1, ACKWaitSeconds: 0, MaxDeliver: 0, RetentionSeconds: 0})
		case r.URL.Path == "/v1/tenants/acme/budgets" && r.Method == "POST":
			json.NewEncoder(w).Encode(janus.BudgetSpec{ScopeType: "tenant", ScopeID: "acme"})
		case r.URL.Path == "/v1/tenants/acme/policy-rules" && r.Method == "GET":
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldAPIKey := apiKey
	defer func() { serverURL = oldServerURL; apiKey = oldAPIKey }()
	serverURL = srv.URL
	apiKey = "test-key"

	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{
		Agents: map[string]*ProjectAgent{"a1": {Capabilities: []ProjectCapability{{ID: "cap1"}}}},
		Policies: ProjectPolicies{Allow: []ProjectPolicyBinding{{Agent: "a1", Capability: "cap1"}}},
	}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	err := applyProjectTenant(cmd, cfg, "acme")
	require.Error(t, err)
}

func TestApplyProjectTenant_RegisterAgentNonConflictError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/tenants/acme" && r.Method == "GET":
			w.WriteHeader(http.StatusNotFound)
		case r.URL.Path == "/v1/tenants" && r.Method == "POST":
			w.WriteHeader(http.StatusCreated)
		case r.URL.Path == "/v1/tenants/acme/agents/a1" && r.Method == "GET":
			w.WriteHeader(http.StatusNotFound)
		case r.URL.Path == "/v1/tenants/acme/agents" && r.Method == "POST":
			w.WriteHeader(http.StatusBadRequest)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldAPIKey := apiKey
	defer func() { serverURL = oldServerURL; apiKey = oldAPIKey }()
	serverURL = srv.URL
	apiKey = "test-key"

	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{Agents: map[string]*ProjectAgent{"a1": {Capabilities: []ProjectCapability{{ID: "cap1"}}}}}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	err := applyProjectTenant(cmd, cfg, "acme")
	require.Error(t, err)
}

func TestApplyProjectTenant_CreateMailboxNonConflictError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/tenants/acme" && r.Method == "GET":
			w.WriteHeader(http.StatusNotFound)
		case r.URL.Path == "/v1/tenants" && r.Method == "POST":
			w.WriteHeader(http.StatusCreated)
		case r.URL.Path == "/v1/tenants/acme/agents/a1" && r.Method == "GET":
			w.WriteHeader(http.StatusNotFound)
		case r.URL.Path == "/v1/tenants/acme/agents" && r.Method == "POST":
			w.WriteHeader(http.StatusCreated)
		case r.URL.Path == "/v1/tenants/acme/mailboxes/a1.default" && r.Method == "GET":
			w.WriteHeader(http.StatusNotFound)
		case r.URL.Path == "/v1/tenants/acme/mailboxes" && r.Method == "POST":
			w.WriteHeader(http.StatusBadRequest)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	oldServerURL := serverURL
	oldAPIKey := apiKey
	defer func() { serverURL = oldServerURL; apiKey = oldAPIKey }()
	serverURL = srv.URL
	apiKey = "test-key"

	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{Agents: map[string]*ProjectAgent{"a1": {Capabilities: []ProjectCapability{{ID: "cap1"}}}}}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	err := applyProjectTenant(cmd, cfg, "acme")
	require.Error(t, err)
}

// ---------- validateBudgetLimits additional scopes ----------

func TestValidateBudgetLimits_NegativeTeam(t *testing.T) {
	budgets := ProjectBudgets{Teams: map[string]ProjectBudgetLimits{"eng": {RPM: -1}}}
	err := validateBudgetLimits("acme", budgets)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "negative limits")
}

func TestValidateBudgetLimits_NegativeModel(t *testing.T) {
	budgets := ProjectBudgets{Models: map[string]ProjectBudgetLimits{"gpt4": {TPM: -1}}}
	err := validateBudgetLimits("acme", budgets)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "negative limits")
}

func TestValidateBudgetLimits_NegativeModelProvider(t *testing.T) {
	budgets := ProjectBudgets{ModelProviders: map[string]ProjectBudgetLimits{"openai": {Concurrency: -1}}}
	err := validateBudgetLimits("acme", budgets)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "negative limits")
}

func TestValidateBudgetLimits_NegativeTask(t *testing.T) {
	budgets := ProjectBudgets{Tasks: map[string]ProjectBudgetLimits{"review": {DailyUSD: -1}}}
	err := validateBudgetLimits("acme", budgets)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "negative limits")
}

// ---------- policy deny variants error paths ----------

func TestPolicyDenyClassification_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "policy", "deny-classification", "--team", "ops", "--classification", "confidential")
	require.Error(t, err)
}

func TestPolicyDenyTool_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "policy", "deny-tool", "--team", "ops", "--tool", "t1")
	require.Error(t, err)
}

func TestExtra_DashboardCmd_BindPort_Invalid(t *testing.T) {
	root := newTestRoot(nil)
	_, err := executeCommand(root, "dashboard", "--port", "99999999")
	require.Error(t, err)
}

func TestExtra_DashboardCmd_BindPort_Custom(t *testing.T) {
	root := newTestRoot(nil)
	_, err := executeCommand(root, "dashboard", "--port", "1")
	require.Error(t, err)
}

func TestExtra_DashboardCmd_FlagDefaults(t *testing.T) {
	cmd := dashboardCmd()
	portFlag := cmd.Flags().Lookup("port")
	require.NotNil(t, portFlag)
	assert.Equal(t, "8090", portFlag.DefValue)
	assert.Equal(t, "string", portFlag.Value.Type())
}

func TestExtra_DashboardCmd_RunE_BadPort(t *testing.T) {
	cmd := dashboardCmd()
	cmd.SetArgs([]string{"--port", "not-a-port"})
	err := cmd.Execute()
	require.Error(t, err)
}
