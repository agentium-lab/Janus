package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateProjectConfig_Valid(t *testing.T) {
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{
		Name: "Acme",
		Agents: map[string]*ProjectAgent{
			"agent-1": {
				Capabilities: []ProjectCapability{{ID: "code_review"}},
			},
		},
	}
	require.NoError(t, validateProjectConfig(cfg))
}

func TestValidateProjectConfig_Nil(t *testing.T) {
	err := validateProjectConfig(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "project config is required")
}

func TestValidateProjectConfig_BadVersion(t *testing.T) {
	cfg := emptyProjectConfig()
	cfg.Version = "v999"
	err := validateProjectConfig(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported project config version")
}

func TestValidateProjectConfig_DefaultTenantNotDeclared(t *testing.T) {
	cfg := emptyProjectConfig()
	cfg.DefaultTenant = "ghost"
	err := validateProjectConfig(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "default_tenant ghost is not declared")
}

func TestValidateProjectConfig_EmptyTenantID(t *testing.T) {
	cfg := emptyProjectConfig()
	cfg.Tenants["  "] = &ProjectTenant{}
	err := validateProjectConfig(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tenant id is required")
}

func TestValidateProjectConfig_NilAgentConfig(t *testing.T) {
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{
		Agents: map[string]*ProjectAgent{"agent-1": nil},
	}
	err := validateProjectConfig(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "config is required")
}

func TestValidateProjectConfig_AgentMissingCapability(t *testing.T) {
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{
		Agents: map[string]*ProjectAgent{"agent-1": {}},
	}
	err := validateProjectConfig(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires at least one capability")
}

func TestValidateProjectConfig_AgentEmptyCapabilityID(t *testing.T) {
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{
		Agents: map[string]*ProjectAgent{
			"agent-1": {Capabilities: []ProjectCapability{{ID: "  "}}},
		},
	}
	err := validateProjectConfig(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty capability")
}

func TestValidateProjectConfig_InvalidClassification(t *testing.T) {
	cfg := emptyProjectConfig()
	cfg.Tenants["acme"] = &ProjectTenant{
		Agents: map[string]*ProjectAgent{
			"agent-1": {Capabilities: []ProjectCapability{{ID: "x", DataClassifications: []string{"top_secret"}}}},
		},
	}
	err := validateProjectConfig(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid classification")
}

func TestValidateBudgetLimits_NegativeRPM(t *testing.T) {
	budgets := ProjectBudgets{
		Agents: map[string]ProjectBudgetLimits{"a1": {RPM: -1}},
	}
	err := validateBudgetLimits("acme", budgets)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "negative limits")
}

func TestValidateBudgetLimits_Valid(t *testing.T) {
	budgets := ProjectBudgets{
		Tenant: &ProjectBudgetLimits{RPM: 100, TPM: 1000, Concurrency: 5, DailyUSD: 1.5},
		Teams:  map[string]ProjectBudgetLimits{"team-a": {RPM: 50}},
	}
	require.NoError(t, validateBudgetLimits("acme", budgets))
}

func TestValidateBudgetLimits_AllScopes(t *testing.T) {
	budgets := ProjectBudgets{
		Agents:         map[string]ProjectBudgetLimits{"a1": {}},
		Models:         map[string]ProjectBudgetLimits{"gpt4": {}},
		ModelProviders: map[string]ProjectBudgetLimits{"openai": {}},
		Tasks:          map[string]ProjectBudgetLimits{"review": {}},
	}
	require.NoError(t, validateBudgetLimits("acme", budgets))
}

func TestValidProjectClassification(t *testing.T) {
	assert.True(t, validProjectClassification("public"))
	assert.True(t, validProjectClassification("internal"))
	assert.True(t, validProjectClassification("confidential"))
	assert.True(t, validProjectClassification("restricted"))
	assert.True(t, validProjectClassification(""))
	assert.True(t, validProjectClassification("  public  "))
	assert.False(t, validProjectClassification("top_secret"))
}

func TestEmptyProjectConfig(t *testing.T) {
	cfg := emptyProjectConfig()
	assert.Equal(t, "v1", cfg.Version)
	assert.NotNil(t, cfg.Tenants)
	assert.Empty(t, cfg.Tenants)
}

func TestProjectConfig_Normalize(t *testing.T) {
	cfg := &ProjectConfig{}
	cfg.normalize()
	assert.Equal(t, projectConfigVersion, cfg.Version)
	assert.NotNil(t, cfg.Tenants)

	cfg2 := &ProjectConfig{Tenants: map[string]*ProjectTenant{"x": nil}}
	cfg2.normalize()
	assert.NotNil(t, cfg2.Tenants["x"])
}

func TestAgentConcurrency(t *testing.T) {
	// Agent-level overrides default.
	agent := ProjectAgent{Concurrency: 5}
	defaults := ProjectDefaults{Capacity: ProjectCapacity{MaxConcurrency: 10}}
	assert.Equal(t, 5, agentConcurrency(agent, defaults))

	// Falls back to default when agent concurrency is 0.
	agent2 := ProjectAgent{}
	assert.Equal(t, 10, agentConcurrency(agent2, defaults))

	// Both zero → default of 1.
	agent3 := ProjectAgent{}
	defaults3 := ProjectDefaults{}
	assert.Equal(t, 1, agentConcurrency(agent3, defaults3))
}

func TestSortedTenantIDs(t *testing.T) {
	cfg := emptyProjectConfig()
	cfg.Tenants["zebra"] = &ProjectTenant{}
	cfg.Tenants["apple"] = &ProjectTenant{}
	cfg.Tenants["mango"] = &ProjectTenant{}
	ids := sortedTenantIDs(cfg)
	assert.Equal(t, []string{"apple", "mango", "zebra"}, ids)
}

func TestRegisterAgentRequest_AllFields(t *testing.T) {
	agent := ProjectAgent{
		Name: "Reviewer", Team: "team-a", Protocol: "a2a", Endpoint: "http://x",
		Description: "Code reviewer", Concurrency: 3,
		Capabilities: []ProjectCapability{
			{ID: "code_review", Description: "Reviews code", DataClassifications: []string{"internal"}},
		},
	}
	req, err := registerAgentRequest("agent-1", agent, ProjectDefaults{})
	require.NoError(t, err)
	assert.Equal(t, "agent-1", req.ID)
	assert.Equal(t, "Reviewer", req.DisplayName)
	assert.Equal(t, "team-a", req.TeamID)
	assert.Equal(t, "a2a", req.Protocol)
	assert.Equal(t, "http://x", req.Endpoint)
	assert.Equal(t, 3, req.MaxConcurrency)
	require.Len(t, req.Capabilities, 1)
	assert.Equal(t, "code_review", req.Capabilities[0].Capability)
	assert.Contains(t, req.Capabilities[0].Schema, "allowed_data_classifications")
}

func TestRegisterAgentRequest_DefaultsFromConfig(t *testing.T) {
	agent := ProjectAgent{
		Capabilities: []ProjectCapability{{ID: "x"}},
	}
	defaults := ProjectDefaults{Protocol: "acp", Capacity: ProjectCapacity{MaxConcurrency: 5}}
	req, err := registerAgentRequest("agent-1", agent, defaults)
	require.NoError(t, err)
	assert.Equal(t, "agent-1", req.DisplayName, "falls back to agentID when name empty")
	assert.Equal(t, "acp", req.Protocol, "falls back to defaults protocol")
	assert.Equal(t, 5, req.MaxConcurrency, "falls back to defaults capacity")
}

func TestRegisterAgentRequest_DefaultProtocolFallback(t *testing.T) {
	agent := ProjectAgent{Capabilities: []ProjectCapability{{ID: "x"}}}
	req, err := registerAgentRequest("a1", agent, ProjectDefaults{})
	require.NoError(t, err)
	assert.Equal(t, "custom-sdk", req.Protocol)
}

func TestCapabilitySchemaJSON_WithClassifications(t *testing.T) {
	schema, err := capabilitySchemaJSON(ProjectCapability{DataClassifications: []string{"public", "internal"}})
	require.NoError(t, err)
	assert.Contains(t, schema, "allowed_data_classifications")
	assert.Contains(t, schema, "public")
}

func TestCapabilitySchemaJSON_Empty(t *testing.T) {
	schema, err := capabilitySchemaJSON(ProjectCapability{})
	require.NoError(t, err)
	assert.Equal(t, "", schema)
}

func TestMailboxRequest_DefaultsFromAgent(t *testing.T) {
	agent := ProjectAgent{Concurrency: 5, Mailbox: &ProjectMailbox{ID: "custom-mb"}}
	req := mailboxRequest("agent-1", agent, ProjectDefaults{Mailbox: ProjectMailboxDefaults{ACKWaitSeconds: 60, MaxDeliver: 3}})
	assert.Equal(t, "custom-mb", req.ID)
	assert.Equal(t, 5, req.MaxConcurrency)
	assert.Equal(t, 60, req.ACKWaitSeconds)
	assert.Equal(t, 3, req.MaxDeliver)
}

func TestMailboxRequest_DefaultIdAndConcurrency(t *testing.T) {
	agent := ProjectAgent{}
	defaults := ProjectDefaults{Capacity: ProjectCapacity{MaxConcurrency: 10}, Mailbox: ProjectMailboxDefaults{ACKWaitSeconds: 300}}
	req := mailboxRequest("agent-1", agent, defaults)
	assert.Equal(t, "agent-1.default", req.ID)
	assert.Equal(t, 10, req.MaxConcurrency)
	assert.Equal(t, 300, req.ACKWaitSeconds)
}

func TestBudgetRequests_AllScopes(t *testing.T) {
	budgets := ProjectBudgets{
		Tenant:  &ProjectBudgetLimits{RPM: 100},
		Teams:   map[string]ProjectBudgetLimits{"team-a": {RPM: 50}},
		Agents:  map[string]ProjectBudgetLimits{"a1": {TPM: 1000}},
		Models:  map[string]ProjectBudgetLimits{"gpt4": {Concurrency: 3}},
		Tasks:   map[string]ProjectBudgetLimits{"review": {DailyUSD: 1.5}},
	}
	reqs := budgetRequests("acme", budgets)
	assert.Len(t, reqs, 5)
	// Tenant first.
	assert.Equal(t, "tenant", reqs[0].ScopeType)
	assert.Equal(t, "acme", reqs[0].ScopeID)
	assert.Equal(t, 100, reqs[0].RPM)
}

func TestBudgetRequests_Empty(t *testing.T) {
	reqs := budgetRequests("acme", ProjectBudgets{})
	assert.Empty(t, reqs)
}

func TestBudgetRequest(t *testing.T) {
	req := budgetRequest("agent", "a1", ProjectBudgetLimits{RPM: 60, TPM: 6000, Concurrency: 4, DailyUSD: 2.5, MonthlyUSD: 50})
	assert.Equal(t, "agent", req.ScopeType)
	assert.Equal(t, "a1", req.ScopeID)
	assert.Equal(t, 60, req.RPM)
	assert.Equal(t, 6000, req.TPM)
	assert.Equal(t, 4, req.MaxConcurrency)
	assert.Equal(t, 2.5, req.DailyCostUSD)
	assert.Equal(t, 50.0, req.MonthlyCostUSD)
}

func TestPolicyTemplateRequests_CapabilityAllow(t *testing.T) {
	policies := ProjectPolicies{
		Allow: []ProjectPolicyBinding{{Agent: "a1", Capability: "code_review"}},
	}
	reqs, err := policyTemplateRequests(policies, 100)
	require.NoError(t, err)
	require.NotEmpty(t, reqs)
	assert.Contains(t, reqs[0].Template, "allow")
	assert.Equal(t, "a1", reqs[0].AgentID)
	assert.Equal(t, "code_review", reqs[0].Capability)
	assert.Equal(t, 100, reqs[0].Priority)
}

func TestPolicyTemplateRequests_TeamDeny(t *testing.T) {
	policies := ProjectPolicies{
		Deny: []ProjectPolicyBinding{{Team: "ops", Capability: "deploy"}},
	}
	reqs, err := policyTemplateRequests(policies, 0)
	require.NoError(t, err)
	require.NotEmpty(t, reqs)
	assert.Contains(t, reqs[0].Template, "deny")
}

func TestPolicyTemplateRequests_ToolApproval(t *testing.T) {
	policies := ProjectPolicies{
		RequireApproval: ProjectApprovalPolicy{Tools: []string{"mcp.exec"}},
	}
	reqs, err := policyTemplateRequests(policies, 0)
	require.NoError(t, err)
	require.NotEmpty(t, reqs)
	assert.Contains(t, reqs[0].Template, "require_approval_tool")
}

func TestValidatePolicySubject(t *testing.T) {
	// Neither agent nor team → error.
	err := validatePolicySubject("", "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "required")

	// Both agent and team → error.
	err = validatePolicySubject("a1", "t1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mutually exclusive")

	// Only agent → ok.
	err = validatePolicySubject("a1", "")
	assert.NoError(t, err)

	// Only team → ok.
	err = validatePolicySubject("", "t1")
	assert.NoError(t, err)
}

func TestProjectConfigYAML_MarshalUnmarshal(t *testing.T) {
	// Test that a ProjectConfig with custom YAML tags can round-trip.
	original := &ProjectConfig{
		Version:       projectConfigVersion,
		DefaultTenant: "acme",
		Tenants: map[string]*ProjectTenant{
			"acme": {
				Name: "Acme Corp",
				Agents: map[string]*ProjectAgent{
					"reviewer": {
						Name:        "Code Reviewer",
						Team:        "eng",
						Protocol:    "a2a",
						Endpoint:    "http://reviewer:8080",
						Concurrency: 3,
						Capabilities: []ProjectCapability{
							{ID: "code_review", Description: "Reviews PRs"},
						},
					},
				},
			},
		},
	}

	original.normalize()
	err := validateProjectConfig(original)
	require.NoError(t, err, "config should be valid")
}

func TestProjectConfigYAML_DataClassificationPolicy(t *testing.T) {
	policies := ProjectPolicies{
		DataClassification: ProjectClassificationPolicy{
			Allow: []ProjectClassificationBinding{{Agent: "a1", Classification: "internal"}},
		},
	}
	reqs, err := policyTemplateRequests(policies, 0)
	require.NoError(t, err)
	require.NotEmpty(t, reqs)
	// Should produce an allow_agent_data_classification template.
	found := false
	for _, r := range reqs {
		if r.Template == "allow_agent_data_classification" {
			found = true
			break
		}
	}
	assert.True(t, found, "should produce allow_agent_data_classification template")
}

func TestProjectConfigYAML_DenyToolPolicy(t *testing.T) {
	policies := ProjectPolicies{
		Tools: ProjectToolPolicy{
			Deny: []ProjectToolBinding{{Agent: "a1", Tool: "rm -rf"}},
		},
	}
	reqs, err := policyTemplateRequests(policies, 0)
	require.NoError(t, err)
	require.NotEmpty(t, reqs)
	found := false
	for _, r := range reqs {
		if r.Template == "deny_agent_tool" {
			found = true
			break
		}
	}
	assert.True(t, found, "should produce deny_agent_tool template")
}
