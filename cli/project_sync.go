package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/agentium-lab/Janus/core"
	janus "github.com/agentium-lab/Janus/sdk/go"
	"github.com/spf13/cobra"
)

func mailboxRequest(agentID string, agent ProjectAgent, defaults ProjectDefaults) janus.CreateMailboxRequest {
	mailbox := ProjectMailbox{}
	if agent.Mailbox != nil {
		mailbox = *agent.Mailbox
	}
	id := strings.TrimSpace(mailbox.ID)
	if id == "" {
		id = agentID + ".default"
	}
	concurrency := mailbox.Concurrency
	if concurrency <= 0 {
		concurrency = agentConcurrency(agent, defaults)
	}
	ackWait := mailbox.ACKWaitSeconds
	if ackWait <= 0 {
		ackWait = defaults.Mailbox.ACKWaitSeconds
	}
	maxDeliver := mailbox.MaxDeliver
	if maxDeliver <= 0 {
		maxDeliver = defaults.Mailbox.MaxDeliver
	}
	retention := mailbox.RetentionSeconds
	if retention <= 0 {
		retention = defaults.Mailbox.RetentionSeconds
	}
	return janus.CreateMailboxRequest{
		ID:               id,
		AgentID:          agentID,
		MaxConcurrency:   concurrency,
		ACKWaitSeconds:   ackWait,
		MaxDeliver:       maxDeliver,
		RetentionSeconds: retention,
	}
}

func agentConcurrency(agent ProjectAgent, defaults ProjectDefaults) int {
	if agent.Concurrency > 0 {
		return agent.Concurrency
	}
	if agent.Capacity.MaxConcurrency > 0 {
		return agent.Capacity.MaxConcurrency
	}
	if defaults.Capacity.MaxConcurrency > 0 {
		return defaults.Capacity.MaxConcurrency
	}
	return 1
}

func budgetRequests(tenantID string, budgets ProjectBudgets) []janus.BudgetRequest {
	var out []janus.BudgetRequest
	if budgets.Tenant != nil {
		out = append(out, budgetRequest("tenant", tenantID, *budgets.Tenant))
	}
	appendMapBudgets := func(scope string, values map[string]ProjectBudgetLimits) {
		keys := make([]string, 0, len(values))
		for key := range values {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			out = append(out, budgetRequest(scope, key, values[key]))
		}
	}
	appendMapBudgets("team", budgets.Teams)
	appendMapBudgets("agent", budgets.Agents)
	appendMapBudgets("model_provider", budgets.ModelProviders)
	appendMapBudgets("model", budgets.Models)
	appendMapBudgets("task", budgets.Tasks)
	return out
}

func budgetRequest(scope, id string, limits ProjectBudgetLimits) janus.BudgetRequest {
	return janus.BudgetRequest{
		ScopeType:      scope,
		ScopeID:        id,
		RPM:            limits.RPM,
		TPM:            limits.TPM,
		MaxConcurrency: limits.Concurrency,
		DailyCostUSD:   limits.DailyUSD,
		MonthlyCostUSD: limits.MonthlyUSD,
	}
}

func policyTemplateRequests(policies ProjectPolicies, defaultPriority int) ([]core.PolicyRuleTemplateRequest, error) {
	var out []core.PolicyRuleTemplateRequest
	addPriority := func(req core.PolicyRuleTemplateRequest) core.PolicyRuleTemplateRequest {
		if req.Priority == 0 && defaultPriority > 0 {
			req.Priority = defaultPriority
		}
		return req
	}
	appendApproval := func(approval ProjectApprovalPolicy) {
		for _, capability := range cleanStringSlice(approval.Capabilities) {
			out = append(out, addPriority(core.PolicyRuleTemplateRequest{
				Template:   core.PolicyTemplateRequireApprovalCapability,
				Capability: capability,
			}))
		}
		for _, tool := range cleanStringSlice(approval.Tools) {
			out = append(out, addPriority(core.PolicyRuleTemplateRequest{
				Template: core.PolicyTemplateRequireApprovalTool,
				Tool:     tool,
			}))
		}
	}
	appendBindings := func(bindings []ProjectPolicyBinding, allow bool) error {
		for _, binding := range bindings {
			agent := strings.TrimSpace(binding.Agent)
			team := strings.TrimSpace(binding.Team)
			capability := strings.TrimSpace(binding.Capability)
			tool := strings.TrimSpace(binding.Tool)
			if agent == "" && team == "" {
				return fmt.Errorf("policy binding requires agent or team")
			}
			if agent != "" && team != "" {
				return fmt.Errorf("policy binding cannot set both agent and team")
			}
			if capability == "" && tool == "" {
				return fmt.Errorf("policy binding requires capability or tool")
			}
			if capability != "" && tool != "" {
				return fmt.Errorf("policy binding cannot set both capability and tool")
			}
			req := core.PolicyRuleTemplateRequest{AgentID: agent, TeamID: team, Capability: capability, Tool: tool}
			switch {
			case capability != "" && agent != "" && allow:
				req.Template = core.PolicyTemplateAllowAgentCapability
			case capability != "" && agent != "":
				req.Template = core.PolicyTemplateDenyAgentCapability
			case capability != "" && allow:
				req.Template = core.PolicyTemplateAllowTeamCapability
			case capability != "":
				req.Template = core.PolicyTemplateDenyTeamCapability
			case tool != "" && agent != "" && allow:
				req.Template = core.PolicyTemplateAllowAgentTool
			case tool != "" && agent != "":
				req.Template = core.PolicyTemplateDenyAgentTool
			case tool != "" && allow:
				req.Template = core.PolicyTemplateAllowTeamTool
			default:
				req.Template = core.PolicyTemplateDenyTeamTool
			}
			out = append(out, addPriority(req))
		}
		return nil
	}
	appendClassifications := func(bindings []ProjectClassificationBinding, allow bool) error {
		for _, binding := range bindings {
			agent := strings.TrimSpace(binding.Agent)
			team := strings.TrimSpace(binding.Team)
			if agent == "" && team == "" {
				return fmt.Errorf("classification policy requires agent or team")
			}
			if agent != "" && team != "" {
				return fmt.Errorf("classification policy cannot set both agent and team")
			}
			classifications := cleanStringSlice(binding.Classifications)
			if binding.Classification != "" {
				classifications = appendUniqueString(classifications, binding.Classification)
			}
			if len(classifications) == 0 {
				return fmt.Errorf("classification policy requires classification")
			}
			for _, classification := range classifications {
				if !validProjectClassification(classification) {
					return fmt.Errorf("invalid classification %s", classification)
				}
				req := core.PolicyRuleTemplateRequest{
					AgentID:            agent,
					TeamID:             team,
					DataClassification: classification,
				}
				switch {
				case agent != "" && allow:
					req.Template = core.PolicyTemplateAllowAgentDataClassification
				case agent != "":
					req.Template = core.PolicyTemplateDenyAgentDataClassification
				case allow:
					req.Template = core.PolicyTemplateAllowTeamDataClassification
				default:
					req.Template = core.PolicyTemplateDenyTeamDataClassification
				}
				out = append(out, addPriority(req))
			}
		}
		return nil
	}
	appendTools := func(bindings []ProjectToolBinding, allow bool) error {
		converted := make([]ProjectPolicyBinding, 0, len(bindings))
		for _, binding := range bindings {
			converted = append(converted, ProjectPolicyBinding{Agent: binding.Agent, Team: binding.Team, Tool: binding.Tool})
		}
		return appendBindings(converted, allow)
	}
	appendApproval(policies.Approve)
	appendApproval(policies.RequireApproval)
	if err := appendBindings(policies.Allow, true); err != nil {
		return nil, err
	}
	if err := appendBindings(policies.Deny, false); err != nil {
		return nil, err
	}
	if err := appendClassifications(policies.DataClassification.Allow, true); err != nil {
		return nil, err
	}
	if err := appendClassifications(policies.DataClassification.Deny, false); err != nil {
		return nil, err
	}
	if err := appendTools(policies.Tools.Allow, true); err != nil {
		return nil, err
	}
	if err := appendTools(policies.Tools.Deny, false); err != nil {
		return nil, err
	}
	sort.SliceStable(out, func(i, j int) bool {
		left, _ := out[i].BuildPolicyRule("sort")
		right, _ := out[j].BuildPolicyRule("sort")
		return left.ID < right.ID
	})
	return out, nil
}

func diffProjectTenant(cmd *cobra.Command, cfg *ProjectConfig, tenantID string) error {
	tenant := cfg.Tenants[tenantID]
	c := projectClient(tenantID)
	ctx := cmd.Context()
	fmt.Fprintf(cmd.OutOrStdout(), "Tenant %s\n", tenantID)
	tenantExists := true
	if _, err := c.GetTenant(ctx, tenantID); err != nil {
		if isAPIStatus(err, http.StatusNotFound) {
			tenantExists = false
			fmt.Fprintf(cmd.OutOrStdout(), "  + tenant %s\n", tenantID)
		} else {
			return err
		}
	} else {
		fmt.Fprintf(cmd.OutOrStdout(), "  = tenant %s\n", tenantID)
	}
	for _, agentID := range sortedAgentIDs(tenant) {
		agent := *tenant.Agents[agentID]
		if !tenantExists {
			fmt.Fprintf(cmd.OutOrStdout(), "  + agent %s\n", agentID)
			fmt.Fprintf(cmd.OutOrStdout(), "  + mailbox %s\n", mailboxRequest(agentID, agent, cfg.Defaults).ID)
			continue
		}
		if _, err := c.GetAgent(ctx, agentID); err != nil {
			if isAPIStatus(err, http.StatusNotFound) {
				fmt.Fprintf(cmd.OutOrStdout(), "  + agent %s\n", agentID)
			} else {
				return err
			}
		} else {
			fmt.Fprintf(cmd.OutOrStdout(), "  = agent %s\n", agentID)
		}
		mb := mailboxRequest(agentID, agent, cfg.Defaults)
		if existing, err := c.GetMailbox(ctx, mb.ID); err != nil {
			if isAPIStatus(err, http.StatusNotFound) {
				fmt.Fprintf(cmd.OutOrStdout(), "  + mailbox %s\n", mb.ID)
			} else {
				return err
			}
		} else if existing.MaxConcurrency != mb.MaxConcurrency ||
			existing.ACKWaitSeconds != mb.ACKWaitSeconds ||
			existing.MaxDeliver != mb.MaxDeliver ||
			existing.RetentionSeconds != mb.RetentionSeconds {
			fmt.Fprintf(cmd.OutOrStdout(), "  ~ mailbox %s\n", mb.ID)
		} else {
			fmt.Fprintf(cmd.OutOrStdout(), "  = mailbox %s\n", mb.ID)
		}
	}
	existingBudgets := map[string]struct{}{}
	if tenantExists {
		budgets, err := c.ListBudgets(ctx)
		if err != nil {
			return err
		}
		for _, budget := range budgets {
			existingBudgets[budget.ScopeType+"/"+budget.ScopeID] = struct{}{}
		}
	}
	for _, req := range budgetRequests(tenantID, tenant.Budgets) {
		key := req.ScopeType + "/" + req.ScopeID
		if _, ok := existingBudgets[key]; ok {
			fmt.Fprintf(cmd.OutOrStdout(), "  = budget %s\n", key)
		} else {
			fmt.Fprintf(cmd.OutOrStdout(), "  + budget %s\n", key)
		}
	}
	existingPolicies := map[string]struct{}{}
	if tenantExists {
		rules, err := c.ListPolicyRules(ctx)
		if err != nil {
			return err
		}
		for _, rule := range rules {
			existingPolicies[rule.ID] = struct{}{}
		}
	}
	templates, err := policyTemplateRequests(tenant.Policies, cfg.Defaults.Policy.Priority)
	if err != nil {
		return err
	}
	for _, template := range templates {
		rule, err := template.BuildPolicyRule(tenantID)
		if err != nil {
			return err
		}
		if _, ok := existingPolicies[rule.ID]; ok {
			fmt.Fprintf(cmd.OutOrStdout(), "  = policy %s\n", rule.ID)
		} else {
			fmt.Fprintf(cmd.OutOrStdout(), "  + policy %s\n", rule.ID)
		}
	}
	return nil
}

func applyProjectTenant(cmd *cobra.Command, cfg *ProjectConfig, tenantID string) error {
	tenant := cfg.Tenants[tenantID]
	c := projectClient(tenantID)
	ctx := cmd.Context()
	fmt.Fprintf(cmd.OutOrStdout(), "Applying tenant %s\n", tenantID)
	if _, err := c.GetTenant(ctx, tenantID); err != nil {
		if !isAPIStatus(err, http.StatusNotFound) {
			return err
		}
		name := tenant.Name
		if name == "" {
			name = tenantID
		}
		if err := c.CreateTenant(ctx, tenantID, name); err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "  created tenant %s\n", tenantID)
	}
	for _, agentID := range sortedAgentIDs(tenant) {
		agent := *tenant.Agents[agentID]
		if _, err := c.GetAgent(ctx, agentID); err != nil {
			if !isAPIStatus(err, http.StatusNotFound) {
				return err
			}
			req, err := registerAgentRequest(agentID, agent, cfg.Defaults)
			if err != nil {
				return err
			}
			if err := c.RegisterAgent(ctx, req); err != nil && !isAPIStatus(err, http.StatusConflict) {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "  created agent %s\n", agentID)
		} else {
			fmt.Fprintf(cmd.OutOrStdout(), "  existing agent %s\n", agentID)
		}
		mb := mailboxRequest(agentID, agent, cfg.Defaults)
		if _, err := c.GetMailbox(ctx, mb.ID); err != nil {
			if !isAPIStatus(err, http.StatusNotFound) {
				return err
			}
			if _, err := c.CreateMailboxWithConfig(ctx, mb); err != nil && !isAPIStatus(err, http.StatusConflict) {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "  created mailbox %s\n", mb.ID)
		} else {
			update := janus.UpdateMailboxRequest{
				MaxConcurrency:   intPtr(mb.MaxConcurrency),
				ACKWaitSeconds:   intPtr(mb.ACKWaitSeconds),
				MaxDeliver:       intPtr(mb.MaxDeliver),
				RetentionSeconds: intPtr(mb.RetentionSeconds),
			}
			if _, err := c.UpdateMailbox(ctx, mb.ID, update); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "  updated mailbox %s\n", mb.ID)
		}
	}
	for _, req := range budgetRequests(tenantID, tenant.Budgets) {
		if _, err := c.UpsertBudget(ctx, req); err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "  upserted budget %s/%s\n", req.ScopeType, req.ScopeID)
	}
	existingPolicies := map[string]struct{}{}
	rules, err := c.ListPolicyRules(ctx)
	if err != nil {
		return err
	}
	for _, rule := range rules {
		existingPolicies[rule.ID] = struct{}{}
	}
	templates, err := policyTemplateRequests(tenant.Policies, cfg.Defaults.Policy.Priority)
	if err != nil {
		return err
	}
	for _, template := range templates {
		rule, err := template.BuildPolicyRule(tenantID)
		if err != nil {
			return err
		}
		if _, ok := existingPolicies[rule.ID]; ok {
			fmt.Fprintf(cmd.OutOrStdout(), "  existing policy %s\n", rule.ID)
			continue
		}
		if _, err := c.CreatePolicyRuleFromTemplate(ctx, template); err != nil && !isAPIStatus(err, http.StatusConflict) {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "  created policy %s\n", rule.ID)
	}
	return nil
}

func syncProjectTenant(ctx context.Context, cfg *ProjectConfig, tenantID string, overwrite bool) error {
	c := projectClient(tenantID)
	remoteTenant, err := c.GetTenant(ctx, tenantID)
	if err != nil {
		return err
	}
	cfg.normalize()
	pt := cfg.Tenants[tenantID]
	if pt == nil {
		pt = &ProjectTenant{}
		cfg.Tenants[tenantID] = pt
	}
	if pt.Name == "" || overwrite {
		pt.Name = remoteTenant.Name
	}
	if cfg.DefaultTenant == "" {
		cfg.DefaultTenant = tenantID
	}
	agents, err := c.ListAgents(ctx)
	if err != nil {
		return err
	}
	if pt.Agents == nil {
		pt.Agents = make(map[string]*ProjectAgent)
	}
	for _, agent := range agents {
		if _, ok := pt.Agents[agent.ID]; ok && !overwrite {
			continue
		}
		pt.Agents[agent.ID] = projectAgentFromCore(agent)
	}
	budgets, err := c.ListBudgets(ctx)
	if err != nil {
		return err
	}
	mergeBudgetsIntoProject(&pt.Budgets, budgets, overwrite)
	rules, err := c.ListPolicyRules(ctx)
	if err != nil {
		return err
	}
	mergePolicyRulesIntoProject(&pt.Policies, rules, overwrite)
	return nil
}

func projectAgentFromCore(agent core.Agent) *ProjectAgent {
	out := &ProjectAgent{
		Name:        agent.DisplayName,
		Team:        agent.TeamID,
		Protocol:    string(agent.Protocol),
		Endpoint:    agent.Endpoint,
		Description: agent.Description,
		Concurrency: agent.MaxConcurrency,
		RPM:         agent.RPM,
		TPM:         agent.TPM,
	}
	for _, capability := range agent.Capabilities {
		out.Capabilities = append(out.Capabilities, projectCapabilityFromCore(capability))
	}
	return out
}

func projectCapabilityFromCore(capability core.AgentCapability) ProjectCapability {
	out := ProjectCapability{ID: capability.Capability, Description: capability.Description}
	var schema map[string][]string
	if capability.Schema != "" && json.Unmarshal([]byte(capability.Schema), &schema) == nil {
		for _, key := range []string{"allowed_data_classifications", "data_classifications"} {
			out.DataClassifications = append(out.DataClassifications, schema[key]...)
		}
		out.DataClassifications = cleanStringSlice(out.DataClassifications)
	}
	return out
}

func mergeBudgetsIntoProject(target *ProjectBudgets, budgets []janus.BudgetSpec, overwrite bool) {
	for _, budget := range budgets {
		limits := ProjectBudgetLimits{
			RPM:         budget.RPM,
			TPM:         budget.TPM,
			Concurrency: budget.MaxConcurrency,
			DailyUSD:    budget.DailyCostUSD,
			MonthlyUSD:  budget.MonthlyCostUSD,
		}
		switch budget.ScopeType {
		case "tenant":
			if target.Tenant == nil || overwrite {
				target.Tenant = &limits
			}
		case "team":
			if target.Teams == nil {
				target.Teams = make(map[string]ProjectBudgetLimits)
			}
			if _, ok := target.Teams[budget.ScopeID]; !ok || overwrite {
				target.Teams[budget.ScopeID] = limits
			}
		case "agent":
			if target.Agents == nil {
				target.Agents = make(map[string]ProjectBudgetLimits)
			}
			if _, ok := target.Agents[budget.ScopeID]; !ok || overwrite {
				target.Agents[budget.ScopeID] = limits
			}
		case "model_provider":
			if target.ModelProviders == nil {
				target.ModelProviders = make(map[string]ProjectBudgetLimits)
			}
			if _, ok := target.ModelProviders[budget.ScopeID]; !ok || overwrite {
				target.ModelProviders[budget.ScopeID] = limits
			}
		case "model":
			if target.Models == nil {
				target.Models = make(map[string]ProjectBudgetLimits)
			}
			if _, ok := target.Models[budget.ScopeID]; !ok || overwrite {
				target.Models[budget.ScopeID] = limits
			}
		case "task":
			if target.Tasks == nil {
				target.Tasks = make(map[string]ProjectBudgetLimits)
			}
			if _, ok := target.Tasks[budget.ScopeID]; !ok || overwrite {
				target.Tasks[budget.ScopeID] = limits
			}
		}
	}
}

func mergePolicyRulesIntoProject(target *ProjectPolicies, rules []core.PolicyRule, overwrite bool) {
	if overwrite {
		*target = ProjectPolicies{}
	}
	for _, rule := range rules {
		mergePolicyRuleIntoProject(target, rule)
	}
}

func mergePolicyRuleIntoProject(target *ProjectPolicies, rule core.PolicyRule) {
	condition := map[string]interface{}{}
	action := map[string]interface{}{}
	if json.Unmarshal(rule.Condition, &condition) != nil || json.Unmarshal(rule.Action, &action) != nil {
		return
	}
	decision := stringValue(action["decision"])
	actionName := stringValue(condition["action"])
	resourceType := stringValue(condition["resource.type"])
	resourceValue := stringValue(condition["resource.value"])
	agentID := stringValue(condition["actor.id"])
	teamID := stringValue(condition["actor.team_id"])
	switch decision {
	case string(core.PolicyDecisionApprovalRequired):
		if actionName == "task.publish" && resourceType == "capability" {
			target.Approve.Capabilities = appendUniqueString(target.Approve.Capabilities, resourceValue)
		}
		if actionName == "tool.invoke" && resourceType == "tool" {
			target.Approve.Tools = appendUniqueString(target.Approve.Tools, resourceValue)
		}
	case string(core.PolicyDecisionAllow), string(core.PolicyDecisionDeny):
		allow := decision == string(core.PolicyDecisionAllow)
		if actionName == "task.publish" && resourceType == "capability" {
			binding := ProjectPolicyBinding{Agent: agentID, Team: teamID, Capability: resourceValue}
			if allow {
				target.Allow = appendUniquePolicyBinding(target.Allow, binding)
			} else {
				target.Deny = appendUniquePolicyBinding(target.Deny, binding)
			}
			return
		}
		if actionName == "tool.invoke" && resourceType == "tool" {
			binding := ProjectToolBinding{Agent: agentID, Team: teamID, Tool: resourceValue}
			if allow {
				target.Tools.Allow = appendUniqueToolBinding(target.Tools.Allow, binding)
			} else {
				target.Tools.Deny = appendUniqueToolBinding(target.Tools.Deny, binding)
			}
			return
		}
		if actionName == "task.route" {
			binding := ProjectClassificationBinding{
				Agent:          stringValue(condition["context.target_agent_id"]),
				Team:           stringValue(condition["context.target_team_id"]),
				Classification: stringValue(condition["context.data_classification"]),
			}
			if allow {
				target.DataClassification.Allow = appendUniqueClassificationBinding(target.DataClassification.Allow, binding)
			} else {
				target.DataClassification.Deny = appendUniqueClassificationBinding(target.DataClassification.Deny, binding)
			}
		}
	}
}

func sortedAgentIDs(tenant *ProjectTenant) []string {
	names := make([]string, 0, len(tenant.Agents))
	for agent := range tenant.Agents {
		names = append(names, agent)
	}
	sort.Strings(names)
	return names
}

func intPtr(v int) *int {
	if v <= 0 {
		return nil
	}
	return &v
}

func cleanStringSlice(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{})
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func appendUniqueString(values []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return values
	}
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func appendUniquePolicyBinding(values []ProjectPolicyBinding, value ProjectPolicyBinding) []ProjectPolicyBinding {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func appendUniqueToolBinding(values []ProjectToolBinding, value ProjectToolBinding) []ProjectToolBinding {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func appendUniqueClassificationBinding(values []ProjectClassificationBinding, value ProjectClassificationBinding) []ProjectClassificationBinding {
	for _, existing := range values {
		if existing.Agent == value.Agent &&
			existing.Team == value.Team &&
			existing.Classification == value.Classification &&
			strings.Join(cleanStringSlice(existing.Classifications), "\x00") == strings.Join(cleanStringSlice(value.Classifications), "\x00") {
			return values
		}
	}
	return append(values, value)
}

func stringValue(value interface{}) string {
	if value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return v
	default:
		return fmt.Sprintf("%v", v)
	}
}
