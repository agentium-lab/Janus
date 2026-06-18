package core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	PolicyTemplateAllowAgentCapability         = "allow_agent_capability"
	PolicyTemplateDenyAgentCapability          = "deny_agent_capability"
	PolicyTemplateAllowTeamCapability          = "allow_team_capability"
	PolicyTemplateDenyTeamCapability           = "deny_team_capability"
	PolicyTemplateRequireApprovalCapability    = "require_approval_capability"
	PolicyTemplateRequireApprovalTool          = "require_approval_tool"
	PolicyTemplateAllowAgentDataClassification = "allow_agent_data_classification"
	PolicyTemplateDenyAgentDataClassification  = "deny_agent_data_classification"
	PolicyTemplateAllowTeamDataClassification  = "allow_team_data_classification"
	PolicyTemplateDenyTeamDataClassification   = "deny_team_data_classification"
	PolicyTemplateAllowAgentTool               = "allow_agent_tool"
	PolicyTemplateDenyAgentTool                = "deny_agent_tool"
	PolicyTemplateAllowTeamTool                = "allow_team_tool"
	PolicyTemplateDenyTeamTool                 = "deny_team_tool"
	defaultPolicyTemplatePriority              = 100
	defaultPolicyTemplateStatus                = "active"
)

// PolicyRuleTemplateRequest is the simplified governance entry point used by
// API, SDKs, and CLI. It always compiles into a standard PolicyRule.
type PolicyRuleTemplateRequest struct {
	Template           string `json:"template"`
	AgentID            string `json:"agent_id,omitempty"`
	TeamID             string `json:"team_id,omitempty"`
	Capability         string `json:"capability,omitempty"`
	Tool               string `json:"tool,omitempty"`
	DataClassification string `json:"data_classification,omitempty"`
	Name               string `json:"name,omitempty"`
	Status             string `json:"status,omitempty"`
	Priority           int    `json:"priority,omitempty"`
}

// BuildPolicyRule compiles a simplified template request into the canonical
// tenant-scoped policy_rules model consumed by PolicyService.Evaluate.
func (r PolicyRuleTemplateRequest) BuildPolicyRule(tenantID string) (PolicyRule, error) {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return PolicyRule{}, fmt.Errorf("tenant id is required")
	}

	req := r.normalized()
	condition, action, name, err := req.compile()
	if err != nil {
		return PolicyRule{}, err
	}

	status := strings.TrimSpace(req.Status)
	if status == "" {
		status = defaultPolicyTemplateStatus
	}
	priority := req.Priority
	if priority == 0 {
		priority = defaultPolicyTemplatePriority
	}
	if strings.TrimSpace(req.Name) != "" {
		name = strings.TrimSpace(req.Name)
	}

	conditionJSON, err := json.Marshal(condition)
	if err != nil {
		return PolicyRule{}, fmt.Errorf("marshal policy condition: %w", err)
	}
	actionJSON, err := json.Marshal(action)
	if err != nil {
		return PolicyRule{}, fmt.Errorf("marshal policy action: %w", err)
	}

	return PolicyRule{
		TenantID:  tenantID,
		ID:        req.stableID(),
		Name:      name,
		Status:    status,
		Priority:  priority,
		Condition: json.RawMessage(conditionJSON),
		Action:    json.RawMessage(actionJSON),
	}, nil
}

func (r PolicyRuleTemplateRequest) normalized() PolicyRuleTemplateRequest {
	r.Template = normalizePolicyTemplate(r.Template)
	r.AgentID = strings.TrimSpace(r.AgentID)
	r.TeamID = strings.TrimSpace(r.TeamID)
	r.Capability = strings.TrimSpace(r.Capability)
	r.Tool = strings.TrimSpace(r.Tool)
	r.DataClassification = strings.TrimSpace(r.DataClassification)
	r.Name = strings.TrimSpace(r.Name)
	r.Status = strings.TrimSpace(r.Status)
	return r
}

func (r PolicyRuleTemplateRequest) compile() (map[string]string, map[string]string, string, error) {
	switch r.Template {
	case PolicyTemplateAllowAgentCapability:
		return r.agentCapability(PolicyDecisionAllow)
	case PolicyTemplateDenyAgentCapability:
		return r.agentCapability(PolicyDecisionDeny)
	case PolicyTemplateAllowTeamCapability:
		return r.teamCapability(PolicyDecisionAllow)
	case PolicyTemplateDenyTeamCapability:
		return r.teamCapability(PolicyDecisionDeny)
	case PolicyTemplateRequireApprovalCapability:
		return r.requireApprovalCapability()
	case PolicyTemplateRequireApprovalTool:
		return r.requireApprovalTool()
	case PolicyTemplateAllowAgentDataClassification:
		return r.agentDataClassification(PolicyDecisionAllow)
	case PolicyTemplateDenyAgentDataClassification:
		return r.agentDataClassification(PolicyDecisionDeny)
	case PolicyTemplateAllowTeamDataClassification:
		return r.teamDataClassification(PolicyDecisionAllow)
	case PolicyTemplateDenyTeamDataClassification:
		return r.teamDataClassification(PolicyDecisionDeny)
	case PolicyTemplateAllowAgentTool:
		return r.agentTool(PolicyDecisionAllow)
	case PolicyTemplateDenyAgentTool:
		return r.agentTool(PolicyDecisionDeny)
	case PolicyTemplateAllowTeamTool:
		return r.teamTool(PolicyDecisionAllow)
	case PolicyTemplateDenyTeamTool:
		return r.teamTool(PolicyDecisionDeny)
	default:
		return nil, nil, "", fmt.Errorf("unsupported policy template %q", r.Template)
	}
}

func (r PolicyRuleTemplateRequest) agentCapability(decision PolicyDecisionType) (map[string]string, map[string]string, string, error) {
	if r.AgentID == "" || r.Capability == "" {
		return nil, nil, "", fmt.Errorf("agent_id and capability are required")
	}
	return map[string]string{
		"actor.type":     "agent",
		"actor.id":       r.AgentID,
		"action":         "task.publish",
		"resource.type":  "capability",
		"resource.value": r.Capability,
	}, decisionAction(decision), fmt.Sprintf("%s %s to %s", titleDecision(decision), r.AgentID, r.Capability), nil
}

func (r PolicyRuleTemplateRequest) teamCapability(decision PolicyDecisionType) (map[string]string, map[string]string, string, error) {
	if r.TeamID == "" || r.Capability == "" {
		return nil, nil, "", fmt.Errorf("team_id and capability are required")
	}
	return map[string]string{
		"actor.type":     "agent",
		"actor.team_id":  r.TeamID,
		"action":         "task.publish",
		"resource.type":  "capability",
		"resource.value": r.Capability,
	}, decisionAction(decision), fmt.Sprintf("%s team %s to %s", titleDecision(decision), r.TeamID, r.Capability), nil
}

func (r PolicyRuleTemplateRequest) requireApprovalCapability() (map[string]string, map[string]string, string, error) {
	if r.Capability == "" {
		return nil, nil, "", fmt.Errorf("capability is required")
	}
	return map[string]string{
		"action":         "task.publish",
		"resource.type":  "capability",
		"resource.value": r.Capability,
	}, decisionAction(PolicyDecisionApprovalRequired), fmt.Sprintf("Require approval for %s", r.Capability), nil
}

func (r PolicyRuleTemplateRequest) requireApprovalTool() (map[string]string, map[string]string, string, error) {
	if r.Tool == "" {
		return nil, nil, "", fmt.Errorf("tool is required")
	}
	return map[string]string{
		"action":         "tool.invoke",
		"resource.type":  "tool",
		"resource.value": r.Tool,
	}, decisionAction(PolicyDecisionApprovalRequired), fmt.Sprintf("Require approval for tool %s", r.Tool), nil
}

func (r PolicyRuleTemplateRequest) agentDataClassification(decision PolicyDecisionType) (map[string]string, map[string]string, string, error) {
	if r.AgentID == "" || r.DataClassification == "" {
		return nil, nil, "", fmt.Errorf("agent_id and data_classification are required")
	}
	return map[string]string{
		"action":                      "task.route",
		"resource.type":               "agent",
		"resource.value":              r.AgentID,
		"context.target_agent_id":     r.AgentID,
		"context.data_classification": r.DataClassification,
	}, decisionAction(decision), fmt.Sprintf("%s %s data for agent %s", titleDecision(decision), r.DataClassification, r.AgentID), nil
}

func (r PolicyRuleTemplateRequest) teamDataClassification(decision PolicyDecisionType) (map[string]string, map[string]string, string, error) {
	if r.TeamID == "" || r.DataClassification == "" {
		return nil, nil, "", fmt.Errorf("team_id and data_classification are required")
	}
	return map[string]string{
		"action":                      "task.route",
		"resource.type":               "agent",
		"context.target_team_id":      r.TeamID,
		"context.data_classification": r.DataClassification,
	}, decisionAction(decision), fmt.Sprintf("%s %s data for team %s", titleDecision(decision), r.DataClassification, r.TeamID), nil
}

func (r PolicyRuleTemplateRequest) agentTool(decision PolicyDecisionType) (map[string]string, map[string]string, string, error) {
	if r.AgentID == "" || r.Tool == "" {
		return nil, nil, "", fmt.Errorf("agent_id and tool are required")
	}
	return map[string]string{
		"actor.type":     "agent",
		"actor.id":       r.AgentID,
		"action":         "tool.invoke",
		"resource.type":  "tool",
		"resource.value": r.Tool,
	}, decisionAction(decision), fmt.Sprintf("%s %s to tool %s", titleDecision(decision), r.AgentID, r.Tool), nil
}

func (r PolicyRuleTemplateRequest) teamTool(decision PolicyDecisionType) (map[string]string, map[string]string, string, error) {
	if r.TeamID == "" || r.Tool == "" {
		return nil, nil, "", fmt.Errorf("team_id and tool are required")
	}
	return map[string]string{
		"actor.type":     "agent",
		"actor.team_id":  r.TeamID,
		"action":         "tool.invoke",
		"resource.type":  "tool",
		"resource.value": r.Tool,
	}, decisionAction(decision), fmt.Sprintf("%s team %s to tool %s", titleDecision(decision), r.TeamID, r.Tool), nil
}

func decisionAction(decision PolicyDecisionType) map[string]string {
	return map[string]string{"decision": string(decision)}
}

func titleDecision(decision PolicyDecisionType) string {
	switch decision {
	case PolicyDecisionDeny:
		return "Deny"
	case PolicyDecisionApprovalRequired:
		return "Require approval"
	default:
		return "Allow"
	}
}

func normalizePolicyTemplate(template string) string {
	template = strings.TrimSpace(strings.ToLower(template))
	template = strings.ReplaceAll(template, "-", "_")
	template = strings.ReplaceAll(template, ".", "_")
	return template
}

func (r PolicyRuleTemplateRequest) stableID() string {
	canonical := map[string]string{
		"template":            r.Template,
		"agent_id":            r.AgentID,
		"team_id":             r.TeamID,
		"capability":          r.Capability,
		"tool":                r.Tool,
		"data_classification": r.DataClassification,
	}
	data, _ := json.Marshal(canonical)
	sum := sha256.Sum256(data)
	return "policy-tpl-" + policyTemplateSlug(r.Template) + "-" + hex.EncodeToString(sum[:])[:12]
}

func policyTemplateSlug(value string) string {
	value = normalizePolicyTemplate(value)
	value = strings.ReplaceAll(value, "_", "-")
	value = strings.Trim(value, "-")
	if value == "" {
		return "custom"
	}
	if len(value) > 48 {
		return value[:48]
	}
	return value
}
