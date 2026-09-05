package core

import (
	"context"
	"encoding/json"
	"time"
)

// PolicyDecisionType represents the outcome of a policy evaluation.
type PolicyDecisionType string

const (
	PolicyDecisionAllow            PolicyDecisionType = "allow"
	PolicyDecisionDeny             PolicyDecisionType = "deny"
	PolicyDecisionApprovalRequired PolicyDecisionType = "approval_required"
	PolicyDecisionRedactContext    PolicyDecisionType = "redact_context"
	PolicyDecisionReduceScope      PolicyDecisionType = "reduce_context_scope"
	PolicyDecisionThrottle         PolicyDecisionType = "throttle"
)

// PolicyInput is the input to a policy evaluation.
type PolicyInput struct {
	TenantID string            `json:"tenant_id"`
	Actor    PolicyActor       `json:"actor"`
	Action   string            `json:"action"`
	Resource PolicyResource    `json:"resource"`
	Context  PolicyContextData `json:"context"`
}

// PolicyActor represents the entity performing an action.
type PolicyActor struct {
	Type   string `json:"type"` // "agent", "user", "system"
	ID     string `json:"id"`
	TeamID string `json:"team_id,omitempty"`
}

// PolicyResource represents the target of an action.
type PolicyResource struct {
	Type  string `json:"type"` // "capability", "agent", "mailbox", "tool"
	Value string `json:"value"`
}

// PolicyContextData carries additional context for policy evaluation.
type PolicyContextData struct {
	DataClassification string   `json:"data_classification,omitempty"`
	Tools              []string `json:"tools,omitempty"`
	CostEstimateUSD    float64  `json:"cost_estimate_usd,omitempty"`
	TargetAgentID      string   `json:"target_agent_id,omitempty"`
	TargetTeamID       string   `json:"target_team_id,omitempty"`
}

// PolicyDecision is the output of a policy evaluation.
type PolicyDecision struct {
	Decision     PolicyDecisionType `json:"decision"`
	DecisionID   string             `json:"decision_id"`
	MatchedRules []string           `json:"matched_rules,omitempty"`
	Reason       string             `json:"reason,omitempty"`
}

// PolicyEngine is the interface for policy evaluation.
// Implementations: BuiltinPolicyEngine (MVP), OPAPolicyEngine (enterprise).
type PolicyEngine interface {
	Evaluate(ctx context.Context, input PolicyInput) (PolicyDecision, error)
}

type PolicyRule struct {
	TenantID  string          `json:"tenant_id"`
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Status    string          `json:"status"`
	Priority  int             `json:"priority"`
	Condition json.RawMessage `json:"condition"`
	Action    json.RawMessage `json:"action"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}
