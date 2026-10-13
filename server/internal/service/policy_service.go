package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/agentium-lab/Janus/core"
)

type PolicyService struct {
	repo PolicyRuleRepo
	// agentRepo, when set, supplies trusted governance attributes (team_id)
	// for agent actors. Team-scoped template rules (actor.team_id) can
	// only match when the actor is enriched from the agent record.
	agentRepo AgentRepo
}

func NewPolicyService(repo PolicyRuleRepo) *PolicyService {
	return &PolicyService{repo: repo}
}

// WithAgentRepo enables actor enrichment from trusted agent records.
func (s *PolicyService) WithAgentRepo(repo AgentRepo) *PolicyService {
	s.agentRepo = repo
	return s
}

// enrichActor loads trusted governance attributes for agent actors.
// Failures are non-fatal: evaluation proceeds without the attributes
// (same behavior as before enrichment existed).
func (s *PolicyService) enrichActor(ctx context.Context, input *core.PolicyInput) {
	if s.agentRepo == nil || input.Actor.Type != "agent" || input.Actor.ID == "" || input.Actor.TeamID != "" {
		return
	}
	agent, err := s.agentRepo.Get(ctx, input.TenantID, input.Actor.ID)
	if err != nil || agent == nil {
		return
	}
	input.Actor.TeamID = agent.TeamID
}

func (s *PolicyService) Evaluate(ctx context.Context, input core.PolicyInput) (core.PolicyDecision, error) {
	if input.TenantID == "" {
		return core.PolicyDecision{}, fmt.Errorf("tenant id is required")
	}

	s.enrichActor(ctx, &input)

	rules, err := s.repo.ListActive(ctx, input.TenantID)
	if err != nil {
		return core.PolicyDecision{}, fmt.Errorf("load policy rules: %w", err)
	}

	inputJSON, _ := json.Marshal(input)
	var inputMap map[string]interface{}
	_ = json.Unmarshal(inputJSON, &inputMap)

	for _, rule := range rules {
		if matchCondition(rule.Condition, inputMap) {
			action, ok := parseAction(rule.Action)
			if !ok {
				continue
			}
			return core.PolicyDecision{
				Decision:     action.Decision,
				DecisionID:   rule.ID,
				MatchedRules: []string{rule.ID},
				Reason:       fmt.Sprintf("matched rule %q", rule.Name),
			}, nil
		}
	}

	return core.PolicyDecision{
		Decision:   core.PolicyDecisionAllow,
		DecisionID: "default_allow",
		Reason:     "no matching deny rules",
	}, nil
}

func matchCondition(condition json.RawMessage, input map[string]interface{}) bool {
	var cond map[string]interface{}
	if err := json.Unmarshal(condition, &cond); err != nil {
		return false
	}
	for key, expected := range cond {
		val, ok := lookupNested(input, key)
		if !ok {
			return false
		}
		if fmt.Sprintf("%v", val) != fmt.Sprintf("%v", expected) {
			return false
		}
	}
	return true
}

func lookupNested(m map[string]interface{}, key string) (interface{}, bool) {
	parts := strings.Split(key, ".")
	var current interface{} = m
	for _, p := range parts {
		cm, ok := current.(map[string]interface{})
		if !ok {
			return nil, false
		}
		current, ok = cm[p]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

type policyAction struct {
	Decision core.PolicyDecisionType `json:"decision"`
}

func parseAction(raw json.RawMessage) (policyAction, bool) {
	var a policyAction
	if err := json.Unmarshal(raw, &a); err != nil {
		return a, false
	}
	if a.Decision == "" {
		return a, false
	}
	return a, true
}
