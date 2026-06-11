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
}

func NewPolicyService(repo PolicyRuleRepo) *PolicyService {
	return &PolicyService{repo: repo}
}

func (s *PolicyService) Evaluate(ctx context.Context, input core.PolicyInput) (core.PolicyDecision, error) {
	if input.TenantID == "" {
		return core.PolicyDecision{}, fmt.Errorf("tenant id is required")
	}

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
