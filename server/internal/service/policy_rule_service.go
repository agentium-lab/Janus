package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/agentium-lab/Janus/core"
)

type PolicyRuleWriter interface {
	Create(ctx context.Context, rule core.PolicyRule) error
	ListActive(ctx context.Context, tenantID string) ([]*core.PolicyRule, error)
}

type PolicyRuleService struct {
	repo PolicyRuleWriter
	now  func() time.Time
}

func NewPolicyRuleService(repo PolicyRuleWriter) *PolicyRuleService {
	return &PolicyRuleService{repo: repo, now: func() time.Time { return time.Now().UTC() }}
}

var allowedRuleStatuses = map[string]bool{"active": true, "disabled": true}

func (s *PolicyRuleService) Create(ctx context.Context, tenantID string, rule core.PolicyRule) (core.PolicyRule, error) {
	rule.TenantID = tenantID
	if rule.ID == "" {
		return core.PolicyRule{}, fmt.Errorf("id is required")
	}
	if rule.Name == "" {
		return core.PolicyRule{}, fmt.Errorf("name is required")
	}
	if len(rule.Condition) == 0 || !json.Valid(rule.Condition) {
		return core.PolicyRule{}, fmt.Errorf("condition must be a non-empty JSON object")
	}
	if len(rule.Action) == 0 || !json.Valid(rule.Action) {
		return core.PolicyRule{}, fmt.Errorf("action must be a non-empty JSON object")
	}
	if rule.Status == "" {
		rule.Status = "active"
	}
	if !allowedRuleStatuses[rule.Status] {
		return core.PolicyRule{}, fmt.Errorf("status must be active or disabled")
	}
	now := s.now()
	rule.CreatedAt = now
	rule.UpdatedAt = now
	return rule, s.repo.Create(ctx, rule)
}

func (s *PolicyRuleService) List(ctx context.Context, tenantID string) ([]*core.PolicyRule, error) {
	return s.repo.ListActive(ctx, tenantID)
}
