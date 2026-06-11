package service

import (
	"context"
	"fmt"

	"github.com/agentium-lab/Janus/core"
)

type BudgetService struct {
	repo BudgetRepo
}

func NewBudgetService(repo BudgetRepo) *BudgetService {
	return &BudgetService{repo: repo}
}

func (s *BudgetService) CheckConcurrency(ctx context.Context, tenantID, agentID string, currentRunning int) error {
	if tenantID == "" {
		return fmt.Errorf("tenant id is required")
	}

	agentBudget, err := s.repo.Get(ctx, tenantID, core.BudgetScopeAgent, agentID)
	if err == nil && agentBudget.MaxConcurrency > 0 {
		if currentRunning >= agentBudget.MaxConcurrency {
			return &core.BackpressureError{
				Reason:  core.ReasonAgentConcurrencyExceeded,
				Message: fmt.Sprintf("agent %s: %d/%d concurrent tasks", agentID, currentRunning, agentBudget.MaxConcurrency),
			}
		}
		return nil
	}

	tenantBudget, err := s.repo.Get(ctx, tenantID, core.BudgetScopeTenant, tenantID)
	if err == nil && tenantBudget.MaxConcurrency > 0 {
		if currentRunning >= tenantBudget.MaxConcurrency {
			return &core.BackpressureError{
				Reason:  core.ReasonAgentConcurrencyExceeded,
				Message: fmt.Sprintf("tenant %s: %d/%d concurrent tasks", tenantID, currentRunning, tenantBudget.MaxConcurrency),
			}
		}
	}

	return nil
}

func (s *BudgetService) GetLimits(ctx context.Context, tenantID, agentID string) (tenantLimit, agentLimit *core.BudgetSpec, err error) {
	if tenantID == "" {
		return nil, nil, fmt.Errorf("tenant id is required")
	}

	tenantBudget, err := s.repo.Get(ctx, tenantID, core.BudgetScopeTenant, tenantID)
	if err == nil {
		tenantLimit = tenantBudget
	}

	if agentID != "" {
		agentBudget, err := s.repo.Get(ctx, tenantID, core.BudgetScopeAgent, agentID)
		if err == nil {
			agentLimit = agentBudget
		}
	}

	return tenantLimit, agentLimit, nil
}
