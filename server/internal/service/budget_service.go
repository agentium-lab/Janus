package service

import (
	"context"
	"fmt"

	"github.com/agentium-lab/Janus/core"
)

type BudgetUsageRepo interface {
	ReserveTask(ctx context.Context, tenantID, scopeType, scopeID string) error
	SettleUsage(ctx context.Context, tenantID, scopeType, scopeID string, tokens int, costUSD float64) error
	ReleaseTask(ctx context.Context, tenantID, scopeType, scopeID string) error
	GetDailyUsage(ctx context.Context, tenantID, scopeType, scopeID string) (tokens int, costUSD float64, taskCount int, err error)
}

type BudgetService struct {
	repo     BudgetRepo
	usageRepo BudgetUsageRepo
}

func NewBudgetService(repo BudgetRepo) *BudgetService {
	return &BudgetService{repo: repo}
}

func NewBudgetServiceWithUsage(repo BudgetRepo, usageRepo BudgetUsageRepo) *BudgetService {
	return &BudgetService{repo: repo, usageRepo: usageRepo}
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

func (s *BudgetService) Reserve(ctx context.Context, tenantID, agentID string, budget *core.Budget) error {
	if s.usageRepo == nil {
		return nil
	}

	if budget != nil && budget.MaxCostUSD > 0 {
		_, dailyCost, _, err := s.usageRepo.GetDailyUsage(ctx, tenantID, string(core.BudgetScopeAgent), agentID)
		if err != nil {
			return err
		}
		agentBudget, berr := s.repo.Get(ctx, tenantID, core.BudgetScopeAgent, agentID)
		if berr == nil && agentBudget.DailyCostUSD > 0 && dailyCost >= agentBudget.DailyCostUSD {
			return &core.BackpressureError{
				Reason:  core.ReasonDailyBudgetExceeded,
				Message: fmt.Sprintf("agent %s: daily cost $%.2f >= limit $%.2f", agentID, dailyCost, agentBudget.DailyCostUSD),
			}
		}
	}

	return s.usageRepo.ReserveTask(ctx, tenantID, string(core.BudgetScopeAgent), agentID)
}

func (s *BudgetService) Settle(ctx context.Context, tenantID, agentID string, usage *core.TokenUsage) error {
	if s.usageRepo == nil {
		return nil
	}
	if usage == nil {
		return s.usageRepo.ReleaseTask(ctx, tenantID, string(core.BudgetScopeAgent), agentID)
	}

	tokens := usage.TotalTokens
	costUSD := float64(tokens) * 0.00003

	if err := s.usageRepo.SettleUsage(ctx, tenantID, string(core.BudgetScopeAgent), agentID, tokens, costUSD); err != nil {
		return err
	}

	return s.usageRepo.ReleaseTask(ctx, tenantID, string(core.BudgetScopeAgent), agentID)
}

func (s *BudgetService) Release(ctx context.Context, tenantID, agentID string) error {
	if s.usageRepo == nil {
		return nil
	}
	return s.usageRepo.ReleaseTask(ctx, tenantID, string(core.BudgetScopeAgent), agentID)
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
