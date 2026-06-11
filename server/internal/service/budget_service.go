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

type RateLimiter interface {
	CheckRPM(ctx context.Context, tenantID, scopeType, scopeID string, limit int) error
	CheckTPM(ctx context.Context, tenantID, scopeType, scopeID string, limit, tokenCount int) error
}

type BudgetService struct {
	repo        BudgetRepo
	usageRepo   BudgetUsageRepo
	rateLimiter RateLimiter
}

func NewBudgetService(repo BudgetRepo) *BudgetService {
	return &BudgetService{repo: repo}
}

func NewBudgetServiceWithUsage(repo BudgetRepo, usageRepo BudgetUsageRepo) *BudgetService {
	return &BudgetService{repo: repo, usageRepo: usageRepo}
}

func (s *BudgetService) WithRateLimiter(rl RateLimiter) *BudgetService {
	s.rateLimiter = rl
	return s
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
	if s.rateLimiter != nil {
		agentBudget, _ := s.repo.Get(ctx, tenantID, core.BudgetScopeAgent, agentID)
		if agentBudget != nil {
			if err := s.rateLimiter.CheckRPM(ctx, tenantID, "agent", agentID, agentBudget.RPM); err != nil {
				return &core.BackpressureError{Reason: core.ReasonModelRPMExceeded, Message: err.Error()}
			}
			if budget != nil {
				if err := s.rateLimiter.CheckTPM(ctx, tenantID, "agent", agentID, agentBudget.TPM, budget.MaxTokens); err != nil {
					return &core.BackpressureError{Reason: core.ReasonTenantTPMExceeded, Message: err.Error()}
				}
			}
		}
		tenantBudget, _ := s.repo.Get(ctx, tenantID, core.BudgetScopeTenant, tenantID)
		if tenantBudget != nil {
			if err := s.rateLimiter.CheckRPM(ctx, tenantID, "tenant", tenantID, tenantBudget.RPM); err != nil {
				return &core.BackpressureError{Reason: core.ReasonModelRPMExceeded, Message: err.Error()}
			}
			if budget != nil {
				if err := s.rateLimiter.CheckTPM(ctx, tenantID, "tenant", tenantID, tenantBudget.TPM, budget.MaxTokens); err != nil {
					return &core.BackpressureError{Reason: core.ReasonTenantTPMExceeded, Message: err.Error()}
				}
			}
		}
	}

	if s.usageRepo == nil {
		return nil
	}

	tenantBudget, _ := s.repo.Get(ctx, tenantID, core.BudgetScopeTenant, tenantID)
	if tenantBudget != nil && tenantBudget.DailyCostUSD > 0 {
		_, dailyCost, _, err := s.usageRepo.GetDailyUsage(ctx, tenantID, string(core.BudgetScopeTenant), tenantID)
		if err != nil {
			return err
		}
		if dailyCost >= tenantBudget.DailyCostUSD {
			return &core.BackpressureError{
				Reason:  core.ReasonDailyBudgetExceeded,
				Message: fmt.Sprintf("tenant %s: daily cost $%.2f >= limit $%.2f", tenantID, dailyCost, tenantBudget.DailyCostUSD),
			}
		}
	}

	agentBudget, _ := s.repo.Get(ctx, tenantID, core.BudgetScopeAgent, agentID)
	if agentBudget != nil && agentBudget.DailyCostUSD > 0 {
		_, dailyCost, _, err := s.usageRepo.GetDailyUsage(ctx, tenantID, string(core.BudgetScopeAgent), agentID)
		if err != nil {
			return err
		}
		if dailyCost >= agentBudget.DailyCostUSD {
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
