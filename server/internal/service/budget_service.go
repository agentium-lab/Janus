package service

import (
	"context"
	"fmt"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/nilguard"
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
	s.rateLimiter = nilguard.Interface(rl)
	return s
}

// CheckConcurrency validates that the caller may start another concurrent task.
// agentRunning is the count of in-flight tasks for this specific agent;
// tenantRunning is the count for the whole tenant. These must be computed
// separately because agent and tenant budgets are independent limits.
func (s *BudgetService) CheckConcurrency(ctx context.Context, tenantID, agentID string, agentRunning, tenantRunning int) error {
	if tenantID == "" {
		return fmt.Errorf("tenant id is required")
	}

	// Agent-scoped limit first (more specific).
	agentBudget, err := s.repo.Get(ctx, tenantID, core.BudgetScopeAgent, agentID)
	if err == nil && agentBudget.MaxConcurrency > 0 {
		if agentRunning >= agentBudget.MaxConcurrency {
			return &core.BackpressureError{
				Reason:  core.ReasonAgentConcurrencyExceeded,
				Message: fmt.Sprintf("agent %s: %d/%d concurrent tasks", agentID, agentRunning, agentBudget.MaxConcurrency),
			}
		}
	}

	// Tenant-scoped limit.
	tenantBudget, err := s.repo.Get(ctx, tenantID, core.BudgetScopeTenant, tenantID)
	if err == nil && tenantBudget.MaxConcurrency > 0 {
		if tenantRunning >= tenantBudget.MaxConcurrency {
			return &core.BackpressureError{
				Reason:  core.ReasonAgentConcurrencyExceeded,
				Message: fmt.Sprintf("tenant %s: %d/%d concurrent tasks", tenantID, tenantRunning, tenantBudget.MaxConcurrency),
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

// EstimatedCostPerTokenUSD is the interim flat rate used until trusted token
// metering supplies authoritative costs.
const EstimatedCostPerTokenUSD = 0.00003

func EstimateCostUSD(totalTokens int64) float64 {
	return float64(totalTokens) * EstimatedCostPerTokenUSD
}

func (s *BudgetService) Settle(ctx context.Context, tenantID, agentID string, usage *core.TokenUsage) error {
	if s.usageRepo == nil {
		return nil
	}
	if usage == nil {
		return s.usageRepo.ReleaseTask(ctx, tenantID, string(core.BudgetScopeAgent), agentID)
	}

	tokens := usage.TotalTokens
	costUSD := EstimateCostUSD(int64(tokens))

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
