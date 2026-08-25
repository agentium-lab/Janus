package service

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/agentium-lab/Janus/core"
)

type BudgetSpecRepo interface {
	Upsert(ctx context.Context, spec core.BudgetSpec) error
	Get(ctx context.Context, tenantID string, scopeType core.BudgetScopeType, scopeID string) (*core.BudgetSpec, error)
	ListByTenant(ctx context.Context, tenantID string) ([]*core.BudgetSpec, error)
}

type BudgetSpecService struct {
	repo BudgetSpecRepo
}

func NewBudgetSpecService(repo BudgetSpecRepo) *BudgetSpecService {
	return &BudgetSpecService{repo: repo}
}

var validScopeTypes = map[string]core.BudgetScopeType{
	"tenant":         core.BudgetScopeTenant,
	"team":           core.BudgetScopeTeam,
	"agent":          core.BudgetScopeAgent,
	"model_provider": core.BudgetScopeModelProvider,
	"model":          core.BudgetScopeModel,
	"task":           core.BudgetScopeTask,
}

func (s *BudgetSpecService) Upsert(ctx context.Context, tenantID, scopeType, scopeID string, rpm, tpm, maxConcurrency int, dailyCostUSD, monthlyCostUSD float64) (core.BudgetSpec, error) {
	st, ok := validScopeTypes[scopeType]
	if !ok {
		return core.BudgetSpec{}, errors.New("unknown scope_type")
	}
	if scopeType != "tenant" && scopeID == "" {
		return core.BudgetSpec{}, errors.New("scope_id is required for non-tenant scopes")
	}
	spec := core.BudgetSpec{
		TenantID:       tenantID,
		ScopeType:      st,
		ScopeID:        scopeID,
		RPM:            rpm,
		TPM:            tpm,
		MaxConcurrency: maxConcurrency,
		DailyCostUSD:   dailyCostUSD,
		MonthlyCostUSD: monthlyCostUSD,
	}
	return spec, s.repo.Upsert(ctx, spec)
}

func (s *BudgetSpecService) Get(ctx context.Context, tenantID, scopeType, scopeID string) (*core.BudgetSpec, error) {
	st, ok := validScopeTypes[scopeType]
	if !ok {
		return nil, errors.New("unknown scope_type")
	}
	spec, err := s.repo.Get(ctx, tenantID, st, scopeID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return spec, err
}

func (s *BudgetSpecService) List(ctx context.Context, tenantID string) ([]*core.BudgetSpec, error) {
	return s.repo.ListByTenant(ctx, tenantID)
}
