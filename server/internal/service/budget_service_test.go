package service

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

type mockBudgetRepo struct {
	budgets map[string]*core.BudgetSpec
	err     error
}

func (m *mockBudgetRepo) Upsert(_ context.Context, spec core.BudgetSpec) error {
	if m.err != nil {
		return m.err
	}
	if m.budgets == nil {
		m.budgets = make(map[string]*core.BudgetSpec)
	}
	key := spec.TenantID + ":" + string(spec.ScopeType) + ":" + spec.ScopeID
	m.budgets[key] = &spec
	return nil
}

func (m *mockBudgetRepo) Get(_ context.Context, tenantID string, scopeType core.BudgetScopeType, scopeID string) (*core.BudgetSpec, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.budgets == nil {
		return nil, fmt.Errorf("not found")
	}
	key := tenantID + ":" + string(scopeType) + ":" + scopeID
	spec, ok := m.budgets[key]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return spec, nil
}

func (m *mockBudgetRepo) ListByTenant(_ context.Context, tenantID string) ([]*core.BudgetSpec, error) {
	if m.err != nil {
		return nil, m.err
	}
	var result []*core.BudgetSpec
	for _, b := range m.budgets {
		if b.TenantID == tenantID {
			result = append(result, b)
		}
	}
	return result, nil
}

func TestBudgetService_CheckConcurrency_AllowNoBudget(t *testing.T) {
	svc := NewBudgetService(&mockBudgetRepo{})
	ctx := context.Background()

	err := svc.CheckConcurrency(ctx, "acme", "agent-1", 5, 5)
	assert.NoError(t, err)
}

func TestBudgetService_CheckConcurrency_AllowUnderLimit(t *testing.T) {
	repo := &mockBudgetRepo{
		budgets: map[string]*core.BudgetSpec{
			"acme:agent:agent-1": {TenantID: "acme", ScopeType: core.BudgetScopeAgent, ScopeID: "agent-1", MaxConcurrency: 10},
		},
	}
	svc := NewBudgetService(repo)
	ctx := context.Background()

	err := svc.CheckConcurrency(ctx, "acme", "agent-1", 5, 5)
	assert.NoError(t, err)
}

func TestBudgetService_CheckConcurrency_DenyOverLimit(t *testing.T) {
	repo := &mockBudgetRepo{
		budgets: map[string]*core.BudgetSpec{
			"acme:agent:agent-1": {TenantID: "acme", ScopeType: core.BudgetScopeAgent, ScopeID: "agent-1", MaxConcurrency: 3},
		},
	}
	svc := NewBudgetService(repo)
	ctx := context.Background()

	err := svc.CheckConcurrency(ctx, "acme", "agent-1", 3, 3)
	require.Error(t, err)
	var bpErr *core.BackpressureError
	require.ErrorAs(t, err, &bpErr)
	assert.Equal(t, core.ReasonAgentConcurrencyExceeded, bpErr.Reason)
}

func TestBudgetService_CheckConcurrency_FallbackToTenant(t *testing.T) {
	repo := &mockBudgetRepo{
		budgets: map[string]*core.BudgetSpec{
			"acme:tenant:acme": {TenantID: "acme", ScopeType: core.BudgetScopeTenant, ScopeID: "acme", MaxConcurrency: 5},
		},
	}
	svc := NewBudgetService(repo)
	ctx := context.Background()

	err := svc.CheckConcurrency(ctx, "acme", "agent-1", 5, 5)
	require.Error(t, err)
	var bpErr *core.BackpressureError
	require.ErrorAs(t, err, &bpErr)
	assert.Equal(t, core.ReasonAgentConcurrencyExceeded, bpErr.Reason)
}

func TestBudgetService_CheckConcurrency_ZeroLimitIgnored(t *testing.T) {
	repo := &mockBudgetRepo{
		budgets: map[string]*core.BudgetSpec{
			"acme:agent:agent-1": {TenantID: "acme", ScopeType: core.BudgetScopeAgent, ScopeID: "agent-1", MaxConcurrency: 0},
		},
	}
	svc := NewBudgetService(repo)
	ctx := context.Background()

	err := svc.CheckConcurrency(ctx, "acme", "agent-1", 100, 100)
	assert.NoError(t, err)
}

func TestBudgetService_CheckConcurrency_EmptyTenantID(t *testing.T) {
	svc := NewBudgetService(&mockBudgetRepo{})
	ctx := context.Background()

	err := svc.CheckConcurrency(ctx, "", "agent-1", 0, 0)
	assert.EqualError(t, err, "tenant id is required")
}

func TestBudgetService_GetLimits(t *testing.T) {
	repo := &mockBudgetRepo{
		budgets: map[string]*core.BudgetSpec{
			"acme:tenant:acme":    {TenantID: "acme", ScopeType: core.BudgetScopeTenant, ScopeID: "acme", MaxConcurrency: 20, RPM: 100},
			"acme:agent:agent-1": {TenantID: "acme", ScopeType: core.BudgetScopeAgent, ScopeID: "agent-1", MaxConcurrency: 5, TPM: 50000},
		},
	}
	svc := NewBudgetService(repo)
	ctx := context.Background()

	tenantLimit, agentLimit, err := svc.GetLimits(ctx, "acme", "agent-1")
	require.NoError(t, err)
	require.NotNil(t, tenantLimit)
	assert.Equal(t, 100, tenantLimit.RPM)
	require.NotNil(t, agentLimit)
	assert.Equal(t, 50000, agentLimit.TPM)
}

func TestBudgetService_GetLimits_NoBudget(t *testing.T) {
	svc := NewBudgetService(&mockBudgetRepo{})
	ctx := context.Background()

	tenantLimit, agentLimit, err := svc.GetLimits(ctx, "acme", "agent-1")
	require.NoError(t, err)
	assert.Nil(t, tenantLimit)
	assert.Nil(t, agentLimit)
}

func TestBudgetService_GetLimits_EmptyTenantID(t *testing.T) {
	svc := NewBudgetService(&mockBudgetRepo{})
	ctx := context.Background()

	_, _, err := svc.GetLimits(ctx, "", "agent-1")
	assert.EqualError(t, err, "tenant id is required")
}

func TestBudgetService_GetLimits_NoAgentID(t *testing.T) {
	repo := &mockBudgetRepo{
		budgets: map[string]*core.BudgetSpec{
			"acme:tenant:acme": {TenantID: "acme", ScopeType: core.BudgetScopeTenant, ScopeID: "acme", RPM: 100},
		},
	}
	svc := NewBudgetService(repo)
	ctx := context.Background()

	tenantLimit, agentLimit, err := svc.GetLimits(ctx, "acme", "")
	require.NoError(t, err)
	require.NotNil(t, tenantLimit)
	assert.Nil(t, agentLimit)
}

func TestBudgetService_CheckConcurrency_RepoError(t *testing.T) {
	repo := &mockBudgetRepo{err: fmt.Errorf("db down")}
	svc := NewBudgetService(repo)
	ctx := context.Background()

	err := svc.CheckConcurrency(ctx, "acme", "agent-1", 0, 0)
	assert.NoError(t, err)
}

type mockRateLimiter struct {
	rpmErr error
	tpmErr error
}

func (m *mockRateLimiter) CheckRPM(_ context.Context, _, _, _ string, _ int) error {
	return m.rpmErr
}

func (m *mockRateLimiter) CheckTPM(_ context.Context, _, _, _ string, _, _ int) error {
	return m.tpmErr
}

func TestBudgetService_Reserve_RPMExceeded(t *testing.T) {
	repo := &mockBudgetRepo{
		budgets: map[string]*core.BudgetSpec{
			"acme:agent:agent-1": {TenantID: "acme", ScopeType: core.BudgetScopeAgent, ScopeID: "agent-1", RPM: 10},
		},
	}
	svc := NewBudgetService(repo).WithRateLimiter(&mockRateLimiter{
		rpmErr: fmt.Errorf("rpm limit exceeded"),
	})
	ctx := context.Background()

	err := svc.Reserve(ctx, "acme", "agent-1", &core.Budget{MaxTokens: 1000})
	require.Error(t, err)
	var bpErr *core.BackpressureError
	require.ErrorAs(t, err, &bpErr)
	assert.Equal(t, core.ReasonModelRPMExceeded, bpErr.Reason)
}

func TestBudgetService_Reserve_TPMExceeded(t *testing.T) {
	repo := &mockBudgetRepo{
		budgets: map[string]*core.BudgetSpec{
			"acme:agent:agent-1": {TenantID: "acme", ScopeType: core.BudgetScopeAgent, ScopeID: "agent-1", TPM: 100},
		},
	}
	svc := NewBudgetService(repo).WithRateLimiter(&mockRateLimiter{
		tpmErr: fmt.Errorf("tpm limit exceeded"),
	})
	ctx := context.Background()

	err := svc.Reserve(ctx, "acme", "agent-1", &core.Budget{MaxTokens: 1000})
	require.Error(t, err)
	var bpErr *core.BackpressureError
	require.ErrorAs(t, err, &bpErr)
	assert.Equal(t, core.ReasonTenantTPMExceeded, bpErr.Reason)
}

func TestBudgetService_Reserve_NoRateLimiter(t *testing.T) {
	svc := NewBudgetService(&mockBudgetRepo{})
	ctx := context.Background()

	err := svc.Reserve(ctx, "acme", "agent-1", nil)
	assert.NoError(t, err)
}

func TestBudgetService_Reserve_TenantRPMExceeded(t *testing.T) {
	repo := &mockBudgetRepo{
		budgets: map[string]*core.BudgetSpec{
			"acme:tenant:acme": {TenantID: "acme", ScopeType: core.BudgetScopeTenant, ScopeID: "acme", RPM: 5},
		},
	}
	svc := NewBudgetService(repo).WithRateLimiter(&mockRateLimiter{
		rpmErr: fmt.Errorf("tenant rpm exceeded"),
	})
	ctx := context.Background()

	err := svc.Reserve(ctx, "acme", "agent-1", nil)
	require.Error(t, err)
}

type mockBudgetUsageRepo struct {
	dailyTokens int
	dailyCost   float64
	dailyTasks  int
	reserveErr  error
	settleErr   error
	releaseErr  error
}

func (m *mockBudgetUsageRepo) ReserveTask(_ context.Context, _, _, _ string) error {
	return m.reserveErr
}

func (m *mockBudgetUsageRepo) SettleUsage(_ context.Context, _, _, _ string, _ int, _ float64) error {
	return m.settleErr
}

func (m *mockBudgetUsageRepo) ReleaseTask(_ context.Context, _, _, _ string) error {
	return m.releaseErr
}

func (m *mockBudgetUsageRepo) GetDailyUsage(_ context.Context, _, _, _ string) (int, float64, int, error) {
	return m.dailyTokens, m.dailyCost, m.dailyTasks, nil
}

func TestBudgetService_NewWithUsage(t *testing.T) {
	svc := NewBudgetServiceWithUsage(&mockBudgetRepo{}, &mockBudgetUsageRepo{})
	assert.NotNil(t, svc)
}

func TestBudgetService_Reserve_WithUsageRepo(t *testing.T) {
	repo := &mockBudgetRepo{}
	usageRepo := &mockBudgetUsageRepo{}
	svc := NewBudgetServiceWithUsage(repo, usageRepo)
	ctx := context.Background()

	err := svc.Reserve(ctx, "acme", "agent-1", nil)
	assert.NoError(t, err)
}

func TestBudgetService_Reserve_DailyCostExceeded_Tenant(t *testing.T) {
	repo := &mockBudgetRepo{
		budgets: map[string]*core.BudgetSpec{
			"acme:tenant:acme": {TenantID: "acme", ScopeType: core.BudgetScopeTenant, ScopeID: "acme", DailyCostUSD: 10.0},
		},
	}
	usageRepo := &mockBudgetUsageRepo{dailyCost: 15.0}
	svc := NewBudgetServiceWithUsage(repo, usageRepo)
	ctx := context.Background()

	err := svc.Reserve(ctx, "acme", "agent-1", nil)
	require.Error(t, err)
	var bpErr *core.BackpressureError
	require.ErrorAs(t, err, &bpErr)
	assert.Equal(t, core.ReasonDailyBudgetExceeded, bpErr.Reason)
}

func TestBudgetService_Reserve_DailyCostExceeded_Agent(t *testing.T) {
	repo := &mockBudgetRepo{
		budgets: map[string]*core.BudgetSpec{
			"acme:agent:agent-1": {TenantID: "acme", ScopeType: core.BudgetScopeAgent, ScopeID: "agent-1", DailyCostUSD: 5.0},
		},
	}
	usageRepo := &mockBudgetUsageRepo{dailyCost: 8.0}
	svc := NewBudgetServiceWithUsage(repo, usageRepo)
	ctx := context.Background()

	err := svc.Reserve(ctx, "acme", "agent-1", nil)
	require.Error(t, err)
	var bpErr *core.BackpressureError
	require.ErrorAs(t, err, &bpErr)
	assert.Equal(t, core.ReasonDailyBudgetExceeded, bpErr.Reason)
}

func TestBudgetService_Reserve_ReserveError(t *testing.T) {
	repo := &mockBudgetRepo{}
	usageRepo := &mockBudgetUsageRepo{reserveErr: fmt.Errorf("redis down")}
	svc := NewBudgetServiceWithUsage(repo, usageRepo)
	ctx := context.Background()

	err := svc.Reserve(ctx, "acme", "agent-1", nil)
	assert.Error(t, err)
}

func TestBudgetService_Settle(t *testing.T) {
	repo := &mockBudgetRepo{}
	usageRepo := &mockBudgetUsageRepo{}
	svc := NewBudgetServiceWithUsage(repo, usageRepo)
	ctx := context.Background()

	err := svc.Settle(ctx, "acme", "agent-1", &core.TokenUsage{PromptTokens: 100, CompletionTokens: 50, TotalTokens: 150})
	assert.NoError(t, err)
}

func TestBudgetService_Settle_NilUsage(t *testing.T) {
	repo := &mockBudgetRepo{}
	usageRepo := &mockBudgetUsageRepo{}
	svc := NewBudgetServiceWithUsage(repo, usageRepo)
	ctx := context.Background()

	err := svc.Settle(ctx, "acme", "agent-1", nil)
	assert.NoError(t, err)
}

func TestBudgetService_Settle_NoUsageRepo(t *testing.T) {
	svc := NewBudgetService(&mockBudgetRepo{})
	ctx := context.Background()

	err := svc.Settle(ctx, "acme", "agent-1", &core.TokenUsage{TotalTokens: 100})
	assert.NoError(t, err)
}

func TestBudgetService_Settle_Error(t *testing.T) {
	repo := &mockBudgetRepo{}
	usageRepo := &mockBudgetUsageRepo{settleErr: fmt.Errorf("db error")}
	svc := NewBudgetServiceWithUsage(repo, usageRepo)
	ctx := context.Background()

	err := svc.Settle(ctx, "acme", "agent-1", &core.TokenUsage{TotalTokens: 100})
	assert.Error(t, err)
}

func TestBudgetService_Release(t *testing.T) {
	repo := &mockBudgetRepo{}
	usageRepo := &mockBudgetUsageRepo{}
	svc := NewBudgetServiceWithUsage(repo, usageRepo)
	ctx := context.Background()

	err := svc.Release(ctx, "acme", "agent-1")
	assert.NoError(t, err)
}

func TestBudgetService_Release_NoUsageRepo(t *testing.T) {
	svc := NewBudgetService(&mockBudgetRepo{})
	ctx := context.Background()

	err := svc.Release(ctx, "acme", "agent-1")
	assert.NoError(t, err)
}

func TestBudgetService_Release_Error(t *testing.T) {
	repo := &mockBudgetRepo{}
	usageRepo := &mockBudgetUsageRepo{releaseErr: fmt.Errorf("db error")}
	svc := NewBudgetServiceWithUsage(repo, usageRepo)
	ctx := context.Background()

	err := svc.Release(ctx, "acme", "agent-1")
	assert.Error(t, err)
}
