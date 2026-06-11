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

	err := svc.CheckConcurrency(ctx, "acme", "agent-1", 5)
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

	err := svc.CheckConcurrency(ctx, "acme", "agent-1", 5)
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

	err := svc.CheckConcurrency(ctx, "acme", "agent-1", 3)
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

	err := svc.CheckConcurrency(ctx, "acme", "agent-1", 5)
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

	err := svc.CheckConcurrency(ctx, "acme", "agent-1", 100)
	assert.NoError(t, err)
}

func TestBudgetService_CheckConcurrency_EmptyTenantID(t *testing.T) {
	svc := NewBudgetService(&mockBudgetRepo{})
	ctx := context.Background()

	err := svc.CheckConcurrency(ctx, "", "agent-1", 0)
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

	err := svc.CheckConcurrency(ctx, "acme", "agent-1", 0)
	assert.NoError(t, err)
}
