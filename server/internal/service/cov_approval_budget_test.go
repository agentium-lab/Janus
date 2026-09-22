package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

// Approval + budget behavior coverage.

func TestCov_ApprovalService_WithTxPath(t *testing.T) {
	svc := NewApprovalService(&mockApprovalRepo{}, nil, nil)
	ret := svc.WithTxPath(nil, nil)
	assert.Same(t, svc, ret)
}

func TestCov_ApprovalService_Expire_GetError(t *testing.T) {
	svc := NewApprovalService(&mockApprovalRepo{err: errors.New("db down")}, nil, nil)
	err := svc.Expire(context.Background(), "acme", "a1")
	require.Error(t, err)
}

func TestCov_ApprovalService_Expire_UpdateError(t *testing.T) {
	repo := &mockApprovalRepo{approvals: map[string]*core.Approval{
		"acme:a1": {ID: "a1", TenantID: "acme", Status: "pending", TaskID: "t1"},
	}, updateErr: errors.New("update fail")}
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusApprovalPending},
	}}
	svc := NewApprovalService(repo, NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil), nil)
	err := svc.Expire(context.Background(), "acme", "a1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "update approval")
}

func TestCov_ApprovalService_Expire_TaskGetFailure(t *testing.T) {
	repo := &mockApprovalRepo{approvals: map[string]*core.Approval{
		"acme:a1": {ID: "a1", TenantID: "acme", Status: "pending", TaskID: "missing-task"},
	}}
	taskSvc := NewTaskService(&mockTaskRepo{err: errors.New("db down")}, &mockQueueDriver{}, nil, nil)
	svc := NewApprovalService(repo, taskSvc, nil)
	err := svc.Expire(context.Background(), "acme", "a1")
	require.Error(t, err, "atomic expire fails when the task cannot be loaded")
	assert.Contains(t, err.Error(), "get task")
}

func TestCov_ApprovalService_Approve_ExpiredRoutesToExpire(t *testing.T) {
	repo := &mockApprovalRepo{approvals: map[string]*core.Approval{
		"acme:a1": {ID: "a1", TenantID: "acme", Status: "pending", TaskID: "t1",
			ExpiresAt: time.Now().Add(-time.Hour)},
	}}
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusApprovalPending},
	}}
	taskSvc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	svc := NewApprovalService(repo, taskSvc, nil)
	err := svc.Approve(context.Background(), "acme", "a1", "boss", "ok")
	require.NoError(t, err)
	got, _ := repo.Get(context.Background(), "acme", "a1")
	assert.Equal(t, "expired", got.Status)
	assert.Equal(t, core.TaskStatusCancelled, taskRepo.tasks["acme:t1"].Status)
}

// --- budget ---

func TestCov_BudgetReserve_RateLimiterBranches(t *testing.T) {
	ctx := context.Background()

	agentSpec := &core.BudgetSpec{TenantID: "acme", ScopeType: core.BudgetScopeAgent, ScopeID: "a1", RPM: 10, TPM: 100}
	tenantSpec := &core.BudgetSpec{TenantID: "acme", ScopeType: core.BudgetScopeTenant, ScopeID: "acme", RPM: 10, TPM: 100}
	budget := &core.Budget{MaxTokens: 50}

	t.Run("agent rpm denied", func(t *testing.T) {
		repo := &mockBudgetRepo{budgets: map[string]*core.BudgetSpec{"acme:agent:a1": agentSpec}}
		svc := NewBudgetService(repo).WithRateLimiter(&cbRateLimiter{rpmErr: errors.New("rpm exceeded")})
		err := svc.Reserve(ctx, "acme", "a1", nil)
		var bp *core.BackpressureError
		require.ErrorAs(t, err, &bp)
		assert.Equal(t, core.ReasonModelRPMExceeded, bp.Reason)
	})

	t.Run("agent tpm denied", func(t *testing.T) {
		repo := &mockBudgetRepo{budgets: map[string]*core.BudgetSpec{"acme:agent:a1": agentSpec}}
		svc := NewBudgetService(repo).WithRateLimiter(&cbRateLimiter{tpmErr: errors.New("tpm exceeded")})
		err := svc.Reserve(ctx, "acme", "a1", budget)
		var bp *core.BackpressureError
		require.ErrorAs(t, err, &bp)
		assert.Equal(t, core.ReasonTenantTPMExceeded, bp.Reason)
	})

	t.Run("tenant rpm denied", func(t *testing.T) {
		repo := &mockBudgetRepo{budgets: map[string]*core.BudgetSpec{"acme:tenant:acme": tenantSpec}}
		svc := NewBudgetService(repo).WithRateLimiter(&cbRateLimiter{rpmErr: errors.New("rpm exceeded")})
		err := svc.Reserve(ctx, "acme", "a1", nil)
		var bp *core.BackpressureError
		require.ErrorAs(t, err, &bp)
		assert.Equal(t, core.ReasonModelRPMExceeded, bp.Reason)
	})

	t.Run("tenant tpm denied", func(t *testing.T) {
		repo := &mockBudgetRepo{budgets: map[string]*core.BudgetSpec{"acme:tenant:acme": tenantSpec}}
		svc := NewBudgetService(repo).WithRateLimiter(&cbRateLimiter{tpmErr: errors.New("tpm exceeded")})
		err := svc.Reserve(ctx, "acme", "a1", budget)
		var bp *core.BackpressureError
		require.ErrorAs(t, err, &bp)
		assert.Equal(t, core.ReasonTenantTPMExceeded, bp.Reason)
	})

	t.Run("no budgets with limiter passes", func(t *testing.T) {
		svc := NewBudgetService(&mockBudgetRepo{}).WithRateLimiter(&cbRateLimiter{})
		require.NoError(t, svc.Reserve(ctx, "acme", "a1", budget))
	})

	t.Run("limiter ok with usage repo", func(t *testing.T) {
		repo := &mockBudgetRepo{budgets: map[string]*core.BudgetSpec{"acme:agent:a1": agentSpec}}
		svc := NewBudgetServiceWithUsage(repo, &cbBudgetUsage{}).WithRateLimiter(&cbRateLimiter{})
		require.NoError(t, svc.Reserve(ctx, "acme", "a1", budget))
	})
}

func TestCov_BudgetReserve_DailyUsageLookupError(t *testing.T) {
	ctx := context.Background()

	t.Run("tenant usage error", func(t *testing.T) {
		repo := &mockBudgetRepo{budgets: map[string]*core.BudgetSpec{
			"acme:tenant:acme": {TenantID: "acme", ScopeType: core.BudgetScopeTenant, ScopeID: "acme", DailyCostUSD: 5},
		}}
		svc := NewBudgetServiceWithUsage(repo, &cbBudgetUsage{dailyErr: errors.New("usage db down")})
		err := svc.Reserve(ctx, "acme", "a1", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "usage db down")
	})

	t.Run("agent usage error", func(t *testing.T) {
		repo := &mockBudgetRepo{budgets: map[string]*core.BudgetSpec{
			"acme:agent:a1": {TenantID: "acme", ScopeType: core.BudgetScopeAgent, ScopeID: "a1", DailyCostUSD: 5},
		}}
		svc := NewBudgetServiceWithUsage(repo, &cbBudgetUsage{dailyErr: errors.New("usage db down")})
		err := svc.Reserve(ctx, "acme", "a1", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "usage db down")
	})
}

// --- budget spec service ---

func TestCov_BudgetSpecService_Get(t *testing.T) {
	ctx := context.Background()

	spec := &core.BudgetSpec{TenantID: "acme", ScopeType: core.BudgetScopeAgent, ScopeID: "a1", RPM: 10}

	_, err := NewBudgetSpecService(&cbBudgetSpecRepo{}).Get(ctx, "acme", "bogus_scope", "a1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown scope_type")

	got, err := NewBudgetSpecService(&cbBudgetSpecRepo{notFound: true}).Get(ctx, "acme", "agent", "a1")
	require.NoError(t, err)
	assert.Nil(t, got, "ErrNoRows must map to nil spec")

	_, err = NewBudgetSpecService(&cbBudgetSpecRepo{getErr: errors.New("db down")}).Get(ctx, "acme", "agent", "a1")
	require.Error(t, err)

	got, err = NewBudgetSpecService(&cbBudgetSpecRepo{specs: []*core.BudgetSpec{spec}}).Get(ctx, "acme", "agent", "a1")
	require.NoError(t, err)
	assert.Equal(t, spec, got)
}

func TestCov_BudgetSpecService_List(t *testing.T) {
	ctx := context.Background()
	spec := &core.BudgetSpec{TenantID: "acme", ScopeType: core.BudgetScopeTenant}

	got, err := NewBudgetSpecService(&cbBudgetSpecRepo{specs: []*core.BudgetSpec{spec}}).List(ctx, "acme")
	require.NoError(t, err)
	assert.Len(t, got, 1)

	_, err = NewBudgetSpecService(&cbBudgetSpecRepo{listErr: errors.New("db down")}).List(ctx, "acme")
	require.Error(t, err)
}

// --- context ref service ---

func TestExtra_ParseAction_ValidApprovalRequired(t *testing.T) {
	raw := json.RawMessage(`{"decision":"approval_required"}`)
	a, ok := parseAction(raw)
	assert.True(t, ok)
	assert.Equal(t, core.PolicyDecisionApprovalRequired, a.Decision)
}

func TestExtra_Reject_ApprovalAlreadyDecided(t *testing.T) {
	approvalRepo := &mockApprovalRepo{
		approvals: map[string]*core.Approval{
			"acme:appr-1": {ID: "appr-1", TenantID: "acme", Status: "approved"},
		},
	}
	svc := NewApprovalService(approvalRepo, nil, nil)
	ctx := context.Background()

	err := svc.Reject(ctx, "acme", "appr-1", "approver", "reason")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already decided")
}

func TestExtra_Approve_ApprovalAlreadyDecided(t *testing.T) {
	approvalRepo := &mockApprovalRepo{
		approvals: map[string]*core.Approval{
			"acme:appr-1": {ID: "appr-1", TenantID: "acme", Status: "rejected"},
		},
	}
	svc := NewApprovalService(approvalRepo, nil, nil)
	ctx := context.Background()

	err := svc.Approve(ctx, "acme", "appr-1", "approver", "reason")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already decided")
}

func TestExtra_BudgetService_Reserve_AgentUnderLimitThenError(t *testing.T) {
	repo := &mockBudgetRepo{
		budgets: map[string]*core.BudgetSpec{
			"acme:agent:agent-1": {TenantID: "acme", ScopeType: core.BudgetScopeAgent, ScopeID: "agent-1", DailyCostUSD: 5.0},
		},
	}
	usageRepo := &mockBudgetUsageRepo{dailyCost: 1.0, reserveErr: fmt.Errorf("reserve fail")}
	svc := NewBudgetServiceWithUsage(repo, usageRepo)
	ctx := context.Background()

	err := svc.Reserve(ctx, "acme", "agent-1", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "reserve fail")
}
