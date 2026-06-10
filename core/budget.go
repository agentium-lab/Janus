package core

import "time"

// BudgetScopeType represents the scope level of a budget.
type BudgetScopeType string

const (
	BudgetScopeTenant       BudgetScopeType = "tenant"
	BudgetScopeTeam         BudgetScopeType = "team"
	BudgetScopeAgent        BudgetScopeType = "agent"
	BudgetScopeModelProvider BudgetScopeType = "model_provider"
	BudgetScopeModel        BudgetScopeType = "model"
	BudgetScopeTask         BudgetScopeType = "task"
)

// BudgetSpec defines resource limits at a given scope.
type BudgetSpec struct {
	TenantID       string
	ScopeType      BudgetScopeType
	ScopeID        string
	RPM            int
	TPM            int
	MaxConcurrency int
	DailyCostUSD   float64
	MonthlyCostUSD float64
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// BackpressureReason represents why a task cannot be dispatched.
type BackpressureReason string

const (
	ReasonTenantTPMExceeded     BackpressureReason = "tenant_tpm_exceeded"
	ReasonAgentConcurrencyExceeded BackpressureReason = "agent_concurrency_exceeded"
	ReasonModelRPMExceeded      BackpressureReason = "model_rpm_exceeded"
	ReasonDailyBudgetExceeded   BackpressureReason = "daily_budget_exceeded"
	ReasonApprovalRequired      BackpressureReason = "approval_required"
)

// BackpressureError is returned when dispatch is blocked.
type BackpressureError struct {
	Reason BackpressureReason
	Message string
}

func (e *BackpressureError) Error() string {
	return string(e.Reason) + ": " + e.Message
}
