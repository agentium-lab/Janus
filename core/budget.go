package core

import "time"

// BudgetScopeType represents the scope level of a budget.
type BudgetScopeType string

const (
	BudgetScopeTenant        BudgetScopeType = "tenant"
	BudgetScopeTeam          BudgetScopeType = "team"
	BudgetScopeAgent         BudgetScopeType = "agent"
	BudgetScopeModelProvider BudgetScopeType = "model_provider"
	BudgetScopeModel         BudgetScopeType = "model"
	BudgetScopeTask          BudgetScopeType = "task"
)

// BudgetSpec defines resource limits at a given scope.
type BudgetSpec struct {
	TenantID       string          `json:"tenant_id"`
	ScopeType      BudgetScopeType `json:"scope_type"`
	ScopeID        string          `json:"scope_id,omitempty"`
	RPM            int             `json:"rpm,omitempty"`
	TPM            int             `json:"tpm,omitempty"`
	MaxConcurrency int             `json:"max_concurrency,omitempty"`
	// Self-reported observability only (ADR-0002): values come from agent ACKs
	// and are never verified or enforced. Aggregation/display is fine; any
	// rejection logic reading these fields is a design error.
	DailyCostUSD   float64   `json:"daily_cost_usd,omitempty"`
	MonthlyCostUSD float64   `json:"monthly_cost_usd,omitempty"`
	CreatedAt      time.Time `json:"created_at,omitempty"`
	UpdatedAt      time.Time `json:"updated_at,omitempty"`
}

// BackpressureReason represents why a task cannot be dispatched.
type BackpressureReason string

const (
	ReasonTenantTPMExceeded        BackpressureReason = "tenant_tpm_exceeded"
	ReasonAgentConcurrencyExceeded BackpressureReason = "agent_concurrency_exceeded"
	ReasonModelRPMExceeded         BackpressureReason = "model_rpm_exceeded"
	ReasonDailyBudgetExceeded      BackpressureReason = "daily_budget_exceeded"
	ReasonApprovalRequired         BackpressureReason = "approval_required"
	ReasonPolicyDenied             BackpressureReason = "policy_denied"
)

// BackpressureError is returned when dispatch is blocked.
type BackpressureError struct {
	Reason  BackpressureReason
	Message string
}

func (e *BackpressureError) Error() string {
	return string(e.Reason) + ": " + e.Message
}

// LedgerEntry is one idempotent budget settlement record for a task attempt.
// Primary key (TenantID, TaskID, Attempt, ScopeType, ScopeID) guarantees a
// given attempt is settled at most once per scope.
type LedgerEntry struct {
	TenantID         string
	TaskID           string
	Attempt          int
	ScopeType        string // "tenant" | "agent"
	ScopeID          string
	PromptTokens     int64
	CompletionTokens int64
	TotalTokens      int64
	CostUSD          float64
}
