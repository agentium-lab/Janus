package handler

import (
	"context"
	"net/http"
	"strings"

	"github.com/agentium-lab/Janus/core"
)

type BudgetSpecAdminService interface {
	Upsert(ctx context.Context, tenantID, scopeType, scopeID string, rpm, tpm, maxConcurrency int, dailyCostUSD, monthlyCostUSD float64) (core.BudgetSpec, error)
	Get(ctx context.Context, tenantID, scopeType, scopeID string) (*core.BudgetSpec, error)
	List(ctx context.Context, tenantID string) ([]*core.BudgetSpec, error)
}

type BudgetHandler struct {
	svc BudgetSpecAdminService
}

func NewBudgetHandler(svc BudgetSpecAdminService) *BudgetHandler {
	return &BudgetHandler{svc: svc}
}

type upsertBudgetReq struct {
	ScopeType      string  `json:"scope_type"`
	ScopeID        string  `json:"scope_id,omitempty"`
	RPM            int     `json:"rpm,omitempty"`
	TPM            int     `json:"tpm,omitempty"`
	MaxConcurrency int     `json:"max_concurrency,omitempty"`
	DailyCostUSD   float64 `json:"daily_cost_usd,omitempty"`
	MonthlyCostUSD float64 `json:"monthly_cost_usd,omitempty"`
}

func (h *BudgetHandler) Upsert(w http.ResponseWriter, r *http.Request) {
	tenantID := tenantIDFromPath(r.URL.Path)
	var req upsertBudgetReq
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	spec, err := h.svc.Upsert(r.Context(), tenantID, req.ScopeType, req.ScopeID,
		req.RPM, req.TPM, req.MaxConcurrency, req.DailyCostUSD, req.MonthlyCostUSD)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, spec)
}

func (h *BudgetHandler) Get(w http.ResponseWriter, r *http.Request) {
	tenantID := tenantIDFromPath(r.URL.Path)
	scopeType, scopeID := budgetScopeFromPath(r.URL.Path)
	if scopeType == "" {
		writeError(w, http.StatusBadRequest, "missing scope_type")
		return
	}
	spec, err := h.svc.Get(r.Context(), tenantID, scopeType, scopeID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if spec == nil {
		writeError(w, http.StatusNotFound, "budget not found")
		return
	}
	writeJSON(w, http.StatusOK, spec)
}

func (h *BudgetHandler) List(w http.ResponseWriter, r *http.Request) {
	tenantID := tenantIDFromPath(r.URL.Path)
	specs, err := h.svc.List(r.Context(), tenantID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"budgets": specs})
}

// budgetScopeFromPath extracts scope_type and scope_id following the
// ".../budgets/" segment, tolerating missing scope_id for tenant-level paths.
func budgetScopeFromPath(path string) (string, string) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, seg := range parts {
		if seg != "budgets" {
			continue
		}
		rest := parts[i+1:]
		if len(rest) >= 2 {
			return rest[0], rest[1]
		}
		if len(rest) == 1 {
			return rest[0], ""
		}
	}
	return "", ""
}
