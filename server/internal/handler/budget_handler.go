package handler

import (
	"context"
	"net/http"

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
	tenantID := r.PathValue("tenantID")
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
	tenantID := r.PathValue("tenantID")
	// Both budget Get patterns reach this handler; the one-segment shape
	// carries no {scopeID} wildcard, so PathValue returns "" there.
	scopeType, scopeID := r.PathValue("scopeType"), r.PathValue("scopeID")
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
	tenantID := r.PathValue("tenantID")
	specs, err := h.svc.List(r.Context(), tenantID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"budgets": specs})
}
