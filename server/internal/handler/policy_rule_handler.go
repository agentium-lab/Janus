package handler

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/agentium-lab/Janus/core"
)

type PolicyRuleAdminService interface {
	Create(ctx context.Context, tenantID string, rule core.PolicyRule) (core.PolicyRule, error)
	List(ctx context.Context, tenantID string) ([]*core.PolicyRule, error)
}

type PolicyRuleHandler struct {
	svc PolicyRuleAdminService
}

func NewPolicyRuleHandler(svc PolicyRuleAdminService) *PolicyRuleHandler {
	return &PolicyRuleHandler{svc: svc}
}

type createPolicyRuleReq struct {
	ID        string                 `json:"id"`
	Name      string                 `json:"name"`
	Status    string                 `json:"status,omitempty"`
	Priority  int                    `json:"priority,omitempty"`
	Condition map[string]interface{} `json:"condition"`
	Action    map[string]interface{} `json:"action"`
}

func (h *PolicyRuleHandler) Create(w http.ResponseWriter, r *http.Request) {
	tenantID := tenantIDFromPath(r.URL.Path)
	var req createPolicyRuleReq
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	condition, err := json.Marshal(req.Condition)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid condition")
		return
	}
	action, err := json.Marshal(req.Action)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid action")
		return
	}
	rule, err := h.svc.Create(r.Context(), tenantID, core.PolicyRule{
		ID:        req.ID,
		Name:      req.Name,
		Status:    req.Status,
		Priority:  req.Priority,
		Condition: condition,
		Action:    action,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, rule)
}

func (h *PolicyRuleHandler) List(w http.ResponseWriter, r *http.Request) {
	tenantID := tenantIDFromPath(r.URL.Path)
	rules, err := h.svc.List(r.Context(), tenantID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"policy_rules": rules})
}

// CreateFromTemplate is an explicit placeholder: template semantics are not
// specified yet, and inventing condition/action shapes here would produce
// rules the policy engine silently never matches.
func (h *PolicyRuleHandler) CreateFromTemplate(w http.ResponseWriter, r *http.Request) {
	writeError(w, http.StatusNotImplemented,
		"policy rule templates are not implemented yet; create rules directly via POST /policy-rules")
}
