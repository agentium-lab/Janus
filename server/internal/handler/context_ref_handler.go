package handler

import (
	"net/http"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/service"
)

type ContextRefHandler struct {
	svc *service.ContextRefService
}

func NewContextRefHandler(svc *service.ContextRefService) *ContextRefHandler {
	return &ContextRefHandler{svc: svc}
}

type attachContextRefReq struct {
	Type           string   `json:"type"`
	URI            string   `json:"uri"`
	Hash           string   `json:"hash,omitempty"`
	Classification string   `json:"classification,omitempty"`
	AccessScope    []string `json:"access_scope,omitempty"`
}

func (h *ContextRefHandler) Attach(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenantID")
	if tenantID == "" {
		writeError(w, http.StatusBadRequest, "tenant_id required")
		return
	}
	var req attachContextRefReq
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	ref, err := h.svc.Attach(r.Context(), tenantID, req.Type, req.URI, req.Hash, req.Classification, req.AccessScope)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, ref)
}

func (h *ContextRefHandler) Get(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenantID")
	refID := r.PathValue("refID")
	ref, err := h.svc.Get(r.Context(), tenantID, refID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, ref)
}

func (h *ContextRefHandler) ListByTask(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenantID")
	taskID := r.PathValue("taskID")
	refs, err := h.svc.ListByTask(r.Context(), tenantID, taskID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if refs == nil {
		refs = []*core.ContextRef{}
	}
	writeJSON(w, http.StatusOK, refs)
}

func (h *ContextRefHandler) Detach(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenantID")
	refID := r.PathValue("refID")
	if err := h.svc.Detach(r.Context(), tenantID, refID); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
