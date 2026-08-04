package handler

import (
	"net/http"
	"strings"

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
	tenantID := pathSegmentByMarker(r.URL.Path, "tenants")
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
	tenantID := pathSegmentByMarker(r.URL.Path, "tenants")
	refID := pathSegmentByMarker(r.URL.Path, "context-refs")
	ref, err := h.svc.Get(r.Context(), tenantID, refID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, ref)
}

func (h *ContextRefHandler) ListByTask(w http.ResponseWriter, r *http.Request) {
	tenantID := pathSegmentByMarker(r.URL.Path, "tenants")
	taskID := pathSegmentBefore(r.URL.Path, "context-refs")
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
	tenantID := pathSegmentByMarker(r.URL.Path, "tenants")
	refID := pathSegmentByMarker(r.URL.Path, "context-refs")
	if err := h.svc.Detach(r.Context(), tenantID, refID); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func pathSegmentByMarker(path, marker string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, p := range parts {
		if p == marker && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func pathSegmentBefore(path, marker string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, p := range parts {
		if p == marker && i > 0 {
			return parts[i-1]
		}
	}
	return ""
}

func lastPathSegment(path string) string {
	parts := strings.Split(strings.TrimRight(path, "/"), "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return ""
}
