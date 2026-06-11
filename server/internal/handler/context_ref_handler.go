package handler

import (
	"encoding/json"
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
		http.Error(w, "tenant_id required", http.StatusBadRequest)
		return
	}
	var req attachContextRefReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ref, err := h.svc.Attach(r.Context(), tenantID, req.Type, req.URI, req.Hash, req.Classification, req.AccessScope)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(ref)
}

func (h *ContextRefHandler) Get(w http.ResponseWriter, r *http.Request) {
	tenantID := pathSegmentByMarker(r.URL.Path, "tenants")
	refID := lastPathSegment(r.URL.Path)
	ref, err := h.svc.Get(r.Context(), tenantID, refID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(ref)
}

func (h *ContextRefHandler) ListByTask(w http.ResponseWriter, r *http.Request) {
	tenantID := pathSegmentByMarker(r.URL.Path, "tenants")
	taskID := pathSegmentBefore(r.URL.Path, "context-refs")
	refs, err := h.svc.ListByTask(r.Context(), tenantID, taskID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if refs == nil {
		refs = []*core.ContextRef{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(refs)
}

func (h *ContextRefHandler) Detach(w http.ResponseWriter, r *http.Request) {
	tenantID := pathSegmentByMarker(r.URL.Path, "tenants")
	refID := lastPathSegment(r.URL.Path)
	if err := h.svc.Detach(r.Context(), tenantID, refID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
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
