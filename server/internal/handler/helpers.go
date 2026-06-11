package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/agentium-lab/Janus/core"
)

type TenantService interface {
	Create(ctx context.Context, id, name string) error
	Get(ctx context.Context, id string) (*core.Tenant, error)
}

type TenantHandler struct {
	svc TenantService
}

func NewTenantHandler(svc TenantService) *TenantHandler {
	return &TenantHandler{svc: svc}
}

func (h *TenantHandler) Create(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := h.svc.Create(r.Context(), req.ID, req.Name); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{"id": req.ID})
}

func (h *TenantHandler) Get(w http.ResponseWriter, r *http.Request) {
	tenantID := tenantIDFromPath(r.URL.Path)
	if tenantID == "" {
		writeError(w, http.StatusBadRequest, "tenant id is required")
		return
	}

	tenant, err := h.svc.Get(r.Context(), tenantID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, tenant)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func readJSON(r *http.Request, v interface{}) error {
	if r.Body == nil {
		return fmt.Errorf("request body is required")
	}
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(v)
}

func pathParam(path, prefix string) string {
	return strings.TrimPrefix(path, prefix)
}

func tenantIDFromPath(path string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == "tenants" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func isDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "duplicate key") || strings.Contains(err.Error(), "23505")
}
