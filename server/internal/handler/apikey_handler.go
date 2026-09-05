package handler

import (
	"context"
	"net/http"
	"strings"

	"github.com/agentium-lab/Janus/core"
)

type APIKeyService interface {
	Create(ctx context.Context, tenantID, name string, scopes []string, boundAgentID string) (core.APIKey, string, error)
	List(ctx context.Context, tenantID string) ([]core.APIKey, error)
	Revoke(ctx context.Context, tenantID, keyID string) (*core.APIKey, error)
}

type APIKeyHandler struct {
	svc APIKeyService
}

func NewAPIKeyHandler(svc APIKeyService) *APIKeyHandler {
	return &APIKeyHandler{svc: svc}
}

type createAPIKeyReq struct {
	Name         string   `json:"name"`
	Scopes       []string `json:"scopes,omitempty"`
	BoundAgentID string   `json:"bound_agent_id,omitempty"`
}

type createdAPIKeyResp struct {
	core.APIKey
	Key string `json:"key"`
}

func (h *APIKeyHandler) Create(w http.ResponseWriter, r *http.Request) {
	tenantID := tenantIDFromPath(r.URL.Path)
	var req createAPIKeyReq
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	k, raw, err := h.svc.Create(r.Context(), tenantID, req.Name, req.Scopes, req.BoundAgentID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, createdAPIKeyResp{APIKey: k, Key: raw})
}

func (h *APIKeyHandler) List(w http.ResponseWriter, r *http.Request) {
	tenantID := tenantIDFromPath(r.URL.Path)
	keys, err := h.svc.List(r.Context(), tenantID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"api_keys": keys})
}

func (h *APIKeyHandler) Revoke(w http.ResponseWriter, r *http.Request) {
	tenantID := tenantIDFromPath(r.URL.Path)
	keyID := lastSegment(strings.TrimSuffix(r.URL.Path, "/revoke"))
	if keyID == "" {
		writeError(w, http.StatusBadRequest, "missing key id")
		return
	}
	k, err := h.svc.Revoke(r.Context(), tenantID, keyID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if k == nil {
		writeError(w, http.StatusNotFound, "api key not found")
		return
	}
	writeJSON(w, http.StatusOK, k)
}
