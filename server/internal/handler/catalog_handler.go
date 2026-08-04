package handler

import (
	"context"
	"net/http"

	"github.com/agentium-lab/Janus/core"
)

type CatalogStore interface {
	ListOnlineWithCapabilities(ctx context.Context, tenantID string) ([]*core.Agent, error)
}

type CatalogHandler struct {
	store CatalogStore
}

func NewCatalogHandler(store CatalogStore) *CatalogHandler {
	return &CatalogHandler{store: store}
}

func (h *CatalogHandler) List(w http.ResponseWriter, r *http.Request) {
	tenantID := tenantIDFromPath(r.URL.Path)
	agents, err := h.store.ListOnlineWithCapabilities(r.Context(), tenantID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "catalog query failed"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"tenant_id": tenantID,
		"agents":    agents,
	})
}
