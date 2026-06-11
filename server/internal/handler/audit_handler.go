package handler

import (
	"context"
	"fmt"
	"net/http"
	"strings"
)

type AuditService interface {
	QueryByTask(ctx context.Context, tenantID, taskID string, limit int) (interface{}, error)
	QueryByTrace(ctx context.Context, tenantID, traceID string, limit int) (interface{}, error)
	QueryByTenant(ctx context.Context, tenantID string, limit int) (interface{}, error)
}

type AuditHandler struct {
	svc AuditService
}

func NewAuditHandler(svc AuditService) *AuditHandler {
	return &AuditHandler{svc: svc}
}

func (h *AuditHandler) QueryByTask(w http.ResponseWriter, r *http.Request) {
	tenantID, taskID := tenantAndTaskFromPath(r.URL.Path)
	limit := intQuery(r, "limit", 50)
	events, err := h.svc.QueryByTask(r.Context(), tenantID, taskID, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"events": events})
}

func (h *AuditHandler) QueryByTrace(w http.ResponseWriter, r *http.Request) {
	tenantID := tenantIDFromPath(r.URL.Path)
	traceID := lastSegment(r.URL.Path)
	if strings.HasPrefix(traceID, "traces") {
		traceID = ""
	}
	if traceID == "" {
		parts := strings.Split(strings.TrimRight(r.URL.Path, "/"), "/")
		for i, p := range parts {
			if p == "traces" && i+1 < len(parts) {
				traceID = parts[i+1]
			}
		}
	}
	limit := intQuery(r, "limit", 50)
	events, err := h.svc.QueryByTrace(r.Context(), tenantID, traceID, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"events": events})
}

func (h *AuditHandler) QueryByTenant(w http.ResponseWriter, r *http.Request) {
	tenantID := tenantIDFromPath(r.URL.Path)
	limit := intQuery(r, "limit", 50)
	events, err := h.svc.QueryByTenant(r.Context(), tenantID, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"events": events})
}

func intQuery(r *http.Request, key string, def int) int {
	v := r.URL.Query().Get(key)
	if v == "" {
		return def
	}
	n := def
	fmt.Sscanf(v, "%d", &n)
	return n
}
