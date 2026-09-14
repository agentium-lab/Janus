package handler

import (
	"context"
	"fmt"
	"github.com/agentium-lab/Janus/server/internal/outbox"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type AuditService interface {
	QueryByTask(ctx context.Context, tenantID, taskID string, limit int) (interface{}, error)
	QueryByTrace(ctx context.Context, tenantID, traceID string, limit int) (interface{}, error)
	QueryByTenant(ctx context.Context, tenantID string, limit int) (interface{}, error)
}

// AuditReplayer re-projects audit events from the outbox (ADR-0006 replay).
type AuditReplayer interface {
	Replay(ctx context.Context, tenantID string, from, to time.Time, limit int) (outbox.ReplayResult, error)
}

type AuditHandler struct {
	svc      AuditService
	replayer AuditReplayer
}

func NewAuditHandler(svc AuditService) *AuditHandler {
	return &AuditHandler{svc: svc}
}

func (h *AuditHandler) WithReplayer(r AuditReplayer) *AuditHandler {
	h.replayer = r
	return h
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

// ReplayAudit re-projects audit events from the outbox table for a
// tenant/time range. Safe to call repeatedly (idempotent writes).
func (h *AuditHandler) ReplayAudit(w http.ResponseWriter, r *http.Request) {
	tenantID := tenantIDFromPath(r.URL.Path)
	fromStr := r.URL.Query().Get("from")
	toStr := r.URL.Query().Get("to")
	if fromStr == "" || toStr == "" {
		writeError(w, http.StatusBadRequest, "from and to query parameters required (RFC3339)")
		return
	}
	from, err := time.Parse(time.RFC3339, fromStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid from timestamp")
		return
	}
	to, err := time.Parse(time.RFC3339, toStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid to timestamp")
		return
	}
	limit := 500
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 1000 {
			limit = n
		}
	}
	result, err := h.replayer.Replay(r.Context(), tenantID, from, to, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	code := http.StatusOK
	if result.Status == "partial_failure" {
		code = http.StatusMultiStatus
	} else if result.Status == "all_failed" {
		code = http.StatusUnprocessableEntity
	}
	writeJSON(w, code, map[string]interface{}{
		"projected":  result.Projected,
		"failed":     result.Failed,
		"skipped":    result.Skipped,
		"status":     result.Status,
		"last_error": result.LastError,
		"from":       fromStr,
		"to":         toStr,
	})
}
