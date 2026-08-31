package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/agentium-lab/Janus/core"
)

type TenantService interface {
	Create(ctx context.Context, id, name string) error
	Get(ctx context.Context, id string) (*core.Tenant, error)
	List(ctx context.Context) ([]core.Tenant, error)
}

type TenantHandler struct {
	svc TenantService
}

func NewTenantHandler(svc TenantService) *TenantHandler {
	return &TenantHandler{svc: svc}
}

func (h *TenantHandler) List(w http.ResponseWriter, r *http.Request) {
	tenants, err := h.svc.List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, tenants)
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
	writeErrorWithCode(w, status, errorCodeForStatus(status), sanitizeError(msg, status))
}

// writeErrorWithCode writes a structured APIError envelope with an explicit
// ErrorCode. Per docs/Janus-api-contract.md §2, all error responses use the
// shape {error, code, message, status}.
func writeErrorWithCode(w http.ResponseWriter, status int, code string, msg string) {
	writeJSON(w, status, map[string]interface{}{
		"error":   msg,
		"code":    code,
		"message": msg,
		"status":  status,
	})
}

// errorCodeForStatus maps an HTTP status to the canonical ErrorCode string
// per docs/Janus-api-contract.md §2.
func errorCodeForStatus(status int) string {
	switch status {
	case 400:
		return "INVALID_ARGUMENT"
	case 401:
		return "UNAUTHENTICATED"
	case 403:
		return "PERMISSION_DENIED"
	case 404:
		return "NOT_FOUND"
	case 409:
		return "CONFLICT"
	case 429:
		return "RESOURCE_EXHAUSTED"
	case 503:
		return "UNAVAILABLE"
	case 500:
		return "INTERNAL"
	default:
		if status >= 500 {
			return "INTERNAL"
		}
		return "UNKNOWN"
	}
}

func readJSON(r *http.Request, v interface{}) error {
	return readJSONWithLimit(r, v, 10<<20)
}

const maxJSONBodyBytes = 10 << 20

func readJSONWithLimit(r *http.Request, v interface{}, maxBytes int64) error {
	if r.Body == nil {
		return fmt.Errorf("request body is required")
	}
	defer r.Body.Close()
	// io.LimitReader is used instead of http.MaxBytesReader because the latter
	// requires a non-nil http.ResponseWriter to write a 413 response when the
	// limit is exceeded, and readJSON does not have access to one. A truncated
	// body simply produces a JSON decode error which the caller maps to a 400.
	r.Body = io.NopCloser(io.LimitReader(r.Body, maxBytes))
	return json.NewDecoder(r.Body).Decode(v)
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

var internalPatterns = []string{
	"sql:", "pq:", "pgx:", "connection refused", "dial tcp",
	"no rows in result set", "context deadline exceeded",
	"panic:", "goroutine", "runtime error",
	"driver:", "sslmode", "postgres://", "host=",
	"port=", "dbname=", "redis:", "nats:",
	"failed to connect", "lookup", "i/o timeout",
	"server closed", "duplicate key",
}

func sanitizeError(msg string, status int) string {
	lower := strings.ToLower(msg)
	for _, pattern := range internalPatterns {
		if strings.Contains(lower, pattern) {
			if status >= 500 {
				return "internal server error"
			}
			return "bad request"
		}
	}
	if status >= 500 {
		return "internal server error"
	}
	return msg
}
