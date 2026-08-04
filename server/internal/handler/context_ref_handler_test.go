package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/service"
)

type errorEnvelope struct {
	Error   string `json:"error"`
	Code    string `json:"code"`
	Message string `json:"message"`
	Status  int    `json:"status"`
}

func TestContextRefHandler_Attach_ReturnsRef(t *testing.T) {
	svc := service.NewContextRefService(&mockContextRefRepo{})
	h := NewContextRefHandler(svc)

	body := `{"type":"document","uri":"s3://bucket/file.txt","hash":"abc","classification":"internal"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/context-refs", strings.NewReader(body))
	w := httptest.NewRecorder()

	h.Attach(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var ref core.ContextRef
	require.NoError(t, json.NewDecoder(w.Body).Decode(&ref))
	assert.Equal(t, "acme", ref.TenantID)
	assert.Equal(t, "document", ref.Type)
	assert.Equal(t, "s3://bucket/file.txt", ref.URI)
	assert.Contains(t, ref.ID, "ctxref_")
}

func TestContextRefHandler_Attach_MissingTenant_JSONEnvelope(t *testing.T) {
	svc := service.NewContextRefService(&mockContextRefRepo{})
	h := NewContextRefHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/context-refs", strings.NewReader(`{"type":"document","uri":"s3://b/f"}`))
	w := httptest.NewRecorder()

	h.Attach(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "tenant_id required")
	var env errorEnvelope
	require.NoError(t, json.NewDecoder(w.Body).Decode(&env))
	assert.Equal(t, http.StatusBadRequest, env.Status)
	assert.Equal(t, "INVALID_ARGUMENT", env.Code)
}

func TestContextRefHandler_Attach_BadJSON_JSONEnvelope(t *testing.T) {
	svc := service.NewContextRefService(&mockContextRefRepo{})
	h := NewContextRefHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/context-refs", strings.NewReader("not-json"))
	w := httptest.NewRecorder()

	h.Attach(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	var env errorEnvelope
	require.NoError(t, json.NewDecoder(w.Body).Decode(&env))
	assert.Equal(t, "INVALID_ARGUMENT", env.Code)
}

func TestContextRefHandler_Attach_ServiceError_JSONEnvelope(t *testing.T) {
	svc := service.NewContextRefService(&mockContextRefRepo{err: errors.New("db unavailable")})
	h := NewContextRefHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/context-refs",
		strings.NewReader(`{"type":"document","uri":"s3://b/f"}`))
	w := httptest.NewRecorder()

	h.Attach(w, req)

	require.Equal(t, http.StatusInternalServerError, w.Code)
	var env errorEnvelope
	require.NoError(t, json.NewDecoder(w.Body).Decode(&env))
	assert.Equal(t, "INTERNAL", env.Code)
}

func TestContextRefHandler_Get_ReturnsRef(t *testing.T) {
	svc := service.NewContextRefService(&mockContextRefRepo{refs: map[string]*core.ContextRef{
		"ref-1": {ID: "ref-1", TenantID: "acme", Type: "document", URI: "s3://x"},
	}})
	h := NewContextRefHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/context-refs/ref-1", nil)
	w := httptest.NewRecorder()

	h.Get(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var ref core.ContextRef
	require.NoError(t, json.NewDecoder(w.Body).Decode(&ref))
	assert.Equal(t, "ref-1", ref.ID)
}

func TestContextRefHandler_Get_NotFound_JSONEnvelope(t *testing.T) {
	svc := service.NewContextRefService(&mockContextRefRepo{})
	h := NewContextRefHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/context-refs/missing", nil)
	w := httptest.NewRecorder()

	h.Get(w, req)

	require.Equal(t, http.StatusNotFound, w.Code)
	var env errorEnvelope
	require.NoError(t, json.NewDecoder(w.Body).Decode(&env))
	assert.Equal(t, "NOT_FOUND", env.Code)
}

func TestContextRefHandler_ListByTask_EmptyArray(t *testing.T) {
	svc := service.NewContextRefService(&mockContextRefRepo{})
	h := NewContextRefHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/tasks/task-9/context-refs", nil)
	w := httptest.NewRecorder()

	h.ListByTask(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
	assert.Equal(t, "[]", strings.TrimSpace(w.Body.String()))
}

func TestContextRefHandler_Detach_NoContent(t *testing.T) {
	svc := service.NewContextRefService(&mockContextRefRepo{})
	h := NewContextRefHandler(svc)

	req := httptest.NewRequest(http.MethodDelete, "/v1/tenants/acme/context-refs/ref-1", nil)
	w := httptest.NewRecorder()

	h.Detach(w, req)

	assert.Equal(t, http.StatusNoContent, w.Code)
	assert.Empty(t, w.Body.String())
}

func TestContextRefHandler_Detach_Error_JSONEnvelope(t *testing.T) {
	svc := service.NewContextRefService(&mockContextRefRepo{err: errors.New("not found")})
	h := NewContextRefHandler(svc)

	req := httptest.NewRequest(http.MethodDelete, "/v1/tenants/acme/context-refs/ref-1", nil)
	w := httptest.NewRecorder()

	h.Detach(w, req)

	require.Equal(t, http.StatusInternalServerError, w.Code)
	var env errorEnvelope
	require.NoError(t, json.NewDecoder(w.Body).Decode(&env))
	assert.Equal(t, "INTERNAL", env.Code)
}
