package handler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/agentium-lab/Janus/core"
)

// --- Mailbox Update ---

func TestMailboxHandler_Update(t *testing.T) {
	svc := &mockMailboxService{
		mailboxes: map[string]*core.Mailbox{
			"acme:mb-1": {ID: "mb-1", TenantID: "acme", AgentID: "a1", MaxConcurrency: 5},
		},
	}
	h := NewMailboxHandler(svc)

	req := httptest.NewRequest(http.MethodPatch, "/v1/tenants/acme/mailboxes/mb-1", bytes.NewBufferString(`{"max_concurrency": 10, "ack_wait_seconds": 60}`))
	w := httptest.NewRecorder()
	h.Update(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]string
	json.NewDecoder(w.Body).Decode(&resp)
	assert.Equal(t, "updated", resp["status"])
}

func TestMailboxHandler_Update_BadBody(t *testing.T) {
	svc := &mockMailboxService{}
	h := NewMailboxHandler(svc)

	req := httptest.NewRequest(http.MethodPatch, "/v1/tenants/acme/mailboxes/mb-1", bytes.NewBufferString("{invalid"))
	w := httptest.NewRecorder()
	h.Update(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestMailboxHandler_Update_NotFound(t *testing.T) {
	svc := &mockMailboxService{err: fmt.Errorf("not found")}
	h := NewMailboxHandler(svc)

	req := httptest.NewRequest(http.MethodPatch, "/v1/tenants/acme/mailboxes/mb-missing", bytes.NewBufferString(`{}`))
	w := httptest.NewRecorder()
	h.Update(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

// --- Task Block / Unblock ---

func TestTaskHandler_Block(t *testing.T) {
	svc := &mockTaskService{}
	h := NewTaskHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/block", bytes.NewBufferString(`{"reason":"manual review"}`))
	w := httptest.NewRecorder()
	h.Block(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]string
	json.NewDecoder(w.Body).Decode(&resp)
	assert.Equal(t, "blocked", resp["status"])
}

func TestTaskHandler_Block_Error(t *testing.T) {
	svc := &mockTaskService{err: fmt.Errorf("task is terminal")}
	h := NewTaskHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/block", nil)
	w := httptest.NewRecorder()
	h.Block(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestTaskHandler_Unblock(t *testing.T) {
	svc := &mockTaskService{}
	h := NewTaskHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/unblock", nil)
	w := httptest.NewRecorder()
	h.Unblock(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]string
	json.NewDecoder(w.Body).Decode(&resp)
	assert.Equal(t, "running", resp["status"])
}

func TestTaskHandler_Unblock_Error(t *testing.T) {
	svc := &mockTaskService{err: fmt.Errorf("not blocked")}
	h := NewTaskHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/unblock", nil)
	w := httptest.NewRecorder()
	h.Unblock(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// --- DLQ edge cases ---

func TestDLQHandler_Query_NoMailbox(t *testing.T) {
	svc := &mockDLQService{}
	h := NewDLQHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/dlq", nil)
	w := httptest.NewRecorder()
	h.Query(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

// --- errorCodeForStatus ---

func TestErrorCodeForStatus_AllMappings(t *testing.T) {
	cases := []struct {
		status int
		code   string
	}{
		{400, "INVALID_ARGUMENT"},
		{401, "UNAUTHENTICATED"},
		{403, "PERMISSION_DENIED"},
		{404, "NOT_FOUND"},
		{409, "CONFLICT"},
		{429, "RESOURCE_EXHAUSTED"},
		{503, "UNAVAILABLE"},
		{500, "INTERNAL"},
		{502, "INTERNAL"},
		{418, "UNKNOWN"},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.code, errorCodeForStatus(tc.status), "status %d", tc.status)
	}
}

// --- Context ref handler: skipped (NewContextRefHandler takes concrete *service.ContextRefService, not interface) ---

