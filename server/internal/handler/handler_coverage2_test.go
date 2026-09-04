package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

// --- TenantHandler.List ---

func TestTenantHandler_List(t *testing.T) {
	svc := &mockTenantService{tenants: map[string]*core.Tenant{
		"acme": {ID: "acme", Name: "Acme"},
		"globex": {ID: "globex", Name: "Globex"},
	}}
	h := NewTenantHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants", nil)
	w := httptest.NewRecorder()
	h.List(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var tenants []core.Tenant
	require.NoError(t, json.NewDecoder(w.Body).Decode(&tenants))
	assert.Len(t, tenants, 2)
}

func TestTenantHandler_List_Error(t *testing.T) {
	h := NewTenantHandler(&mockTenantService{err: fmt.Errorf("db error")})

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants", nil)
	w := httptest.NewRecorder()
	h.List(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

// --- Mailbox Pause / Resume lifecycle ---

type mailboxLifecycleSvc struct {
	mailboxes map[string]*core.Mailbox
	pauseErr  error
	resumeErr error
	getErr    error
	paused    []string
	resumed   []string
}

func (m *mailboxLifecycleSvc) Create(_ context.Context, mb core.Mailbox) error { return nil }

func (m *mailboxLifecycleSvc) Get(_ context.Context, tenantID, mailboxID string) (*core.Mailbox, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	if mb, ok := m.mailboxes[tenantID+":"+mailboxID]; ok {
		return mb, nil
	}
	return nil, fmt.Errorf("not found")
}

func (m *mailboxLifecycleSvc) ListByAgent(_ context.Context, tenantID, agentID string) ([]*core.Mailbox, error) {
	return nil, nil
}

func (m *mailboxLifecycleSvc) UpdateConfig(_ context.Context, tenantID, mailboxID string, maxConcurrency, ackWaitSeconds, maxDeliver, retentionSeconds int) error {
	return nil
}

func (m *mailboxLifecycleSvc) Pause(_ context.Context, tenantID, mailboxID string) error {
	if m.pauseErr != nil {
		return m.pauseErr
	}
	m.paused = append(m.paused, mailboxID)
	return nil
}

func (m *mailboxLifecycleSvc) Resume(_ context.Context, tenantID, mailboxID string) error {
	if m.resumeErr != nil {
		return m.resumeErr
	}
	m.resumed = append(m.resumed, mailboxID)
	return nil
}

func TestMailboxHandler_Pause(t *testing.T) {
	svc := &mailboxLifecycleSvc{mailboxes: map[string]*core.Mailbox{
		"acme:mb-1": {ID: "mb-1", TenantID: "acme"},
	}}
	h := NewMailboxHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/mailboxes/mb-1/pause", nil)
	w := httptest.NewRecorder()
	h.Pause(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var mb core.Mailbox
	require.NoError(t, json.NewDecoder(w.Body).Decode(&mb))
	assert.Equal(t, "mb-1", mb.ID)
	assert.Equal(t, []string{"mb-1"}, svc.paused)
}

func TestMailboxHandler_Resume(t *testing.T) {
	svc := &mailboxLifecycleSvc{mailboxes: map[string]*core.Mailbox{
		"acme:mb-1": {ID: "mb-1", TenantID: "acme"},
	}}
	h := NewMailboxHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/mailboxes/mb-1/resume", nil)
	w := httptest.NewRecorder()
	h.Resume(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, []string{"mb-1"}, svc.resumed)
}

func TestMailboxHandler_Pause_MissingMailboxID(t *testing.T) {
	h := NewMailboxHandler(&mailboxLifecycleSvc{})

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/mailboxes/pause", nil)
	w := httptest.NewRecorder()
	h.Pause(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	var env errorEnvelope
	require.NoError(t, json.NewDecoder(w.Body).Decode(&env))
	assert.Contains(t, env.Message, "missing mailbox id")
}

func TestMailboxHandler_Resume_MissingMailboxID(t *testing.T) {
	h := NewMailboxHandler(&mailboxLifecycleSvc{})

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/mailboxes/resume", nil)
	w := httptest.NewRecorder()
	h.Resume(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestMailboxHandler_Pause_ServiceError(t *testing.T) {
	svc := &mailboxLifecycleSvc{pauseErr: fmt.Errorf("already paused")}
	h := NewMailboxHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/mailboxes/mb-1/pause", nil)
	w := httptest.NewRecorder()
	h.Pause(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestMailboxHandler_Resume_ServiceError(t *testing.T) {
	svc := &mailboxLifecycleSvc{resumeErr: fmt.Errorf("not paused")}
	h := NewMailboxHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/mailboxes/mb-1/resume", nil)
	w := httptest.NewRecorder()
	h.Resume(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestMailboxHandler_Pause_GetAfterToggleFails(t *testing.T) {
	svc := &mailboxLifecycleSvc{getErr: fmt.Errorf("db error")}
	h := NewMailboxHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/mailboxes/mb-1/pause", nil)
	w := httptest.NewRecorder()
	h.Pause(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

// --- Task handler edge cases ---

type preExistingTaskSvc struct{ mockTaskService }

func (m *preExistingTaskSvc) Create(_ context.Context, task core.Task) (*core.Task, error) {
	task.CreatedAt = time.Now().Add(-time.Hour)
	return &task, nil
}

type freshCreateTaskSvc struct {
	mockTaskService
	captured *core.Task
}

func (m *freshCreateTaskSvc) Create(_ context.Context, task core.Task) (*core.Task, error) {
	task.CreatedAt = time.Now()
	m.captured = &task
	return &task, nil
}

type nilResultTaskSvc struct{ mockTaskService }

func (m *nilResultTaskSvc) Create(_ context.Context, task core.Task) (*core.Task, error) {
	return nil, nil
}

func TestTaskHandler_Create_EnvelopeTenantMismatch(t *testing.T) {
	h := NewTaskHandler(&mockTaskService{})

	body := `{"id":"t1","source_agent":"a","target_type":"capability","target_value":"r","envelope":{"tenant_id":"other"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.Create(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	var env errorEnvelope
	require.NoError(t, json.NewDecoder(w.Body).Decode(&env))
	assert.Contains(t, env.Message, "does not match path tenant")
}

func TestTaskHandler_Create_InvalidDeadline(t *testing.T) {
	h := NewTaskHandler(&mockTaskService{})

	body := `{"id":"t1","deadline":"not-rfc3339"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.Create(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "invalid deadline format")
}

func TestTaskHandler_Create_InvalidEnvelopeDeadline(t *testing.T) {
	h := NewTaskHandler(&mockTaskService{})

	body := `{"id":"t1","envelope":{"deadline":"tomorrow"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.Create(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "invalid envelope deadline format")
}

func TestTaskHandler_Create_EnvelopeDeadlineUsed(t *testing.T) {
	svc := &mockTaskService{}
	h := NewTaskHandler(svc)

	deadline := time.Now().Add(2 * time.Hour).UTC().Format(time.RFC3339)
	body := fmt.Sprintf(`{"id":"t1","envelope":{"deadline":%q}}`, deadline)
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.Create(w, req)

	require.Equal(t, http.StatusCreated, w.Code)
	created := svc.tasks["acme:t1"]
	require.NotNil(t, created)
	require.NotNil(t, created.Deadline)
	assert.WithinDuration(t, time.Now().Add(2*time.Hour), *created.Deadline, time.Minute)
}

func TestTaskHandler_Create_EnvelopeIdempotencyFallback(t *testing.T) {
	svc := &freshCreateTaskSvc{}
	h := NewTaskHandler(svc)

	body := `{"id":"t1","envelope":{"idempotency_key":"idem-9"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.Create(w, req)

	require.Equal(t, http.StatusCreated, w.Code)
	require.NotNil(t, svc.captured)
	assert.Equal(t, "idem-9", svc.captured.IdempotencyKey)
}

func TestTaskHandler_Create_MailboxTargetFallback(t *testing.T) {
	svc := &mockTaskService{}
	h := NewTaskHandler(svc)

	body := `{"id":"t1","target_type":"mailbox","target_value":"mb-7"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.Create(w, req)

	require.Equal(t, http.StatusCreated, w.Code)
	created := svc.tasks["acme:t1"]
	require.NotNil(t, created)
	assert.Equal(t, "mb-7", created.MailboxID)
	assert.Equal(t, core.PriorityNormal, created.Priority)
}

func TestTaskHandler_Create_ToolInvocation_TopLevelWins(t *testing.T) {
	svc := &mockTaskService{}
	h := NewTaskHandler(svc)

	body := `{"id":"t1","tool_invocation":{"name":"search"},"envelope":{"tool_invocation":{"name":"calc"}}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.Create(w, req)

	require.Equal(t, http.StatusCreated, w.Code)
	created := svc.tasks["acme:t1"]
	require.NotNil(t, created)
	require.NotNil(t, created.Envelope.ToolInvocation)
	assert.Equal(t, "search", created.Envelope.ToolInvocation.Name)
}

func TestTaskHandler_Create_ToolInvocation_EnvelopeFallback(t *testing.T) {
	svc := &mockTaskService{}
	h := NewTaskHandler(svc)

	body := `{"id":"t1","envelope":{"tool_invocation":{"name":"calc"}}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.Create(w, req)

	require.Equal(t, http.StatusCreated, w.Code)
	created := svc.tasks["acme:t1"]
	require.NotNil(t, created)
	require.NotNil(t, created.Envelope.ToolInvocation)
	assert.Equal(t, "calc", created.Envelope.ToolInvocation.Name)
}

func TestTaskHandler_Create_NilResult(t *testing.T) {
	h := NewTaskHandler(&nilResultTaskSvc{})

	body := `{"id":"t1"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.Create(w, req)

	require.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Contains(t, w.Body.String(), "unexpected nil result")
}

func TestTaskHandler_Create_IdempotentDuplicateReturns200(t *testing.T) {
	h := NewTaskHandler(&preExistingTaskSvc{})

	body := `{"id":"t1","idempotency_key":"idem-1"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.Create(w, req)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestTaskHandler_Fail_ServiceError(t *testing.T) {
	h := NewTaskHandler(&mockTaskService{err: fmt.Errorf("task is terminal")})

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/fail", nil)
	w := httptest.NewRecorder()
	h.Fail(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestTaskHandler_Replay_ServiceError(t *testing.T) {
	h := NewTaskHandler(&mockTaskService{err: fmt.Errorf("cannot replay")})

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/replay", nil)
	w := httptest.NewRecorder()
	h.Replay(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestFirstNonNil(t *testing.T) {
	a := &core.ToolInvocation{Name: "a"}
	b := &core.ToolInvocation{Name: "b"}
	assert.Same(t, a, firstNonNil(a, b))
	assert.Same(t, b, firstNonNil(nil, b))
	assert.Nil(t, firstNonNil(nil, nil))
}

// --- Dispatch heartbeat bad body ---

func TestDispatchHandler_Heartbeat_BadJSON(t *testing.T) {
	h := NewDispatchHandler(&mockDispatchSvc{})

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/heartbeat", strings.NewReader("{bad"))
	w := httptest.NewRecorder()
	h.Heartbeat(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// --- Audit trace edge cases + intQuery ---

type limitCaptureAuditSvc struct {
	taskLimit  int
	traceLimit int
	tenantLimit int
	gotTraceID string
}

func (m *limitCaptureAuditSvc) QueryByTask(_ context.Context, tenantID, taskID string, limit int) (interface{}, error) {
	m.taskLimit = limit
	return nil, nil
}

func (m *limitCaptureAuditSvc) QueryByTrace(_ context.Context, tenantID, traceID string, limit int) (interface{}, error) {
	m.traceLimit = limit
	m.gotTraceID = traceID
	return nil, nil
}

func (m *limitCaptureAuditSvc) QueryByTenant(_ context.Context, tenantID string, limit int) (interface{}, error) {
	m.tenantLimit = limit
	return nil, nil
}

func TestAuditHandler_QueryByTrace_TrailingTracesSegment(t *testing.T) {
	svc := &limitCaptureAuditSvc{}
	h := NewAuditHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/audit/traces", nil)
	w := httptest.NewRecorder()
	h.QueryByTrace(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "", svc.gotTraceID)
}

func TestAuditHandler_QueryByTrace_TrailingSlash(t *testing.T) {
	svc := &limitCaptureAuditSvc{}
	h := NewAuditHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/audit/traces/trace-9/", nil)
	w := httptest.NewRecorder()
	h.QueryByTrace(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "trace-9", svc.gotTraceID)
}

func TestIntQuery(t *testing.T) {
	svc := &limitCaptureAuditSvc{}
	h := NewAuditHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/audit?limit=7", nil)
	w := httptest.NewRecorder()
	h.QueryByTenant(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, 7, svc.tenantLimit)

	req = httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/audit?limit=notanumber", nil)
	w = httptest.NewRecorder()
	h.QueryByTenant(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, 50, svc.tenantLimit)

	req = httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/audit", nil)
	w = httptest.NewRecorder()
	h.QueryByTenant(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, 50, svc.tenantLimit)
}

// --- readJSONWithLimit truncation ---

func TestReadJSONWithLimit_TruncatedBody(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks", strings.NewReader(`{"id":"task-with-long-body"}`))
	var v map[string]string
	err := readJSONWithLimit(req, &v, 5)
	assert.Error(t, err)
}

func TestReadJSONWithLimit_SmallBody(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks", strings.NewReader(`{}`))
	var v map[string]string
	err := readJSONWithLimit(req, &v, 10)
	assert.NoError(t, err)
}
