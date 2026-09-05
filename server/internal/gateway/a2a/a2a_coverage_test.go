package a2a

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubTaskCreator struct {
	mu        sync.Mutex
	task      *core.Task
	err       error
	cancelled []string
}

func (s *stubTaskCreator) Create(_ context.Context, task core.Task) (*core.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	if s.task != nil {
		return s.task, nil
	}
	cp := task
	if cp.Status == "" {
		cp.Status = core.TaskStatusCreated
	}
	s.task = &cp
	return s.task, nil
}

func (s *stubTaskCreator) Cancel(_ context.Context, _, taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.cancelled = append(s.cancelled, taskID)
	return nil
}

func (s *stubTaskCreator) captured() *core.Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.task
}

type chanSubscriber struct {
	mu   sync.Mutex
	ch   chan core.JanusEvent
	subs int
}

func newChanSubscriber() *chanSubscriber {
	return &chanSubscriber{ch: make(chan core.JanusEvent, 8)}
}

func (c *chanSubscriber) Subscribe(string) <-chan core.JanusEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.subs++
	return c.ch
}

func (c *chanSubscriber) Unsubscribe(_ string, _ <-chan core.JanusEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.subs--
}

func (c *chanSubscriber) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.subs
}

func (c *chanSubscriber) emit(evt core.JanusEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ch <- evt
}

func (c *chanSubscriber) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	close(c.ch)
}

type recordingStreamer struct {
	mu    sync.Mutex
	paths []string
}

func (s *recordingStreamer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.paths = append(s.paths, r.URL.Path)
	s.mu.Unlock()
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("streamed"))
}

func (s *recordingStreamer) recorded() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.paths...)
}

type noFlushRW struct {
	header http.Header
	body   strings.Builder
	code   int
}

func newNoFlushRW() *noFlushRW                   { return &noFlushRW{header: make(http.Header)} }
func (w *noFlushRW) Header() http.Header         { return w.header }
func (w *noFlushRW) Write(b []byte) (int, error) { return w.body.Write(b) }
func (w *noFlushRW) WriteHeader(code int)        { w.code = code }

func TestSanitizeMsg_LongMessageRedacted(t *testing.T) {
	assert.Equal(t, "internal error", sanitizeMsg(strings.Repeat("a", 201)))
	assert.Equal(t, "harmless", sanitizeMsg("harmless"))
	assert.Equal(t, "internal error", sanitizeMsg("panic: boom"))
	assert.Equal(t, "internal error", sanitizeMsg("failed with dbname=x"))
}

func TestReadJSONLimit_NilBody(t *testing.T) {
	gw := NewGateway(&mockAgentRegistrar{}, &mockTaskCreator{})
	req := withAuthCtx(httptest.NewRequest(http.MethodPost, "/a2a/task/send", nil))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "invalid json")
}

func TestLegacy_TenantReject(t *testing.T) {
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, &mockStatusGetter{})
	cases := []struct {
		name, method, path string
	}{
		{"agent card", http.MethodPost, "/a2a/agent/card"},
		{"task send", http.MethodPost, "/a2a/task/send"},
		{"task status", http.MethodGet, "/a2a/task/t1/status"},
		{"jsonrpc", http.MethodPost, "/a2a/jsonrpc"},
		{"task stream", http.MethodGet, "/a2a/task/stream"},
		{"v1 send", http.MethodPost, "/a2a/message:send"},
		{"v1 stream", http.MethodPost, "/a2a/message:stream"},
		{"v1 get", http.MethodGet, "/a2a/tasks/t1"},
		{"v1 subscribe", http.MethodGet, "/a2a/tasks/t1:subscribe"},
		{"v1 cancel", http.MethodPost, "/a2a/tasks/t1:cancel"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(`{}`))
			w := httptest.NewRecorder()
			gw.ServeHTTP(w, req)
			assert.Equal(t, http.StatusForbidden, w.Code)
			assert.Contains(t, w.Body.String(), "TENANT_REQUIRED")
		})
	}
}

func TestLegacy_JSONRPC_WrongMethod(t *testing.T) {
	gw := NewGateway(&mockAgentRegistrar{}, &mockTaskCreator{})
	req := withAuthCtx(httptest.NewRequest(http.MethodGet, "/a2a/jsonrpc", nil))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestLegacy_TaskStatus_ShortPathDirect(t *testing.T) {
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, &mockStatusGetter{})
	req := withAuthCtx(httptest.NewRequest(http.MethodGet, "/status", nil))
	w := httptest.NewRecorder()
	gw.handleTaskStatus(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "invalid task status path")
}

func TestLegacy_JSONRPC_MessageSendAlias(t *testing.T) {
	tc := &mockTaskCreator{}
	gw := NewGateway(&mockAgentRegistrar{}, tc)
	body := `{"jsonrpc":"2.0","method":"message/send","params":{"id":"msg-9","params":{"message":{"role":"user","parts":[{"type":"text","text":"alias"}]}}},"id":7}`
	req := withAuthCtx(httptest.NewRequest(http.MethodPost, "/a2a/jsonrpc?mailbox_id=mb-q", strings.NewReader(body)))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var resp jsonRPCResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Nil(t, resp.Error)
	assert.Equal(t, float64(7), resp.ID)
	m, ok := resp.Result.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "msg-9", m["task_id"])
	assert.Equal(t, "created", m["status"])
	require.NotNil(t, tc.captured())
	assert.Equal(t, "mb-q", tc.captured().MailboxID)
}

func TestLegacy_JSONRPC_TasksGetAlias(t *testing.T) {
	statusSvc := &mockStatusGetter{task: &core.Task{ID: "task-2", Status: core.TaskStatusRunning}}
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, statusSvc)
	body := `{"jsonrpc":"2.0","method":"tasks/get","params":{"task_id":"task-2"},"id":8}`
	req := withAuthCtx(httptest.NewRequest(http.MethodPost, "/a2a/jsonrpc", strings.NewReader(body)))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var resp jsonRPCResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Nil(t, resp.Error)
	m, ok := resp.Result.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "task-2", m["task_id"])
	assert.Equal(t, "running", m["status"])
}

func TestLegacy_JSONRPC_TaskGet_NoStatusService(t *testing.T) {
	gw := NewGateway(&mockAgentRegistrar{}, &mockTaskCreator{})
	body := `{"jsonrpc":"2.0","method":"task/get","params":{"task_id":"t"},"id":9}`
	req := withAuthCtx(httptest.NewRequest(http.MethodPost, "/a2a/jsonrpc", strings.NewReader(body)))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	var resp jsonRPCResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	require.NotNil(t, resp.Error)
	assert.Equal(t, -32603, resp.Error.Code)
	assert.Contains(t, resp.Error.Message, "status service not configured")
}

func TestLegacy_JSONRPC_TaskGet_Error(t *testing.T) {
	statusSvc := &mockStatusGetter{err: fmt.Errorf("no rows in result set")}
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, statusSvc)
	body := `{"jsonrpc":"2.0","method":"task/get","params":{"task_id":"gone"},"id":10}`
	req := withAuthCtx(httptest.NewRequest(http.MethodPost, "/a2a/jsonrpc", strings.NewReader(body)))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	var resp jsonRPCResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	require.NotNil(t, resp.Error)
	assert.Equal(t, -32601, resp.Error.Code)
	assert.Equal(t, "internal error", resp.Error.Message, "db leak pattern must be sanitized")
}

func TestLegacy_JSONRPC_TasksCancel(t *testing.T) {
	tc := &mockTaskCreator{}
	gw := NewGateway(&mockAgentRegistrar{}, tc)
	body := `{"jsonrpc":"2.0","method":"tasks/cancel","params":{"task_id":"t-9"},"id":11}`
	req := withAuthCtx(httptest.NewRequest(http.MethodPost, "/a2a/jsonrpc", strings.NewReader(body)))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	var resp jsonRPCResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Nil(t, resp.Error)
	m, ok := resp.Result.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "t-9", m["task_id"])
	assert.Equal(t, "cancelling", m["status"])
}

func TestLegacy_JSONRPC_TasksCancel_Error(t *testing.T) {
	tc := &mockTaskCreator{err: fmt.Errorf("sql: deadlock detected")}
	gw := NewGateway(&mockAgentRegistrar{}, tc)
	body := `{"jsonrpc":"2.0","method":"tasks/cancel","params":{"task_id":"t-9"},"id":12}`
	req := withAuthCtx(httptest.NewRequest(http.MethodPost, "/a2a/jsonrpc", strings.NewReader(body)))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	var resp jsonRPCResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	require.NotNil(t, resp.Error)
	assert.Equal(t, -32603, resp.Error.Code)
	assert.Equal(t, "internal error", resp.Error.Message)
}

func TestLegacy_JSONRPC_TasksCancel_BadParams(t *testing.T) {
	gw := NewGateway(&mockAgentRegistrar{}, &mockTaskCreator{})
	for _, body := range []string{
		`{"jsonrpc":"2.0","method":"tasks/cancel","params":"nope","id":13}`,
		`{"jsonrpc":"2.0","method":"tasks/cancel","params":{"task_id":""},"id":14}`,
	} {
		req := withAuthCtx(httptest.NewRequest(http.MethodPost, "/a2a/jsonrpc", strings.NewReader(body)))
		w := httptest.NewRecorder()
		gw.ServeHTTP(w, req)

		var resp jsonRPCResponse
		require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
		require.NotNil(t, resp.Error)
		assert.Equal(t, -32602, resp.Error.Code)
		assert.Contains(t, resp.Error.Message, "task_id required")
	}
}

func TestLegacy_TaskStream_DelegatesToStreamer(t *testing.T) {
	streamer := &recordingStreamer{}
	gw := NewGateway(&mockAgentRegistrar{}, &mockTaskCreator{}).WithTaskStreamer(streamer)

	req := withAuthCtx(httptest.NewRequest(http.MethodGet, "/a2a/task/stream?task_id=t-77", nil))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "streamed", w.Body.String())
	require.Len(t, streamer.recorded(), 1)
	assert.Equal(t, "/v1/tenants/acme/tasks/t-77/stream", streamer.recorded()[0])
}

func TestLegacy_TaskStream_MissingTaskID(t *testing.T) {
	gw := NewGateway(&mockAgentRegistrar{}, &mockTaskCreator{}).WithTaskStreamer(&recordingStreamer{})
	req := withAuthCtx(httptest.NewRequest(http.MethodGet, "/a2a/task/stream", nil))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "task_id query parameter required")
}

func TestLegacy_TaskStream_NotConfigured(t *testing.T) {
	gw := NewGateway(&mockAgentRegistrar{}, &mockTaskCreator{})
	req := withAuthCtx(httptest.NewRequest(http.MethodGet, "/a2a/task/stream?task_id=t-1", nil))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotImplemented, w.Code)
	assert.Contains(t, w.Body.String(), "streaming not configured")
}

func TestAgentCardHandler_Document(t *testing.T) {
	h := AgentCardHandler()
	req := httptest.NewRequest(http.MethodGet, "/.well-known/agent.json", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var card map[string]interface{}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&card))
	assert.Equal(t, "Janus", card["name"])
	assert.Equal(t, "1.1.0", card["version"])
	assert.True(t, strings.HasPrefix(card["url"].(string), "http://"))

	tlsReq := httptest.NewRequest(http.MethodGet, "/.well-known/agent.json", nil)
	tlsReq.TLS = &tls.ConnectionState{}
	tlsW := httptest.NewRecorder()
	h.ServeHTTP(tlsW, tlsReq)
	var tlsCard map[string]interface{}
	require.NoError(t, json.NewDecoder(tlsW.Body).Decode(&tlsCard))
	assert.True(t, strings.HasPrefix(tlsCard["url"].(string), "https://"))
}

func TestAgentCardV1Handler_HTTPS(t *testing.T) {
	h := AgentCardV1Handler()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.TLS = &tls.ConnectionState{}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	var card map[string]interface{}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&card))
	ifaces := card["supportedInterfaces"].([]interface{})
	iface := ifaces[0].(map[string]interface{})
	assert.True(t, strings.HasPrefix(iface["url"].(string), "https://"))
}

func TestV1StreamResponse_MarshalMessageAndArtifact(t *testing.T) {
	b, err := json.Marshal(V1StreamResponse{Message: &V1Message{MessageID: "m1", Role: "ROLE_AGENT"}})
	require.NoError(t, err)
	var m map[string]interface{}
	require.NoError(t, json.Unmarshal(b, &m))
	require.Len(t, m, 1)
	msg := m["message"].(map[string]interface{})
	assert.Equal(t, "m1", msg["messageId"])

	b, err = json.Marshal(V1StreamResponse{ArtifactUpdate: &V1TaskArtifactUpdateEvent{
		TaskID: "t1", Artifact: V1Artifact{ArtifactID: "a1"},
	}})
	require.NoError(t, err)
	m = map[string]interface{}{}
	require.NoError(t, json.Unmarshal(b, &m))
	require.Len(t, m, 1)
	art := m["artifactUpdate"].(map[string]interface{})
	assert.Equal(t, "t1", art["taskId"])
}

func TestWriteSSEData_MarshalError(t *testing.T) {
	w := httptest.NewRecorder()
	err := writeSSEData(w, w, V1StreamResponse{})
	require.Error(t, err)
	assert.Empty(t, w.Body.String())
}

func TestV1Send_MailboxFromQuery(t *testing.T) {
	tc := &mockTaskCreator{}
	gw := NewGateway(&mockAgentRegistrar{}, tc)
	body := `{"message":{"role":"ROLE_USER","parts":[{"text":"x"}]}}`
	req := withAuthCtx(httptest.NewRequest(http.MethodPost, "/a2a/message:send?mailbox_id=mb-from-query", strings.NewReader(body)))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, tc.captured())
	assert.Equal(t, "mb-from-query", tc.captured().MailboxID)
}

func TestV1Send_CreateError_Sanitized(t *testing.T) {
	tc := &stubTaskCreator{err: fmt.Errorf("pgx: pool exhausted")}
	gw := NewGateway(&mockAgentRegistrar{}, tc)
	req := withAuthCtx(httptest.NewRequest(http.MethodPost, "/a2a/message:send", strings.NewReader(`{"message":{"parts":[{"text":"x"}]}}`)))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Contains(t, w.Body.String(), "internal error")
}

func TestV1StreamMessage_TerminalTaskClosesImmediately(t *testing.T) {
	done := completedTask("t-now")
	tc := &stubTaskCreator{}
	tc.mu.Lock()
	tc.task = done
	tc.mu.Unlock()
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, tc, &mockStatusGetter{}).WithEventSubscriber(newChanSubscriber())

	req := withAuthCtx(httptest.NewRequest(http.MethodPost, "/a2a/message:stream", strings.NewReader(`{"message":{"parts":[{"text":"x"}]}}`)))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	out := w.Body.String()
	assert.Contains(t, out, `"task":`)
	assert.Contains(t, out, V1StateCompleted)
	assert.Contains(t, out, `"statusUpdate":`)
}

func TestV1StreamMessage_CreateError(t *testing.T) {
	tc := &stubTaskCreator{err: fmt.Errorf("dial tcp 1.2.3.4:5432")}
	gw := NewGateway(&mockAgentRegistrar{}, tc).WithEventSubscriber(newChanSubscriber())
	req := withAuthCtx(httptest.NewRequest(http.MethodPost, "/a2a/message:stream", strings.NewReader(`{"message":{"parts":[{"text":"x"}]}}`)))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Contains(t, w.Body.String(), "internal error")
}

func TestV1StreamMessage_NonFlusherWriter(t *testing.T) {
	tc := &stubTaskCreator{}
	gw := NewGateway(&mockAgentRegistrar{}, tc).WithEventSubscriber(newChanSubscriber())
	req := withAuthCtx(httptest.NewRequest(http.MethodPost, "/a2a/message:stream", strings.NewReader(`{"message":{"parts":[{"text":"x"}]}}`)))
	w := newNoFlushRW()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.code)
	assert.Contains(t, w.body.String(), "streaming unsupported")
}

func TestV1Subscribe_NonFlusherWriter(t *testing.T) {
	running := &core.Task{
		TenantID: "acme", ID: "t-run", Status: core.TaskStatusRunning,
		Envelope: core.TaskEnvelope{Trace: core.TraceContext{TraceID: "ctx-run"}},
	}
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, &mockStatusGetter{task: running}).WithEventSubscriber(newChanSubscriber())
	req := withAuthCtx(httptest.NewRequest(http.MethodGet, "/a2a/tasks/t-run:subscribe", nil))
	w := newNoFlushRW()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.code)
	assert.Contains(t, w.body.String(), "streaming unsupported")
}

func TestV1Subscribe_StreamUntilChannelClosed(t *testing.T) {
	running := &core.Task{
		TenantID: "acme", ID: "t-run", Status: core.TaskStatusRunning,
		Envelope: core.TaskEnvelope{Trace: core.TraceContext{TraceID: "ctx-run"}},
	}
	sub := newChanSubscriber()
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, &mockStatusGetter{task: running}).WithEventSubscriber(sub)

	req := withAuthCtx(httptest.NewRequest(http.MethodGet, "/a2a/tasks/t-run:subscribe", nil))
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { defer close(done); gw.ServeHTTP(w, req) }()

	deadline := time.Now().Add(2 * time.Second)
	for sub.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	require.NotZero(t, sub.count(), "subscriber registered")

	sub.emit(core.JanusEvent{EventType: core.EventTaskStarted, TaskID: "t-run", TraceID: "ctx-run", Timestamp: time.Now()})
	sub.emit(core.JanusEvent{EventType: core.EventTaskStarted, TaskID: "other-task", Timestamp: time.Now()})
	sub.close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("stream must return when the event channel closes")
	}
	out := w.Body.String()
	assert.Contains(t, out, V1StateWorking)
	assert.NotContains(t, out, "TASK_STATE_UNSPECIFIED-alone")
}

func TestV1Subscribe_ClientDisconnectEndsStream(t *testing.T) {
	running := &core.Task{
		TenantID: "acme", ID: "t-idle", Status: core.TaskStatusRunning,
		Envelope: core.TaskEnvelope{Trace: core.TraceContext{TraceID: "ctx-idle"}},
	}
	sub := newChanSubscriber()
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, &mockStatusGetter{task: running}).WithEventSubscriber(sub)

	req := withAuthCtx(httptest.NewRequest(http.MethodGet, "/a2a/tasks/t-idle:subscribe", nil))
	ctx, cancel := context.WithCancel(req.Context())
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { defer close(done); gw.ServeHTTP(w, req) }()

	deadline := time.Now().Add(2 * time.Second)
	for sub.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	require.NotZero(t, sub.count())

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("stream must return on client disconnect")
	}
	sub.close()
}

func TestV1Subscribe_TerminalUpdateFallsBackToCreatedAt(t *testing.T) {
	created := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	done := &core.Task{
		TenantID: "acme", ID: "t-fb", Status: core.TaskStatusCompleted, CreatedAt: created,
		Envelope: core.TaskEnvelope{Trace: core.TraceContext{TraceID: "ctx-fb"}},
	}
	v1 := JanusTaskToV1(done)
	require.NotNil(t, v1.Status.Timestamp)
	assert.Equal(t, created.UTC(), v1.Status.Timestamp.UTC(), "zero UpdatedAt must fall back to CreatedAt")
}

func TestV1GetTask_NoStatusService(t *testing.T) {
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, nil).WithEventSubscriber(newChanSubscriber())
	req := withAuthCtx(httptest.NewRequest(http.MethodGet, "/a2a/tasks/t-1", nil))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Contains(t, w.Body.String(), "status service not configured")
}

func TestV1Routes_PostUnknownAction(t *testing.T) {
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, &mockStatusGetter{})
	req := withAuthCtx(httptest.NewRequest(http.MethodPost, "/a2a/tasks/t-1:frobnicate", nil))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestAuthTenantContextPreserved(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/a2a/task/x/status", nil)
	ctxReq := req.WithContext(context.WithValue(req.Context(), auth.TenantCtxKey, "custom-tenant"))
	assert.Equal(t, "custom-tenant", auth.TenantFromContext(ctxReq.Context()))
}
