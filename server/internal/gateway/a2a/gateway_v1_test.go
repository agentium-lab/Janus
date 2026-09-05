package a2a

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agentium-lab/Janus/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockSubscriber struct {
	mu    sync.Mutex
	chans map[string][]chan core.JanusEvent
}

func newMockSubscriber() *mockSubscriber {
	return &mockSubscriber{chans: make(map[string][]chan core.JanusEvent)}
}

func (m *mockSubscriber) Subscribe(tenantID string) <-chan core.JanusEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch := make(chan core.JanusEvent, 16)
	m.chans[tenantID] = append(m.chans[tenantID], ch)
	return ch
}

func (m *mockSubscriber) Unsubscribe(tenantID string, ch <-chan core.JanusEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	subs := m.chans[tenantID]
	for i, c := range subs {
		if c == ch {
			m.chans[tenantID] = append(subs[:i], subs[i+1:]...)
			break
		}
	}
}

func (m *mockSubscriber) count(tenantID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.chans[tenantID])
}

func (m *mockSubscriber) emit(tenantID string, evt core.JanusEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ch := range m.chans[tenantID] {
		ch <- evt
	}
}

func (m *mockSubscriber) closeAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, subs := range m.chans {
		for _, ch := range subs {
			close(ch)
		}
	}
}

func completedTask(id string) *core.Task {
	now := time.Now().UTC()
	return &core.Task{
		TenantID: "acme", ID: id, Status: core.TaskStatusCompleted, ResultRef: "s3://results/1",
		CreatedAt: now, UpdatedAt: now,
		Envelope: core.TaskEnvelope{Trace: core.TraceContext{TraceID: "ctx-" + id}},
	}
}

func TestAgentCardV1Handler_Conformance(t *testing.T) {
	h := AgentCardV1Handler()
	req := httptest.NewRequest(http.MethodGet, "/.well-known/agent-card.json", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	var card map[string]interface{}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &card))

	ifaces, ok := card["supportedInterfaces"].([]interface{})
	require.True(t, ok, "supportedInterfaces required in v1.0")
	require.Len(t, ifaces, 1)
	iface := ifaces[0].(map[string]interface{})
	assert.Equal(t, "HTTP+JSON", iface["protocolBinding"])
	assert.Equal(t, "1.0", iface["protocolVersion"])
	assert.Contains(t, iface["url"], "/a2a/")

	caps := card["capabilities"].(map[string]interface{})
	assert.Equal(t, true, caps["streaming"])

	assert.NotNil(t, card["defaultInputModes"])
	assert.NotNil(t, card["defaultOutputModes"])
	assert.NotEmpty(t, card["skills"])
	assert.NotNil(t, card["securitySchemes"])
	assert.NotNil(t, card["securityRequirements"])
}

func TestV1StreamResponse_MarshalOneof(t *testing.T) {
	b, err := json.Marshal(V1StreamResponse{Task: &V1Task{ID: "t1"}})
	require.NoError(t, err)
	assert.Equal(t, `{"task":{"id":"t1","contextId":"","status":{"state":""}}}`, string(b))

	b, err = json.Marshal(V1StreamResponse{StatusUpdate: &V1TaskStatusUpdateEvent{TaskID: "t1", Status: V1TaskStatus{State: V1StateWorking}}})
	require.NoError(t, err)
	var m map[string]interface{}
	require.NoError(t, json.Unmarshal(b, &m))
	assert.Len(t, m, 1)
	assert.Contains(t, m, "statusUpdate")

	_, err = json.Marshal(V1StreamResponse{})
	assert.Error(t, err)
}

func TestJanusStatusToV1State_AllStatuses(t *testing.T) {
	cases := map[core.TaskStatus]string{
		core.TaskStatusCreated:         V1StateSubmitted,
		core.TaskStatusQueued:          V1StateSubmitted,
		core.TaskStatusClaimed:         V1StateSubmitted,
		core.TaskStatusRetryScheduled:  V1StateSubmitted,
		core.TaskStatusRunning:         V1StateWorking,
		core.TaskStatusBlocked:         V1StateInputRequired,
		core.TaskStatusApprovalPending: V1StateInputRequired,
		core.TaskStatusCompleted:       V1StateCompleted,
		core.TaskStatusFailed:          V1StateFailed,
		core.TaskStatusDeadLettered:    V1StateFailed,
		core.TaskStatusExpired:         V1StateFailed,
		core.TaskStatusCancelled:       V1StateCanceled,
	}
	for in, want := range cases {
		assert.Equal(t, want, JanusStatusToV1State(in), string(in))
	}
	assert.Equal(t, V1StateUnspecified, JanusStatusToV1State(core.TaskStatus("bogus")))
}

func TestV1StateIsTerminal(t *testing.T) {
	assert.True(t, V1StateIsTerminal(V1StateCompleted))
	assert.True(t, V1StateIsTerminal(V1StateFailed))
	assert.True(t, V1StateIsTerminal(V1StateCanceled))
	assert.True(t, V1StateIsTerminal(V1StateRejected))
	assert.False(t, V1StateIsTerminal(V1StateWorking))
	assert.False(t, V1StateIsTerminal(V1StateSubmitted))
	assert.False(t, V1StateIsTerminal(V1StateInputRequired))
}

func TestJanusEventToV1Update_Coverage(t *testing.T) {
	ts := time.Now().UTC()
	prog := core.TaskProgress{Message: "half done"}
	payload, _ := json.Marshal(prog)

	cases := []struct {
		evt  core.JanusEvent
		want string
	}{
		{core.JanusEvent{EventType: core.EventTaskCreated, Timestamp: ts}, V1StateSubmitted},
		{core.JanusEvent{EventType: core.EventTaskQueued, Timestamp: ts}, V1StateSubmitted},
		{core.JanusEvent{EventType: core.EventTaskClaimed, Timestamp: ts}, V1StateSubmitted},
		{core.JanusEvent{EventType: core.EventTaskRetryScheduled, Timestamp: ts}, V1StateSubmitted},
		{core.JanusEvent{EventType: core.EventTaskStarted, Timestamp: ts}, V1StateWorking},
		{core.JanusEvent{EventType: core.EventTaskHeartbeat, Timestamp: ts}, V1StateWorking},
		{core.JanusEvent{EventType: core.EventTaskProgress, Timestamp: ts, Payload: payload}, V1StateWorking},
		{core.JanusEvent{EventType: core.EventTaskBlocked, Timestamp: ts}, V1StateInputRequired},
		{core.JanusEvent{EventType: core.EventTaskApprovalPending, Timestamp: ts}, V1StateInputRequired},
		{core.JanusEvent{EventType: core.EventTaskCompleted, Timestamp: ts}, V1StateCompleted},
		{core.JanusEvent{EventType: core.EventTaskFailed, Timestamp: ts}, V1StateFailed},
		{core.JanusEvent{EventType: core.EventTaskDeadLettered, Timestamp: ts}, V1StateFailed},
		{core.JanusEvent{EventType: core.EventTaskExpired, Timestamp: ts}, V1StateFailed},
		{core.JanusEvent{EventType: core.EventTaskCancelled, Timestamp: ts}, V1StateCanceled},
		{core.JanusEvent{EventType: core.EventAgentOnline, Timestamp: ts}, ""},
	}
	for _, c := range cases {
		upd := JanusEventToV1Update(c.evt)
		if c.want == "" {
			assert.Nil(t, upd, string(c.evt.EventType))
			continue
		}
		require.NotNil(t, upd, string(c.evt.EventType))
		assert.Equal(t, c.want, upd.Status.State, string(c.evt.EventType))
	}

	progEvt := core.JanusEvent{EventType: core.EventTaskProgress, Timestamp: ts, Payload: payload}
	upd := JanusEventToV1Update(progEvt)
	require.NotNil(t, upd.Status.Message)
	assert.Equal(t, "ROLE_AGENT", upd.Status.Message.Role)
	assert.Equal(t, "half done", upd.Status.Message.Parts[0].Text)
}

func TestV1MessageToTask_Variants(t *testing.T) {
	req := V1SendMessageRequest{
		Message: V1Message{
			MessageID: "m1", Role: "ROLE_USER",
			Parts: []V1Part{{Text: "hello world"}},
		},
	}
	task := V1MessageToTask(req, "acme", "web", "default")
	assert.Equal(t, "acme", task.TenantID)
	assert.Equal(t, "web", task.SourceAgent)
	assert.Equal(t, core.TaskStatusCreated, task.Status)
	assert.Equal(t, "hello world", task.Envelope.Payload.Content)
	assert.Equal(t, "a2a_message", task.Envelope.Payload.Type)
	assert.NotEmpty(t, task.ID)
	assert.NotEmpty(t, task.Envelope.Trace.TraceID)

	reqStructured := V1SendMessageRequest{
		Message:  V1Message{MessageID: "m2", Role: "ROLE_USER", Parts: []V1Part{{Data: map[string]interface{}{"order": "123"}}}},
		Metadata: map[string]interface{}{"mailbox_id": "vip"},
	}
	task2 := V1MessageToTask(reqStructured, "acme", "web", "default")
	assert.Equal(t, "vip", task2.MailboxID)
	assert.Equal(t, "a2a_structured", task2.Envelope.Payload.Type)
	assert.Contains(t, task2.Envelope.Payload.Content, "123")

	reqCtx := V1SendMessageRequest{Message: V1Message{ContextID: "fixed-ctx"}}
	task3 := V1MessageToTask(reqCtx, "acme", "web", "default")
	assert.Equal(t, "fixed-ctx", task3.Envelope.Trace.TraceID)
}

func TestGatewayV1_MessageSend(t *testing.T) {
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, &mockStatusGetter{})
	body := `{"message":{"role":"ROLE_USER","parts":[{"text":"hi"}],"messageId":"m1"}}`
	req := withAuthCtx(httptest.NewRequest(http.MethodPost, "/a2a/message:send", strings.NewReader(body)))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	var resp map[string]interface{}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	task := resp["task"].(map[string]interface{})
	assert.Equal(t, V1StateSubmitted, task["status"].(map[string]interface{})["state"])
	assert.NotEmpty(t, task["id"])
}

func TestGatewayV1_MessageSend_BadJSON(t *testing.T) {
	gw := NewGateway(&mockAgentRegistrar{}, &mockTaskCreator{})
	req := withAuthCtx(httptest.NewRequest(http.MethodPost, "/a2a/message:send", strings.NewReader("{")))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	var resp map[string]interface{}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	errObj := resp["error"].(map[string]interface{})
	assert.Equal(t, float64(http.StatusBadRequest), errObj["code"])
	assert.Equal(t, "INVALID_ARGUMENT", errObj["status"])
}

func TestGatewayV1_MessageSend_MetadataSource(t *testing.T) {
	gw := NewGateway(&mockAgentRegistrar{}, &mockTaskCreator{})
	body := `{"message":{"role":"ROLE_USER","parts":[{"text":"hi"}]},"metadata":{"source_agent":"crm","mailbox_id":"vip"}}`
	req := withAuthCtx(httptest.NewRequest(http.MethodPost, "/a2a/message:send", strings.NewReader(body)))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
}

func TestGatewayV1_MessageStream_Lifecycle(t *testing.T) {
	sub := newMockSubscriber()
	tc := &mockTaskCreator{}
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, tc, &mockStatusGetter{}).WithEventSubscriber(sub)

	body := `{"message":{"role":"ROLE_USER","parts":[{"text":"stream me"}],"messageId":"m1"}}`
	req := withAuthCtx(httptest.NewRequest(http.MethodPost, "/a2a/message:stream", strings.NewReader(body)))
	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()

	streamDone := make(chan struct{})
	go func() {
		defer close(streamDone)
		gw.ServeHTTP(w, req)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for sub.count("acme") == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	require.NotZero(t, sub.count("acme"), "subscriber registered")

	sub.emit("acme", core.JanusEvent{EventType: core.EventTaskStarted, TaskID: "unknown-task", Timestamp: time.Now()})
	sub.emit("acme", core.JanusEvent{EventType: core.EventAgentOnline, TaskID: "", Timestamp: time.Now()})
	sub.emit("acme", core.JanusEvent{EventType: core.EventTaskStarted, TaskID: taskIDOf(tc), Timestamp: time.Now()})
	sub.emit("acme", core.JanusEvent{EventType: core.EventTaskProgress, TaskID: taskIDOf(tc), Timestamp: time.Now(),
		Payload: []byte(`{"message":"step 1 done","percent":50}`)})
	sub.emit("acme", core.JanusEvent{EventType: core.EventTaskCompleted, TaskID: taskIDOf(tc), Timestamp: time.Now()})

	select {
	case <-streamDone:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("stream did not close after terminal event")
	}

	out := w.Body.String()
	assert.Contains(t, w.Header().Get("Content-Type"), "text/event-stream")
	assert.Contains(t, out, `"task":`)
	assert.Contains(t, out, `"statusUpdate":`)
	assert.Contains(t, out, V1StateWorking)
	assert.Contains(t, out, V1StateCompleted)
	assert.Contains(t, out, "step 1 done")
	assert.Contains(t, out, "data: ")
	sub.closeAll()
}

func taskIDOf(tc *mockTaskCreator) string {
	if tc.task != nil {
		return tc.captured().ID
	}
	return ""
}

func TestGatewayV1_MessageStream_NotConfigured(t *testing.T) {
	gw := NewGateway(&mockAgentRegistrar{}, &mockTaskCreator{})
	req := withAuthCtx(httptest.NewRequest(http.MethodPost, "/a2a/message:stream", strings.NewReader(`{}`)))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotImplemented, w.Code)
}

func TestGatewayV1_Subscribe_TerminalTaskImmediateClose(t *testing.T) {
	// Spec (proto + 3.1.6/9.4.6/10.4.6): subscribing to a terminal task
	// returns UnsupportedOperationError (HTTP 400), not a snapshot stream.
	sub := newMockSubscriber()
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, &mockStatusGetter{task: completedTask("t-done")}).WithEventSubscriber(sub)

	req := withAuthCtx(httptest.NewRequest(http.MethodGet, "/a2a/tasks/t-done:subscribe", nil))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "UNSUPPORTED_OPERATION")
}

func TestGatewayV1_Subscribe_NotFound(t *testing.T) {
	sub := newMockSubscriber()
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, &mockStatusGetter{err: fmt.Errorf("no rows")}).WithEventSubscriber(sub)
	req := withAuthCtx(httptest.NewRequest(http.MethodGet, "/a2a/tasks/nope:subscribe", nil))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestGatewayV1_Subscribe_NoStatusService(t *testing.T) {
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, nil).WithEventSubscriber(newMockSubscriber())
	req := withAuthCtx(httptest.NewRequest(http.MethodGet, "/a2a/tasks/x:subscribe", nil))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}

func TestGatewayV1_GetTask(t *testing.T) {
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, &mockStatusGetter{task: completedTask("t-1")})
	req := withAuthCtx(httptest.NewRequest(http.MethodGet, "/a2a/tasks/t-1", nil))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var resp map[string]interface{}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	task := resp["task"].(map[string]interface{})
	assert.Equal(t, "t-1", task["id"])
}

func TestGatewayV1_GetTask_NotFound(t *testing.T) {
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, &mockStatusGetter{})
	req := withAuthCtx(httptest.NewRequest(http.MethodGet, "/a2a/tasks/missing", nil))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestGatewayV1_Cancel(t *testing.T) {
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, &mockStatusGetter{})
	req := withAuthCtx(httptest.NewRequest(http.MethodPost, "/a2a/tasks/t-1:cancel", nil))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var resp map[string]interface{}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	upd := resp["statusUpdate"].(map[string]interface{})
	assert.Equal(t, V1StateCanceled, upd["status"].(map[string]interface{})["state"])
}

func TestGatewayV1_Cancel_Error(t *testing.T) {
	tc := &mockTaskCreator{err: fmt.Errorf("db: connection refused")}
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, tc, &mockStatusGetter{})
	req := withAuthCtx(httptest.NewRequest(http.MethodPost, "/a2a/tasks/t-1:cancel", nil))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Contains(t, w.Body.String(), "internal error", "must sanitize internal leak")
}

func TestGatewayV1_Routes_UnknownActionAndMethod(t *testing.T) {
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, &mockStatusGetter{})
	req := withAuthCtx(httptest.NewRequest(http.MethodGet, "/a2a/tasks/t-1:bogus", nil))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)

	req2 := withAuthCtx(httptest.NewRequest(http.MethodPost, "/a2a/tasks/", nil))
	w2 := httptest.NewRecorder()
	gw.ServeHTTP(w2, req2)
	assert.Equal(t, http.StatusNotFound, w2.Code)
}

func TestGatewayV1_TenantRequired(t *testing.T) {
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, &mockStatusGetter{})
	req := httptest.NewRequest(http.MethodPost, "/a2a/message:send", strings.NewReader(`{}`))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestParseV1TaskAction(t *testing.T) {
	id, action, ok := parseV1TaskAction("/a2a/tasks/abc:subscribe")
	assert.True(t, ok)
	assert.Equal(t, "abc", id)
	assert.Equal(t, "subscribe", action)

	id, action, ok = parseV1TaskAction("/a2a/tasks/abc")
	assert.True(t, ok)
	assert.Equal(t, "abc", id)
	assert.Equal(t, "", action)

	_, _, ok = parseV1TaskAction("/a2a/tasks/")
	assert.False(t, ok)
}

func TestJanusTaskToV1_Snapshot(t *testing.T) {
	assert.Nil(t, JanusTaskToV1(nil))

	now := time.Now().UTC()
	tk := &core.Task{TenantID: "acme", ID: "t9", Status: core.TaskStatusRunning, CreatedAt: now}
	v1 := JanusTaskToV1(tk)
	assert.Equal(t, "t9", v1.ID)
	assert.Equal(t, V1StateWorking, v1.Status.State)
	assert.Empty(t, v1.Artifacts)
	assert.NotNil(t, v1.Status.Timestamp)

	tk.ResultRef = "ref-1"
	v1 = JanusTaskToV1(tk)
	require.Len(t, v1.Artifacts, 1)
	assert.Equal(t, "result", v1.Artifacts[0].ArtifactID)
}
