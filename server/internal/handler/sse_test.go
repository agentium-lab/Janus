package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/agentium-lab/Janus/core"
)

// fakeBroadcaster implements the EventBroadcaster interface for SSE tests.
type fakeSSEBroadcaster struct {
	ch chan core.JanusEvent
}

func (f *fakeSSEBroadcaster) Subscribe(tenantID string) <-chan core.JanusEvent {
	return f.ch
}

func (f *fakeSSEBroadcaster) Unsubscribe(tenantID string, ch <-chan core.JanusEvent) {}

func (f *fakeSSEBroadcaster) push(evt core.JanusEvent) {
	f.ch <- evt
}

func TestSSEHandler_StreamsProgressAndClosesOnTerminal(t *testing.T) {
	bc := &fakeSSEBroadcaster{ch: make(chan core.JanusEvent, 16)}
	h := NewSSEHandler(bc)

	// Fake progress + terminal event payloads.
	progPayload, _ := json.Marshal(core.TaskProgress{Message: "working", Percent: intPtr(50)})
	termPayload, _ := json.Marshal(map[string]string{"status": "completed"})

	go func() {
		time.Sleep(50 * time.Millisecond)
		bc.push(core.JanusEvent{EventType: core.EventTaskProgress, TenantID: "acme", TaskID: "task-1", EventID: "e1", Payload: progPayload})
		bc.push(core.JanusEvent{EventType: core.EventTaskCompleted, TenantID: "acme", TaskID: "task-1", EventID: "e2", Payload: termPayload})
	}()

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/tasks/task-1/stream", nil)
	rec := httptest.NewRecorder()

	// Use a timeout to avoid hanging if close doesn't work.
	done := make(chan struct{})
	go func() {
		h.ServeHTTP(rec, req)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("SSE handler did not close after terminal event")
	}

	body := rec.Body.String()
	if !strings.Contains(rec.Header().Get("Content-Type"), "text/event-stream") {
		t.Fatalf("content-type: %s", rec.Header().Get("Content-Type"))
	}
	if !strings.Contains(body, "event: task.progress") {
		t.Fatal("progress event not received")
	}
	if !strings.Contains(body, `"message":"working"`) {
		t.Fatal("progress payload missing")
	}
	if !strings.Contains(body, "event: task.completed") {
		t.Fatal("terminal event not received")
	}
}

func TestSSEHandler_FiltersOtherTasks(t *testing.T) {
	bc := &fakeSSEBroadcaster{ch: make(chan core.JanusEvent, 16)}
	h := NewSSEHandler(bc)

	otherPayload, _ := json.Marshal(core.TaskProgress{Message: "other task"})
	go func() {
		time.Sleep(50 * time.Millisecond)
		// Event for a DIFFERENT task — should be filtered out.
		bc.push(core.JanusEvent{EventType: core.EventTaskProgress, TenantID: "acme", TaskID: "task-999", EventID: "e0", Payload: otherPayload})
		// Terminal for OUR task closes the stream.
		bc.push(core.JanusEvent{EventType: core.EventTaskCompleted, TenantID: "acme", TaskID: "task-1", EventID: "e1"})
	}()

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/tasks/task-1/stream", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if strings.Contains(rec.Body.String(), "other task") {
		t.Fatal("event from different task leaked into stream")
	}
}

func intPtr(i int) *int { return &i }
