package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/agentium-lab/Janus/core"
)

// TaskStatusChecker lets the SSE handler detect already-terminal tasks before
// subscribing, so late subscribers get an immediate close instead of a hang.
type TaskStatusChecker interface {
	Get(ctx context.Context, tenantID, taskID string) (*core.Task, error)
}

// SSEHandler serves per-task Server-Sent Events streams. Events are fanned
// out from the in-memory broadcaster; the stream closes when the task reaches
// a terminal state. Already-terminal tasks close immediately.
type SSEHandler struct {
	broadcaster EventBroadcaster
	statusCheck TaskStatusChecker
}

func NewSSEHandler(broadcaster EventBroadcaster) *SSEHandler {
	return &SSEHandler{broadcaster: broadcaster}
}

// WithStatusChecker injects a task status checker for terminal-state pre-check.
func (h *SSEHandler) WithStatusChecker(tc TaskStatusChecker) *SSEHandler {
	h.statusCheck = tc
	return h
}

func (h *SSEHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, `{"error":"streaming unsupported"}`, http.StatusInternalServerError)
		return
	}

	taskID := r.PathValue("taskID")
	if taskID == "" {
		writeError(w, http.StatusBadRequest, "missing task id")
		return
	}
	tenantID := r.PathValue("tenantID")

	// Subscribe BEFORE reading status: if the task transitions to terminal
	// between our status read and the subscribe, the terminal event would be
	// missed and the client would hang. With this ordering a terminal read
	// may duplicate an event already queued on ch — harmless, the stream is
	// about to close.
	ch := h.broadcaster.Subscribe(tenantID)
	defer func() { h.broadcaster.Unsubscribe(tenantID, ch) }()

	if h.statusCheck != nil {
		if task, err := h.statusCheck.Get(r.Context(), tenantID, taskID); err == nil && task != nil {
			if task.Status.IsTerminal() {
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header().Set("Cache-Control", "no-cache")
				payload, _ := json.Marshal(map[string]string{"status": string(task.Status)})
				fmt.Fprintf(w, "event: task.%s\ndata: %s\n\n", task.Status, payload)
				return
			}
		}
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("Retry", "3000")

	heartbeat := time.NewTicker(30 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			fmt.Fprintf(w, ": heartbeat\n\n")
			flusher.Flush()
		case evt, ok := <-ch:
			if !ok {
				return
			}
			if evt.TaskID != taskID {
				continue
			}
			fmt.Fprintf(w, "event: %s\nid: %s\ndata: %s\n\n",
				evt.EventType, evt.EventID, constructSSEData(evt))
			flusher.Flush()
			if isTerminalTaskEvent(evt.EventType) {
				return
			}
		}
	}
}

func isTerminalTaskEvent(t core.EventType) bool {
	switch t {
	case core.EventTaskCompleted, core.EventTaskFailed,
		core.EventTaskCancelled, core.EventTaskDeadLettered, core.EventTaskExpired:
		return true
	}
	return false
}

// sseEvent is the wire shape of one SSE data frame; json.Marshal escapes
// task ids and agent names (a hand-built string let quotes/newlines in an
// id break the frame — the client's JSON.parse died mid-stream).
type sseEvent struct {
	EventType  string          `json:"event_type"`
	TaskID     string          `json:"task_id,omitempty"`
	SourceAgent string         `json:"source_agent,omitempty"`
	Payload    json.RawMessage `json:"payload,omitempty"`
}

func constructSSEData(evt core.JanusEvent) string {
	ev := sseEvent{
		EventType:   string(evt.EventType),
		TaskID:      evt.TaskID,
		SourceAgent: evt.SourceAgent,
	}
	if len(evt.Payload) > 0 {
		ev.Payload = json.RawMessage(evt.Payload)
	}
	b, err := json.Marshal(ev)
	if err != nil {
		// marshal of plain strings cannot fail; keep a valid frame regardless
		return `{"event_type":"` + string(evt.EventType) + `"}`
	}
	return string(b)
}
