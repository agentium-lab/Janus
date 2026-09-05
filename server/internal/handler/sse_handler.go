package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
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

	taskID := lastSegment(strings.TrimSuffix(r.URL.Path, "/stream"))
	if taskID == "" || taskID == "tasks" {
		writeError(w, http.StatusBadRequest, "missing task id")
		return
	}
	tenantID := tenantIDFromPath(r.URL.Path)

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

func constructSSEData(evt core.JanusEvent) string {
	var b strings.Builder
	b.WriteString(`{"event_type":"`)
	b.WriteString(string(evt.EventType))
	b.WriteString(`"`)
	if evt.TaskID != "" {
		b.WriteString(`,"task_id":"`)
		b.WriteString(evt.TaskID)
		b.WriteString(`"`)
	}
	if evt.SourceAgent != "" {
		b.WriteString(`,"source_agent":"`)
		b.WriteString(evt.SourceAgent)
		b.WriteString(`"`)
	}
	if len(evt.Payload) > 0 {
		b.WriteString(`,"payload":`)
		b.Write(evt.Payload)
	}
	b.WriteString(`}`)
	return b.String()
}
