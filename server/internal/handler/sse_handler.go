package handler

import (
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/agentium-lab/Janus/core"
)

// SSEHandler serves per-task Server-Sent Events streams. Events are fanned
// out from the in-memory broadcaster; the stream closes when the task reaches
// a terminal state.
type SSEHandler struct {
	broadcaster EventBroadcaster
	subscribe   func() (<-chan core.JanusEvent, func())
}

func NewSSEHandler(broadcaster EventBroadcaster) *SSEHandler {
	return &SSEHandler{broadcaster: broadcaster}
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

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	tenantID := tenantIDFromPath(r.URL.Path)
	ch := h.broadcaster.Subscribe(tenantID)
	defer func() { h.broadcaster.Unsubscribe(tenantID, ch) }()

	// Heartbeat comment every 30s keeps intermediaries from timing out.
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
			// Filter: only events for this task.
			if evt.TaskID != taskID {
				continue
			}
			// Send the raw payload directly (it's already JSON); wrapping the
			// whole event would base64-encode the []byte Payload field.
			sseData := constructSSEData(evt)
			fmt.Fprintf(w, "event: %s\nid: %s\ndata: %s\n\n", evt.EventType, evt.EventID, sseData)
			flusher.Flush()

			// Auto-close on terminal states.
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

var _ = log.Println

// constructSSEData builds a flat JSON object from the event with the payload
// embedded raw (not base64).
func constructSSEData(evt core.JanusEvent) string {
	var b strings.Builder
	b.WriteString(`{"event_type":`)
	b.WriteString(`"` + string(evt.EventType) + `"`)
	if evt.TaskID != "" {
		b.WriteString(`,"task_id":"` + evt.TaskID + `"`)
	}
	if evt.SourceAgent != "" {
		b.WriteString(`,"source_agent":"` + evt.SourceAgent + `"`)
	}
	if len(evt.Payload) > 0 {
		b.WriteString(`,"payload":`)
		b.Write(evt.Payload)
	}
	b.WriteString(`}`)
	return b.String()
}
