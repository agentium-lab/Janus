package a2a

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/agentium-lab/Janus/core"
)

const serverVersion = "1.5.0"

// EventSubscriber provides access to the in-memory fanout (fast lane of the
// ADR-0004 dual-path). Implemented by handler.FanoutBroadcaster.
type EventSubscriber interface {
	Subscribe(tenantID string) <-chan core.JanusEvent
	Unsubscribe(tenantID string, ch <-chan core.JanusEvent)
}

// WithEventSubscriber injects the broadcaster for v1.0 streaming support.
func (g *Gateway) WithEventSubscriber(sub EventSubscriber) *Gateway {
	g.subscriber = sub
	return g
}

func writeV1Error(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"error": map[string]interface{}{
			"code":    status,
			"status":  code,
			"message": msg,
		},
	})
}

func writeSSEData(w http.ResponseWriter, flusher http.Flusher, resp V1StreamResponse) error {
	b, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "id: %s\ndata: %s\n\n", generateID(), b)
	flusher.Flush()
	return nil
}

func sseHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
}

// handleV1Send implements POST /a2a/message:send (REST binding, non-streaming).
func (g *Gateway) handleV1Send(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromContextOrReject(w, r)
	if !ok {
		return
	}
	var req V1SendMessageRequest
	if err := readJSONLimit(w, r, &req); err != nil {
		writeV1Error(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json")
		return
	}
	sourceAgent := r.URL.Query().Get("source_agent")
	if sourceAgent == "" {
		if sa, ok := req.Metadata["source_agent"].(string); ok {
			sourceAgent = sa
		}
	}
	if sourceAgent == "" {
		sourceAgent = "unknown"
	}
	mailboxID := mailboxFromRequest(r, req)

	task := V1MessageToTask(req, tenantID, sourceAgent, mailboxID)
	created, err := g.taskSvc.Create(r.Context(), task)
	if err != nil {
		writeV1Error(w, http.StatusInternalServerError, "INTERNAL", sanitizeMsg(err.Error()))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(V1StreamResponse{Task: JanusTaskToV1(created)})
}

func mailboxFromRequest(r *http.Request, req V1SendMessageRequest) string {
	if mb := r.URL.Query().Get("mailbox_id"); mb != "" {
		return mb
	}
	if mb, ok := req.Metadata["mailbox_id"].(string); ok && mb != "" {
		return mb
	}
	return "default"
}

// parseV1TaskAction splits "/tasks/{id}" and "/tasks/{id}:action" path forms.
// Returns taskID and action ("" for plain GetTask).
func parseV1TaskAction(rest string) (taskID, action string, ok bool) {
	rest = strings.TrimPrefix(rest, "/a2a/tasks/")
	if rest == "" {
		return "", "", false
	}
	if i := strings.Index(rest, ":"); i >= 0 {
		return rest[:i], rest[i+1:], true
	}
	return rest, "", true
}

// handleV1StreamMessage implements POST /a2a/message:stream (SendStreamingMessage).
// The SSE stream starts with a task snapshot, then statusUpdates, and closes
// after emitting the terminal state.
func (g *Gateway) handleV1StreamMessage(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromContextOrReject(w, r)
	if !ok {
		return
	}
	if g.subscriber == nil {
		writeV1Error(w, http.StatusNotImplemented, "UNSUPPORTED_OPERATION", "streaming not configured")
		return
	}
	var req V1SendMessageRequest
	if err := readJSONLimit(w, r, &req); err != nil {
		writeV1Error(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json")
		return
	}
	sourceAgent := r.URL.Query().Get("source_agent")
	if sourceAgent == "" {
		if sa, ok := req.Metadata["source_agent"].(string); ok {
			sourceAgent = sa
		}
	}
	if sourceAgent == "" {
		sourceAgent = "unknown"
	}

	task := V1MessageToTask(req, tenantID, sourceAgent, mailboxFromRequest(r, req))
	created, err := g.taskSvc.Create(r.Context(), task)
	if err != nil {
		writeV1Error(w, http.StatusInternalServerError, "INTERNAL", sanitizeMsg(err.Error()))
		return
	}

	flusher, canFlush := w.(http.Flusher)
	if !canFlush {
		writeV1Error(w, http.StatusInternalServerError, "INTERNAL", "streaming unsupported")
		return
	}
	sseHeaders(w)
	writeSSEData(w, flusher, V1StreamResponse{Task: JanusTaskToV1(created)})
	if created.Status.IsTerminal() {
		writeSSEData(w, flusher, V1StreamResponse{StatusUpdate: terminalUpdate(created)})
		return
	}
	g.streamTaskEvents(w, r, flusher, tenantID, created.ID, created.Envelope.Trace.TraceID)
}

// handleV1Subscribe implements GET /a2a/tasks/{id}:subscribe.
func (g *Gateway) handleV1Subscribe(w http.ResponseWriter, r *http.Request, taskID string) {
	tenantID, ok := tenantFromContextOrReject(w, r)
	if !ok {
		return
	}
	if g.subscriber == nil {
		writeV1Error(w, http.StatusNotImplemented, "UNSUPPORTED_OPERATION", "streaming not configured")
		return
	}
	if g.statusSvc == nil {
		writeV1Error(w, http.StatusServiceUnavailable, "UNAVAILABLE", "status service not configured")
		return
	}
	task, err := g.statusSvc.Get(r.Context(), tenantID, taskID)
	if err != nil || task == nil {
		writeV1Error(w, http.StatusNotFound, "NOT_FOUND", "task not found")
		return
	}
	flusher, canFlush := w.(http.Flusher)
	if !canFlush {
		writeV1Error(w, http.StatusInternalServerError, "INTERNAL", "streaming unsupported")
		return
	}
	sseHeaders(w)
	writeSSEData(w, flusher, V1StreamResponse{Task: JanusTaskToV1(task)})
	if task.Status.IsTerminal() {
		writeSSEData(w, flusher, V1StreamResponse{StatusUpdate: terminalUpdate(task)})
		return
	}
	g.streamTaskEvents(w, r, flusher, tenantID, task.ID, task.Envelope.Trace.TraceID)
}

// streamTaskEvents is the shared SSE pump: subscribe → translate → close on
// terminal state, client disconnect, or heartbeat timeout.
func (g *Gateway) streamTaskEvents(w http.ResponseWriter, r *http.Request, flusher http.Flusher, tenantID, taskID, contextID string) {
	ch := g.subscriber.Subscribe(tenantID)
	defer func() { g.subscriber.Unsubscribe(tenantID, ch) }()

	heartbeat := time.NewTicker(30 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			fmt.Fprintf(w, ": keep-alive\n\n")
			flusher.Flush()
		case evt, ok := <-ch:
			if !ok {
				return
			}
			if evt.TaskID != taskID {
				continue
			}
			upd := JanusEventToV1Update(evt)
			if upd == nil {
				continue
			}
			if upd.ContextID == "" {
				upd.ContextID = contextID
			}
			writeSSEData(w, flusher, V1StreamResponse{StatusUpdate: upd})
			if V1StateIsTerminal(upd.Status.State) {
				return
			}
		}
	}
}

func terminalUpdate(t *core.Task) *V1TaskStatusUpdateEvent {
	ts := t.UpdatedAt
	if ts.IsZero() {
		ts = t.CreatedAt
	}
	return &V1TaskStatusUpdateEvent{
		TaskID:    t.ID,
		ContextID: t.Envelope.Trace.TraceID,
		Status: V1TaskStatus{
			State:     JanusStatusToV1State(t.Status),
			Timestamp: &ts,
		},
	}
}

// handleV1GetTask implements GET /a2a/tasks/{id}.
func (g *Gateway) handleV1GetTask(w http.ResponseWriter, r *http.Request, taskID string) {
	tenantID, ok := tenantFromContextOrReject(w, r)
	if !ok {
		return
	}
	if g.statusSvc == nil {
		writeV1Error(w, http.StatusServiceUnavailable, "UNAVAILABLE", "status service not configured")
		return
	}
	task, err := g.statusSvc.Get(r.Context(), tenantID, taskID)
	if err != nil || task == nil {
		writeV1Error(w, http.StatusNotFound, "NOT_FOUND", "task not found")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(V1StreamResponse{Task: JanusTaskToV1(task)})
}

// handleV1Cancel implements POST /a2a/tasks/{id}:cancel.
func (g *Gateway) handleV1Cancel(w http.ResponseWriter, r *http.Request, taskID string) {
	tenantID, ok := tenantFromContextOrReject(w, r)
	if !ok {
		return
	}
	if err := g.taskSvc.Cancel(r.Context(), tenantID, taskID); err != nil {
		writeV1Error(w, http.StatusInternalServerError, "INTERNAL", sanitizeMsg(err.Error()))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(V1StreamResponse{StatusUpdate: &V1TaskStatusUpdateEvent{
		TaskID: taskID,
		Status: V1TaskStatus{State: V1StateCanceled, Timestamp: timePtr(time.Now().UTC())},
	}})
}

func timePtr(t time.Time) *time.Time { return &t }

// serveV1Routes dispatches the v1.0 REST binding surface. Legacy v0.x routes
// (task/send, task/{id}/status, jsonrpc, agent/card) remain for compat.
func (g *Gateway) serveV1Routes(w http.ResponseWriter, r *http.Request) bool {
	path := r.URL.Path
	switch {
	case path == "/a2a/message:stream" && r.Method == http.MethodPost:
		g.handleV1StreamMessage(w, r)
		return true
	case path == "/a2a/message:send" && r.Method == http.MethodPost:
		g.handleV1Send(w, r)
		return true
	case strings.HasPrefix(path, "/a2a/tasks/") && r.Method == http.MethodGet:
		taskID, action, ok := parseV1TaskAction(path)
		if !ok {
			http.NotFound(w, r)
			return true
		}
		if action == "subscribe" {
			g.handleV1Subscribe(w, r, taskID)
			return true
		}
		if action == "" {
			g.handleV1GetTask(w, r, taskID)
			return true
		}
		http.NotFound(w, r)
		return true
	case strings.HasPrefix(path, "/a2a/tasks/") && r.Method == http.MethodPost:
		taskID, action, ok := parseV1TaskAction(path)
		if ok && action == "cancel" {
			g.handleV1Cancel(w, r, taskID)
			return true
		}
		http.NotFound(w, r)
		return true
	}
	return false
}

// AgentCardV1Handler serves the v1.0 well-known discovery document with
// supportedInterfaces. Discovery stays unauthenticated by design.
func AgentCardV1Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		base := fmt.Sprintf("%s://%s", scheme, r.Host)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"name":        "Janus",
			"description": "Durable agent task broker with governance, budgeting and audit.",
			"version":     serverVersion,
			"supportedInterfaces": []map[string]string{{
				"url":             base + "/a2a/",
				"protocolBinding": "HTTP+JSON",
				"protocolVersion": "1.0",
			}},
			"capabilities": map[string]interface{}{
				"streaming":         true,
				"pushNotifications": false,
			},
			"defaultInputModes":  []string{"application/json"},
			"defaultOutputModes": []string{"application/json"},
			"skills": []map[string]string{{
				"id":          "task-broker",
				"name":        "Durable Task Broker",
				"description": "Route, govern and audit agent-to-agent task handoffs.",
			}},
			"securitySchemes": map[string]interface{}{
				"apiKey": map[string]string{
					"type": "apiKey",
					"in":   "header",
					"name": "X-API-Key",
				},
			},
			"securityRequirements": []map[string][]string{{"apiKey": {}}},
		})
	})
}
