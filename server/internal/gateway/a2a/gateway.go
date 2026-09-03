package a2a

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/auth"
)

const maxBodyBytes = 10 << 20

var internalLeakPatterns = []string{
	"sql:", "pq:", "pgx:", "connection refused", "dial tcp",
	"no rows in result set", "context deadline exceeded",
	"panic:", "goroutine", "runtime error",
	"driver:", "sslmode", "postgres://", "host=",
	"port=", "dbname=", "redis:", "nats:",
	"failed to connect", "lookup", "i/o timeout",
	"server closed", "duplicate key",
}

func sanitizeMsg(msg string) string {
	lower := strings.ToLower(msg)
	for _, p := range internalLeakPatterns {
		if strings.Contains(lower, p) {
			return "internal error"
		}
	}
	if len(msg) > 200 {
		return "internal error"
	}
	return msg
}

func readJSONLimit(w http.ResponseWriter, r *http.Request, v interface{}) error {
	if r.Body == nil {
		return io.EOF
	}
	defer r.Body.Close()
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	return json.NewDecoder(r.Body).Decode(v)
}

type AgentRegistrar interface {
	Register(ctx context.Context, agent core.Agent) error
}

type TaskCreator interface {
	Create(ctx context.Context, task core.Task) (*core.Task, error)
	Cancel(ctx context.Context, tenantID, taskID string) error
}

type TaskStatusGetter interface {
	Get(ctx context.Context, tenantID, taskID string) (*core.Task, error)
}

type Gateway struct {
	agentSvc     AgentRegistrar
	taskSvc      TaskCreator
	statusSvc    TaskStatusGetter
	taskStreamer TaskStreamer
}

func NewGateway(agentSvc AgentRegistrar, taskSvc TaskCreator) *Gateway {
	return &Gateway{agentSvc: agentSvc, taskSvc: taskSvc}
}

// WithTaskStreamer injects the SSE handler for streaming support.
func (g *Gateway) WithTaskStreamer(ts TaskStreamer) *Gateway {
	g.taskStreamer = ts
	return g
}

func NewGatewayWithStatus(agentSvc AgentRegistrar, taskSvc TaskCreator, statusSvc TaskStatusGetter) *Gateway {
	return &Gateway{agentSvc: agentSvc, taskSvc: taskSvc, statusSvc: statusSvc}
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/a2a/agent/card" && r.Method == http.MethodPost:
		g.handleAgentCard(w, r)
	case r.URL.Path == "/a2a/task/stream" && r.Method == http.MethodGet:
		g.handleTaskStream(w, r)
	case r.URL.Path == "/a2a/task/send" && r.Method == http.MethodPost:
		g.handleTaskSend(w, r)
	case r.URL.Path == "/a2a/jsonrpc" && r.Method == http.MethodPost:
		g.handleJSONRPC(w, r)
	case strings.HasPrefix(r.URL.Path, "/a2a/task/") && strings.HasSuffix(r.URL.Path, "/status") && r.Method == http.MethodGet:
		g.handleTaskStatus(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (g *Gateway) handleAgentCard(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromContextOrReject(w, r)
	if !ok {
		return
	}

	var card AgentCard
	if err := readJSONLimit(w, r, &card); err != nil {
		writeA2AError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json")
		return
	}

	agent := CardToAgent(tenantID, card)
	if err := g.agentSvc.Register(r.Context(), agent); err != nil {
		writeA2AError(w, http.StatusInternalServerError, "INTERNAL", sanitizeMsg(err.Error()))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":   "registered",
		"agent_id": agent.ID,
	})
}

func (g *Gateway) handleTaskSend(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromContextOrReject(w, r)
	if !ok {
		return
	}
	sourceAgent := r.URL.Query().Get("source_agent")
	if sourceAgent == "" {
		sourceAgent = "unknown"
	}
	mailboxID := r.URL.Query().Get("mailbox_id")
	if mailboxID == "" {
		mailboxID = "default"
	}

	var req SendMessageRequest
	if err := readJSONLimit(w, r, &req); err != nil {
		writeA2AError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json")
		return
	}

	task := MessageToTask(req, tenantID, sourceAgent, mailboxID)
	if _, err := g.taskSvc.Create(r.Context(), task); err != nil {
		writeA2AError(w, http.StatusInternalServerError, "INTERNAL", sanitizeMsg(err.Error()))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"task_id": task.ID,
		"status":  string(task.Status),
	})
}

func (g *Gateway) handleTaskStatus(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromContextOrReject(w, r)
	if !ok {
		return
	}

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 3 {
		writeA2AError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid task status path")
		return
	}
	taskID := parts[2]

	if g.statusSvc == nil {
		writeA2AError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "status service not configured")
		return
	}

	task, err := g.statusSvc.Get(r.Context(), tenantID, taskID)
	if err != nil {
		writeA2AError(w, http.StatusNotFound, "NOT_FOUND", sanitizeMsg(err.Error()))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"task_id":       task.ID,
		"status":        string(task.Status),
		"attempt_count": task.AttemptCount,
		"result_ref":    task.ResultRef,
	})
}

type jsonRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      interface{}     `json:"id"`
}

type jsonRPCResponse struct {
	JSONRPC string      `json:"jsonrpc"`
	Result  interface{} `json:"result,omitempty"`
	Error   *jsonRPCErr `json:"error,omitempty"`
	ID      interface{} `json:"id"`
}

type jsonRPCErr struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (g *Gateway) handleJSONRPC(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromContextOrReject(w, r)
	if !ok {
		return
	}
	sourceAgent := r.URL.Query().Get("source_agent")
	if sourceAgent == "" {
		sourceAgent = "unknown"
	}

	var req jsonRPCRequest
	if err := readJSONLimit(w, r, &req); err != nil {
		writeA2AError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json")
		return
	}

	switch req.Method {
	case "task/send", "message/send":
		var msg SendMessageRequest
		if err := json.Unmarshal(req.Params, &msg); err != nil {
			writeJSONRPCError(w, req.ID, -32602, "invalid params")
			return
		}
		mailboxID := r.URL.Query().Get("mailbox_id")
		if mailboxID == "" {
			mailboxID = "default"
		}
		task := MessageToTask(msg, tenantID, sourceAgent, mailboxID)
		result, err := g.taskSvc.Create(r.Context(), task)
		if err != nil {
			writeJSONRPCError(w, req.ID, -32603, sanitizeMsg(err.Error()))
			return
		}
		writeJSONRPCResult(w, req.ID, map[string]string{
			"task_id": result.ID, "status": string(result.Status),
		})

	case "task/get", "tasks/get":
		var params struct {
			TaskID string `json:"task_id"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			writeJSONRPCError(w, req.ID, -32602, "invalid params")
			return
		}
		if g.statusSvc == nil {
			writeJSONRPCError(w, req.ID, -32603, "status service not configured")
			return
		}
		task, err := g.statusSvc.Get(r.Context(), tenantID, params.TaskID)
		if err != nil {
			writeJSONRPCError(w, req.ID, -32601, sanitizeMsg(err.Error()))
			return
		}
		writeJSONRPCResult(w, req.ID, map[string]interface{}{
			"task_id": task.ID, "status": string(task.Status),
		})

	case "tasks/cancel":
		var params struct {
			TaskID string `json:"task_id"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil || params.TaskID == "" {
			writeJSONRPCError(w, req.ID, -32602, "invalid params: task_id required")
			return
		}
		if err := g.taskSvc.Cancel(r.Context(), tenantID, params.TaskID); err != nil {
			writeJSONRPCError(w, req.ID, -32603, sanitizeMsg(err.Error()))
			return
		}
		writeJSONRPCResult(w, req.ID, map[string]string{
			"task_id": params.TaskID, "status": "cancelling",
		})

	default:
		writeJSONRPCError(w, req.ID, -32601, "method not found: "+req.Method)
	}
}

func tenantFromContextOrReject(w http.ResponseWriter, r *http.Request) (string, bool) {
	tid := auth.TenantFromContext(r.Context())
	if tid == "" {
		writeA2AError(w, http.StatusForbidden, "TENANT_REQUIRED", "missing tenant in authenticated context")
		return "", false
	}
	return tid, true
}

func writeA2AError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"error":   msg,
		"code":    code,
		"message": msg,
		"status":  status,
	})
}

func writeJSONRPCResult(w http.ResponseWriter, id interface{}, result interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jsonRPCResponse{
		JSONRPC: "2.0", Result: result, ID: id,
	})
}

func writeJSONRPCError(w http.ResponseWriter, id interface{}, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jsonRPCResponse{
		JSONRPC: "2.0",
		Error:   &jsonRPCErr{Code: code, Message: msg},
		ID:      id,
	})
}

// AgentCardHandler serves the A2A well-known discovery document. Discovery is
// unauthenticated by design so clients can bootstrap before holding keys.
func AgentCardHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"name":        "Janus",
			"description": "Durable agent task broker with governance, budgeting and audit.",
			"url":         fmt.Sprintf("%s://%s/a2a/", scheme, r.Host),
			"version":     "1.1.0",
			"capabilities": map[string]bool{
				"tasks":              true,
				"streaming":          true,
				"push_notifications": false,
			},
			"default_input_modes":  []string{"application/json"},
			"default_output_modes": []string{"application/json"},
			"skills":               []interface{}{},
		})
	})
}

// TaskStreamer streams task events for A2A clients (SSE). Implemented by
// the handler.SSEHandler; injected from main.go to avoid import cycles.
type TaskStreamer interface {
	ServeHTTP(w http.ResponseWriter, r *http.Request)
}

// handleTaskStream delegates to the SSE handler with the A2A task_id query
// parameter mapped to the REST path the SSE handler expects.
func (g *Gateway) handleTaskStream(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromContextOrReject(w, r)
	if !ok {
		return
	}
	taskID := r.URL.Query().Get("task_id")
	if taskID == "" {
		writeA2AError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "task_id query parameter required")
		return
	}
	if g.taskStreamer == nil {
		writeA2AError(w, http.StatusNotImplemented, "INTERNAL", "streaming not configured")
		return
	}
	// Rewrite the path to match the SSE handler's expected route shape.
	r.URL.Path = "/v1/tenants/" + tenantID + "/tasks/" + taskID + "/stream"
	g.taskStreamer.ServeHTTP(w, r)
}
