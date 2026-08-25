package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

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

type TaskCreator interface {
	Create(ctx context.Context, task core.Task) (*core.Task, error)
}
type TaskStatusGetter interface {
	Get(ctx context.Context, tenantID, taskID string) (*core.Task, error)
}
type ResourceRegistrar interface {
	Attach(ctx context.Context, tenantID, refType, uri, hash, classification string, accessScope []string) (*core.ContextRef, error)
}

type EventPublisher interface {
	PublishEvent(ctx context.Context, event core.JanusEvent) error
}

type Gateway struct {
	taskSvc     TaskCreator
	statusSvc   TaskStatusGetter
	resourceSvc ResourceRegistrar
	eventPub    EventPublisher
}

func NewGateway(taskSvc TaskCreator, statusSvc TaskStatusGetter, resourceSvc ResourceRegistrar) *Gateway {
	return &Gateway{taskSvc: taskSvc, statusSvc: statusSvc, resourceSvc: resourceSvc}
}

func (g *Gateway) WithEventPublisher(p EventPublisher) *Gateway {
	g.eventPub = p
	return g
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/mcp" && r.Method == http.MethodPost:
		g.handleJSONRPC(w, r)
	case r.URL.Path == "/mcp/tools/call" && r.Method == http.MethodPost:
		g.handleToolCall(w, r)
	case r.URL.Path == "/mcp/resources" && r.Method == http.MethodPost:
		g.handleResource(w, r)
	case strings.HasPrefix(r.URL.Path, "/mcp/tools/calls/") && strings.HasSuffix(r.URL.Path, "/status") && r.Method == http.MethodGet:
		g.handleToolCallStatus(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (g *Gateway) handleToolCall(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromContextOrReject(w, r)
	if !ok {
		return
	}
	sourceAgent := r.URL.Query().Get("source_agent")
	if sourceAgent == "" {
		sourceAgent = "mcp-client"
	}

	var req struct {
		CallID    string `json:"call_id"`
		ToolName  string `json:"tool_name"`
		Arguments string `json:"arguments"`
		Namespace string `json:"namespace"`
		Target    string `json:"target"`
	}
	if err := readJSONLimit(w, r, &req); err != nil {
		writeMCPError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json")
		return
	}

	callID := req.CallID
	if callID == "" {
		callID = fmt.Sprintf("mcp_call_%d", time.Now().UnixNano())
	}

	if req.Target == "" {
		writeMCPError(w, http.StatusBadRequest, "TARGET_REQUIRED", "target is required; call GET /v1/tenants/{tenant}/catalog to discover capabilities")
		return
	}
	targetType := core.TargetTypeCapability

	task := core.Task{
		ID:          callID,
		TenantID:    tenantID,
		SourceAgent: sourceAgent,
		TargetType:  targetType,
		TargetValue: req.Target,
		Status:      core.TaskStatusCreated,
		Envelope: core.TaskEnvelope{
			JanusVersion: "0.3",
			TaskID:       callID,
			TenantID:     tenantID,
			SourceAgent:  sourceAgent,
			Target:       core.Target{Type: targetType, Value: req.Target},
			Payload:      core.Payload{Type: "mcp_tool_call", Content: req.Arguments},
			Trace:        core.TraceContext{TraceID: fmt.Sprintf("mcp-%s", callID)},
			ToolInvocation: &core.ToolInvocation{
				ID:             callID,
				Name:           req.ToolName,
				Namespace:      "mcp",
				SourceProtocol: "mcp",
			},
		},
	}

	g.emitToolEvent(r.Context(), core.EventToolInvocationRequested, tenantID, callID, sourceAgent, req.ToolName)

	result, err := g.taskSvc.Create(r.Context(), task)
	if err != nil {
		g.emitToolEvent(r.Context(), core.EventToolInvocationFailed, tenantID, callID, sourceAgent, req.ToolName)
		writeMCPError(w, http.StatusInternalServerError, "INTERNAL", sanitizeMsg(err.Error()))
		return
	}

	g.emitToolEvent(r.Context(), core.EventToolInvocationStarted, tenantID, result.ID, sourceAgent, req.ToolName)

	writeJSON(w, http.StatusCreated, map[string]string{
		"call_id": result.ID,
		"status":  string(result.Status),
	})
}

func (g *Gateway) handleToolCallStatus(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	callID := ""
	if len(parts) >= 5 {
		callID = parts[4]
	}
	if callID == "" {
		writeMCPError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "call_id required")
		return
	}

	tenantID, ok := tenantFromContextOrReject(w, r)
	if !ok {
		return
	}

	task, err := g.statusSvc.Get(r.Context(), tenantID, callID)
	if err != nil {
		writeMCPError(w, http.StatusNotFound, "NOT_FOUND", sanitizeMsg(err.Error()))
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"call_id":    task.ID,
		"status":     string(task.Status),
		"result_ref": task.ResultRef,
	})
}

func (g *Gateway) handleResource(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromContextOrReject(w, r)
	if !ok {
		return
	}

	var req struct {
		URI            string   `json:"uri"`
		Hash           string   `json:"hash"`
		Classification string   `json:"classification"`
		AccessScope    []string `json:"access_scope"`
	}
	if err := readJSONLimit(w, r, &req); err != nil {
		writeMCPError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json")
		return
	}

	if g.resourceSvc == nil {
		writeMCPError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "resource service not configured")
		return
	}

	ref, err := g.resourceSvc.Attach(r.Context(), tenantID, "mcp_resource", req.URI, req.Hash, req.Classification, req.AccessScope)
	if err != nil {
		writeMCPError(w, http.StatusInternalServerError, "INTERNAL", sanitizeMsg(err.Error()))
		return
	}

	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"context_ref_id": ref.ID,
		"uri":            ref.URI,
		"hash":           ref.Hash,
	})
}

func (g *Gateway) emitToolEvent(ctx context.Context, typ core.EventType, tenantID, taskID, agent, toolName string) {
	if g.eventPub == nil {
		return
	}
	payload, _ := json.Marshal(map[string]string{"tool_name": toolName})
	_ = g.eventPub.PublishEvent(ctx, core.JanusEvent{
		EventType:   typ,
		TenantID:    tenantID,
		TaskID:      taskID,
		SourceAgent: agent,
		Payload:     payload,
	})
}

func tenantFromContextOrReject(w http.ResponseWriter, r *http.Request) (string, bool) {
	tid := auth.TenantFromContext(r.Context())
	if tid == "" {
		writeMCPError(w, http.StatusForbidden, "TENANT_REQUIRED", "missing tenant in authenticated context")
		return "", false
	}
	return tid, true
}

func writeMCPError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"error": msg, "code": code, "message": msg, "status": status,
	})
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// handleJSONRPC implements the MCP streamable-HTTP JSON-RPC surface on /mcp:
// initialize handshake, ping, tools/list, tools/call and resources/list.
// Tool execution remains the async task pipeline shared with the REST route.
type mcpRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      interface{}     `json:"id"`
}

func (g *Gateway) writeMCPResult(w http.ResponseWriter, id interface{}, result interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"jsonrpc": "2.0", "id": id, "result": result,
	})
}

func (g *Gateway) handleJSONRPC(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromContextOrReject(w, r)
	if !ok {
		return
	}
	var req mcpRPCRequest
	if err := readJSONLimit(w, r, &req); err != nil {
		writeMCPError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json")
		return
	}
	switch req.Method {
	case "initialize":
		var p struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		_ = json.Unmarshal(req.Params, &p)
		version := p.ProtocolVersion
		if version == "" {
			version = "2024-11-05"
		}
		g.writeMCPResult(w, req.ID, map[string]interface{}{
			"protocolVersion": version,
			"capabilities":    map[string]interface{}{"tools": map[string]bool{"listChanged": false}, "resources": map[string]bool{}},
			"serverInfo":      map[string]string{"name": "janus-mcp", "version": "1.1.0"},
		})
	case "notifications/initialized":
		w.WriteHeader(http.StatusAccepted)
	case "ping":
		g.writeMCPResult(w, req.ID, map[string]interface{}{})
	case "tools/list":
		g.writeMCPResult(w, req.ID, map[string]interface{}{"tools": []interface{}{}})
	case "resources/list":
		g.writeMCPResult(w, req.ID, map[string]interface{}{"resources": []interface{}{}})
	case "tools/call":
		var p struct {
			Name      string `json:"name"`
			Arguments string `json:"arguments"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil || p.Name == "" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req.ID,
				"error": map[string]interface{}{"code": -32602, "message": "invalid params: name required"},
			})
			return
		}
		sourceAgent := r.URL.Query().Get("source_agent")
		if sourceAgent == "" {
			sourceAgent = "mcp-client"
		}
		callID := fmt.Sprintf("mcp_%d", time.Now().UnixNano())
		task := core.Task{
			ID: callID, TenantID: tenantID, SourceAgent: sourceAgent,
			TargetType: core.TargetTypeCapability, TargetValue: p.Name,
			Status: core.TaskStatusCreated,
			Envelope: core.TaskEnvelope{
				JanusVersion: "0.3", TaskID: callID, TenantID: tenantID,
				SourceAgent: sourceAgent,
				Target:      core.Target{Type: core.TargetTypeCapability, Value: p.Name},
				Payload:     core.Payload{Type: "mcp_tool_call", Content: p.Arguments},
				Trace:       core.TraceContext{TraceID: "mcp-" + callID},
				Policy:      &core.PolicyContext{DataClassification: "internal"},
			},
		}
		result, err := g.taskSvc.Create(r.Context(), task)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"jsonrpc": "2.0", "id": req.ID,
				"error": map[string]interface{}{"code": -32603, "message": sanitizeMsg(err.Error())},
			})
			return
		}
		g.emitToolEvent(r.Context(), core.EventToolInvocationStarted, tenantID, result.ID, sourceAgent, p.Name)
		g.writeMCPResult(w, req.ID, map[string]interface{}{
			"content": []interface{}{map[string]string{
				"type": "text",
				"text": fmt.Sprintf("tool call accepted: task_id=%s status=%s", result.ID, result.Status),
			}},
			"isError": false,
		})
	default:
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"jsonrpc": "2.0", "id": req.ID,
			"error": map[string]interface{}{"code": -32601, "message": "method not found: " + req.Method},
		})
	}
}
