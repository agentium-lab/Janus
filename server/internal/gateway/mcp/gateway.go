package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/auth"
)

type TaskCreator interface {
	Create(ctx context.Context, task core.Task) (*core.Task, error)
}
type TaskStatusGetter interface {
	Get(ctx context.Context, tenantID, taskID string) (*core.Task, error)
}
type ResourceRegistrar interface {
	Attach(ctx context.Context, tenantID, refType, uri, hash, classification string, accessScope []string) (*core.ContextRef, error)
}

type Gateway struct {
	taskSvc    TaskCreator
	statusSvc  TaskStatusGetter
	resourceSvc ResourceRegistrar
}

func NewGateway(taskSvc TaskCreator, statusSvc TaskStatusGetter, resourceSvc ResourceRegistrar) *Gateway {
	return &Gateway{taskSvc: taskSvc, statusSvc: statusSvc, resourceSvc: resourceSvc}
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeMCPError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json")
		return
	}

	callID := req.CallID
	if callID == "" {
		callID = fmt.Sprintf("mcp_call_%d", time.Now().UnixNano())
	}

	targetType := core.TargetType("intent")
	if req.Target != "" {
		targetType = core.TargetTypeCapability
	}

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

	result, err := g.taskSvc.Create(r.Context(), task)
	if err != nil {
		writeMCPError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}

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
		writeMCPError(w, http.StatusNotFound, "NOT_FOUND", err.Error())
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
		URI           string   `json:"uri"`
		Hash          string   `json:"hash"`
		Classification string  `json:"classification"`
		AccessScope   []string `json:"access_scope"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeMCPError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json")
		return
	}

	if g.resourceSvc == nil {
		writeMCPError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "resource service not configured")
		return
	}

	ref, err := g.resourceSvc.Attach(r.Context(), tenantID, "mcp_resource", req.URI, req.Hash, req.Classification, req.AccessScope)
	if err != nil {
		writeMCPError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"context_ref_id": ref.ID,
		"uri":            ref.URI,
		"hash":           ref.Hash,
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
