package acp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/auth"
)

type AgentRegistrar interface {
	Register(ctx context.Context, agent core.Agent) error
}
type TaskCreator interface {
	Create(ctx context.Context, task core.Task) (*core.Task, error)
}
type TaskStatusGetter interface {
	Get(ctx context.Context, tenantID, taskID string) (*core.Task, error)
}

type Gateway struct {
	agentSvc AgentRegistrar
	taskSvc  TaskCreator
	statusSvc TaskStatusGetter
}

func NewGateway(agentSvc AgentRegistrar, taskSvc TaskCreator, statusSvc TaskStatusGetter) *Gateway {
	return &Gateway{agentSvc: agentSvc, taskSvc: taskSvc, statusSvc: statusSvc}
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/acp/agent/manifest" && r.Method == http.MethodPost:
		g.handleManifest(w, r)
	case r.URL.Path == "/acp/runs" && r.Method == http.MethodPost:
		g.handleRun(w, r)
	case r.URL.Path == "/acp/runs" && r.Method == http.MethodGet:
		g.handleListRuns(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (g *Gateway) handleManifest(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromContextOrReject(w, r)
	if !ok {
		return
	}

	var req struct {
		AgentID   string `json:"agent_id"`
		Name      string `json:"name"`
		Skills    []struct {
			Name        string `json:"name"`
			Description string `json:"description"`
		} `json:"skills"`
		Endpoint string `json:"endpoint"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeACPError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json")
		return
	}

	var caps []core.AgentCapability
	for _, s := range req.Skills {
		caps = append(caps, core.AgentCapability{Capability: s.Name, Description: s.Description})
	}

	agent := core.Agent{
		ID:           req.AgentID,
		TenantID:     tenantID,
		DisplayName:  req.Name,
		Protocol:     "acp",
		Endpoint:     req.Endpoint,
		Status:       core.AgentStatusOnline,
		Capabilities: caps,
	}

	if err := g.agentSvc.Register(r.Context(), agent); err != nil {
		writeACPError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "registered", "agent_id": agent.ID})
}

func (g *Gateway) handleRun(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromContextOrReject(w, r)
	if !ok {
		return
	}
	sourceAgent := r.URL.Query().Get("source_agent")
	if sourceAgent == "" {
		sourceAgent = "unknown"
	}

	var req struct {
		RunID      string `json:"run_id"`
		TargetType string `json:"target_type"`
		Target     string `json:"target"`
		Input      string `json:"input"`
		ContextRef string `json:"context_ref"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeACPError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json")
		return
	}

	taskID := req.RunID
	if taskID == "" {
		taskID = fmt.Sprintf("run_%d", time.Now().UnixNano())
	}

	targetType := core.TargetType(req.TargetType)
	if targetType == "" {
		targetType = core.TargetType("intent")
	}

	task := core.Task{
		ID:         taskID,
		TenantID:   tenantID,
		SourceAgent: sourceAgent,
		TargetType: targetType,
		TargetValue: req.Target,
		Status:     core.TaskStatusCreated,
		Envelope: core.TaskEnvelope{
			JanusVersion: "0.3",
			TaskID:       taskID,
			TenantID:     tenantID,
			SourceAgent:  sourceAgent,
			Target:       core.Target{Type: targetType, Value: req.Target},
			Payload:      core.Payload{Type: "acp_run", Content: req.Input},
			Trace:        core.TraceContext{TraceID: fmt.Sprintf("acp-%s", taskID)},
		},
	}

	result, err := g.taskSvc.Create(r.Context(), task)
	if err != nil {
		writeACPError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{
		"run_id": result.ID,
		"status": string(result.Status),
	})
}

func (g *Gateway) handleListRuns(w http.ResponseWriter, r *http.Request) {
	runID := r.URL.Query().Get("run_id")
	if runID == "" {
		writeACPError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "run_id required")
		return
	}

	tenantID, ok := tenantFromContextOrReject(w, r)
	if !ok {
		return
	}

	task, err := g.statusSvc.Get(r.Context(), tenantID, runID)
	if err != nil {
		writeACPError(w, http.StatusNotFound, "NOT_FOUND", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"run_id":       task.ID,
		"status":       string(task.Status),
		"result_ref":   task.ResultRef,
	})
}

func tenantFromContextOrReject(w http.ResponseWriter, r *http.Request) (string, bool) {
	tid := auth.TenantFromContext(r.Context())
	if tid == "" {
		writeACPError(w, http.StatusForbidden, "TENANT_REQUIRED", "missing tenant in authenticated context")
		return "", false
	}
	return tid, true
}

func writeACPError(w http.ResponseWriter, status int, code, msg string) {
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
