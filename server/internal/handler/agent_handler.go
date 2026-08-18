package handler

import (
	"context"
	"net/http"
	"strings"

	"github.com/agentium-lab/Janus/core"
)

type AgentService interface {
	Register(ctx context.Context, agent core.Agent) error
	Get(ctx context.Context, tenantID, agentID string) (*core.Agent, error)
	Heartbeat(ctx context.Context, tenantID, agentID string) error
	UpdateStatus(ctx context.Context, tenantID, agentID string, status core.AgentStatus) error
	List(ctx context.Context, tenantID string) ([]*core.Agent, error)
	ListByStatus(ctx context.Context, tenantID string, status core.AgentStatus) ([]*core.Agent, error)
}

type AgentHandler struct {
	svc AgentService
}

func NewAgentHandler(svc AgentService) *AgentHandler {
	return &AgentHandler{svc: svc}
}

func (h *AgentHandler) Register(w http.ResponseWriter, r *http.Request) {
	tenantID := tenantIDFromPath(r.URL.Path)
	var req struct {
		ID             string `json:"id"`
		TeamID         string `json:"team_id"`
		DisplayName    string `json:"display_name"`
		Protocol       string `json:"protocol"`
		Endpoint       string `json:"endpoint"`
		Description    string `json:"description"`
		MaxConcurrency int    `json:"max_concurrency"`
		RPM            int    `json:"rpm"`
		TPM            int    `json:"tpm"`
		Capabilities   []struct {
			Capability  string `json:"capability"`
			Description string `json:"description"`
			Schema      string `json:"schema"`
		} `json:"capabilities"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	agent := core.Agent{
		ID: req.ID, TenantID: tenantID, TeamID: req.TeamID, DisplayName: req.DisplayName,
		Protocol: core.AgentProtocol(req.Protocol), Endpoint: req.Endpoint,
		Description: req.Description, MaxConcurrency: req.MaxConcurrency,
		RPM: req.RPM, TPM: req.TPM,
		Status: core.AgentStatusOffline,
	}
	for _, c := range req.Capabilities {
		agent.Capabilities = append(agent.Capabilities, core.AgentCapability{
			TenantID: tenantID, AgentID: req.ID,
			Capability: c.Capability, Description: c.Description, Schema: c.Schema,
		})
	}
	if agent.MaxConcurrency <= 0 {
		agent.MaxConcurrency = 1
	}

	if err := h.svc.Register(r.Context(), agent); err != nil {
		if isDuplicateKeyError(err) {
			writeError(w, http.StatusConflict, "agent already exists")
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{"id": req.ID, "status": string(core.AgentStatusOnline)})
}

func (h *AgentHandler) Get(w http.ResponseWriter, r *http.Request) {
	tenantID := tenantIDFromPath(r.URL.Path)
	agentID := lastSegment(r.URL.Path)

	agent, err := h.svc.Get(r.Context(), tenantID, agentID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, agent)
}

func (h *AgentHandler) Heartbeat(w http.ResponseWriter, r *http.Request) {
	tenantID := tenantIDFromPath(r.URL.Path)
	agentID := agentIDFromHeartbeatPath(r.URL.Path)

	if err := h.svc.Heartbeat(r.Context(), tenantID, agentID); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *AgentHandler) List(w http.ResponseWriter, r *http.Request) {
	tenantID := tenantIDFromPath(r.URL.Path)

	agents, err := h.svc.List(r.Context(), tenantID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"agents": agents})
}

func agentIDFromHeartbeatPath(path string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == "agents" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func lastSegment(path string) string {
	path = strings.TrimRight(path, "/")
	parts := strings.Split(path, "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return ""
}
