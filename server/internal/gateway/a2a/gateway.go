package a2a

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/agentium-lab/Janus/core"
)

type AgentRegistrar interface {
	Register(ctx context.Context, agent core.Agent) error
}

type TaskCreator interface {
	Create(ctx context.Context, task core.Task) (*core.Task, error)
}

type Gateway struct {
	agentSvc AgentRegistrar
	taskSvc  TaskCreator
}

func NewGateway(agentSvc AgentRegistrar, taskSvc TaskCreator) *Gateway {
	return &Gateway{agentSvc: agentSvc, taskSvc: taskSvc}
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/a2a/agent/card" && r.Method == http.MethodPost:
		g.handleAgentCard(w, r)
	case r.URL.Path == "/a2a/task/send" && r.Method == http.MethodPost:
		g.handleTaskSend(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (g *Gateway) handleAgentCard(w http.ResponseWriter, r *http.Request) {
	tenantID := r.URL.Query().Get("tenant_id")
	if tenantID == "" {
		tenantID = "default"
	}

	var card AgentCard
	if err := json.NewDecoder(r.Body).Decode(&card); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}

	agent := CardToAgent(tenantID, card)
	if err := g.agentSvc.Register(r.Context(), agent); err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":   "registered",
		"agent_id": agent.ID,
	})
}

func (g *Gateway) handleTaskSend(w http.ResponseWriter, r *http.Request) {
	tenantID := r.URL.Query().Get("tenant_id")
	if tenantID == "" {
		tenantID = "default"
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}

	task := MessageToTask(req, tenantID, sourceAgent, mailboxID)
	if _, err := g.taskSvc.Create(r.Context(), task); err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"task_id": task.ID,
		"status":  string(task.Status),
	})
}
