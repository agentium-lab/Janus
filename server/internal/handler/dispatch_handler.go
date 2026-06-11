package handler

import (
	"context"
	"net/http"
	"strings"

	"github.com/agentium-lab/Janus/core"
)

type DispatchService interface {
	PullTask(ctx context.Context, tenantID, mailboxID, agentID string) (*ServicePullResult, error)
	StartTask(ctx context.Context, tenantID, taskID, leaseID string) error
	TaskHeartbeat(ctx context.Context, tenantID, taskID, leaseID string) error
	AckTask(ctx context.Context, tenantID, taskID, leaseID string, resultRef string, usage *core.TokenUsage) error
	NackTask(ctx context.Context, tenantID, taskID, leaseID string, retriable bool, taskErr *core.TaskError) error
}

type ServicePullResult struct {
	Task      *core.Task
	LeaseID   string
	ExpiresAt interface{}
}

type DispatchHandler struct {
	svc DispatchService
}

func NewDispatchHandler(svc DispatchService) *DispatchHandler {
	return &DispatchHandler{svc: svc}
}

func (h *DispatchHandler) Pull(w http.ResponseWriter, r *http.Request) {
	tenantID, mailboxID := tenantAndMailboxFromPath(r.URL.Path)
	if tenantID == "" || mailboxID == "" {
		writeError(w, http.StatusBadRequest, "tenant id and mailbox id are required")
		return
	}
	var req struct {
		AgentID string `json:"agent_id"`
	}
	_ = readJSON(r, &req)
	if req.AgentID == "" {
		req.AgentID = "default"
	}

	result, err := h.svc.PullTask(r.Context(), tenantID, mailboxID, req.AgentID)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	if result == nil {
		writeJSON(w, http.StatusNoContent, nil)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"task": result.Task,
		"lease": map[string]interface{}{
			"lease_id":   result.LeaseID,
			"expires_at": result.ExpiresAt,
		},
	})
}

func (h *DispatchHandler) Start(w http.ResponseWriter, r *http.Request) {
	tenantID, taskID := tenantAndTaskFromPath(r.URL.Path)
	var req struct {
		LeaseID string `json:"lease_id"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.svc.StartTask(r.Context(), tenantID, taskID, req.LeaseID); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "running"})
}

func (h *DispatchHandler) Heartbeat(w http.ResponseWriter, r *http.Request) {
	tenantID, taskID := tenantAndTaskFromPath(r.URL.Path)
	var req struct {
		LeaseID string `json:"lease_id"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.svc.TaskHeartbeat(r.Context(), tenantID, taskID, req.LeaseID); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *DispatchHandler) Ack(w http.ResponseWriter, r *http.Request) {
	tenantID, taskID := tenantAndTaskFromPath(r.URL.Path)
	var req struct {
		LeaseID   string `json:"lease_id"`
		ResultRef string `json:"result_ref"`
		TokenUsage *struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
			TotalTokens      int `json:"total_tokens"`
		} `json:"token_usage"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var usage *core.TokenUsage
	if req.TokenUsage != nil {
		usage = &core.TokenUsage{
			PromptTokens:     req.TokenUsage.PromptTokens,
			CompletionTokens: req.TokenUsage.CompletionTokens,
			TotalTokens:      req.TokenUsage.TotalTokens,
		}
	}
	if err := h.svc.AckTask(r.Context(), tenantID, taskID, req.LeaseID, req.ResultRef, usage); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "completed"})
}

func (h *DispatchHandler) Nack(w http.ResponseWriter, r *http.Request) {
	tenantID, taskID := tenantAndTaskFromPath(r.URL.Path)
	var req struct {
		LeaseID   string `json:"lease_id"`
		Retriable bool   `json:"retriable"`
		Error     *struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var taskErr *core.TaskError
	if req.Error != nil {
		taskErr = &core.TaskError{Code: req.Error.Code, Message: req.Error.Message}
	}
	if err := h.svc.NackTask(r.Context(), tenantID, taskID, req.LeaseID, req.Retriable, taskErr); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "nacked"})
}

func tenantAndMailboxFromPath(path string) (string, string) {
	parts := strings.Split(path, "/")
	tenantID, mailboxID := "", ""
	for i, p := range parts {
		if p == "tenants" && i+1 < len(parts) {
			tenantID = parts[i+1]
		}
		if p == "mailboxes" && i+1 < len(parts) {
			mailboxID = parts[i+1]
		}
	}
	return tenantID, mailboxID
}
