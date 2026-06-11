package handler

import (
	"context"
	"net/http"
	"strings"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/service"
)

type TaskService interface {
	Create(ctx context.Context, task core.Task) error
	Get(ctx context.Context, tenantID, taskID string) (*core.Task, error)
	Start(ctx context.Context, tenantID, taskID string) error
	Complete(ctx context.Context, tenantID, taskID string) error
	Fail(ctx context.Context, tenantID, taskID string, taskErr *core.TaskError) error
	Cancel(ctx context.Context, tenantID, taskID string) error
	ListByStatus(ctx context.Context, tenantID string, status core.TaskStatus, limit int) ([]*core.Task, error)
}

type TaskHandler struct {
	svc TaskService
}

func NewTaskHandler(svc TaskService) *TaskHandler {
	return &TaskHandler{svc: svc}
}

func (h *TaskHandler) Create(w http.ResponseWriter, r *http.Request) {
	tenantID := tenantIDFromPath(r.URL.Path)
	var req struct {
		ID             string `json:"id"`
		SourceAgent    string `json:"source_agent"`
		TargetType     string `json:"target_type"`
		TargetValue    string `json:"target_value"`
		MailboxID      string `json:"mailbox_id"`
		IdempotencyKey string `json:"idempotency_key"`
		Priority       string `json:"priority"`
		Envelope       struct {
			JanusVersion string `json:"janus_version"`
			TaskID       string `json:"task_id"`
			TenantID     string `json:"tenant_id"`
			SourceAgent  string `json:"source_agent"`
			Target       struct {
				Type  string `json:"type"`
				Value string `json:"value"`
			} `json:"target"`
			Payload struct {
				Type    string `json:"type"`
				Content string `json:"content"`
			} `json:"payload"`
			Trace struct {
				TraceID string `json:"trace_id"`
			} `json:"trace"`
		} `json:"envelope"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	priority := core.Priority(req.Priority)
	if priority == "" {
		priority = core.PriorityNormal
	}

	task := core.Task{
		TenantID:       tenantID,
		ID:             req.ID,
		SourceAgent:    req.SourceAgent,
		TargetType:     core.TargetType(req.TargetType),
		TargetValue:    req.TargetValue,
		MailboxID:      req.MailboxID,
		IdempotencyKey: req.IdempotencyKey,
		Priority:       priority,
		Status:         core.TaskStatusCreated,
		Envelope: core.TaskEnvelope{
			JanusVersion: req.Envelope.JanusVersion,
			TaskID:       req.Envelope.TaskID,
			TenantID:     req.Envelope.TenantID,
			SourceAgent:  req.Envelope.SourceAgent,
			Target: core.Target{
				Type:  core.TargetType(req.Envelope.Target.Type),
				Value: req.Envelope.Target.Value,
			},
			Payload: core.Payload{
				Type:    req.Envelope.Payload.Type,
				Content: req.Envelope.Payload.Content,
			},
			Trace: core.TraceContext{TraceID: req.Envelope.Trace.TraceID},
		},
	}

	if err := h.svc.Create(r.Context(), task); err != nil {
		if _, ok := err.(*service.IdempotentError); ok {
			writeJSON(w, http.StatusOK, map[string]string{"id": req.ID, "status": "existing"})
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{"id": req.ID, "status": "created"})
}

func (h *TaskHandler) Get(w http.ResponseWriter, r *http.Request) {
	tenantID := tenantIDFromPath(r.URL.Path)
	taskID := lastSegment(r.URL.Path)

	task, err := h.svc.Get(r.Context(), tenantID, taskID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, task)
}

func (h *TaskHandler) Start(w http.ResponseWriter, r *http.Request) {
	tenantID, taskID := tenantAndTaskFromPath(r.URL.Path)
	if err := h.svc.Start(r.Context(), tenantID, taskID); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "running"})
}

func (h *TaskHandler) Complete(w http.ResponseWriter, r *http.Request) {
	tenantID, taskID := tenantAndTaskFromPath(r.URL.Path)
	if err := h.svc.Complete(r.Context(), tenantID, taskID); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "completed"})
}

func (h *TaskHandler) Fail(w http.ResponseWriter, r *http.Request) {
	tenantID, taskID := tenantAndTaskFromPath(r.URL.Path)
	var req struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	_ = readJSON(r, &req)

	taskErr := &core.TaskError{Code: req.Code, Message: req.Message}
	if err := h.svc.Fail(r.Context(), tenantID, taskID, taskErr); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "failed"})
}

func (h *TaskHandler) Cancel(w http.ResponseWriter, r *http.Request) {
	tenantID, taskID := tenantAndTaskFromPath(r.URL.Path)
	if err := h.svc.Cancel(r.Context(), tenantID, taskID); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "cancelled"})
}

func (h *TaskHandler) Replay(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "replay not yet implemented"})
}

func tenantAndTaskFromPath(path string) (string, string) {
	parts := strings.Split(path, "/")
	tenantID := ""
	taskID := ""
	for i, p := range parts {
		if p == "tenants" && i+1 < len(parts) {
			tenantID = parts[i+1]
		}
		if p == "tasks" && i+1 < len(parts) {
			taskID = parts[i+1]
		}
	}
	return tenantID, taskID
}
