package handler

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/agentium-lab/Janus/core"
)

type TaskService interface {
	Create(ctx context.Context, task core.Task) (*core.Task, error)
	Get(ctx context.Context, tenantID, taskID string) (*core.Task, error)
	Start(ctx context.Context, tenantID, taskID string) error
	Complete(ctx context.Context, tenantID, taskID string) error
	Fail(ctx context.Context, tenantID, taskID string, taskErr *core.TaskError) error
	Cancel(ctx context.Context, tenantID, taskID string) error
	Block(ctx context.Context, tenantID, taskID, reason string) error
	Unblock(ctx context.Context, tenantID, taskID string) error
	Replay(ctx context.Context, tenantID, taskID string) (*core.Task, error)
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
		TTLSeconds     int    `json:"ttl_seconds"`
		Deadline       string `json:"deadline"`
		Envelope       struct {
			JanusVersion   string `json:"janus_version"`
			TaskID         string `json:"task_id"`
			IdempotencyKey string `json:"idempotency_key"`
			TenantID       string `json:"tenant_id"`
			SourceAgent    string `json:"source_agent"`
			Priority       string `json:"priority"`
			TTLSeconds     int    `json:"ttl_seconds"`
			Deadline       string `json:"deadline"`
			Target         struct {
				Type  string `json:"type"`
				Value string `json:"value"`
			} `json:"target"`
			Payload struct {
				Type    string `json:"type"`
				Content string `json:"content"`
			} `json:"payload"`
			Trace struct {
				TraceID      string `json:"trace_id"`
				ParentTaskID string `json:"parent_task_id"`
				SpanID       string `json:"span_id"`
			} `json:"trace"`
			Budget *struct {
				MaxTokens   int      `json:"max_tokens"`
				MaxCostUSD  float64  `json:"max_cost_usd"`
				ModelClasses []string `json:"model_classes"`
			} `json:"budget"`
			Policy *struct {
				DataClassification     string   `json:"data_classification"`
				RequiresHumanApproval bool     `json:"requires_human_approval"`
				AllowedTools           []string `json:"allowed_tools"`
			} `json:"policy"`
			ContextRefs []struct {
				Type           string   `json:"type"`
				URI            string   `json:"uri"`
				Hash           string   `json:"hash"`
				Classification string   `json:"classification"`
				AccessScope    []string `json:"access_scope"`
			} `json:"context_refs"`
		} `json:"envelope"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// The task tenant is always derived from the URL path. If the caller also
	// supplies a tenant in the envelope it must agree with the path tenant,
	// otherwise the request is rejected to prevent cross-tenant confusion.
	if req.Envelope.TenantID != "" && req.Envelope.TenantID != tenantID {
		writeError(w, http.StatusBadRequest, "envelope tenant_id does not match path tenant")
		return
	}

	priority := core.Priority(req.Priority)
	if priority == "" {
		priority = core.PriorityNormal
	}

	mailboxID := req.MailboxID
	if mailboxID == "" && core.TargetType(req.TargetType) == core.TargetTypeMailbox {
		mailboxID = req.TargetValue
	}

	idempotencyKey := req.IdempotencyKey
	if idempotencyKey == "" {
		idempotencyKey = req.Envelope.IdempotencyKey
	}

	var deadline *time.Time
	if req.Deadline != "" {
		d, err := time.Parse(time.RFC3339, req.Deadline)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid deadline format, expected RFC3339")
			return
		}
		deadline = &d
	} else if req.Envelope.Deadline != "" {
		d, err := time.Parse(time.RFC3339, req.Envelope.Deadline)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid envelope deadline format, expected RFC3339")
			return
		}
		deadline = &d
	}

	var budget *core.Budget
	if req.Envelope.Budget != nil {
		budget = &core.Budget{
			MaxTokens:    req.Envelope.Budget.MaxTokens,
			MaxCostUSD:   req.Envelope.Budget.MaxCostUSD,
			ModelClasses: req.Envelope.Budget.ModelClasses,
		}
	}

	var policy *core.PolicyContext
	if req.Envelope.Policy != nil {
		policy = &core.PolicyContext{
			DataClassification:     req.Envelope.Policy.DataClassification,
			RequiresHumanApproval: req.Envelope.Policy.RequiresHumanApproval,
			AllowedTools:           req.Envelope.Policy.AllowedTools,
		}
	}

	var contextRefs []core.ContextRef
	for _, ref := range req.Envelope.ContextRefs {
		contextRefs = append(contextRefs, core.ContextRef{
			Type:           ref.Type,
			URI:            ref.URI,
			Hash:           ref.Hash,
			Classification: ref.Classification,
			AccessScope:    ref.AccessScope,
		})
	}

	task := core.Task{
		TenantID:       tenantID,
		ID:             req.ID,
		SourceAgent:    req.SourceAgent,
		TargetType:     core.TargetType(req.TargetType),
		TargetValue:    req.TargetValue,
		MailboxID:      mailboxID,
		IdempotencyKey: idempotencyKey,
		Priority:       priority,
		TTLSeconds:     req.TTLSeconds,
		Deadline:       deadline,
		Status:         core.TaskStatusCreated,
		Envelope: core.TaskEnvelope{
			JanusVersion:   req.Envelope.JanusVersion,
			TaskID:         req.Envelope.TaskID,
			IdempotencyKey: idempotencyKey,
			TenantID:       tenantID,
			SourceAgent:    req.Envelope.SourceAgent,
			Priority:       core.Priority(req.Envelope.Priority),
			TTLSeconds:     req.Envelope.TTLSeconds,
			Deadline:       deadline,
			Budget:         budget,
			Policy:         policy,
			ContextRefs:    contextRefs,
			Target: core.Target{
				Type:  core.TargetType(req.Envelope.Target.Type),
				Value: req.Envelope.Target.Value,
			},
			Payload: core.Payload{
				Type:    req.Envelope.Payload.Type,
				Content: req.Envelope.Payload.Content,
			},
			Trace: core.TraceContext{
				TraceID:      req.Envelope.Trace.TraceID,
				ParentTaskID: req.Envelope.Trace.ParentTaskID,
				SpanID:       req.Envelope.Trace.SpanID,
			},
		},
	}

	createStart := time.Now()
	result, err := h.svc.Create(r.Context(), task)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if result == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "unexpected nil result"})
		return
	}
	// Per the API contract: duplicate create with the same idempotency key
	// returns the existing task with 200 OK. Dedup is detected by comparing
	// the returned task's CreatedAt against a timestamp captured before Create;
	// if they differ, the task pre-existed.
	status := http.StatusCreated
	if idempotencyKey != "" && result.CreatedAt.Before(createStart.Add(-1 * time.Millisecond)) {
		status = http.StatusOK
	}
	writeJSON(w, status, struct {
		ID     string     `json:"id"`
		Status string     `json:"status"`
		Task   *core.Task `json:"task"`
	}{
		ID:     result.ID,
		Status: string(result.Status),
		Task:   result,
	})
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
	tenantID, taskID := tenantAndTaskFromPath(r.URL.Path)
	result, err := h.svc.Replay(r.Context(), tenantID, taskID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *TaskHandler) Block(w http.ResponseWriter, r *http.Request) {
	tenantID, taskID := tenantAndTaskFromPath(r.URL.Path)
	var req struct {
		Reason string `json:"reason"`
	}
	_ = readJSON(r, &req)
	if err := h.svc.Block(r.Context(), tenantID, taskID, req.Reason); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "blocked"})
}

func (h *TaskHandler) Unblock(w http.ResponseWriter, r *http.Request) {
	tenantID, taskID := tenantAndTaskFromPath(r.URL.Path)
	if err := h.svc.Unblock(r.Context(), tenantID, taskID); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "running"})
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
