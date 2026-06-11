package handler

import (
	"context"
	"net/http"

	"github.com/agentium-lab/Janus/core"
)

type MailboxService interface {
	Create(ctx context.Context, mailbox core.Mailbox) error
	Get(ctx context.Context, tenantID, mailboxID string) (*core.Mailbox, error)
	ListByAgent(ctx context.Context, tenantID, agentID string) ([]*core.Mailbox, error)
}

type MailboxHandler struct {
	svc MailboxService
}

func NewMailboxHandler(svc MailboxService) *MailboxHandler {
	return &MailboxHandler{svc: svc}
}

func (h *MailboxHandler) Create(w http.ResponseWriter, r *http.Request) {
	tenantID := tenantIDFromPath(r.URL.Path)
	var req struct {
		ID               string `json:"id"`
		AgentID          string `json:"agent_id"`
		MaxConcurrency   int    `json:"max_concurrency"`
		ACKWaitSeconds   int    `json:"ack_wait_seconds"`
		MaxDeliver       int    `json:"max_deliver"`
		RetentionSeconds int    `json:"retention_seconds"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	mailbox := core.Mailbox{
		TenantID:         tenantID,
		ID:               req.ID,
		AgentID:          req.AgentID,
		MaxConcurrency:   req.MaxConcurrency,
		ACKWaitSeconds:   req.ACKWaitSeconds,
		MaxDeliver:       req.MaxDeliver,
		RetentionSeconds: req.RetentionSeconds,
		Priority:         core.PriorityNormal,
	}

	if err := h.svc.Create(r.Context(), mailbox); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{"id": req.ID, "status": "active"})
}

func (h *MailboxHandler) Get(w http.ResponseWriter, r *http.Request) {
	tenantID := tenantIDFromPath(r.URL.Path)
	mailboxID := lastSegment(r.URL.Path)

	mailbox, err := h.svc.Get(r.Context(), tenantID, mailboxID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, mailbox)
}
