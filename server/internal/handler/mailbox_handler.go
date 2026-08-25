package handler

import (
	"context"
	"net/http"
	"strings"

	"github.com/agentium-lab/Janus/core"
)

type MailboxService interface {
	Create(ctx context.Context, mailbox core.Mailbox) error
	Get(ctx context.Context, tenantID, mailboxID string) (*core.Mailbox, error)
	ListByAgent(ctx context.Context, tenantID, agentID string) ([]*core.Mailbox, error)
	UpdateConfig(ctx context.Context, tenantID, mailboxID string, maxConcurrency, ackWaitSeconds, maxDeliver, retentionSeconds int) error
	Pause(ctx context.Context, tenantID, mailboxID string) error
	Resume(ctx context.Context, tenantID, mailboxID string) error
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

func (h *MailboxHandler) Update(w http.ResponseWriter, r *http.Request) {
	tenantID := tenantIDFromPath(r.URL.Path)
	mailboxID := lastSegment(r.URL.Path)

	var req struct {
		MaxConcurrency   *int `json:"max_concurrency"`
		ACKWaitSeconds   *int `json:"ack_wait_seconds"`
		MaxDeliver       *int `json:"max_deliver"`
		RetentionSeconds *int `json:"retention_seconds"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	mailbox, err := h.svc.Get(r.Context(), tenantID, mailboxID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	mc := mailbox.MaxConcurrency
	aw := mailbox.ACKWaitSeconds
	md := mailbox.MaxDeliver
	rs := mailbox.RetentionSeconds
	if req.MaxConcurrency != nil {
		mc = *req.MaxConcurrency
	}
	if req.ACKWaitSeconds != nil {
		aw = *req.ACKWaitSeconds
	}
	if req.MaxDeliver != nil {
		md = *req.MaxDeliver
	}
	if req.RetentionSeconds != nil {
		rs = *req.RetentionSeconds
	}

	if err := h.svc.UpdateConfig(r.Context(), tenantID, mailboxID, mc, aw, md, rs); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"id": mailboxID, "status": "updated"})
}

func (h *MailboxHandler) Pause(w http.ResponseWriter, r *http.Request) {
	h.toggleLifecycle(w, r, false)
}

func (h *MailboxHandler) Resume(w http.ResponseWriter, r *http.Request) {
	h.toggleLifecycle(w, r, true)
}

func (h *MailboxHandler) toggleLifecycle(w http.ResponseWriter, r *http.Request, resume bool) {
	tenantID := tenantIDFromPath(r.URL.Path)
	suffix := "/pause"
	if resume {
		suffix = "/resume"
	}
	mailboxID := lastSegment(strings.TrimSuffix(r.URL.Path, suffix))
	if mailboxID == "" || mailboxID == "mailboxes" {
		writeError(w, http.StatusBadRequest, "missing mailbox id")
		return
	}
	var err error
	if resume {
		err = h.svc.Resume(r.Context(), tenantID, mailboxID)
	} else {
		err = h.svc.Pause(r.Context(), tenantID, mailboxID)
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	mb, err := h.svc.Get(r.Context(), tenantID, mailboxID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, mb)
}
