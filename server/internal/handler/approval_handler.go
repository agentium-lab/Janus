package handler

import (
	"context"
	"net/http"

	"github.com/agentium-lab/Janus/core"
)

type ApprovalService interface {
	RequestApproval(ctx context.Context, approval core.Approval) (*core.Approval, error)
	Approve(ctx context.Context, tenantID, approvalID, approver, reason string) error
	Reject(ctx context.Context, tenantID, approvalID, approver, reason string) error
	Get(ctx context.Context, tenantID, approvalID string) (*core.Approval, error)
	ListPending(ctx context.Context, tenantID string, limit int) ([]*core.Approval, error)
}

type ApprovalHandler struct {
	svc ApprovalService
}

func NewApprovalHandler(svc ApprovalService) *ApprovalHandler {
	return &ApprovalHandler{svc: svc}
}

type approvalRequestReq struct {
	TaskID      string `json:"task_id"`
	RequestedBy string `json:"requested_by"`
}

func (h *ApprovalHandler) Request(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenantID")
	var req approvalRequestReq
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	result, err := h.svc.RequestApproval(r.Context(), core.Approval{
		TenantID:    tenantID,
		TaskID:      req.TaskID,
		RequestedBy: req.RequestedBy,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

type approvalDecisionReq struct {
	Approver string `json:"approver"`
	Reason   string `json:"reason"`
}

func (h *ApprovalHandler) Approve(w http.ResponseWriter, r *http.Request) {
	tenantID, approvalID := r.PathValue("tenantID"), r.PathValue("approvalID")
	var req approvalDecisionReq
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if err := h.svc.Approve(r.Context(), tenantID, approvalID, req.Approver, req.Reason); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "approved"})
}

func (h *ApprovalHandler) Reject(w http.ResponseWriter, r *http.Request) {
	tenantID, approvalID := r.PathValue("tenantID"), r.PathValue("approvalID")
	var req approvalDecisionReq
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if err := h.svc.Reject(r.Context(), tenantID, approvalID, req.Approver, req.Reason); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "rejected"})
}

func (h *ApprovalHandler) ListPending(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenantID")
	approvals, err := h.svc.ListPending(r.Context(), tenantID, 50)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if approvals == nil {
		approvals = []*core.Approval{}
	}
	writeJSON(w, http.StatusOK, approvals)
}

func (h *ApprovalHandler) Get(w http.ResponseWriter, r *http.Request) {
	tenantID, approvalID := r.PathValue("tenantID"), r.PathValue("approvalID")
	approval, err := h.svc.Get(r.Context(), tenantID, approvalID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, approval)
}
