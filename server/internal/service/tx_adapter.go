package service

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/agentium-lab/Janus/core"
)

// TxAdapter wraps non-transactional repos to satisfy the Tx interfaces.
// The pgx.Tx parameter is ignored — these adapters bridge in-memory repos
// into the unified transactional code path (MemoryLifecycle passes nil tx).

type TaskRepoTxAdapter struct {
	TaskRepo
}

func (a TaskRepoTxAdapter) CreateTx(ctx context.Context, _ pgx.Tx, task core.Task) error {
	return a.Create(ctx, task)
}

func (a TaskRepoTxAdapter) UpdateStatusTx(ctx context.Context, _ pgx.Tx, tenantID, taskID string, status core.TaskStatus, attempt int) error {
	return a.UpdateStatus(ctx, tenantID, taskID, status, attempt)
}

func (a TaskRepoTxAdapter) UpdateStatusWithCheckTx(ctx context.Context, _ pgx.Tx, tenantID, taskID string, expected, next core.TaskStatus, attempt int) (bool, error) {
	return a.UpdateStatusWithCheck(ctx, tenantID, taskID, expected, next, attempt)
}

func (a TaskRepoTxAdapter) SetResultRefTx(ctx context.Context, _ pgx.Tx, tenantID, taskID, ref string) error {
	return a.SetResultRef(ctx, tenantID, taskID, ref)
}

func (a TaskRepoTxAdapter) UpdateRetryAtTx(ctx context.Context, _ pgx.Tx, tenantID, taskID string, retryAt time.Time) error {
	return a.UpdateRetryAt(ctx, tenantID, taskID, retryAt)
}

type AttemptRepoTxAdapter struct {
	TaskAttemptRepo
}

func (a AttemptRepoTxAdapter) CreateTx(ctx context.Context, _ pgx.Tx, attempt core.TaskAttempt) error {
	return a.Create(ctx, attempt)
}

func (a AttemptRepoTxAdapter) UpdateFinishedWithCheckTx(ctx context.Context, _ pgx.Tx, tenantID, taskID string, attempt int, status string, errJSON, usageJSON []byte) (bool, error) {
	return a.UpdateFinishedWithCheck(ctx, tenantID, taskID, attempt, status, errJSON, usageJSON)
}

type ApprovalRepoTxAdapter struct {
	ApprovalRepo
}

func (a ApprovalRepoTxAdapter) UpdateDecisionTx(ctx context.Context, _ pgx.Tx, tenantID, approvalID, decision, approver, reason string) error {
	return a.UpdateDecision(ctx, tenantID, approvalID, decision, approver, reason)
}
