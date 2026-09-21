package service

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"time"

	"github.com/agentium-lab/Janus/core"
)

type TenantRepo interface {
	Create(ctx context.Context, id, name string) error
	GetName(ctx context.Context, id string) (string, error)
	ListIDs(ctx context.Context) ([]string, error)
}

type AgentRepo interface {
	Register(ctx context.Context, agent core.Agent) error
	UpsertCapabilities(ctx context.Context, agent core.Agent) error
	Get(ctx context.Context, tenantID, agentID string) (*core.Agent, error)
	List(ctx context.Context, tenantID string) ([]*core.Agent, error)
	ListByStatus(ctx context.Context, tenantID string, status core.AgentStatus) ([]*core.Agent, error)
	ListAllByStatus(ctx context.Context, status core.AgentStatus) ([]*core.Agent, error)
	UpdateHeartbeat(ctx context.Context, tenantID, agentID string) error
	UpdateStatus(ctx context.Context, tenantID, agentID string, status core.AgentStatus) error
	FindByCapability(ctx context.Context, tenantID, capability string) ([]*core.Agent, error)
}

type TaskRepo interface {
	Create(ctx context.Context, task core.Task) error
	Get(ctx context.Context, tenantID, taskID string) (*core.Task, error)
	GetByIdempotencyKey(ctx context.Context, tenantID, key string) (*core.Task, error)
	UpdateStatus(ctx context.Context, tenantID, taskID string, status core.TaskStatus, attemptIncrement int) error
	UpdateStatusWithCheck(ctx context.Context, tenantID, taskID string, expectedStatus core.TaskStatus, newStatus core.TaskStatus, attemptIncrement int) (bool, error)
	UpdateRetryAt(ctx context.Context, tenantID, taskID string, retryAt time.Time) error
	ListByStatus(ctx context.Context, tenantID string, status core.TaskStatus, limit int) ([]*core.Task, error)
	SetResultRef(ctx context.Context, tenantID, taskID, resultRef string) error
	CountByStatus(ctx context.Context, tenantID string, status core.TaskStatus) (int, error)
	CountRunningByAgent(ctx context.Context, tenantID, agentID string) (int, error)
	ResetForReplay(ctx context.Context, tenantID, taskID string) error
}

type MailboxRepo interface {
	Create(ctx context.Context, mailbox core.Mailbox) error
	Get(ctx context.Context, tenantID, mailboxID string) (*core.Mailbox, error)
	ListByAgent(ctx context.Context, tenantID, agentID string) ([]*core.Mailbox, error)
	Backlog(ctx context.Context, tenantID, mailboxID string) (int, error)
	UpdateStatus(ctx context.Context, tenantID, mailboxID string, status core.MailboxStatus) error
	UpdateConfig(ctx context.Context, tenantID, mailboxID string, maxConcurrency, ackWaitSeconds, maxDeliver, retentionSeconds int) error
}

type TaskAttemptRepo interface {
	Create(ctx context.Context, attempt core.TaskAttempt) error
	GetLatest(ctx context.Context, tenantID, taskID string) (*core.TaskAttempt, error)
	UpdateHeartbeat(ctx context.Context, tenantID, taskID string, attempt int) error
	UpdateFinished(ctx context.Context, tenantID, taskID string, attempt int, status string, errJSON []byte, usageJSON []byte) error
	UpdateFinishedWithCheck(ctx context.Context, tenantID, taskID string, attempt int, status string, errJSON []byte, usageJSON []byte) (bool, error)
}

type BudgetRepo interface {
	Upsert(ctx context.Context, spec core.BudgetSpec) error
	Get(ctx context.Context, tenantID string, scopeType core.BudgetScopeType, scopeID string) (*core.BudgetSpec, error)
	ListByTenant(ctx context.Context, tenantID string) ([]*core.BudgetSpec, error)
}

type PolicyRuleRepo interface {
	Create(ctx context.Context, rule core.PolicyRule) error
	ListActive(ctx context.Context, tenantID string) ([]*core.PolicyRule, error)
}

type QueueDriver interface {
	core.QueueEventDriver
}

type OutboxWriter interface {
	InsertDirect(ctx context.Context, id, tenantID, kind string, payload json.RawMessage) error
}

type HeartbeatDriver interface {
	core.HeartbeatDriver
}

// Tx-scoped interfaces for the unified transaction path (Priority 1
// simplification). The pgx.Tx parameter is nil for in-memory adapters.

type TaskTxRepo interface {
	TaskRepo
	CreateTx(ctx context.Context, tx pgx.Tx, task core.Task) error
	UpdateStatusTx(ctx context.Context, tx pgx.Tx, tenantID, taskID string, status core.TaskStatus, attemptIncrement int) error
	UpdateStatusWithCheckTx(ctx context.Context, tx pgx.Tx, tenantID, taskID string, expectedStatus, newStatus core.TaskStatus, attemptIncrement int) (bool, error)
	SetResultRefTx(ctx context.Context, tx pgx.Tx, tenantID, taskID, resultRef string) error
	UpdateRetryAtTx(ctx context.Context, tx pgx.Tx, tenantID, taskID string, retryAt time.Time) error
}

type AttemptTxRepo interface {
	TaskAttemptRepo
	CreateTx(ctx context.Context, tx pgx.Tx, attempt core.TaskAttempt) error
	UpdateFinishedWithCheckTx(ctx context.Context, tx pgx.Tx, tenantID, taskID string, attempt int, status string, errJSON, usageJSON []byte) (bool, error)
}

type ApprovalTxRepo interface {
	ApprovalRepo
	UpdateDecisionTx(ctx context.Context, tx pgx.Tx, tenantID, approvalID, decision, approver, reason string) error
}

type OutboxTxWriter interface {
	OutboxWriter
	Insert(ctx context.Context, tx pgx.Tx, id, tenantID, kind string, payload json.RawMessage) error
}

type OutboxDedupeWriter interface {
	OutboxTxWriter
	InsertDirectWithDedupe(ctx context.Context, id, tenantID, kind, dedupeKey string, payload json.RawMessage) error
}
