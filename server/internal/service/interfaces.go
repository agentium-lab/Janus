package service

import (
	"context"

	"github.com/agentium-lab/Janus/core"
)

type TenantRepo interface {
	Create(ctx context.Context, id, name string) error
	GetName(ctx context.Context, id string) (string, error)
}

type AgentRepo interface {
	Register(ctx context.Context, agent core.Agent) error
	Get(ctx context.Context, tenantID, agentID string) (*core.Agent, error)
	UpdateStatus(ctx context.Context, tenantID, agentID string, status core.AgentStatus) error
	UpdateHeartbeat(ctx context.Context, tenantID, agentID string) error
	List(ctx context.Context, tenantID string) ([]*core.Agent, error)
	ListByStatus(ctx context.Context, tenantID string, status core.AgentStatus) ([]*core.Agent, error)
}

type TaskRepo interface {
	Create(ctx context.Context, task core.Task) error
	Get(ctx context.Context, tenantID, taskID string) (*core.Task, error)
	GetByIdempotencyKey(ctx context.Context, tenantID, key string) (*core.Task, error)
	UpdateStatus(ctx context.Context, tenantID, taskID string, status core.TaskStatus, attemptIncrement int) error
	ListByStatus(ctx context.Context, tenantID string, status core.TaskStatus, limit int) ([]*core.Task, error)
}

type MailboxRepo interface {
	Create(ctx context.Context, mailbox core.Mailbox) error
	Get(ctx context.Context, tenantID, mailboxID string) (*core.Mailbox, error)
	ListByAgent(ctx context.Context, tenantID, agentID string) ([]*core.Mailbox, error)
}

type QueueDriver interface {
	core.QueueEventDriver
}

type HeartbeatDriver interface {
	core.HeartbeatDriver
}
