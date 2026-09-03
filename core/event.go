package core

import (
	"encoding/json"
	"time"
)

// EventType represents the type of a Janus event.
type EventType string

// Task events
const (
	EventTaskCreated         EventType = "task.created"
	EventTaskQueued          EventType = "task.queued"
	EventTaskApprovalPending EventType = "task.approval_pending"
	EventTaskClaimed         EventType = "task.claimed"
	EventTaskStarted         EventType = "task.started"
	EventTaskHeartbeat       EventType = "task.heartbeat"
	EventTaskProgress        EventType = "task.progress"
	EventTaskBlocked         EventType = "task.blocked"
	EventTaskCompleted       EventType = "task.completed"
	EventTaskFailed          EventType = "task.failed"
	EventTaskRetryScheduled  EventType = "task.retry_scheduled"
	EventTaskDeadLettered    EventType = "task.dead_lettered"
	EventTaskCancelled       EventType = "task.cancelled"
	EventTaskExpired         EventType = "task.expired"
)

// Agent events
const (
	EventAgentRegistered      EventType = "agent.registered"
	EventAgentUpdated         EventType = "agent.updated"
	EventAgentOnline          EventType = "agent.online"
	EventAgentOffline         EventType = "agent.offline"
	EventAgentHeartbeat       EventType = "agent.heartbeat"
	EventAgentCapacityChanged EventType = "agent.capacity_changed"
	EventAgentCapChanged      EventType = "agent.capability_changed"
)

// Policy events
const (
	EventPolicyAllowed          EventType = "policy.allowed"
	EventPolicyDenied           EventType = "policy.denied"
	EventPolicyApprovalRequired EventType = "policy.approval_required"
	EventPolicyDLPRedacted      EventType = "policy.dlp_redacted"
	EventPolicyScopeReduced     EventType = "policy.context_scope_reduced"
)

// Budget events
const (
	EventBudgetReserved EventType = "budget.reserved"
	EventBudgetConsumed EventType = "budget.consumed"
	EventBudgetReleased EventType = "budget.released"
	EventBudgetExceeded EventType = "budget.exceeded"
)

// Tool events
const (
	EventToolInvocationRequested EventType = "tool.invocation_requested"
	EventToolInvocationAllowed   EventType = "tool.invocation_allowed"
	EventToolInvocationDenied    EventType = "tool.invocation_denied"
	EventToolInvocationStarted   EventType = "tool.invocation_started"
	EventToolInvocationCompleted EventType = "tool.invocation_completed"
	EventToolInvocationFailed    EventType = "tool.invocation_failed"
)

// System events
const (
	EventQueueBacklogHigh   EventType = "queue.backlog_high"
	EventConsumerLagHigh    EventType = "consumer.lag_high"
	EventSchedulerThrottled EventType = "scheduler.throttled"
	EventStorageError       EventType = "storage.error"
	EventNodeUnhealthy      EventType = "node.unhealthy"
)

// Security audit events (SEC-09)
const (
	EventSecurityAPIKeyCreated     EventType = "security.api_key_created"
	EventSecurityAPIKeyRevoked     EventType = "security.api_key_revoked"
	EventSecurityAuthFailed        EventType = "security.auth_failed"
	EventSecurityTenantGuardDenied EventType = "security.tenant_guard_denied"
)

// JanusEvent is the immutable fact record in Janus.
type JanusEvent struct {
	EventID     string    `json:"event_id"`
	EventType   EventType `json:"event_type"`
	Timestamp   time.Time `json:"timestamp"`
	TenantID    string    `json:"tenant_id"`
	TraceID     string    `json:"trace_id,omitempty"`
	TaskID      string    `json:"task_id,omitempty"`
	SourceAgent string    `json:"source_agent,omitempty"`
	TargetAgent string    `json:"target_agent,omitempty"`
	ActorType   string    `json:"actor_type,omitempty"`
	ActorID     string    `json:"actor_id,omitempty"`
	Payload     []byte    `json:"payload"`
}

// TaskProgress is the payload for task.progress events. Message is the only
// required field; percent and data let agents express richer status without
// the schema needing to enumerate every use case.
type TaskProgress struct {
	Message string          `json:"message"`
	Percent *int            `json:"percent,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
}
