package core

import (
	"fmt"
	"time"
)

// TaskStatus represents the lifecycle state of a task.
type TaskStatus string

const (
	TaskStatusCreated         TaskStatus = "created"
	TaskStatusQueued          TaskStatus = "queued"
	TaskStatusApprovalPending TaskStatus = "approval_pending"
	TaskStatusClaimed         TaskStatus = "claimed"
	TaskStatusRunning         TaskStatus = "running"
	TaskStatusBlocked         TaskStatus = "blocked"
	TaskStatusCompleted       TaskStatus = "completed"
	TaskStatusFailed          TaskStatus = "failed"
	TaskStatusRetryScheduled  TaskStatus = "retry_scheduled"
	TaskStatusDeadLettered    TaskStatus = "dead_lettered"
	TaskStatusExpired         TaskStatus = "expired"
	TaskStatusCancelled       TaskStatus = "cancelled"
)

// TerminalStates are states from which no further transition is allowed.
var TerminalStates = map[TaskStatus]bool{
	TaskStatusCompleted:    true,
	TaskStatusDeadLettered: true,
	TaskStatusExpired:      true,
	TaskStatusCancelled:    true,
}

// IsTerminal returns true if the status is a terminal state.
func (s TaskStatus) IsTerminal() bool {
	return TerminalStates[s]
}

// ValidTransitions defines allowed state transitions.
// Key = current state, Value = set of allowed next states.
var ValidTransitions = map[TaskStatus]map[TaskStatus]bool{
	TaskStatusCreated: {
		TaskStatusQueued: true,
	},
	TaskStatusQueued: {
		TaskStatusClaimed:         true,
		TaskStatusApprovalPending: true,
		TaskStatusExpired:         true,
		TaskStatusCancelled:       true,
	},
	TaskStatusApprovalPending: {
		TaskStatusQueued:    true,
		TaskStatusCancelled: true,
	},
	TaskStatusClaimed: {
		TaskStatusRunning: true,
	},
	TaskStatusRunning: {
		TaskStatusCompleted: true,
		TaskStatusFailed:    true,
		TaskStatusBlocked:   true,
		TaskStatusCancelled: true,
	},
	TaskStatusBlocked: {
		TaskStatusRunning:   true,
		TaskStatusCancelled: true,
	},
	TaskStatusFailed: {
		TaskStatusRetryScheduled: true,
		TaskStatusDeadLettered:    true,
	},
	TaskStatusRetryScheduled: {
		TaskStatusQueued: true,
	},
}

// CanTransition checks whether transitioning from current to next is valid.
func CanTransition(current, next TaskStatus) bool {
	allowed, ok := ValidTransitions[current]
	if !ok {
		return false
	}
	return allowed[next]
}

// Priority represents task priority level.
type Priority string

const (
	PriorityLow      Priority = "low"
	PriorityNormal   Priority = "normal"
	PriorityHigh     Priority = "high"
	PriorityCritical Priority = "critical"
)

// TargetType represents the type of task target.
type TargetType string

const (
	TargetTypeAgent      TargetType = "agent"
	TargetTypeMailbox    TargetType = "mailbox"
	TargetTypeCapability TargetType = "capability"
	TargetTypeGroup      TargetType = "group"
	TargetTypeHuman      TargetType = "human"
)

// Target specifies where a task should be routed.
type Target struct {
	Type  TargetType `json:"type"`
	Value string     `json:"value"`
}

// Budget specifies resource constraints for a task.
type Budget struct {
	MaxTokens   int      `json:"max_tokens,omitempty"`
	MaxCostUSD  float64  `json:"max_cost_usd,omitempty"`
	ModelClasses []string `json:"model_classes,omitempty"`
}

// PolicyContext specifies policy-related context for a task.
type PolicyContext struct {
	DataClassification     string   `json:"data_classification,omitempty"`
	RequiresHumanApproval bool     `json:"requires_human_approval,omitempty"`
	AllowedTools           []string `json:"allowed_tools,omitempty"`
}

// ContextRef represents a reference to external context.
type ContextRef struct {
	TenantID       string   `json:"tenant_id,omitempty"`
	ID             string   `json:"id,omitempty"`
	Type           string   `json:"type"`
	URI            string   `json:"uri"`
	Hash           string   `json:"hash,omitempty"`
	Classification string   `json:"classification,omitempty"`
	AccessScope    []string `json:"access_scope,omitempty"`
	ExpiresAt      string   `json:"expires_at,omitempty"`
	CreatedAt      string   `json:"created_at,omitempty"`
}

// Payload represents the business payload of a task.
type Payload struct {
	Type    string `json:"type"`
	Content string `json:"content"`
}

// TraceContext carries distributed tracing information.
type TraceContext struct {
	TraceID      string `json:"trace_id"`
	ParentTaskID string `json:"parent_task_id,omitempty"`
	SpanID       string `json:"span_id,omitempty"`
}

// TaskEnvelope is the standard message envelope for all Janus tasks.
type TaskEnvelope struct {
	JanusVersion   string        `json:"janus_version"`
	TaskID         string        `json:"task_id"`
	IdempotencyKey string        `json:"idempotency_key,omitempty"`
	TenantID       string        `json:"tenant_id"`
	SourceAgent    string        `json:"source_agent"`
	Target         Target        `json:"target"`
	Priority       Priority      `json:"priority"`
	Deadline       *time.Time    `json:"deadline,omitempty"`
	TTLSeconds     int           `json:"ttl_seconds,omitempty"`
	Budget         *Budget       `json:"budget,omitempty"`
	Policy         *PolicyContext `json:"policy,omitempty"`
	ContextRefs    []ContextRef  `json:"context_refs,omitempty"`
	Payload        Payload       `json:"payload"`
	Trace          TraceContext  `json:"trace"`
}

// Task represents the current state of a task in Janus.
type Task struct {
	TenantID       string        `json:"tenant_id"`
	ID             string        `json:"id"`
	IdempotencyKey string        `json:"idempotency_key,omitempty"`
	SourceAgent    string        `json:"source_agent"`
	TargetType     TargetType    `json:"target_type"`
	TargetValue    string        `json:"target_value"`
	MailboxID      string        `json:"mailbox_id"`
	Status         TaskStatus    `json:"status"`
	Priority       Priority      `json:"priority"`
	Deadline       *time.Time    `json:"deadline,omitempty"`
	TTLSeconds     int           `json:"ttl_seconds,omitempty"`
	Envelope       TaskEnvelope  `json:"envelope"`
	ResultRef      string        `json:"result_ref,omitempty"`
	Error          *TaskError    `json:"error,omitempty"`
	AttemptCount   int           `json:"attempt_count"`
	CreatedAt      time.Time     `json:"created_at"`
	UpdatedAt      time.Time     `json:"updated_at"`
	CompletedAt    *time.Time    `json:"completed_at,omitempty"`
}

// TaskError represents error information for a failed task.
type TaskError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// TaskAttempt represents a single execution attempt of a task.
type TaskAttempt struct {
	TenantID    string
	TaskID      string
	Attempt     int
	AgentID     string
	LeaseID     string
	DeliveryRef string
	Status      string
	StartedAt   time.Time
	HeartbeatAt *time.Time
	FinishedAt  *time.Time
	Error       *TaskError
	TokenUsage  *TokenUsage
}

type TokenUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

var validTargetTypes = map[TargetType]bool{
	TargetTypeAgent:      true,
	TargetTypeMailbox:    true,
	TargetTypeCapability: true,
	TargetTypeGroup:      true,
	TargetTypeHuman:      true,
}

var validPriorities = map[Priority]bool{
	PriorityLow:      true,
	PriorityNormal:   true,
	PriorityHigh:     true,
	PriorityCritical: true,
}

type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Field, e.Message)
}

func (e *TaskEnvelope) Validate() error {
	if e.JanusVersion == "" {
		return &ValidationError{Field: "janus_version", Message: "is required"}
	}
	if e.TaskID == "" {
		return &ValidationError{Field: "task_id", Message: "is required"}
	}
	if e.TenantID == "" {
		return &ValidationError{Field: "tenant_id", Message: "is required"}
	}
	if e.SourceAgent == "" {
		return &ValidationError{Field: "source_agent", Message: "is required"}
	}
	if e.Target.Type == "" {
		return &ValidationError{Field: "target.type", Message: "is required"}
	}
	if !validTargetTypes[e.Target.Type] {
		return &ValidationError{Field: "target.type", Message: fmt.Sprintf("invalid target type: %s", e.Target.Type)}
	}
	if e.Target.Value == "" {
		return &ValidationError{Field: "target.value", Message: "is required"}
	}
	if e.Priority != "" && !validPriorities[e.Priority] {
		return &ValidationError{Field: "priority", Message: fmt.Sprintf("invalid priority: %s", e.Priority)}
	}
	if e.Deadline != nil && e.Deadline.Before(time.Now()) {
		return &ValidationError{Field: "deadline", Message: "must be in the future"}
	}
	if e.TTLSeconds < 0 {
		return &ValidationError{Field: "ttl_seconds", Message: "must be positive"}
	}
	if e.Budget != nil {
		if e.Budget.MaxTokens < 0 {
			return &ValidationError{Field: "budget.max_tokens", Message: "must be non-negative"}
		}
		if e.Budget.MaxCostUSD < 0 {
			return &ValidationError{Field: "budget.max_cost_usd", Message: "must be non-negative"}
		}
	}
	if e.Payload.Type == "" {
		return &ValidationError{Field: "payload.type", Message: "is required"}
	}
	if e.Trace.TraceID == "" {
		return &ValidationError{Field: "trace.trace_id", Message: "is required"}
	}
	return nil
}
