package core

import (
	"context"
	"encoding/json"
	"time"
)

// DeliveryRef is an opaque reference to a delivered message.
// The underlying driver assigns and interprets this.
type DeliveryRef string

// NackReason specifies why a task was negatively acknowledged.
type NackReason string

const (
	NackRetriable    NackReason = "retriable"
	NackRetriableDelayed NackReason = "retriable_delayed"
	NackNonRetriable NackReason = "non_retriable"
)

// FetchOptions controls pull behavior.
type FetchOptions struct {
	MaxMessages int
	WaitTime    time.Duration
}

// TaskMessage is the driver-level representation of a task to be published.
type TaskMessage struct {
	TenantID string
	MailboxID string
	TaskID   string
	Attempt	  int
	Priority Priority
	Payload  []byte
	DedupeKey   string
	Headers  map[string]string
}

// TaskDelivery is a task fetched from a mailbox.
type TaskDelivery struct {
	TaskID          string
	Attempt			int
	Payload         []byte
	DeliveryRef     DeliveryRef
	RedeliveryCount int
}

// DLQMessage is the outbox payload used to publish a task to a mailbox DLQ
type DLQMessage struct {
	Message		TaskMessage  `json:"message"`
	ErrorPayload 	json.RawMessage  `json:"error_payload"`
}

type DLQStreamMessage struct {
	TenantID		string		`json:"tenant_id"`
	MailboxID		string		`json:"mailbox_id"`
	TaskID			string		`json:"task_id"`
	Attempt			int			`json:"attempt"`
	AttemptCount	int			`json:"attempt_count,omitempty"`
	Priority 		Priority	`json:"priority,omitempty"`
	OriginalEnvelope json.RawMessage	`json:"original_envelope,omitempty"`
	ErrorPayload	json.RawMessage 	`json:"error_payload,omitempty"`
	FailureReason	string		`json:"failure_reason,omitempty"`
	PolicyDecisionID	string	`json:"policy_decision_id,omitempty"`
	FirstFailedAt	*time.Time	`json:"first_failed_at,omitempty"`
	DeadLetteredAt	*time.Time	`json:"dead_lettered_at"`
	DedupeKey		string		`json:"dedupe_key,omitempty"`
	Headers			map[string]string	`json:"headers,omitempty"`
}

// MailboxSpec defines the configuration for creating a mailbox in the driver.
type MailboxSpec struct {
	TenantID         string
	MailboxID        string
	AgentID          string
	MaxConcurrency   int
	ACKWaitSeconds   int
	MaxDeliver       int
	RetentionSeconds int
}

// ConsumerSpec defines the configuration for creating a consumer in the driver.
type ConsumerSpec struct {
	TenantID        string
	MailboxID       string
	DurableName     string
	ACKWaitSeconds  int
	MaxDeliver      int
	MaxACKPending   int
}

// EventReplayFilter controls which events to replay.
type EventReplayFilter struct {
	TenantID  string
	EventTypes []EventType
	StartTime *time.Time
	EndTime   *time.Time
	TaskID    string
	TraceID   string
}

// EventIterator streams replayed events.
type EventIterator interface {
	Next(ctx context.Context) (*JanusEvent, error)
	Close() error
}

// HeartbeatDriver abstracts heartbeat storage and offline detection.
// Default implementation: Redis. Future: Valkey, Dragonfly, etc.
type HeartbeatDriver interface {
	// Ping records an agent heartbeat with TTL.
	Ping(ctx context.Context, tenantID, agentID string) error

	// GetLastHeartbeat returns the last heartbeat timestamp for an agent.
	// Returns nil if no heartbeat found (key expired or never set).
	GetLastHeartbeat(ctx context.Context, tenantID, agentID string) (*time.Time, error)

	// ScanExpired returns agent IDs whose heartbeats have expired within a tenant.
	ScanExpired(ctx context.Context, tenantID string) ([]string, error)

	// Remove deletes a heartbeat entry.
	Remove(ctx context.Context, tenantID, agentID string) error

	// Close releases resources.
	Close() error
}

// QueueEventDriver is the abstraction over the message/event backend.
// Default implementation: NATS JetStream. Future: Pulsar, Kafka, etc.
type QueueEventDriver interface {
	// Task operations
	PublishTask(ctx context.Context, msg TaskMessage) error
	FetchTasks(ctx context.Context, tenantID, mailbox string, opts FetchOptions) ([]TaskDelivery, error)
	AckTask(ctx context.Context, tenantID string, ref DeliveryRef) error
	NackTask(ctx context.Context, tenantID string, ref DeliveryRef, reason NackReason) error
	PublishDLQ(ctx context.Context, msg TaskMessage, errPayload []byte) error

	// Event operations
	PublishEvent(ctx context.Context, event JanusEvent) error
	ReplayEvents(ctx context.Context, filter EventReplayFilter) (EventIterator, error)

	// Lifecycle
	EnsureTenant(ctx context.Context, tenantID string) error
	EnsureMailbox(ctx context.Context, spec MailboxSpec) error
	EnsureConsumer(ctx context.Context, spec ConsumerSpec) error
	Close() error
}
