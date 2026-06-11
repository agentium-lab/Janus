package core

import (
	"context"
	"time"
)

// DeliveryRef is an opaque reference to a delivered message.
// The underlying driver assigns and interprets this.
type DeliveryRef string

// NackReason specifies why a task was negatively acknowledged.
type NackReason string

const (
	NackRetriable    NackReason = "retriable"
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
	Priority Priority
	Payload  []byte
	Headers  map[string]string
}

// TaskDelivery is a task fetched from a mailbox.
type TaskDelivery struct {
	TaskID          string
	Payload         []byte
	DeliveryRef     DeliveryRef
	RedeliveryCount int
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
	FetchTasks(ctx context.Context, mailbox string, opts FetchOptions) ([]TaskDelivery, error)
	AckTask(ctx context.Context, ref DeliveryRef) error
	NackTask(ctx context.Context, ref DeliveryRef, reason NackReason) error
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
