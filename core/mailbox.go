package core

import (
	"math"
	"time"
)

// MailboxStatus represents the status of a mailbox.
type MailboxStatus string

const (
	MailboxStatusActive  MailboxStatus = "active"
	MailboxStatusPaused  MailboxStatus = "paused"
)

// RetryPolicy defines the retry configuration for a mailbox.
type RetryPolicy struct {
	MaxAttempts     int  `json:"max_attempts"`
	BackoffType     string `json:"backoff_type"` // "exponential"
	InitialSeconds  int  `json:"initial_seconds"`
	MaxSeconds      int  `json:"max_seconds"`
	Jitter          bool `json:"jitter"`
}

// DefaultRetryPolicy returns the default retry policy.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:    5,
		BackoffType:    "exponential",
		InitialSeconds: 10,
		MaxSeconds:     900,
		Jitter:         true,
	}
}

// Mailbox represents an agent's persistent task inbox.
func (p RetryPolicy) BackoffDuration(attempt int) time.Duration {
	if attempt <= 0 {
		return time.Duration(p.InitialSeconds) * time.Second
	}
	seconds := float64(p.InitialSeconds) * math.Pow(2, float64(attempt-1))
	if seconds > float64(p.MaxSeconds) {
		seconds = float64(p.MaxSeconds)
	}
	return time.Duration(seconds) * time.Second
}

func (p RetryPolicy) ExceedsMaxAttempts(attemptCount int) bool {
	return attemptCount >= p.MaxAttempts
}

type Mailbox struct {
	TenantID         string       `json:"tenant_id"`
	ID               string       `json:"id"`
	AgentID          string       `json:"agent_id"`
	Status           MailboxStatus `json:"status"`
	Priority         Priority     `json:"priority"`
	MaxConcurrency   int          `json:"max_concurrency"`
	ACKWaitSeconds   int          `json:"ack_wait_seconds"`
	MaxDeliver       int          `json:"max_deliver"`
	RetentionSeconds int          `json:"retention_seconds"`
	RetryPolicy      RetryPolicy  `json:"retry_policy"`
	CreatedAt        time.Time    `json:"created_at"`
	UpdatedAt        time.Time    `json:"updated_at"`
}
