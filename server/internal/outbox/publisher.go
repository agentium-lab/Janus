package outbox

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/driver/postgres"
)

// OutboxRepo defines the interface needed by Publisher for outbox operations.
type OutboxRepo interface {
	FetchPending(ctx context.Context, limit int) ([]postgres.OutboxEntry, error)
	MarkPublished(ctx context.Context, id string) error
	MarkFailedWithReason(ctx context.Context, id string, reason string) error
}

// Publisher dispatches outbox events to the queue driver.
type Publisher struct {
	repo   OutboxRepo
	driver core.QueueEventDriver
	done   chan struct{}
}

// NewPublisher creates a Publisher with the given repo and driver.
func NewPublisher(repo OutboxRepo, driver core.QueueEventDriver) *Publisher {
	return &Publisher{
		repo:   repo,
		driver: driver,
		done:   make(chan struct{}),
	}
}

func (p *Publisher) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.done:
			return
		case <-ticker.C:
			p.publishBatch(ctx)
		}
	}
}

func (p *Publisher) Stop() {
	close(p.done)
}

func (p *Publisher) publishBatch(ctx context.Context) {
	entries, err := p.repo.FetchPending(ctx, 100)
	if err != nil {
		log.Printf("outbox fetch: %v", err)
		return
	}

	for _, e := range entries {
		if err := p.publishOne(ctx, e); err != nil {
			log.Printf("outbox publish %s: %v", e.ID, err)
			_ = p.repo.MarkFailedWithReason(ctx, e.ID, err.Error())
			continue
		}
		// NATS publish succeeded. If MarkPublished fails here, the row stays
		// 'publishing' until its lease expires, after which another worker
		// reclaims and re-publishes. NATS Nats-Msg-Id dedupe (when enabled)
		// prevents the redelivery from causing a duplicate task. Log the mark
		// failure so it is observable; do NOT call MarkFailed (that would retry
		// immediately, increasing duplicate risk).
		if err := p.repo.MarkPublished(ctx, e.ID); err != nil {
			log.Printf("outbox mark-published %s failed (will be reclaimed via lease): %v", e.ID, err)
		}
	}
}

func (p *Publisher) publishOne(ctx context.Context, e postgres.OutboxEntry) error {
	switch e.Kind {
	case "task_publish":
		var msg core.TaskMessage
		if err := json.Unmarshal(e.Payload, &msg); err != nil {
			return err
		}
		return p.driver.PublishTask(ctx, msg)
	case "event_publish":
		var event core.JanusEvent
		if err := json.Unmarshal(e.Payload, &event); err != nil {
			return err
		}
		return p.driver.PublishEvent(ctx, event)
	case "dlq_publish":
		var msg core.TaskMessage
		if err := json.Unmarshal(e.Payload, &msg); err != nil {
			return err
		}
		// The DLQ error payload is carried in msg.Headers["error"].
		errPayload := []byte(msg.Headers["error"])
		return p.driver.PublishDLQ(ctx, msg, errPayload)
	default:
		return nil
	}
}
