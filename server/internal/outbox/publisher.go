package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/driver/postgres"
	"github.com/agentium-lab/Janus/server/internal/metrics"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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

// publishBatchTimeout caps how long a single publishBatch run may take. A
// batch that exceeds this window aborts in-flight publishes (their entries stay
// in 'publishing' and are reclaimed via the lease once it expires).
const publishBatchTimeout = 30 * time.Second

func (p *Publisher) publishBatch(ctx context.Context) {
	batchCtx, cancel := context.WithTimeout(ctx, publishBatchTimeout)
	defer cancel()

	entries, err := p.repo.FetchPending(batchCtx, 100)
	if err != nil {
		log.Printf("outbox fetch: %v", err)
		return
	}
	metrics.OutboxPending.Set(float64(len(entries)))

	for _, e := range entries {
		metrics.OutboxPublishTotal.Inc()
		if err := p.publishOne(batchCtx, e); err != nil {
			metrics.OutboxPublishFailedTotal.Inc()
			log.Printf("outbox publish %s: %v", e.ID, err)
			_ = p.repo.MarkFailedWithReason(batchCtx, e.ID, err.Error())
			continue
		}
		// NATS publish succeeded. If MarkPublished fails here, the row stays
		// 'publishing' until its lease expires, after which another worker
		// reclaims and re-publishes. The outbox entry ID is set as the NATS
		// Nats-Msg-Id (via TaskMessage.DedupeKey), so JetStream's dedup window
		// drops the redelivery as a duplicate. Log the mark failure so it is
		// observable; do NOT call MarkFailed (that would retry immediately,
		// increasing duplicate risk).
		if err := p.repo.MarkPublished(batchCtx, e.ID); err != nil {
			log.Printf("outbox mark-published %s failed (will be reclaimed via lease): %v", e.ID, err)
		}
		if batchCtx.Err() != nil {
			break
		}
	}
}

func (p *Publisher) publishOne(ctx context.Context, e postgres.OutboxEntry) error {
	ctx, span := otel.Tracer("janus").Start(ctx, "outbox.publishOne",
		trace.WithAttributes(attribute.String("outbox.kind", e.Kind)),
	)
	defer span.End()
	switch e.Kind {
	case "task_publish":
		var msg core.TaskMessage
		if err := json.Unmarshal(e.Payload, &msg); err != nil {
			return err
		}
		msg.DedupeKey = e.ID
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
		errPayload := []byte(msg.Headers["error"])
		msg.DedupeKey = e.ID
		return p.driver.PublishDLQ(ctx, msg, errPayload)
	default:
		return fmt.Errorf("unknown outbox kind: %q", e.Kind)
	}
}
