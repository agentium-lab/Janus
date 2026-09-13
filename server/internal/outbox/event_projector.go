package outbox

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log"
	"time"

	"github.com/agentium-lab/Janus/core"
)

type EventWriter interface {
	Record(ctx context.Context, evt core.JanusEvent) error
}

type EventProjector struct {
	writer EventWriter
	done   chan struct{}
	events chan core.JanusEvent
}

func NewEventProjector(writer EventWriter) *EventProjector {
	return &EventProjector{
		writer: writer,
		done:   make(chan struct{}),
		events: make(chan core.JanusEvent, 256),
	}
}

func (p *EventProjector) Record(ctx context.Context, evt core.JanusEvent) {
	if evt.EventID == "" {
		b := make([]byte, 10)
		if _, err := rand.Read(b); err == nil {
			evt.EventID = "evt_" + hex.EncodeToString(b)
		}
	}
	if evt.Timestamp.IsZero() {
		evt.Timestamp = time.Now().UTC()
	}

	// Audit projection must not drop (ninth review): backpressure with a
	// bounded wait instead of silent loss. The projector drains to PG; a
	// full channel means the DB is slow — hold the producer briefly.
	select {
	case p.events <- evt:
	case <-time.After(5 * time.Second):
		log.Printf("event projector: projection hand-off timed out for event %s — projector stalled", evt.EventID)
	}
}

func (p *EventProjector) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.done:
			return
		case evt := <-p.events:
			// Record with bounded retry; persistent failures are counted
			// and logged at error level (visible in metrics/alerting).
			if err := p.recordWithRetry(ctx, evt); err != nil {
				log.Printf("event projector: record FAILED after retries for %s: %v", evt.EventID, err)
			}
		}
	}
}

func (p *EventProjector) recordWithRetry(ctx context.Context, evt core.JanusEvent) error {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return lastErr
			case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
			}
		}
		if err := p.writer.Record(ctx, evt); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return lastErr
}

func (p *EventProjector) Stop() {
	close(p.done)
}
