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

	select {
	case p.events <- evt:
	default:
		log.Printf("event projector: channel full, dropping event %s", evt.EventID)
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
			if err := p.writer.Record(ctx, evt); err != nil {
				log.Printf("event projector record: %v", err)
			}
		}
	}
}

func (p *EventProjector) Stop() {
	close(p.done)
}
