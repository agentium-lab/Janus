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
)

// AuditProjector reads audit events from the outbox table (persistent
// source) and writes them into the audit_event_projection table. This
// replaces the memory-channel approach: the outbox row IS the durable
// record; the projector is a pull-based consumer that can crash and resume
// without losing events (ADR-0006).
//
// Idempotency: audit_event_projection has PRIMARY KEY (tenant_id, event_id);
// re-processing an already-written event is a no-op (ON CONFLICT DO NOTHING
// in the writer, or a duplicate-key error that is swallowed as success).

// AuditWriter is the durable sink for audit events.
type AuditWriter interface {
	Record(ctx context.Context, evt core.JanusEvent) error
	// RecordIdempotent writes with ON CONFLICT DO NOTHING semantics.
	RecordIdempotent(ctx context.Context, evt core.JanusEvent) error
}

// OutboxReader provides access to outbox entries of kind event_publish.
type OutboxReader interface {
	FetchPending(ctx context.Context, limit int) ([]postgres.OutboxEntry, error)
	MarkProjected(ctx context.Context, id string) error
	// FetchByRange replays entries for a tenant/time window (REST replay).
	FetchByRange(ctx context.Context, tenantID string, from, to time.Time, limit int) ([]postgres.OutboxEntry, error)
}

type AuditProjector struct {
	reader    AuditReader
	writer    AuditWriter
	interval  time.Duration
	batchSize int
	done      chan struct{}
}

type AuditReader = OutboxReader

func NewAuditProjector(reader AuditReader, writer AuditWriter) *AuditProjector {
	return &AuditProjector{
		reader:    reader,
		writer:    writer,
		interval:  500 * time.Millisecond,
		batchSize: 100,
		done:      make(chan struct{}),
	}
}

// Start runs the pull loop until Stop. Each iteration fetches pending
// event_publish entries from the outbox and projects them. Crashes are
// safe: unprocessed entries remain in the outbox with their status intact.
func (p *AuditProjector) Start(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.done:
			return
		case <-ticker.C:
			p.projectBatch(ctx)
		}
	}
}

func (p *AuditProjector) Stop() { close(p.done) }

func (p *AuditProjector) projectBatch(ctx context.Context) {
	entries, err := p.reader.FetchPending(ctx, p.batchSize)
	if err != nil {
		log.Printf("audit projector: fetch pending: %v", err)
		return
	}
	for _, entry := range entries {
		if entry.Kind != "event_publish" {
			continue // task_publish entries are handled by the NATS publisher
		}
		var evt core.JanusEvent
		if err := json.Unmarshal(entry.Payload, &evt); err != nil {
			log.Printf("audit projector: malformed payload for %s: %v", entry.ID, err)
			metrics.AuditProjectionErrors.Inc()
			_ = p.reader.MarkProjected(ctx, entry.ID)
			continue
		}
		if err := p.writer.RecordIdempotent(ctx, evt); err != nil {
			log.Printf("audit projector: write %s: %v", evt.EventID, err)
			metrics.AuditProjectionErrors.Inc()
			continue // leave in outbox for the next tick (natural retry)
		}
		if err := p.reader.MarkProjected(ctx, entry.ID); err != nil {
			log.Printf("audit projector: mark projected %s: %v", entry.ID, err)
		}
		metrics.AuditProjectionWritten.Inc()
	}
}

// Replay re-projects a time range for a tenant (REST replay endpoint).
// Idempotent writes make this safe to call repeatedly.
func (p *AuditProjector) Replay(ctx context.Context, tenantID string, from, to time.Time, limit int) (int, error) {
	if limit <= 0 || limit > 1000 {
		limit = 500
	}
	entries, err := p.reader.FetchByRange(ctx, tenantID, from, to, limit)
	if err != nil {
		return 0, fmt.Errorf("replay fetch: %w", err)
	}
	projected := 0
	for _, entry := range entries {
		if entry.Kind != "event_publish" {
			continue
		}
		var evt core.JanusEvent
		if err := json.Unmarshal(entry.Payload, &evt); err != nil {
			continue
		}
		if err := p.writer.RecordIdempotent(ctx, evt); err != nil {
			continue
		}
		projected++
	}
	return projected, nil
}
