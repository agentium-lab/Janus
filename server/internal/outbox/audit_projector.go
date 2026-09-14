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

// AuditProjector reads event_publish entries from the outbox table using
// its OWN cursor (projected_at column) — independent of the status field
// that the NATS Publisher owns. This separation (ADR-0006) means the two
// consumers never compete: the Publisher tracks publish progress via
// status, the Projector tracks audit progress via projected_at.
//
// Idempotency: audit_event_projection has PRIMARY KEY (tenant_id, event_id)
// with ON CONFLICT DO NOTHING — reprocessing is always safe.

// AuditWriter is the durable sink for audit events.
type AuditWriter interface {
	RecordIdempotent(ctx context.Context, evt core.JanusEvent) error
}

// AuditReader provides the projector's independent cursor.
type AuditReader interface {
	FetchUnprojected(ctx context.Context, limit int) ([]postgres.OutboxEntry, error)
	MarkProjected(ctx context.Context, id string) error
	FetchByRange(ctx context.Context, tenantID string, from, to time.Time, limit int) ([]postgres.OutboxEntry, error)
}

type AuditProjector struct {
	reader    AuditReader
	writer    AuditWriter
	interval  time.Duration
	batchSize int
	done      chan struct{}
}

func NewAuditProjector(reader AuditReader, writer AuditWriter) *AuditProjector {
	return &AuditProjector{
		reader:    reader,
		writer:    writer,
		interval:  500 * time.Millisecond,
		batchSize: 100,
		done:      make(chan struct{}),
	}
}

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
	entries, err := p.reader.FetchUnprojected(ctx, p.batchSize)
	if err != nil {
		log.Printf("audit projector: fetch unprojected: %v", err)
		return
	}
	for _, entry := range entries {
		if entry.Kind != "event_publish" {
			continue // defensive: FetchUnprojected already filters, but belt-and-suspenders
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
			continue
		}
		if err := p.reader.MarkProjected(ctx, entry.ID); err != nil {
			log.Printf("audit projector: mark projected %s: %v", entry.ID, err)
			metrics.AuditProjectionErrors.Inc()
			continue
		}
		metrics.AuditProjectionWritten.Inc()
	}
}

// Replay re-projects a time range for a tenant (REST replay endpoint).
func (p *AuditProjector) Replay(ctx context.Context, tenantID string, from, to time.Time, limit int) (int, error) {
	if limit <= 0 || limit > 1000 {
		limit = 500
	}
	entries, err := p.reader.FetchByRange(ctx, tenantID, from, to, limit)
	if err != nil {
		return 0, fmt.Errorf("replay fetch: %w", err)
	}
	projected, skipped := 0, 0
	var lastErr error
	for _, entry := range entries {
		if entry.Kind != "event_publish" {
			continue
		}
		var evt core.JanusEvent
		if err := json.Unmarshal(entry.Payload, &evt); err != nil {
			skipped++
			lastErr = err
			continue
		}
		if err := p.writer.RecordIdempotent(ctx, evt); err != nil {
			skipped++
			lastErr = err
			continue
		}
		projected++
	}
	if skipped > 0 && projected == 0 {
		return projected, fmt.Errorf("replay: all %d entries failed; last error: %w", skipped, lastErr)
	}
	return projected, nil
}
