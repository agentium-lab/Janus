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
		if evt.EventID == "" {
			// Legacy rows written before write-point enrichment. Derive the
			// identity from the outbox row ID so re-projection stays
			// idempotent AND distinct rows cannot collapse onto one audit
			// record (event repo dedupes on (tenant_id, event_id)).
			evt.EventID = "obx_" + entry.ID
		}
		if evt.Timestamp.IsZero() {
			evt.Timestamp = entry.CreatedAt
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
type ReplayResult struct {
	Projected int    `json:"projected"`
	Failed    int    `json:"failed"`
	Skipped   int    `json:"skipped"`
	Status    string `json:"status"` // "success" | "partial_failure" | "all_failed"
	LastError string `json:"last_error,omitempty"`
}

func (p *AuditProjector) Replay(ctx context.Context, tenantID string, from, to time.Time, limit int) (ReplayResult, error) {
	if limit <= 0 || limit > 1000 {
		limit = 500
	}
	entries, err := p.reader.FetchByRange(ctx, tenantID, from, to, limit)
	if err != nil {
		return ReplayResult{}, fmt.Errorf("replay fetch: %w", err)
	}
	result := ReplayResult{Status: "success"}
	for _, entry := range entries {
		if entry.Kind != "event_publish" {
			result.Skipped++
			continue
		}
		var evt core.JanusEvent
		if err := json.Unmarshal(entry.Payload, &evt); err != nil {
			result.Failed++
			result.LastError = err.Error()
			continue
		}
		if evt.EventID == "" {
			evt.EventID = "obx_" + entry.ID
		}
		if evt.Timestamp.IsZero() {
			evt.Timestamp = entry.CreatedAt
		}
		if err := p.writer.RecordIdempotent(ctx, evt); err != nil {
			result.Failed++
			result.LastError = err.Error()
			continue
		}
		result.Projected++
	}
	switch {
	case result.Failed > 0 && result.Projected > 0:
		result.Status = "partial_failure"
	case result.Failed > 0:
		result.Status = "all_failed"
	}
	return result, nil
}

// OutboxEventRecorder writes events directly to the outbox table as
// event_publish entries — the persistent path. The outbox Publisher later
// delivers to NATS, and the AuditProjector projects to the audit table.
// This replaces direct PublishEvent calls from gateways (P1 fix).
type OutboxEventRecorder struct {
	repo  OutboxDirectWriter
	clock func() string
}

// OutboxDirectWriter is the subset of OutboxRepo needed for event recording.
type OutboxDirectWriter interface {
	InsertDirectWithDedupe(ctx context.Context, id, tenantID, kind, dedupeKey string, payload json.RawMessage) error
}

func NewOutboxEventRecorder(repo OutboxDirectWriter) *OutboxEventRecorder {
	return &OutboxEventRecorder{repo: repo, clock: func() string {
		return fmt.Sprintf("evt_%d", time.Now().UnixNano())
	}}
}

func (r *OutboxEventRecorder) PublishEvent(ctx context.Context, event core.JanusEvent) error {
	if event.EventID == "" {
		event.EventID = r.clock()
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	return r.repo.InsertDirectWithDedupe(ctx, event.EventID, event.TenantID,
		"event_publish", event.EventID, payload)
}
