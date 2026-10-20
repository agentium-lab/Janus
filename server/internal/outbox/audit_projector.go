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

// tickLocker lets a reader serialize projector ticks across replicas
// (pg_try_advisory_lock). Readers without it fall back to idempotent
// concurrent projection (audit rows dedupe on (tenant_id, event_id)).
// The Executor is the connection holding the tick lock — projection DB
// work under it must run through that connection (a pool-scoped query
// borrows a second connection and self-starves a small pool).
type tickLocker interface {
	AcquireTickLock(ctx context.Context, key string) (exec postgres.Executor, release func(), ok bool, err error)
}

// execAuditReader is implemented by readers that can run their cursor
// operations through a caller-held executor (the tick-lock connection).
type execAuditReader interface {
	FetchUnprojectedOn(ctx context.Context, exec postgres.Executor, limit int) ([]postgres.OutboxEntry, error)
	MarkProjectedOn(ctx context.Context, exec postgres.Executor, id string) error
}

// execAuditWriter is the executor-scoped AuditWriter path.
type execAuditWriter interface {
	RecordIdempotentOn(ctx context.Context, exec postgres.Executor, evt core.JanusEvent) error
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

func (p *AuditProjector) WithInterval(d time.Duration) *AuditProjector {
	if d > 0 {
		p.interval = d
	}
	return p
}

func (p *AuditProjector) WithBatchSize(n int) *AuditProjector {
	if n > 0 {
		p.batchSize = n
	}
	return p
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
			if locker, ok := p.reader.(tickLocker); ok {
				exec, release, locked, err := locker.AcquireTickLock(ctx, "audit_projector")
				if err != nil {
					log.Printf("audit projector: tick lock: %v", err)
					continue
				}
				if !locked {
					continue // another replica owns this tick
				}
				p.projectBatch(ctx, exec)
				release()
				continue
			}
			p.projectBatch(ctx, nil)
		}
	}
}

func (p *AuditProjector) Stop() { close(p.done) }

func (p *AuditProjector) projectBatch(ctx context.Context, exec postgres.Executor) {
	entries, err := p.fetchUnprojected(ctx, exec)
	if err != nil {
		log.Printf("audit projector: fetch unprojected: %v", err)
		return
	}
	for _, entry := range entries {
		if entry.Kind != "event_publish" {
			continue // defensive: FetchUnprojected already filters, but belt-and-suspenders
		}
		evt, err := NormalizeLegacyEvent(entry)
		if err != nil {
			log.Printf("audit projector: malformed payload for %s: %v", entry.ID, err)
			metrics.AuditProjectionErrors.Inc()
			_ = p.markProjected(ctx, exec, entry.ID)
			continue
		}
		if err := p.record(ctx, exec, evt); err != nil {
			log.Printf("audit projector: write %s: %v", evt.EventID, err)
			metrics.AuditProjectionErrors.Inc()
			continue
		}
		if err := p.markProjected(ctx, exec, entry.ID); err != nil {
			log.Printf("audit projector: mark projected %s: %v", entry.ID, err)
			metrics.AuditProjectionErrors.Inc()
			continue
		}
		metrics.AuditProjectionWritten.Inc()
	}
}

// fetchUnprojected, markProjected and record run on the supplied executor
// when exec is non-nil and the reader/writer supports it (the tick-lock
// connection); otherwise they use the pool-scoped methods.
func (p *AuditProjector) fetchUnprojected(ctx context.Context, exec postgres.Executor) ([]postgres.OutboxEntry, error) {
	if exec != nil {
		if r, ok := p.reader.(execAuditReader); ok {
			return r.FetchUnprojectedOn(ctx, exec, p.batchSize)
		}
	}
	return p.reader.FetchUnprojected(ctx, p.batchSize)
}

func (p *AuditProjector) markProjected(ctx context.Context, exec postgres.Executor, id string) error {
	if exec != nil {
		if r, ok := p.reader.(execAuditReader); ok {
			return r.MarkProjectedOn(ctx, exec, id)
		}
	}
	return p.reader.MarkProjected(ctx, id)
}

func (p *AuditProjector) record(ctx context.Context, exec postgres.Executor, evt core.JanusEvent) error {
	if exec != nil {
		if w, ok := p.writer.(execAuditWriter); ok {
			return w.RecordIdempotentOn(ctx, exec, evt)
		}
	}
	return p.writer.RecordIdempotent(ctx, evt)
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
		evt, err := NormalizeLegacyEvent(entry)
		if err != nil {
			result.Failed++
			result.LastError = err.Error()
			continue
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
