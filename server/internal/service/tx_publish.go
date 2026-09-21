package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/jackc/pgx/v5"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/auth"
	"github.com/agentium-lab/Janus/server/internal/driver/postgres"
)

// PublishingOutbox is the memory-mode OutboxDedupeWriter: instead of
// persisting rows for a background relay, it routes each entry to the queue
// driver synchronously — mirroring outbox.Publisher.publishOne. Tests that
// wire this run the SAME service code path as production; only the relay
// transport differs (direct call vs outbox table + poller).
type PublishingOutbox struct {
	driver core.QueueEventDriver

	mu       sync.Mutex
	dedupeKV map[string]struct{}
}

func NewPublishingOutbox(driver core.QueueEventDriver) *PublishingOutbox {
	return &PublishingOutbox{driver: driver, dedupeKV: make(map[string]struct{})}
}

func (o *PublishingOutbox) Insert(ctx context.Context, _ pgx.Tx, id, tenantID, kind string, payload json.RawMessage) error {
	return o.route(ctx, id, tenantID, kind, payload)
}

func (o *PublishingOutbox) InsertDirect(ctx context.Context, id, tenantID, kind string, payload json.RawMessage) error {
	return o.route(ctx, id, tenantID, kind, payload)
}

func (o *PublishingOutbox) InsertDirectWithDedupe(ctx context.Context, id, tenantID, kind, dedupeKey string, payload json.RawMessage) error {
	o.mu.Lock()
	if _, seen := o.dedupeKV[dedupeKey]; seen {
		o.mu.Unlock()
		return nil
	}
	o.dedupeKV[dedupeKey] = struct{}{}
	o.mu.Unlock()
	return o.route(ctx, id, tenantID, kind, payload)
}

func (o *PublishingOutbox) route(ctx context.Context, id, tenantID, kind string, payload json.RawMessage) error {
	if o.driver == nil {
		return nil
	}
	switch kind {
	case "task_publish":
		var msg core.TaskMessage
		if err := json.Unmarshal(payload, &msg); err != nil {
			return err
		}
		msg.DedupeKey = id
		return o.driver.PublishTask(ctx, msg)
	case "event_publish":
		var event core.JanusEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			return err
		}
		if actor := auth.ActingUserFromContext(ctx); actor != "" {
			event.Payload = appendClaimedActor(event.Payload, actor)
		}
		if err := enrichEvent(&event); err != nil {
			return err
		}
		return o.driver.PublishEvent(ctx, event)
	case "dlq_publish":
		var msg core.TaskMessage
		if err := json.Unmarshal(payload, &msg); err != nil {
			return err
		}
		errPayload := []byte(msg.Headers["error"])
		msg.DedupeKey = id
		return o.driver.PublishDLQ(ctx, msg, errPayload)
	default:
		return fmt.Errorf("publishing outbox: unknown kind %q", kind)
	}
}

// BudgetLedger settles token usage for a finished attempt. The PG
// implementation writes an idempotent ledger row (insert-only) plus usage
// increments inside the caller's transaction; the memory implementation
// delegates to BudgetService.Settle.
type BudgetLedger interface {
	SettleTx(ctx context.Context, tx pgx.Tx, e core.LedgerEntry) error
}

type budgetLedgerFunc func(ctx context.Context, tx pgx.Tx, e core.LedgerEntry) error

func (f budgetLedgerFunc) SettleTx(ctx context.Context, tx pgx.Tx, e core.LedgerEntry) error {
	return f(ctx, tx, e)
}

// MemoryBudgetLedger adapts BudgetService.Settle into the BudgetLedger
// interface (tx is ignored, matching MemoryLifecycle's nil tx).
func MemoryBudgetLedger(budgetSvc *BudgetService) BudgetLedger {
	return budgetLedgerFunc(func(ctx context.Context, _ pgx.Tx, e core.LedgerEntry) error {
		usage := &core.TokenUsage{
			PromptTokens:     int(e.PromptTokens),
			CompletionTokens: int(e.CompletionTokens),
			TotalTokens:      int(e.TotalTokens),
		}
		return budgetSvc.Settle(ctx, e.TenantID, e.ScopeID, usage)
	})
}

// logOutboxWrite is the shared non-fatal outbox failure logger (audit write
// failures must never block real-time delivery).
func logOutboxWrite(taskID string, err error) {
	log.Printf("task %s: outbox write failed: %v", taskID, err)
}

// PGBudgetLedger adapts the PG BudgetUsageRepo into BudgetLedger: insert-only
// ledger row, usage increments only on the first insert (idempotent settle).
func PGBudgetLedger(repo *postgres.BudgetUsageRepo) BudgetLedger {
	return budgetLedgerFunc(func(ctx context.Context, tx pgx.Tx, e core.LedgerEntry) error {
		inserted, err := repo.InsertLedgerTx(ctx, tx, e)
		if err != nil {
			return err
		}
		if !inserted {
			return nil
		}
		return repo.IncrementUsageTx(ctx, tx, e.TenantID, e.ScopeType, e.ScopeID,
			e.PromptTokens, e.CompletionTokens, e.TotalTokens, e.CostUSD)
	})
}
