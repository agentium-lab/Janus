# ADR-0006: Audit Pipeline — Outbox Pull-Based Projection

**Status:** Accepted
**Date:** 2026-09-09
**Replaces:** The memory-channel EventProjector (pre-v1.6.5)

## Context

Rounds 9-11 of adversarial review consistently found that audit events
could be lost when the in-memory projection channel was full, the projector
crashed, or the process restarted. Incremental fixes (bounded waits, retry
with backoff, better logging) reduced the loss window but never eliminated
it because the fundamental architecture — a memory channel between the
outbox and the audit table — was inherently volatile.

## Decision

Replace the memory-channel projector with a **pull-based AuditProjector**
that reads directly from the `outbox_events` table:

1. **Audit events are written to the outbox in the same transaction as the
   task state change** (existing outbox pattern — unchanged).
2. The AuditProjector periodically fetches `event_publish` entries from the
   outbox (SELECT ... FOR UPDATE SKIP LOCKED) and writes them to
   `audit_event_projection` using **ON CONFLICT (tenant_id, event_id) DO
   NOTHING** (idempotent — safe to reprocess).
3. Successful projection marks the entry as `projected`. Failed writes
   leave the entry `pending` — the next tick retries naturally.
4. A **REST replay endpoint** re-projects any tenant/time range from the
   outbox (idempotent, safe to call repeatedly).
5. **Dead outbox entries** (exhausted retries) are requeued by a background
   job with exponential backoff; the `janus_outbox_dead_total` counter
   tracks this for alerting.

## What this solves

- Audit events survive projector crashes and process restarts (the outbox
  row is the durable record; the projector is stateless)
- No memory channel to overflow
- Reprocessing is safe (idempotent writes)
- Historical replay is available via REST

## Known limitations (explicitly documented, not hidden)

- Dead entries requeue automatically, but if the underlying fault (e.g.,
  schema mismatch) persists, entries will cycle between pending and dead
  indefinitely — the `janus_outbox_dead_total` counter makes this visible
  for alerting, and manual intervention (fix the fault, call replay) is
  the operational path
- Multi-instance deployment: two projector instances may process the same
  entries concurrently; idempotent writes make this correct (no duplicates)
  but wasteful — leader election is a future optimization
- `projected` status is a new outbox state alongside `pending/retry/publishing/
  published/dead`; entries transition pending→projected for audit, independent
  of the publish path (pending→published for NATS)
