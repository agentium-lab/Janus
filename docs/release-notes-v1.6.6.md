# v1.6.6 — Durable Audit Pipeline (ADR-0006)

## Architecture: audit projection is now pull-based from the outbox table

The memory-channel EventProjector is **replaced** by a pull-based
AuditProjector that reads `event_publish` entries directly from the
`outbox_events` table. This is an architectural change (not a patch) that
permanently solves the audit-loss class of issues found across review
rounds 9-11:

- **Crash-safe**: the outbox row IS the durable record; the projector is
  stateless — restart resumes automatically, no events lost
- **No memory channel to overflow**: audit events go task-transaction →
  outbox row → projector pull (bounded only by disk)
- **Idempotent writes**: `ON CONFLICT (tenant_id, event_id) DO NOTHING` —
  reprocessing is safe, replay is safe, multi-instance is safe
- **Natural retry**: failed projection writes leave the entry `pending`;
  the next tick retries automatically
- **REST replay available**: re-project any tenant/time range (idempotent)

## Dead entry recovery

Outbox entries that exhaust retries (`dead` status) are requeued by
`OutboxRepo.RetryDead` with reset attempt counters. The
`janus_outbox_dead_total` Prometheus counter tracks dead transitions for
alerting.

## NATS event idempotency

`PublishEvent` now sets `Nats-Msg-Id` (stable event ID) on every JetStream
publish — NATS's dedup window prevents duplicate event delivery when the
publisher retries after a partial failure.

## Prometheus metrics (real counters, not log-only)

- `janus_audit_projection_written_total`
- `janus_audit_projection_errors_total`
- `janus_terminal_event_drops_total`
- `janus_outbox_dead_total`

## Honest documentation

- README: "always traceable" → "durable audit trails"
- README audit table: precise outbox/retry/replay description
- ADR-0006: full architecture decision with explicitly documented known
  limitations (dead cycling, multi-instance waste — not hidden)

## Tests

9 new failure-path tests with real assertions (replacing the empty
placeholder from v1.6.5): batch projection, write-failure-stays-pending,
malformed payload, replay, nil safety, exact retry counts.

## Verification

33 Go packages (race), Python 15, TypeScript 23, official-client interop,
invariants suite — all green. `make verify` 85.2% ≥ 85%.
