# v1.6.8 — Coverage Restored to 90%+ | Full Pipeline Verified

## Coverage: 84.9% → 91.5% (threshold raised 85% → 90%)

Coverage had drifted from 93.9% to 84.9% over eleven review rounds because
new code was added without proportional tests. Three targeted fixes:

- **auth** 48.1% → 85.1%: identity_guard functions now have direct
  in-package tests (previously tested only from invariants package)
- **retry** 32.5% → 97.4% (with PG): PG-backed scheduler tests enabled
- **TransitionInTx** error message aligned with sentinel error chain

## Full pipeline verification (real process + real PostgreSQL)

End-to-end audit projection verified in PG-only mode (no NATS, no Redis):
- Task created → outbox entries written → Publisher publishes (status
  cursor) → AuditProjector projects (projected_at cursor) → audit table
  contains task.created + task.queued

Dual-cursor architecture confirmed working: both consumers independently
process the same outbox table without competing.

## Smart customer service scenario (race × 3)

All 4 subtests green: approval gate, supervisor approval + agent lifecycle,
multi-agent fan-out, crash recovery with requeue.

## Cumulative fixes since v1.6.7

- P0-1: dual-cursor (projected_at + status) — Publisher and Projector
  use independent cursors
- P0-2: projectorCh deadlock removed (memory channel fully deleted)
- P0-3: SQL column fix (projected_at, not updated_at)
- P1: RetryDead background job, replay REST endpoint, metrics fix
- P2: audit replay requires admin scope
- E2E TestMain wires production event pipeline (outbox + Publisher +
  AuditProjector)
- Integration tests reuse openOutboxTestDB (proper DB creation + migrations)
