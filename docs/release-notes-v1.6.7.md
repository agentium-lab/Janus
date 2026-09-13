# v1.6.7 — P0 Regression Remediation + Self-Review Fixes

## Critical fixes (P0 regressions from v1.6.6)

v1.6.6 introduced three P0 regressions in the audit pipeline. This release
fixes all three and adds the integration tests that would have caught them:

### Dual-cursor architecture (P0-1)
The Publisher (NATS path) and AuditProjector (audit path) now use
**independent cursors** on the outbox table:
- Publisher: `status` field (pending → publishing → published, as before)
- AuditProjector: `projected_at` column (NULL → timestamp, new migration 000017)

Both consumers work concurrently without competing or swallowing each
other's entries. Verified by real-PostgreSQL integration tests.

### projectorCh deadlock removed (P0-2)
The orphaned memory channel (256-buffer, filled then blocked every event
for 5 seconds) is fully deleted along with the old EventProjector dead code.

### SQL column fix (P0-3)
`MarkProjected` and `RetryDead` now reference the correct `projected_at`
column (migration 000017 adds it; the old code referenced a non-existent
`updated_at`).

## P1 wiring

- `RetryDead` background goroutine requeues dead entries every 60 seconds
- `POST /v1/tenants/{t}/audit/replay?from=&to=` REST endpoint (idempotent)
- `AuditProjectionWritten` metric only increments after successful `MarkProjected`

## P2 self-review fix

- `POST /audit/replay` requires admin scope (was accessible with task:write)

## Integration tests (new — the tests v1.6.6 lacked)

- `TestIntegration_PublisherAndProjector_Independent`: 40 mixed entries,
  both consumers run concurrently, assert ALL task_publish published AND
  ALL event_publish projected AND event_publish also published (dual-cursor
  independence)
- `TestIntegration_ProjectorAfterPublisher`: Publisher marks published
  first, Projector still projects (projected_at independent of status)
- Both run against real PostgreSQL with `-race`, verified green

## End-to-end verification (real process, real PostgreSQL)

Clean E2E: create task → 3 seconds → outbox shows all published + projected,
audit table contains task.created and task.queued events.

## Verification

33 Go packages (race), PG integration tests, official-client interop,
invariants suite — all green. `make verify` 85.0% ≥ 85%.
