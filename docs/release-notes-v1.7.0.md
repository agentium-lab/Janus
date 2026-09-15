# v1.7.0 — Unified Event Pipeline + Production Readiness

## Breaking: MCP tool events enter the persistent audit chain

MCP gateway tool lifecycle events (requested/started/failed) now flow
through the outbox table via `OutboxEventRecorder` — the same persistent
path as task events. Previously they bypassed the outbox entirely and
were invisible to the AuditProjector.

## Replay with detailed status

Audit replay returns `{projected, failed, skipped, status, last_error}`:
- Partial failure → `207 Multi-Status`
- All failed → `422 Unprocessable Entity`
- Success → `200 OK`

## Metrics precision

- `janus_outbox_dead_total` increments ONLY on actual dead transition
  (RETURNING clause detects the state change in the UPDATE)
- New `janus_outbox_dead_retried_total` for requeue counting

## Helm chart hardened

- Schema.sql includes all migration columns (bound_agent_id was missing)
- CI validates chart migrations match canonical (file-level diff)
- CI validates chart templates render correctly
- `scripts/sync_helm_migrations.sh` generates chart copies from canonical

## Integration regression: crash recovery

New test: create 20 tasks → `kill -9` → restart → verify:
- All 20 tasks survive (zero loss)
- All outbox entries processed (published + projected)
- Audit table populated

## Security regression

Tenant isolation, key revocation, and idempotent replay tests added
to the invariants suite.

## Verification

33 Go packages (race), Python 15, TypeScript 23, official-client interop,
invariants suite, crash recovery — all green. `make verify` 91.2% ≥ 90%.
