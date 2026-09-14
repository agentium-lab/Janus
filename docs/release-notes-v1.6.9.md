# v1.6.9 — Stability Release

## Helm chart fixed
- Migrations 000016 (bound_agent_id) and 000017 (projected_at) synced
  into the chart — Helm installs now get the correct schema

## MCP tool events hardened
- `emitToolEvent` generates unique EventID + Timestamp (fixes NATS
  Msg-Id collision where multiple tool events for one call were deduped)

## Audit replay error reporting
- Replay returns error when ALL entries fail (was silently 200 + 0)

## Metrics precision
- `janus_outbox_dead_total` increments on actual dead requeues, not on
  every retry failure

## Coverage
- Local: 91.5% ≥ 90% (Makefile default)
- CI: 86.5% ≥ 85% (CI threshold; PG timing variance accounted for)
- auth package: 48% → 85% (identity guard tests moved in-package)
- retry package: 33% → 97% (PG scheduler tests enabled)

## Verification (all local + CI green)
- 32 Go packages (race), Python 15, TypeScript 23
- PG-only process guard + audit projection E2E (dual-cursor verified)
- Official a2a-go client interop suite
- Smart customer service scenario (race × 3)
