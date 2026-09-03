# v1.3.0 — Security & Reliability Hardening

This release closes every P0 from the second and third external security audits.

## Security

- **PostgreSQL queue tenant isolation**: every SQL operation (fetch/ack/nack/dead-letter/lease) filters by `tenant_id`; the `QueueEventDriver` interface now requires tenant context end-to-end. Cross-tenant mailbox/task ID collisions can no longer leak or mutate data.
- **Scope enforcement unified across all protocols**: A2A, MCP, ACP, WebSocket, and gRPC now enforce API-key scopes. A `task:read`-only key cannot write through any protocol gateway. gRPC injects the Principal before scope checks and correctly maps dispatch methods (Pull/Start/Heartbeat) to `task:write`.
- **`/v1/tenants` (exact path) now scope-enforced**: the tenant list/create endpoints were previously exempt from scope checks.
- **Agent identity preconditions**: `source_agent` must be a registered agent under the publishing tenant.

## Reliability

- **PG-only mode fully boot-safe**: outbox publisher and retry scheduler use the actual queue driver (not a nil NATS driver); `/readyz` guards against nil Redis/NATS in PG mode; Redis connection failure in PG mode degrades to a warning instead of a crash.
- **CI now boots the server in PG-only mode (no NATS)** and asserts `/readyz` passes on every commit.
- **CI runs `helm lint`** against the core chart.

## Deployment

- Helm chart values complete: every `.Values` reference in every template resolves (verified programmatically); `config.llm`, `prometheusRule` sections added; `appVersion` tracks releases.
- Default `replicaCount: 1` (the RWO + multi-replica guard now guides scale-up instead of blocking default installs).
- Mailbox `pause`/`resume` routes ordered before the generic mailbox handler (SDK was getting 404).
- A2A discovery serves both `/.well-known/agent.json` and `/.well-known/agent-card.json`.

## SDK

- **TypeScript**: `TaskEnvelope` interface with optional `envelope` field; `publishTask` auto-constructs the envelope when absent and passes caller `payload` through (no more hardcoded empty JSON).
- **Python**: no changes needed (envelope construction already correct).

## Known Limitations (unchanged)

- At-least-once delivery: external side effects must be idempotent; use heartbeat to renew leases on long tasks.
- Token & cost figures are agent-reported and unverified (ADR-0002).
- Agent identity binding (key ↔ agent) arrives with the enterprise authd gateway.

**Full changelog**: https://github.com/agentium-lab/Janus/compare/v1.2.1...v1.3.0
