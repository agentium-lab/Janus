# ADR-0001: Dual-write persistence (PostgreSQL + NATS JetStream)

- Status: Accepted (v0.x, revisited for the PostgreSQL-only mode on the roadmap)
- Context: Janus brokers tasks between agents; delivery guarantees are the product.

## Decision

Every state change is committed to PostgreSQL first (source of truth) and mirrored
to NATS JetStream through a transactional outbox worker for fan-out and delivery.

## Alternatives considered

1. **NATS as source of truth** — rejected: queryability, joins with governance data
   (policy/budget/audit) and compliance exports all live in SQL.
2. **PG LISTEN/NOTIFY only** — rejected for the default path: no redelivery semantics,
   no consumer groups across replicas at the time of evaluation.
3. **Kafka** — rejected: operational weight disproportionate to per-tenant streams.

## Consequences

- Crash recovery replays from PG; NATS loss is recoverable by outbox replay.
- Outbox retry is purely PG-based; NATS availability affects delivery latency,
  not correctness.
- Cost: dual-write complexity is contained by the outbox pattern (single writer).
- Follow-up (ROADMAP): a PG-backed QueueDriver makes NATS optional for
  single-dependency deployments.
