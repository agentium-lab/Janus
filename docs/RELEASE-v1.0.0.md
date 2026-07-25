# Janus Core v1.0.0 Release Note

## Summary

Janus Core v1.0.0 is the first production General Availability release.
It provides a durable task broker with transactional outbox, capability-based
routing, multi-protocol gateways (HTTP/gRPC, A2A, ACP, MCP), governance
(policy/budget/approval), and operational readiness (Helm, Prometheus,
Grafana, OpenTelemetry).

## P0 Capabilities — All Covered

All 68 P0 capabilities in `docs/Janus-core-capability-matrix.md` are Covered.

## P1 Risk Acceptance

The following P1 items are Partial at GA. They have identified owners and
v1.1 targets.

| ID | Capability | Status | Risk | Owner | v1.1 Target |
| --- | --- | --- | --- | --- | --- |
| REL-15 | Dead-letter retention policy and compaction | Partial | DLQ grows unbounded without retention | TBD | Configurable retention + compaction job |
| REL-16 | Outbox poison message quarantine | Partial | Repeatedly failing outbox entries retry indefinitely | TBD | Max-attempts quarantine table |
| GOV-08 | Policy rule priority ordering (numeric) | Partial | Rule evaluation order not guaranteed when priorities collide | TBD | Stable sort by priority + insertion order |
| GOV-14 | Budget monthly reset semantics | Partial | Monthly cost counter does not auto-reset | TBD | Cron-based monthly reset |
| SDK-06 | TypeScript SDK browser/edge compatibility | Partial | Uses fetch API not available on all edge runtimes | TBD | Conditional fetch adapter |
| OPS-05 | Horizontal autoscaling tested at scale | Partial | HPA template exists but not load-tested at 1000+ agents | TBD | Load test with k6 |

## Verification

```sh
make verify-production
```

This runs: verify, contract-check, beta-fast, verify-reliability,
verify-security, verify-protocol, verify-governance, verify-sdk-cli,
verify-ops-chaos, verify-release-ops, ga-readiness.

## Artifacts

- Docker image: `ghcr.io/agentium-lab/janus-core:v1.0.0`
- Helm chart: `deployments/helm/janus-core/`
- Probe binaries: `janus-migration-probe`, `janus-nats-persistence-probe`, `janus-event-replay-probe`

## Breaking Changes from v0.3

None. v1.0.0 is API-compatible with v0.3.

## Migration

See `docs/Janus-v0.3-migration.md` for v0.2→v0.3 migration steps.
Run migrations:

```sh
helm upgrade --install janus deployments/helm/janus-core
```

The Helm chart includes a pre-install migration job.
