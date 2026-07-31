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

## v1.1 Planned — Intent Resolution (catalog-first)

**Status at GA**: the intent subsystem (`server/internal/service/intent/resolver.go`,
the `IntentResolver` interface and `WithIntentResolver` on `TaskService`) exists as
code but is **not wired into the running server**. `WithIntentResolver` is never
called in production wiring (`server/cmd/janus-api/main.go`), and its dependency
`AgentLookup.ListOnlineAgents` has no implementation. The MCP/ACP gateways emit
`target_type=intent` for calls that omit an explicit target, but those tasks today
fall through to "target value is required" rejection. An earlier roadmap entry
described intent resolution as "completed in v0.6.18 / GA P0"; that description
ran ahead of the code — this item re-opens it as a v1.1 deliverable with a
revised, lower-risk approach.

**Why a revised approach (not "just wire the existing resolver")**:

- The existing keyword scorer (`resolver.go`) scores on `payload.Content` against
  capability descriptions with a low acceptance threshold (0.3). In a
  multi-tenant system with data-class policy and audit logging, weak matching
  risks **silent misrouting to a policy-allowed-but-wrong agent** — strictly
  worse than the current explicit rejection. Misrouting is a security/audit
  problem, not a UX problem.
- `go.mod` has zero LLM dependencies. Adding a semantic (LLM) layer into the
  synchronous `Create` path — which also runs policy checks and the outbox
  transaction — is rejected for latency, failure-propagation, and
  determinism/audit reasons.

**v1.1 scope (phased)**:

1. **Catalog endpoint (do first, zero Create-path risk)** — expose
   `GET /v1/tenants/{tenantID}/catalog` returning online agents + capabilities
   (name, description, schema) for the tenant. Read-only, no new dependencies,
   no change to task creation. Lets callers self-resolve NL→capability using
   their own model.
2. **Gateway clarity** — change the empty-target default from silent
   `target_type=intent` to an explicit `400 target required; call GET /catalog`.
3. **Advisory resolution endpoint (only if measured need)** — if a real class
   of thin callers cannot self-resolve after step 1, add
   `POST /v1/tenants/{tenantID}/intents/resolve` as a stateless, advisory
   helper (caller passes payload, gets back a suggested capability, then
   publishes a normal capability task). LLM dep isolated here, per-tenant
   rate-limit + cost budget, capability validated against live catalog.
4. **Deferred / out of scope for v1.1** — sync LLM in `Create` (rejected), and
   async `resolving` task state with background worker (rejected as
   over-engineered for this codebase's lifecycle/audit model).

**Owner**: TBD. **Dependencies**: none on other v1.1 items.

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
