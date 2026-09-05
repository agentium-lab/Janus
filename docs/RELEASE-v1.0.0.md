# Janus Core v1.0.0 Release Note

> ⚠️ **v1.0.0 已于 2026-07-31 重新 GA（re-release）**：原 v1.0.0（commit e088efd）因实现层审计发现 2 个 CRITICAL 安全 release-blocker（SC-1 gRPC 数据面鉴权缺失、SC-2 网关/WebSocket 跨租户访问）而撤回。SC-1/SC-2 + 10 个 HIGH 修复完成后，v1.0.0 tag 已重新打在 commit 4f5b0cb（含全部修复，见 `Janus-production-roadmap.md` §12/§13）。下方原始 GA 内容保留作历史；当前生产基线以 v1.0.0 tag（4f5b0cb）为准。

## Summary

Janus Core v1.0.0 is the first production General Availability release.
It provides a durable task broker with transactional outbox, capability-based
routing, multi-protocol gateways (HTTP/gRPC, A2A, ACP, MCP), governance
(policy/budget/approval), and operational readiness (Helm, Prometheus,
Grafana, OpenTelemetry).

## P0 Capabilities — All Covered

All 68 P0 capabilities in `docs/Janus-core-capability-matrix.md` are Covered.

## P1 Risk Acceptance

The following P1 items are Partial at GA (status and capability descriptions
are aligned with the authoritative `docs/Janus-core-capability-matrix.md`).
They have identified owners and v1.0.1 targets — see
`docs/Janus-production-roadmap.md` §14 (v1.0.1: P1 Smoke Test Coverage).

| ID | Capability | Status | Risk | Owner | v1.0.1 Target |
| --- | --- | --- | --- | --- | --- |
| REL-15 | Task TTL / expiry scanner | Partial | TTL expiry covered by unit tests; real-dependency (PG/NATS) smoke and event/audit verification deferred | TBD | v1.0.1 — roadmap §14 |
| REL-16 | Task cancel/replay/block/unblock via outbox | Partial | service/handler/contract covered; real-dependency orchestration regression deferred | TBD | v1.0.1 — roadmap §14 |
| GOV-08 | Routing target group/human tenant-scoped mailbox mapping | Partial | routing tests exist; real-dependency mapped-target smoke deferred | TBD | v1.0.1 — roadmap §14 |
| GOV-14 | MCP resource/tool calls honor Janus policy/budget/audit | Partial | adapter/gateway unit + protocol smoke exist; full governance smoke deferred | TBD | v1.0.1 — roadmap §14 |
| SDK-06 | CLI dashboard + WebSocket proxy | Partial | CLI dashboard tests + static UI exist; auth-enabled `/ws` dashboard smoke deferred | TBD | v1.0.1 — roadmap §14 |
| OPS-05 | Grafana dashboard panels | Partial | dashboard JSON + provisioning validated; panel query data-integrity verification deferred | TBD | v1.0.1 — roadmap §14 |

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
