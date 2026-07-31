# Janus Core v1.0.0 Release Note

> ⚠️ **GA 已撤回（2026-07-31）**：v1.0.0 GA 后的实现层安全审计发现 2 个 CRITICAL 级 release-blocker（SC-1 gRPC 数据面鉴权完全缺失、SC-2 网关 / WebSocket 跨租户访问），任一未修复都使“生产可用”不成立。**v1.0.0 tag 已从本地与远程删除**，将在 **v1.1**（见 `Janus-production-roadmap.md` §12）修复 SC-1/SC-2 后重新进行 GA。本文件保留作历史记录，下方内容不再代表当前生产状态。

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
They have identified owners and v1.1.2 targets — see
`docs/Janus-production-roadmap.md` §14 (v1.1.2: P1 Smoke Test Coverage).

| ID | Capability | Status | Risk | Owner | v1.1.2 Target |
| --- | --- | --- | --- | --- | --- |
| REL-15 | Task TTL / expiry scanner | Partial | TTL expiry covered by unit tests; real-dependency (PG/NATS) smoke and event/audit verification deferred | TBD | v1.1.2 — roadmap §14 |
| REL-16 | Task cancel/replay/block/unblock via outbox | Partial | service/handler/contract covered; real-dependency orchestration regression deferred | TBD | v1.1.2 — roadmap §14 |
| GOV-08 | Routing target group/human tenant-scoped mailbox mapping | Partial | routing tests exist; real-dependency mapped-target smoke deferred | TBD | v1.1.2 — roadmap §14 |
| GOV-14 | MCP resource/tool calls honor Janus policy/budget/audit | Partial | adapter/gateway unit + protocol smoke exist; full governance smoke deferred | TBD | v1.1.2 — roadmap §14 |
| SDK-06 | CLI dashboard + WebSocket proxy | Partial | CLI dashboard tests + static UI exist; auth-enabled `/ws` dashboard smoke deferred | TBD | v1.1.2 — roadmap §14 |
| OPS-05 | Grafana dashboard panels | Partial | dashboard JSON + provisioning validated; panel query data-integrity verification deferred | TBD | v1.1.2 — roadmap §14 |

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
