# v1.4.0 — Real-Time Task Streaming (SSE)

## Highlights

- 🆕 **Live task progress via SSE** — agents report mid-task progress like a log statement; subscribers watch in real time from any SDK, Console, or A2A client. Streams auto-close on task completion.
- 🛡 **Security hardening (from v1.3.0)** — PostgreSQL queue tenant isolation, unified scope enforcement across all protocols (REST/A2A/MCP/gRPC), hardened Helm chart.
- 📦 **SDK streaming in all three languages** — Worker auto-injects the `progress` callback; `streamTask()` subscribes with auto-close on terminal states.

## Streaming API

```
# Agent reports progress (Worker injects the callback — no HTTP calls)
progress("Analyzing code...", percent=20)

# Subscribe from any client (SSE, auto-closes on completion)
for evt in client.stream_task("task-001"):
    print(f'[{evt["payload"]["percent"]}%] {evt["payload"]["message"]}')
```

- `POST /v1/tenants/{t}/tasks/{id}/progress` — agent reports (message + percent + data)
- `GET /v1/tenants/{t}/tasks/{id}/stream` — SSE subscription
- `GET /a2a/task/stream?task_id=` — A2A protocol bridge
- Dual-path: in-memory fanout (microsecond latency) + outbox (audit trail)
- Rate limit: 10 events/sec/task; reporter must hold the latest attempt
- Agent Card now declares `streaming: true` (A2A v1.0 compliant)

## Security Fixes (carried from v1.3.0, all verified by third audit)

- pgqueue FetchTasks/Ack/Nack/DLQ all filter by `tenant_id` — cross-tenant data isolation
- Scope enforcement covers `/v1/tenants`, `/a2a/*`, `/mcp*`, `/acp/*`, and gRPC
- gRPC AuthInterceptor injects Principal before scope checks; dispatch methods correctly mapped
- Redis optional in PG-only mode (non-fatal degradation)
- Helm chart: all template references resolve; PrometheusRule escaped; default replicaCount 1

## SDK Changes

- **Breaking**: Worker handler signatures gain a `progress` parameter in all three SDKs
- Python: `handler(task, progress)` / TypeScript: `handler(task, agentID, progress)` / Go: `handler(ctx, task, agentID, progress)`
- New: `report_progress()` and `stream_task()` / `streamTask()` / `StreamTask()` in all SDKs
- TypeScript SDK: `repository` field added (required for npm provenance); `TaskEnvelope` optional with auto-construction

## Known Limitations (unchanged)

- At-least-once delivery: external side effects must be idempotent
- Token & cost figures are agent-reported and unverified (ADR-0002)
- Agent identity binding arrives with the enterprise authd gateway

**Full changelog**: https://github.com/agentium-lab/Janus/compare/v1.3.0...v1.4.0
