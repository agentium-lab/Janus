# ADR-0004: Task Streaming via SSE

- Status: Accepted (2026-09)
- Decisions: SSE transport (A2A-aligned); dual-path delivery (in-memory fanout + outbox audit); minimal schema (message + percent? + data?)

## Context

A2A v1.0 requires streaming support (`streaming: true` in agent card). Long-running tasks are
currently opaque to the publisher until terminal ACK. Console and dashboards have no mid-task
visibility.

## Decisions

1. **SSE, not WebSocket**: A2A v1.0 uses SSE; browser-native auto-reconnect; pure HTTP
   (proxy-friendly). Existing `/ws` WebSocket continues serving dashboard events unchanged.
2. **Dual-path**: progress events fan out in-memory (microsecond latency) AND flow through
   the transactional outbox (audit trail, late-subscriber replay via existing events endpoint).
3. **Minimal schema**: `message` (required, human-readable), `percent` (optional, 0-100),
   `data` (optional, free-form JSON). No stage/level enums — agents express freely; sub-typing
   can be added via a `type` field later without breaking changes.

## API

- POST /v1/tenants/{t}/tasks/{id}/progress — agent reports (task:write scope)
- GET /v1/tenants/{t}/tasks/{id}/stream — SSE subscription (task:read scope)
- SSE closes automatically when the task reaches a terminal state

## Guardrails

- Reporter must be the agent holding the latest attempt on that task
- Task must be in claimed/running state
- Rate limit: 10 progress events per task per second
- Progress implies liveness (soft heartbeat) but does not replace the heartbeat API
