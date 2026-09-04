# ADR-0005: A2A Protocol v1.0 Conformance

**Status:** Accepted
**Date:** 2026-09-04
**Supersedes:** the custom A2A surface described in ADR-0004 §Gateway

## Context

Codex audit round 5 flagged that Janus advertised "A2A-native" while exposing a
custom protocol shape: query-parameter streaming (`GET /a2a/task/stream?task_id=`),
v0.2-style lowercase task states, and an Agent Card without
`supportedInterfaces`. Any client built against the official A2A SDKs could
not talk to Janus without an adapter layer.

The A2A v1.0 specification (a2a-protocol.org, canonical proto
`specification/a2a.proto`) defines an HTTP+JSON REST binding with:

- `POST /message:send` — non-streaming send
- `POST /message:stream` — SendStreamingMessage (SSE)
- `GET /tasks/{id}` / `GET /tasks/{id}:subscribe` / `POST /tasks/{id}:cancel`
- Agent Card at `/.well-known/agent-card.json` with `supportedInterfaces`
- `StreamResponse`: a single-field oneof (`task` / `message` / `statusUpdate` /
  `artifactUpdate`) carried in SSE `data:` lines
- `TaskState` enum values (`TASK_STATE_WORKING`, ...); no `final` field in v1.0

## Decision

1. Implement the v1.0 REST binding natively in the A2A gateway
   (`gateway_v1.go`), translating between the wire format and the internal
   Janus model at the boundary.
2. Map Janus states onto v1.0 TaskState at the gateway edge only:
   created/queued/claimed/retry_scheduled → SUBMITTED, running → WORKING,
   blocked/approval_pending → INPUT_REQUIRED, failed/dead_lettered/expired →
   FAILED, cancelled → CANCELED. Internal state machine is unchanged.
3. Serve the v1.0 Agent Card from `/.well-known/agent-card.json` with
   `supportedInterfaces: [{protocolBinding: "HTTP+JSON", protocolVersion: "1.0"}]`,
   `capabilities.streaming: true`, and apiKey security scheme declaration.
4. SSE streams emit an initial `task` snapshot, then `statusUpdate` events,
   and close after a terminal state — per spec §6.2. Late subscribers to
   already-terminal tasks get snapshot + terminal update and an immediate
   close.
5. Keep the legacy v0.x routes (`/a2a/task/send`, `/a2a/jsonrpc`, ...) mounted
   for existing consumers; they are compatibility surface, not the advertised
   protocol.
6. task.progress payloads surface as `statusUpdate.status.message` (ROLE_AGENT
   message with the progress text) so streaming clients see agent-authored
   status without protocol extension.

## Consequences

- Official A2A SDK clients (a2a-go and peers) can interoperate with Janus
  without adapters.
- The dual-path streaming (ADR-0004) now feeds both the internal SSE handler
  and the v1.0 `:subscribe` stream from the same broadcaster.
- v0.x routes will be removed in the next major release; deprecation headers
  follow the ACP precedent.
- E2E coverage: `server/tests/scenario/customer_service_scenario_test.go`
  drives the v1.0 surface through a smart-customer-service narrative (approval
  gate, live progress, fan-out, crash recovery).
