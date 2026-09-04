# v1.5.0 — A2A Protocol v1.0 Conformance + 93.9% Coverage

## Highlights

- 🌐 **A2A v1.0 native conformance** — Janus now speaks the official Agent-to-Agent protocol wire format. Official A2A SDK clients interoperate without adapters.
- 📊 **Test coverage 75.1% → 93.9%** — every core package ≥ 90%, all race-detector clean.
- 🧪 **Smart Customer Service e2e** — a full multi-agent journey driving the live HTTP surface.
- 🔧 **SDK backward-compat** — Worker handlers may omit the `progress` parameter (fixes the v1.4 signature change).

## A2A v1.0 Protocol (ADR-0005)

The custom A2A surface is replaced by the canonical HTTP+JSON REST binding:

| Operation | Endpoint |
|---|---|
| SendMessage | `POST /a2a/message:send` |
| SendStreamingMessage | `POST /a2a/message:stream` (SSE) |
| GetTask | `GET /a2a/tasks/{id}` |
| SubscribeToTask | `GET /a2a/tasks/{id}:subscribe` (SSE) |
| CancelTask | `POST /a2a/tasks/{id}:cancel` |
| Agent Card | `/.well-known/agent-card.json` |

- `StreamResponse` single-field oneof (`task` / `message` / `statusUpdate` / `artifactUpdate`)
- `TaskState` enum (`TASK_STATE_WORKING`, ...) mapped at the gateway edge; no `final` field (v1.0 removed it)
- Agent Card declares `supportedInterfaces` (`HTTP+JSON` / `1.0`), `capabilities.streaming`, `securitySchemes`
- `task.progress` surfaces as `statusUpdate.status.message` (ROLE_AGENT) — streaming clients see live agent status
- Legacy v0.x routes (`/a2a/task/send`, `/a2a/jsonrpc`) remain for compatibility

## SDK Changes (backward-compatible)

- Python: handler may take `(task)` or `(task, progress)` — auto-detected
- TypeScript: `progress` is now an optional parameter
- Go: `FromSimple()` adapts the pre-v1.4 3-param signature

## Coverage

| Package | Coverage |
|---|---|
| handler | 93.4% |
| service | 93.0% |
| retry | 97.4% |
| lease | 97.8% |
| gateway/mcp + acp | 100% |
| auth | 99.1% |
| sdk/go | 99.3% |
| gateway/a2a | 96.2% |
| **total** | **93.9%** |

## Fixes

- **outbox unknown-kind** entries now skip (mark published) instead of retrying forever — `ErrUnknownKind` sentinel handled at batch level for rolling-upgrade safety; `publishOne` still errors for caller visibility.
- **Smart Customer Service e2e** (`server/tests/scenario`) exercises approval gate, live progress, audit trail, multi-agent fan-out, and crash recovery over the real HTTP surface.
