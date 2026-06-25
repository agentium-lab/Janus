# Janus v0.3 API / SDK Contract

状态：v0.3.10 governance management contract

本文档冻结 Janus Core 在 v0.3 的外部调用契约。它覆盖 HTTP、gRPC gateway、Go SDK、Python SDK、TypeScript SDK 和 CLI 需要共同遵守的字段语义。

---

## 1. Contract Principles

1. **tenant-scoped by default**
- 除 `POST /v1/tenants` 外，Core API 路径必须包含 `/v1/tenants/{tenant_id}`。
- SDK 初始化时必须携带 `tenant_id`，并由 SDK 拼接 tenant-scoped path。

2. **API accepted 不等于 queued**
- `POST /tasks` 成功只表示 PostgreSQL transaction 已提交，task 可能仍处于 `created`。
- 只有 outbox publisher 成功写入 mailbox 后，task 才进入 `queued`。

3. **dispatch lifecycle 必须携带 attempt 和 lease**
- `start`、`heartbeat`、`ack`、`nack` 必须携带 `attempt` 和 `lease_id`。
- 服务端必须用 `(tenant_id, task_id, attempt, lease_id)` 校验当前 attempt。
- 旧 attempt、过期 lease、lease mismatch 都必须失败，不能 ACK 掉底层 delivery。

4. **result_ref 是生产级结果引用**
- 大结果必须先写 artifact/object storage，再通过 `result_ref` 引用。
- SDK v0.3 的 ACK contract 使用 `result_ref`；inline `result` 不进入 SDK 稳定面。

5. **错误响应必须结构化**
- HTTP handler 和 grpc-gateway 都返回兼容 envelope：

```json
{
"error": "task not found",
"code": "NOT_FOUND",
"message": "task not found",
"status": 404
}
```

---

## 2. Stable Error Codes

| HTTP | gRPC | SDK code |
| --- | --- | --- |
| 400 | InvalidArgument | `INVALID_ARGUMENT` |
| 401 | Unauthenticated | `UNAUTHENTICATED` |
| 403 | PermissionDenied | `PERMISSION_DENIED` |
| 404 | NotFound | `NOT_FOUND` |
| 409 | AlreadyExists / FailedPrecondition | `CONFLICT` |
| 429 | ResourceExhausted | `RESOURCE_EXHAUSTED` |
| 503 | Unavailable | `UNAVAILABLE` |
| 500+ | Internal | `INTERNAL` |
| other | Unknown | `UNKNOWN` |

SDK behavior:

- Go returns `*janus.APIError` with `StatusCode`, `Code`, `Message`.
- Python raises `JanusAPIError`, compatible with `httpx.HTTPStatusError`, with `code`, `message`, `status`.
- TypeScript throws `JanusAPIError` with `code`, `message`, `status`, `statusCode`.

---

## 3. Stable HTTP Surface

### Tenant

| Operation | Method / Path | Stable SDK |
| --- | --- | --- |
| Create tenant | `POST /v1/tenants` | Go |
| Get tenant | `GET /v1/tenants/{tenant_id}` | Go / Python / TypeScript |

### Agent

| Operation | Method / Path | Stable SDK |
| --- | --- | --- |
| Register agent | `POST /v1/tenants/{tenant_id}/agents` | Go / Python / TypeScript |
| List agents | `GET /v1/tenants/{tenant_id}/agents` | Go / Python / TypeScript |
| Get agent | `GET /v1/tenants/{tenant_id}/agents/{agent_id}` | Go / Python / TypeScript |
| Heartbeat agent | `POST /v1/tenants/{tenant_id}/agents/{agent_id}/heartbeat` | Go / Python / TypeScript |

### Mailbox

| Operation | Method / Path | Stable SDK |
| --- | --- | --- |
| Create mailbox | `POST /v1/tenants/{tenant_id}/mailboxes` | Go / Python / TypeScript |
| Get mailbox | `GET /v1/tenants/{tenant_id}/mailboxes/{mailbox_id}` | Go / Python / TypeScript |
| Update mailbox config | `PATCH /v1/tenants/{tenant_id}/mailboxes/{mailbox_id}` | Go / Python / TypeScript |
| Pause mailbox | `POST /v1/tenants/{tenant_id}/mailboxes/{mailbox_id}/pause` | Go / Python / TypeScript |
| Resume mailbox | `POST /v1/tenants/{tenant_id}/mailboxes/{mailbox_id}/resume` | Go / Python / TypeScript |

### Task Management

| Operation | Method / Path | Stable SDK |
| --- | --- | --- |
| Publish task | `POST /v1/tenants/{tenant_id}/tasks` | Go / Python / TypeScript |
| Get task | `GET /v1/tenants/{tenant_id}/tasks/{task_id}` | Go / Python / TypeScript |
| Cancel task | `POST /v1/tenants/{tenant_id}/tasks/{task_id}/cancel` | Go / Python / TypeScript |
| Replay task | `POST /v1/tenants/{tenant_id}/tasks/{task_id}/replay` | Go / Python / TypeScript |
| Task events | `GET /v1/tenants/{tenant_id}/tasks/{task_id}/events` | Go / Python / TypeScript |

### Dispatch Lifecycle

| Operation | Method / Path | Request body | Stable SDK |
| --- | --- | --- | --- |
| Pull | `POST /v1/tenants/{tenant_id}/mailboxes/{mailbox_id}/pull` | `{"agent_id": "agent-1"}` | Go / Python / TypeScript |
| Start | `POST /v1/tenants/{tenant_id}/tasks/{task_id}/start` | `{"attempt": 1, "lease_id": "..."}` | Go / Python / TypeScript |
| Heartbeat | `POST /v1/tenants/{tenant_id}/tasks/{task_id}/heartbeat` | `{"attempt": 1, "lease_id": "..."}` | Go / Python / TypeScript |
| ACK | `POST /v1/tenants/{tenant_id}/tasks/{task_id}/ack` | `{"attempt": 1, "lease_id": "...", "result_ref": "artifact://..."}` | Go / Python / TypeScript |
| NACK | `POST /v1/tenants/{tenant_id}/tasks/{task_id}/nack` | `{"attempt": 1, "lease_id": "...", "retriable": true, "error": {"code": "...", "message": "..."}}` | Go / Python / TypeScript |

`Pull` requires `agent_id`. The mailbox owner must match the supplied `agent_id`; missing `agent_id` returns `400 INVALID_ARGUMENT`, and an owner mismatch returns `403 PERMISSION_DENIED` without fetching a queue delivery. Dispatch-time policy denial or approval requirement returns `403 PERMISSION_DENIED`; budget, capacity, or rate-limit backpressure returns `429 RESOURCE_EXHAUSTED`. If Janus cannot persist the dispatch-time blocking audit event after a policy, DLP, or budget block, it delayed-NACKs the fetched delivery and returns `503 UNAVAILABLE`.

Pull response:

```json
{
"task": {
"id": "task-1",
"tenant_id": "acme",
"status": "claimed",
"attempt_count": 1
},
"lease": {
"lease_id": "lease-abc",
"attempt": 1,
"expires_at": "2026-06-13T00:00:00Z"
}
}
```

If no task is available, HTTP returns `204 No Content`; SDKs return `nil` / `None` / `null`.

### DLQ

| Operation | Method / Path | Stable SDK |
| --- | --- | --- |
| Query DLQ | `GET /v1/tenants/{tenant_id}/dlq?mailbox={mailbox_id}&limit={n}` | Go / Python / TypeScript |
| Replay DLQ task | `POST /v1/tenants/{tenant_id}/dlq/{task_id}/replay` | Go / Python / TypeScript |
| Discard DLQ task | `POST /v1/tenants/{tenant_id}/dlq/{task_id}/discard` | Go / Python / TypeScript |

### API Keys

| Operation | Method / Path | Stable SDK |
| --- | --- | --- |
| Create API key | `POST /v1/tenants/{tenant_id}/api-keys` | Go / Python / TypeScript |
| List API keys | `GET /v1/tenants/{tenant_id}/api-keys` | Go / Python / TypeScript |
| Revoke API key | `POST /v1/tenants/{tenant_id}/api-keys/{key_id}/revoke` | Go / Python / TypeScript |

API key rules:

- Clients send the secret in `X-API-Key`.
- Create returns raw `key` exactly once.
- List and revoke never return raw secret or hash.

### Governance Management

| Operation | Method / Path | Stable SDK |
| --- | --- | --- |
| Create policy rule | `POST /v1/tenants/{tenant_id}/policy-rules` | Go / Python / TypeScript |
| Create policy rule from template | `POST /v1/tenants/{tenant_id}/policy-rules/templates` | Go / Python / TypeScript |
| List active policy rules | `GET /v1/tenants/{tenant_id}/policy-rules` | Go / Python / TypeScript |
| Upsert budget | `POST /v1/tenants/{tenant_id}/budgets` | Go / Python / TypeScript |
| Get budget | `GET /v1/tenants/{tenant_id}/budgets/{scope_type}/{scope_id}` | Go / Python / TypeScript |
| List budgets | `GET /v1/tenants/{tenant_id}/budgets` | Go / Python / TypeScript |

Policy rule templates are a configuration convenience only. They compile to the same `policy_rules` rows used by `PolicyService.Evaluate`; the runtime does not evaluate a second template model.

Template request:

```json
{
"template": "allow_agent_capability",
"agent_id": "coding-agent",
"capability": "code_review",
"priority": 100
}
```

Template response is the generated standard policy rule:

```json
{
"id": "policy-tpl-...",
"name": "Allow coding-agent to code_review",
"status": "active",
"priority": 100,
"condition": {
"actor.type": "agent",
"actor.id": "coding-agent",
"action": "task.publish",
"resource.type": "capability",
"resource.value": "code_review"
},
"action": {
"decision": "allow"
}
}
```

Supported Core templates:

| Template | Required fields | Generated policy input shape |
| --- | --- | --- |
| `allow_agent_capability` / `deny_agent_capability` | `agent_id`, `capability` | `actor.id`, `action=task.publish`, `resource.type=capability` |
| `allow_team_capability` / `deny_team_capability` | `team_id`, `capability` | `actor.team_id`, `action=task.publish`, `resource.type=capability` |
| `require_approval_capability` | `capability` | `action=task.publish`, `resource.type=capability` |
| `require_approval_tool` | `tool` | `action=tool.invoke`, `resource.type=tool` |
| `allow_agent_data_classification` / `deny_agent_data_classification` | `agent_id`, `data_classification` | `action=task.route`, `context.target_agent_id`, `context.data_classification` |
| `allow_team_data_classification` / `deny_team_data_classification` | `team_id`, `data_classification` | `action=task.route`, `context.target_team_id`, `context.data_classification` |
| `allow_agent_tool` / `deny_agent_tool` | `agent_id`, `tool` | `actor.id`, `action=tool.invoke`, `resource.type=tool` |
| `allow_team_tool` / `deny_team_tool` | `team_id`, `tool` | `actor.team_id`, `action=tool.invoke`, `resource.type=tool` |

Agent Registry remains identity/capability/capacity metadata only. Caller restrictions, approval requirements, tool allow/deny rules, and data classification access belong in policy rules, either hand-written or generated through these templates.

`tool.invoke` is evaluated with the source agent as actor during task publish/create, and with the executing mailbox owner as actor during dispatch. Tool templates therefore constrain the actor at the current policy checkpoint while still compiling to the same standard `policy_rules` shape.

---

## 4. Task Envelope Stable Fields

| Field | Required | Meaning |
| --- | --- | --- |
| `janus_version` | recommended | Envelope schema version. |
| `task_id` | recommended | Same logical task id as outer task when caller provides it. |
| `idempotency_key` | optional | Dedupes publish retries. |
| `tenant_id` | recommended | Must match path tenant when supplied. |
| `source_agent` | required by publish request | Agent that requested the task. |
| `target.type` | required by publish request | `mailbox`, `agent`, `capability`, `group`, or `human`. |
| `target.value` | required by publish request | Mailbox id, agent id, capability name, group id, or human target. |
| `priority` | optional | Defaults to `normal`. |
| `ttl_seconds` | optional | Task expiration hint. |
| `payload.type` | optional | Payload format, usually `json` or `text`. |
| `payload.content` | optional | Caller-controlled payload content. |
| `tool_invocation.id` | optional | Caller-visible tool invocation id; defaults to task id when omitted. |
| `tool_invocation.name` | optional | Marks a Janus-visible tool-like task for protocol-neutral `tool.invocation_*` audit; required when `tool_invocation` is present. |
| `tool_invocation.namespace` | optional | Tool namespace, such as `mcp`, `git`, or an internal SDK namespace. |
| `tool_invocation.source_protocol` | optional | Source protocol that introduced the invocation, such as `sdk`, `a2a`, `acp`, or `mcp`. |
| `trace.trace_id` | optional | Cross-task trace id. |
| `trace.parent_task_id` | optional | Parent task for agent-to-agent chaining. |
| `trace.span_id` | optional | Current task span id. |

---

## 5. gRPC / Gateway Contract

The proto files are the source of truth for gRPC:

- `proto/janus/v1/common.proto`
- `proto/janus/v1/agent.proto`
- `proto/janus/v1/task.proto`
- `proto/janus/v1/dispatch.proto`
- `proto/janus/v1/mailbox.proto`
- `proto/janus/v1/dlq.proto`
- `proto/janus/v1/audit.proto`
- `proto/janus/v1/auth.proto`

The HTTP gateway annotations must stay semantically equivalent to hand-written HTTP handlers. New v0.3 contract changes must update proto, generated gateway files, HTTP handlers, SDKs, and this document together.

grpc-gateway JSON must use proto field names (`snake_case`) rather than proto JSON names (`camelCase`) so gateway responses remain compatible with the SDK HTTP contract. Gateway response status mapping must also preserve stable hand-written HTTP semantics for create operations:

- `POST /v1/tenants/{tenant_id}/agents` returns `201`;
- `POST /v1/tenants/{tenant_id}/tasks` returns `201`;
- `POST /v1/tenants/{tenant_id}/mailboxes` returns `201`;
- `POST /v1/tenants/{tenant_id}/api-keys` returns `201`.

The gateway parity tests must exercise real HTTP requests through the generated gateway mux for the proto-backed stable SDK surface: agent register/get/heartbeat, task create/get/cancel/replay/events, dispatch pull/start/heartbeat/ack/nack, mailbox create/get/update/pause/resume, DLQ query/replay/discard, API key create/list/revoke, and standard error envelopes. Governance management routes (`policy-rules`, `policy-rules/templates`, `budgets`) are explicitly HTTP-only until their control-plane proto contract is frozen.

v0.3 HTTP compatibility is intentionally stricter than raw gRPC response shape. The grpc-gateway layer must preserve the stable hand-written HTTP contract for SDK-visible routes:

- agent register returns `{"id","status"}` over HTTP, while native gRPC still returns full `Agent`;
- agent heartbeat returns `{"status":"ok"}` over HTTP, while native gRPC still returns `HeartbeatResponse`;
- task create returns `{"id","status"}` over HTTP, while native gRPC still returns full `Task`;
- task cancel returns `{"status":"cancelled"}` over HTTP, while native gRPC still returns full `Task`;
- dispatch start/heartbeat/ack/nack return status objects over HTTP, while native gRPC responses remain empty messages;
- empty pull returns `204 No Content` with no body;
- task events accepts the stable HTTP `limit` query parameter and aliases it to proto `page_size`.

---

## 6. v0.3 Verification Gate

`make verify` is the local baseline and must pass before merging v0.3 work. It runs:

- all Go tests across Core, server, CLI, Go SDK, demo, and proto;
- Python SDK syntax check;
- Python SDK unit tests;
- TypeScript SDK unit tests;
- proto / HTTP / SDK API surface drift check;
- Core coverage gate, currently `90.0%`.

For a clean checkout, install Python SDK dev dependencies before running the full gate:

```sh
python3 -m venv .venv
make python-dev-install
make verify
```

API surface changes can also be checked directly:

```sh
make contract-check
```

The drift check compares proto HTTP annotations, stable SDK conformance fixtures, and explicitly documented HTTP-only routes. `docs/Janus-api-surface-audit.md` records the current route inventory and allowlists; v0.3.10 keeps six governance management routes as explicit HTTP-only stable routes until the v0.4 control-plane proto contract is frozen.

---

## 7. Cross-SDK Conformance Fixtures

SDK HTTP contract tests share the same golden fixture file:

```text
sdk/conformance/http_cases.json
```

The fixture covers:

- method and tenant-scoped path;
- API key header;
- JSON request body;
- query parameters;
- `204 No Content` empty pull behavior;
- standard error envelope decoding;
- representative agent, mailbox, dispatch, DLQ, API key, policy rule, policy rule template, and budget calls.

Go, Python, and TypeScript SDK tests must consume this fixture. SDK-specific tests may still exist for language ergonomics, but contract changes must update the shared fixture first.

Adding a stable SDK route requires updating both the fixture and `scripts/check_api_contract.py`'s stable route inventory. Adding an HTTP handler without proto support requires either moving it behind proto/gRPC gateway or documenting it as an explicit HTTP-only route with a migration reason in `docs/Janus-api-surface-audit.md`.

The worker lifecycle sequence has a separate shared fixture:

```text
sdk/conformance/worker_flow.json
```

Go, Python, and TypeScript SDK tests must all consume this fixture to prove the same `pull -> start -> heartbeat -> ack` and `pull -> start -> heartbeat -> nack` flows preserve `task_id`, `lease_id`, `attempt`, `result_ref`, and structured task errors across language implementations.

SDKs expose this lifecycle at two levels:

- Low-level methods remain available for advanced workers: `pullTask` / `pull_task` / `PullTask`, `start`, `heartbeat`, `ack`, and `nack`.
- The recommended Agent integration is the high-level SDK worker helper (`JanusWorker` / `Worker`) where application code only provides a task handler; the SDK owns polling, agent heartbeat, task start, task heartbeat, ACK/NACK, and empty-mailbox backoff.