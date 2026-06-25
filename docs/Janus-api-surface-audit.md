# Janus v0.3 API Surface Audit

状态：v0.3.10 governance management contract

本文档记录 Janus Core v0.3 的 API surface 来源、覆盖关系和已知边界。自动检查入口是：

```sh
make contract-check
```

`make verify` 已包含该检查。

---

## 1. Sources Of Truth

| Surface | Source | Role |
| --- | --- | --- |
| gRPC / grpc-gateway | `proto/janus/v1/*.proto` | proto annotation 是 gRPC gateway 的来源。 |
| Hand-written HTTP router | `server/cmd/janus-api/main.go` | 当前生产 HTTP 入口；稳定 SDK 路由必须保持 proto annotation 覆盖。 |
| SDK contract fixtures | `sdk/conformance/http_cases.json` | Go / Python / TypeScript SDK 共享 golden tests。 |
| Human-readable contract | `docs/Janus-api-contract.md` | 稳定字段语义、错误语义和迁移约束。 |
| Gateway migration inventory | `docs/Janus-http-gateway-migration-inventory.md` | 生产 `/v1` 从手写 HTTP 切到 grpc-gateway 前的 route ownership、shim 和灰度清单。 |

---

## 2. Automated Drift Gate

`scripts/check_api_contract.py` 执行以下检查：

- 解析 proto 中的 `google.api.http` annotations。
- 读取 `sdk/conformance/http_cases.json`。
- 校验稳定 SDK 路由全部进入 conformance fixture。
- 校验 fixture 中的非 HTTP-only 路由能在 proto annotations 中找到。
- 校验 HTTP-only 路由显式登记，不能无声混入 SDK contract。
- 校验新增 proto 路由必须进入 SDK fixture，或登记为已知非 SDK 路由。

当前检查结果：

```text
28 proto routes
32 conformance routes
7 HTTP-only stable routes
```

注意：drift gate 证明稳定 SDK route 要么有 proto annotation 覆盖，要么被显式登记为 HTTP-only stable route。v0.3.9 已为 proto-backed stable SDK surface 补齐 grpc-gateway response shim、`limit -> page_size` query alias、empty pull `204 No Content` 兼容层和 parity tests；v0.3.10 额外冻结治理管理 HTTP route。公开 `/v1` route ownership 是否切到 gateway 仍必须按 `docs/Janus-http-gateway-migration-inventory.md` 逐条灰度。

---

## 3. Proto-Annotated Stable Routes

| Method | Path | Covered By SDK Fixture |
| --- | --- | --- |
| `POST` | `/v1/tenants/{tenant_id}/agents` | yes |
| `PATCH` | `/v1/tenants/{tenant_id}/agents/{agent_id}` | no, not exposed in SDK parity baseline |
| `POST` | `/v1/tenants/{tenant_id}/agents/{agent_id}/heartbeat` | yes |
| `GET` | `/v1/tenants/{tenant_id}/agents` | yes |
| `GET` | `/v1/tenants/{tenant_id}/agents/{agent_id}` | yes |
| `POST` | `/v1/tenants/{tenant_id}/mailboxes` | yes |
| `GET` | `/v1/tenants/{tenant_id}/mailboxes/{mailbox_id}` | yes |
| `PATCH` | `/v1/tenants/{tenant_id}/mailboxes/{mailbox_id}` | yes |
| `POST` | `/v1/tenants/{tenant_id}/mailboxes/{mailbox_id}/pause` | yes |
| `POST` | `/v1/tenants/{tenant_id}/mailboxes/{mailbox_id}/resume` | yes |
| `POST` | `/v1/tenants/{tenant_id}/tasks` | yes, path/header covered; request body remains SDK-specific because TaskEnvelope models differ by language |
| `GET` | `/v1/tenants/{tenant_id}/tasks/{task_id}` | yes |
| `POST` | `/v1/tenants/{tenant_id}/tasks/{task_id}/cancel` | yes |
| `POST` | `/v1/tenants/{tenant_id}/tasks/{task_id}/replay` | yes |
| `POST` | `/v1/tenants/{tenant_id}/mailboxes/{mailbox_id}/pull` | yes |
| `POST` | `/v1/tenants/{tenant_id}/tasks/{task_id}/start` | yes |
| `POST` | `/v1/tenants/{tenant_id}/tasks/{task_id}/heartbeat` | yes |
| `POST` | `/v1/tenants/{tenant_id}/tasks/{task_id}/ack` | yes |
| `POST` | `/v1/tenants/{tenant_id}/tasks/{task_id}/nack` | yes |
| `GET` | `/v1/tenants/{tenant_id}/events` | no, Go SDK only |
| `GET` | `/v1/tenants/{tenant_id}/traces/{trace_id}` | no, not exposed in SDK parity baseline |
| `GET` | `/v1/tenants/{tenant_id}/tasks/{task_id}/events` | yes |
| `GET` | `/v1/tenants/{tenant_id}/dlq` | yes |
| `POST` | `/v1/tenants/{tenant_id}/dlq/{task_id}/replay` | yes |
| `POST` | `/v1/tenants/{tenant_id}/dlq/{task_id}/discard` | yes |
| `POST` | `/v1/tenants/{tenant_id}/api-keys` | yes |
| `GET` | `/v1/tenants/{tenant_id}/api-keys` | yes |
| `POST` | `/v1/tenants/{tenant_id}/api-keys/{key_id}/revoke` | yes |

---

## 4. Stable HTTP-Only Routes

部分控制面 route 作为稳定 Core SDK route 暂时保留为手写 HTTP-only。Tenant read 用于 project config apply/diff/sync 的存在性检查；policy/budget 执行语义已经进入数据面，而管理面的 proto/gateway 控制面 contract 仍需在 v0.4 冻结后再迁移。

| Method | Path | Reason | Follow-up |
| --- | --- | --- | --- |
| `GET` | `/v1/tenants/{tenant_id}` | Tenant bootstrap / project config 读取当前仍是 HTTP-only 管理面。 | v0.4 控制面 proto 冻结后迁入 grpc-gateway。 |
| `POST` | `/v1/tenants/{tenant_id}/policy-rules` | Policy rule 管理面先以 HTTP contract 冻结，供 Core governance gate 和 SDK 使用。 | v0.4 控制面 proto 冻结后迁入 grpc-gateway，并保留 parity tests。 |
| `POST` | `/v1/tenants/{tenant_id}/policy-rules/templates` | Policy template 是简化配置入口，但仍生成标准 `policy_rules`；先以 HTTP-only contract 冻结，供 CLI/SDK/API 一致使用。 | v0.4 控制面 proto 冻结后迁入 grpc-gateway，并保留模板到标准 rule 的 parity tests。 |
| `GET` | `/v1/tenants/{tenant_id}/policy-rules` | 同上。 | 同上。 |
| `POST` | `/v1/tenants/{tenant_id}/budgets` | Budget 管理面先以 HTTP contract 冻结，供 budget enforcement 和 SDK 使用。 | v0.4 控制面 proto 冻结后迁入 grpc-gateway，并保留 parity tests。 |
| `GET` | `/v1/tenants/{tenant_id}/budgets/{scope_type}/{scope_id}` | 同上。 | 同上。 |
| `GET` | `/v1/tenants/{tenant_id}/budgets` | 同上。 | 同上。 |

其它稳定 SDK route 不允许无声变成 HTTP-only。新增 HTTP-only stable route 必须登记在 `scripts/check_api_contract.py` 的 `HTTP_ONLY_ROUTES`，并在本节写明原因和迁移计划。

---

## 5. Known Non-SDK Routes

These routes exist in the hand-written HTTP router but are not part of the v0.3 cross-SDK parity baseline:

- task admin transitions: `complete`, `fail`, `block`, `unblock`;
- approvals: request/list/get/approve/reject;
- context refs: create/get/attach/detach/list;
- WebSocket dashboard stream: `/ws`;
- A2A gateway: `/a2a/*`;
- health/readiness/metrics: `/healthz`, `/readyz`, `/metrics`;
- grpc-gateway mount: `/grpc/*`.

They should not be added to SDK conformance fixtures until their v0.3/v0.4 contract is explicitly frozen.

---

## 6. Gateway Migration State

v0.3.8 新增 `docs/Janus-http-gateway-migration-inventory.md`，把 proto-covered route 进一步分成：

- `canary-ready`：gateway response 与手写 HTTP contract 等价，可以逐条灰度；
- `needs-shim`：gateway 已存在，但需要 response wrapper、status forwarder 或 no-content 兼容层；
- `needs-contract-freeze`：proto route 已存在，但尚未进入 cross-SDK contract baseline；
- `hand-written-only`：没有 proto annotation，或属于 WebSocket、A2A、ops endpoint、未冻结管理面。

v0.3.9 后，proto-backed stable SDK surface 不再有 `needs-shim` route。已解决的兼容点包括：

- agent/task create 的简化 HTTP response；
- agent heartbeat、task cancel、dispatch lifecycle action status response；
- pull 空队列 `204 No Content` 且无 body；
- task events 的 `limit -> page_size` query alias；
- mailbox/DLQ/API key/action route 的 exact body parity。

当前公开 `/v1/...` 仍由手写 HTTP router 负责，grpc-gateway 仍挂载在 `/grpc/v1/...`。v0.3.10 没有改变生产 route ownership。

---

## 7. Rules For Changing API Surface

1. Change proto annotations first when the route is intended to be gRPC/gateway stable.
2. Update the hand-written HTTP handler or remove the duplicate path if gateway becomes the production path.
3. Update `sdk/conformance/http_cases.json`.
4. Update Go, Python, and TypeScript SDK tests if a new operation is added.
5. Run `make contract-check`.
6. Add or update grpc-gateway HTTP parity tests when JSON fields or status codes can drift from hand-written HTTP.
7. Update `docs/Janus-http-gateway-migration-inventory.md` when route ownership, response shape, status mapping, or shim status changes.
8. Run `make verify`.