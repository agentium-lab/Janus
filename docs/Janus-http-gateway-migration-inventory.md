# Janus HTTP Gateway Migration Inventory

状态：v0.3 — grpc-gateway 覆盖清单

本文档记录 Janus Core v0.3 的 HTTP route 来源：哪些由 proto annotation 经 grpc-gateway 提供，哪些是手写 HTTP-only route，以及迁移状态。

自动检查入口：

```sh
make contract-check
```

---

## 1. Proto-Backed Routes (grpc-gateway)

以下 25 条 route 有 `google.api.http` annotation，由 grpc-gateway 自动提供 HTTP 接口：

| Service | Method | Path | Proto File |
| --- | --- | --- | --- |
| Agent | POST | `/v1/tenants/{tenant_id}/agents` | agent.proto |
| Agent | PATCH | `/v1/tenants/{tenant_id}/agents/{agent_id}` | agent.proto |
| Agent | GET | `/v1/tenants/{tenant_id}/agents` | agent.proto |
| Agent | GET | `/v1/tenants/{tenant_id}/agents/{agent_id}` | agent.proto |
| Agent | POST | `/v1/tenants/{tenant_id}/agents/{agent_id}/heartbeat` | agent.proto |
| Task | POST | `/v1/tenants/{tenant_id}/tasks` | task.proto |
| Task | GET | `/v1/tenants/{tenant_id}/tasks/{task_id}` | task.proto |
| Task | POST | `/v1/tenants/{tenant_id}/tasks/{task_id}/start` | task.proto |
| Task | POST | `/v1/tenants/{tenant_id}/tasks/{task_id}/cancel` | task.proto |
| Dispatch | POST | `/v1/tenants/{tenant_id}/mailboxes/{mailbox_id}/pull` | dispatch.proto |
| Dispatch | POST | `/v1/tenants/{tenant_id}/tasks/{task_id}/start` | dispatch.proto |
| Dispatch | POST | `/v1/tenants/{tenant_id}/tasks/{task_id}/heartbeat` | dispatch.proto |
| Dispatch | POST | `/v1/tenants/{tenant_id}/tasks/{task_id}/ack` | dispatch.proto |
| Dispatch | POST | `/v1/tenants/{tenant_id}/tasks/{task_id}/nack` | dispatch.proto |
| Mailbox | POST | `/v1/tenants/{tenant_id}/mailboxes` | mailbox.proto |
| Mailbox | GET | `/v1/tenants/{tenant_id}/mailboxes/{mailbox_id}` | mailbox.proto |
| Mailbox | PATCH | `/v1/tenants/{tenant_id}/mailboxes/{mailbox_id}` | mailbox.proto |
| Mailbox | POST | `/v1/tenants/{tenant_id}/mailboxes/{mailbox_id}/pause` | mailbox.proto |
| Mailbox | POST | `/v1/tenants/{tenant_id}/mailboxes/{mailbox_id}/resume` | mailbox.proto |
| DLQ | GET | `/v1/tenants/{tenant_id}/dlq` | dlq.proto |
| DLQ | POST | `/v1/tenants/{tenant_id}/dlq/{task_id}/replay` | dlq.proto |
| DLQ | POST | `/v1/tenants/{tenant_id}/dlq/{task_id}/discard` | dlq.proto |
| Audit | GET | `/v1/tenants/{tenant_id}/events` | audit.proto |
| Audit | GET | `/v1/tenants/{tenant_id}/traces/{trace_id}` | audit.proto |
| Audit | GET | `/v1/tenants/{tenant_id}/tasks/{task_id}/events` | audit.proto |

---

## 2. HTTP-Only Routes (no proto backing)

以下 15 条 route 是手写 HTTP handler，无 proto annotation。它们在 v0.3 中作为显式 HTTP-only stable route 登记：

| Method | Path | Handler | 迁移原因 |
| --- | --- | --- | --- |
| POST | `/v1/tenants` | TenantHandler.Create | 简单 CRUD，v0.4 控制面 proto 统一 |
| GET | `/v1/tenants/{tenant_id}` | TenantHandler.Get | 同上 |
| POST | `/v1/tenants/{tenant_id}/tasks/{task_id}/replay` | TaskHandler.Replay | action shim，v0.4 合入 Task proto |
| POST | `/v1/tenants/{tenant_id}/tasks/{task_id}/cancel` | TaskHandler.Cancel | action shim，v0.4 合入 Task proto |
| POST | `/v1/tenants/{tenant_id}/api-keys` | APIKeyHandler.Create | 安全管理面，v0.4 安全 proto |
| GET | `/v1/tenants/{tenant_id}/api-keys` | APIKeyHandler.List | 同上 |
| POST | `/v1/tenants/{tenant_id}/api-keys/{key_id}/revoke` | APIKeyHandler.Revoke | 同上 |
| POST | `/v1/tenants/{tenant_id}/policy-rules` | PolicyHandler.Create | 治理管理面，v0.4 控制面 proto |
| POST | `/v1/tenants/{tenant_id}/policy-rules/templates` | PolicyHandler.CreateFromTemplate | 同上 |
| GET | `/v1/tenants/{tenant_id}/policy-rules` | PolicyHandler.List | 同上 |
| POST | `/v1/tenants/{tenant_id}/budgets` | BudgetHandler.Upsert | 同上 |
| GET | `/v1/tenants/{tenant_id}/budgets/{scope_type}/{scope_id}` | BudgetHandler.Get | 同上 |
| GET | `/v1/tenants/{tenant_id}/budgets` | BudgetHandler.List | 同上 |
| POST | `/v1/tenants/{tenant_id}/approvals/{approval_id}/approve` | ApprovalHandler.Approve | 同上 |
| POST | `/v1/tenants/{tenant_id}/approvals/{approval_id}/reject` | ApprovalHandler.Reject | 同上 |

---

## 3. JSON Field Naming Convention

- grpc-gateway 使用 `UseProtoNames: true`，输出 snake_case JSON 字段名。
- 手写 HTTP handler 使用 Go struct tag `json:"snake_case"` 对齐。
- SDK fixture (`sdk/conformance/http_cases.json`) 验证字段名一致性。

---

## 4. HTTP Status Code Conventions

| 场景 | 状态码 | 说明 |
| --- | --- | --- |
| Create (tenant/task/agent/mailbox) | 201 Created | 资源创建成功 |
| Get/List | 200 OK | 读取成功 |
| Action (heartbeat/start/ack/nack/cancel/replay) | 200 OK | 操作成功 |
| Empty pull | 204 No Content | 队列为空 |
| Duplicate idempotent create | 200 OK | 幂等去重返回已有资源 |
| Validation error | 400 | `{error, code:"INVALID_ARGUMENT", message, status:400}` |
| Not found | 404 | `{error, code:"NOT_FOUND", message, status:404}` |
| Policy deny | 403 | `{error, code:"PERMISSION_DENIED", message, status:403}` |
| Budget/concurrency | 429 | `{error, code:"RESOURCE_EXHAUSTED", message, status:429}` |
| Conflict | 409 | `{error, code:"CONFLICT", message, status:409}` |
| Infra unavailable | 503 | `{error, code:"UNAVAILABLE", message, status:503}` |
| Internal | 500 | `{error, code:"INTERNAL", message, status:500}` |

---

## 5. v0.4 迁移计划

v0.4 将统一控制面 proto，将当前 HTTP-only route 迁移到 grpc-gateway：

1. 新建 `tenant.proto`（TenantService：Create/Get）
2. 扩展 `task.proto`（TaskService 加 Cancel/Replay action RPC）
3. 新建 `api_key.proto`（APIKeyService：Create/List/Revoke）
4. 新建 `governance.proto`（PolicyRuleService + BudgetService + ApprovalService）
5. 所有 HTTP-only route 获得 proto annotation 后，手写 handler 可移除

迁移完成后此清单归零，所有 stable route 由 proto 驱动。
