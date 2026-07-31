# Janus Core 生产级路线图

本文档定义 Janus Core 从当前实现走向生产级应用的路线图。路线图只覆盖 Core 能力，不把 OIDC/SSO、完整 RBAC/ABAC、DLP 实现、WORM、SIEM、成本中心、完整物理多租户隔离等 Enterprise 能力纳入 Core 交付门槛。

---

## 1. 路线图原则

1. **先可靠性，后生态适配**
Janus 的核心价值是不丢、不乱、不重复执行、可恢复。A2A/ACP/MCP、SDK 和 Dashboard 必须建立在可靠投递闭环之上。

2. **先 Core 闭环，后 Enterprise 治理**
durable mailbox、ACK/NACK、retry、DLQ、lease timeout、transactional outbox、redelivery reconciliation、基础 audit、基础 policy 和基础 tenant_id 隔离都属于 Core。

3. **每个阶段必须有退出标准**
不以文件数量或功能清单判断进度，而以故障场景是否可验证通过作为阶段完成标准。

4. **不提前做平台大而全**
v1.0 前不做复杂 workflow engine、自研向量数据库、自研 LLM gateway、完整企业 IAM 或复杂拓扑分析。

---

## 2. Release 分层

```text
Current implementation
|
v
0.2.1 Core Reliability Closure
|
v
0.2.2 Core Reliability Fault Closure
|
v
0.2 Core Reliability Alpha
|
v
0.3 API / SDK Contract Beta
|
v
0.4 Interop + Routing Beta
|
v
0.5 Ops + Observability RC
|
v
1.0 Core GA
|
v
Enterprise tracks
```

---

## 3. v0.2.1 当前迭代：Core Reliability Closure

目标：优先收敛 Core 中最容易造成“看似成功但实际丢状态”的可靠性缺口，不引入 Enterprise 能力。

本迭代已经落地的能力：

- ACK completed 路径进入 transactional outbox：
- `task_attempts` 更新为 `completed`。
- `tasks` 更新为 `completed` 并保存 `result_ref`。
- token usage 先写入按 `task_id + attempt + scope` 幂等去重的 `budget_usage_ledger`。
- 只有 ledger 插入成功才累加 `budget_usage` 聚合计数，避免重复 ACK 重复计费。
- 并发预算释放、ledger、聚合表更新与 completed event outbox 在同一 DB transaction 中完成。
- `task.completed` event 通过 outbox 写入，使用稳定 `dedupe_key`。
- DB 事务失败时不 ACK 原始 delivery，避免 broker 误删消息。
- DLQ 路径继续保持 outbox 化：
- final NACK / retry exhausted 同一事务更新 task/attempt。
- `dlq_publish` 与 `task.dead_lettered` event 均通过 outbox 发布。
- DispatchService 生命周期事件进入 outbox：
- 配置 outbox 后，`task.claimed`、`task.started`、`task.retry_scheduled` 不再直接 publish 到事件流。
- event outbox 使用稳定 dedupe key，优先按 `tenant_id + task_id + event_type + attempt` 去重。
- `claimed`、`started`、`retry_scheduled` 的 task/attempt 状态更新与 event outbox 写入在同一 PostgreSQL transaction 内完成。
- TaskService 管理型事件进入 outbox：
- 配置 outbox 后，Block / Unblock / Cancel / Replay 等管理型事件不再直接 publish 到事件流。
- 无 outbox 的测试和本地路径仍保留直接 publish fallback。
- Outbox publisher 补充后置 DB transaction 故障测试：
- 覆盖 NATS task publish 成功但 `MarkTaskPublished` 失败的场景。
- 验证失败会进入 `MarkFailedWithReason`，由 outbox retry 继续恢复。
- Proto 生成链路恢复为显式命令：
- `make proto` 安装固定版本生成器并执行 `buf generate`。
- 生成 gateway 文件。
- 删除会与官方 `genproto` 包冲突的 `google/api/httpbody.pb.go`。
- 复杂生产近似测试用例：
- 7 个 Agent：planner、research、coder、reviewer、security、qa、release。
- 覆盖 agent-to-agent 传递、审批阻塞/恢复、retriable NACK、lease timeout retry、result_ref 写入和事件计数。
- 验证 retry 场景下 `attempt` 递增，且最终任务进入 `completed`。
- 新增 ACK completed Postgres 集成测试：
- 验证 completed 事务、completed event outbox、预算释放/结算。
- 验证 tenant / agent 两个 scope 的 budget ledger 写入。
- 验证重复 ACK 不重复写 event、不重复写 ledger、不重复结算 token、不覆盖原始 `result_ref`。

v0.2.1 完成状态：

- 本迭代定义的 Core Reliability Closure 项已完成。
- 多实例 mailbox ensure、outbox worker lease、leaderless 并发安全、chaos/soak 测试进入后续 Core Reliability Alpha / Production Beta 里程碑，不作为 v0.2.1 阻塞项。

v0.2.1 退出标准：

- 全量 Go 测试通过。
- `make proto` 可复现生成 proto/gateway。
- Python SDK 源码 syntax check 通过；单元测试依赖缺失时必须记录原因。
- 复杂 7-Agent 仿真测试稳定通过。
- 已完成与未完成能力在文档中边界清晰，不把未实现能力写成生产级已完成。

---

## 4. v0.2.2 当前迭代：Core Reliability Fault Closure

目标：收敛 v0.2.1 后仍可能造成消息丢失、重复执行或运维误判的故障路径，不扩展 Enterprise 能力。

本迭代范围：

- Redelivery reconciliation：
- `created` 状态 task 收到 delivery 时，不能 ACK 丢弃；必须 NACK 保留 broker 消息，等待 outbox 后置事务把 task 推进到 `queued`。
- `completed` / `dead_lettered` / `cancelled` / `expired` 等终态 stale delivery 可以 ACK 清理。
- `retry_scheduled` stale delivery 可以 ACK 清理，由 retry/outbox 调度重新入队。
- 旧 attempt delivery 必须 ACK 清理，不能创建新 attempt。
- ACK/NACK/lease timeout 幂等：
- lease timeout 后，原 Agent 再提交 ACK/NACK 必须被 lease 校验拒绝。
- DB 已完成但 broker ACK 失败后的 redelivery，必须由 stale delivery reconciliation 清理。
- DLQ replay/discard 幂等：
- `dead_lettered -> created -> queued` replay 不能重复 publish。
- replay 后已经处于 `queued/claimed/running/retry_scheduled/completed` 的 task，再次 replay 返回当前状态，不重新入队。
- replay 过程中遗留在 `created` 的 task，再次 replay 必须重新确保 task publish/outbox 存在。
- discard 已经 `cancelled` 的 task 必须无副作用返回；discard 非 DLQ task 必须报错。
- 验证入口：
- 增加 `make test`、`make python-compile`、`make verify`。
- `make verify` 成为 Core 本地基线命令。

v0.2.2 完成状态：

- `created` delivery 不再被 ACK 丢弃，改为 NACK 等待 outbox 恢复。
- terminal / retry_scheduled / old attempt delivery reconciliation 已有单测覆盖。
- lease timeout 后旧 ACK 已被测试覆盖，旧 lease 会被拒绝。
- DLQ replay/discard 幂等语义已落地，并新增单测覆盖。
- PostgreSQL outbox 增加 DLQ replay 事务方法，生产路径不再由 handler 分散执行非原子步骤。
- `make coverage` 已加入 Core 覆盖率门禁，要求合并覆盖率不低于 90.0%。
- `make verify` 已可直接执行全量 Go 测试、Python SDK syntax check 和 Core 覆盖率门禁。

v0.2.2 退出标准：

- Dispatch redelivery 单测覆盖 created / terminal / retry_scheduled / old attempt。
- lease timeout 后旧 ACK 单测通过。
- DLQ replay/discard 幂等单测通过。
- 全量 Go 测试通过。
- Python SDK syntax check 通过。
- Core 覆盖率门禁通过，最低阈值为 90.0%。
- `make verify` 可在本地直接执行。

---

## 5. Milestone 0：基线冻结与工程卫生

目标：在继续补生产能力前，先让当前仓库的状态、测试和文档基线可控。

范围：

- 固化 Core / Enterprise 边界。
- 清理主设计文档 CRLF / trailing whitespace，避免后续 diff 噪声。
- 建立统一测试命令和本地依赖启动方式。
- 明确每个 package 的职责边界。
- 标记当前未完成的 P0/P1/P2 backlog。

退出标准：

- `go test` 覆盖 core/server/cli/sdk-go/demo/proto。
- Python SDK 至少通过 syntax check 和单元测试依赖安装说明。
- `docker compose up` 能启动 PostgreSQL、NATS、Redis、Janus API。
- 文档中没有互相冲突的 Core / Enterprise 边界描述。

---

## 6. Milestone 1：Core Reliability Alpha

目标：补齐 Janus 作为生产级 durable broker 的可靠性闭环。

必须完成：

- Transactional outbox 使用稳定 `dedupe_key`。
- NATS publish 使用 `Nats-Msg-Id` 或等价去重机制。
- `task.created` 后保持 `created`；只有 mailbox publish 成功后才推进到 `queued`。
- Outbox publisher 在同一个后置 DB transaction 中：
- 标记 outbox `published`。
- 对 `task_publish` 推进 task status。
- 补写 `task.queued` event_publish outbox。
- `TaskMessage` / `TaskDelivery` 携带 `attempt`。
- ACK/NACK/Start/Heartbeat 都校验 `(tenant_id, task_id, attempt, lease_id)`。
- Redelivery reconciliation 覆盖旧 delivery、终态 task、retry_scheduled task、created-but-published task。
- Lease timeout scanner 可把 `claimed/running` attempt 推进到 retry 或 DLQ。
- Retry 只由 outbox `next_attempt_at` 驱动，不创建独立 retry stream。
- DLQ replay 和 discard 幂等。
- NATS tenant streams 在 API 启动时从 PostgreSQL tenants 自动 ensure；mailbox DLQ stream 和 consumer 在首次 pull 时 lazy ensure，避免大规模 mailbox 启动风暴。

v0.2-alpha.1：Broker Bootstrap & Recovery 完成状态：

- 新增独立 bootstrapper，从 PostgreSQL tenants 和 mailboxes 恢复 broker 资源。
- API 启动时执行 bootstrap，失败时 fail-fast，不进入部分可用状态。
- tenant-level NATS streams 不再依赖 mailbox 推导；没有 mailbox 的 tenant 也会被 ensure。
- 每个 active mailbox 的 DLQ stream 和 task consumer 在首次 pull 时 ensure。
- bootstrap 行为有单元测试覆盖正常恢复、tenant 去重、防御性 mailbox tenant ensure 和 list 失败；mailbox/consumer lazy ensure 由 dispatch service 和 ops chaos 覆盖。

v0.2-alpha.2：Outbox Worker Lease & Publishing Timeout 完成状态：

- outbox worker fetch 显式写入 `locked_by`、`locked_at`、`lease_expires_at`。
- `pending/retry` 记录按 `next_attempt_at` 到期后获取；`publishing` 记录只有 lease 过期后才允许其他 worker 接管。
- fetch 继续使用 `FOR UPDATE SKIP LOCKED`，多实例并发不会抢同一批记录。
- publish 成功后清理 worker lease 字段。
- publish 或后置 DB transaction 失败后清理 worker lease 字段，并按 retry backoff 重新调度。
- publisher 拥有稳定 worker id 和可配置 lease duration，默认 lease 为 1 分钟。
- PostgreSQL 测试覆盖 active lease 不被抢、expired lease 被接管、MarkPublished/MarkFailed 清理 lease。

v0.2-alpha.3：Pull-Ack Crash Recovery 完成状态：

- `claimed/running` 状态下收到同 attempt redelivery 时，不再归类为泛化 `stale_delivery`，而是明确标记为 `inflight_delivery`。
- in-flight redelivery 会 ACK broker 重复投递，避免第二个 Agent 创建重复 attempt 或重复执行。
- 比当前 attempt 更新的 in-flight delivery 会被 NACK 并报错，避免吞掉异常 future attempt。
- 单测覆盖 Pull 成功后、业务 ACK 前 API 重启：
- redelivery 不重复 claim。
- attempt 数量保持不变。
- lease timeout 后推进 `retry_scheduled`。
- 原 Agent 使用旧 lease 再 ACK 会被拒绝。

v0.2-alpha.4：Retry Exhausted DLQ Replay Requeue 完成状态：

- 新增 PostgreSQL-backed reliability 测试，串起真实 repo、DispatchService、DLQServiceAdapter 和 outbox 状态推进。
- 覆盖 retriable NACK 在 attempt 达到 mailbox `max_attempts` 后进入 `dead_lettered`。
- 验证 DLQ outbox 产生 `dlq_publish`，并携带稳定 `task.dlq_enqueue:<tenant>:<task>:<attempt>` dedupe key。
- 验证 DLQ replay 先把 task 恢复到 `created`，再通过 `task_publish` outbox 重新入队。
- 验证 `MarkTaskPublished` 后 task 进入 `queued`，下一次 attempt 为原 attempt + 1。
- 验证 replay 后再次 replay 是幂等返回，不重复 publish。

v0.2-alpha.5：Completed Ack Failure Redelivery Recovery 完成状态：

- 新增 PostgreSQL-backed reliability 测试，覆盖 task completed DB transaction 已提交但 broker ACK 失败的故障点。
- 验证 `AckTask` 返回 broker ACK error 时，task 和 attempt 已保持 `completed`，`result_ref` 已持久化。
- 验证 broker redelivery 后，`PullTask` 会按 terminal stale delivery ACK 清理，不重新分配任务。
- 验证重复 ACK 会再次尝试 ACK 原 delivery，但不会重复写 completed event outbox。
- 验证重复 ACK 不会覆盖首次成功写入的 `result_ref`。

v0.2-alpha.6：Old Attempt Duplicate ACK/NACK Hardening 完成状态：

- 旧 attempt ACK/NACK 在已有更新 attempt 时会被 `attempt mismatch` 拒绝，不 ACK delivery、不写 outbox、不改变 task 状态。
- 重复 retriable NACK 在 attempt 已经进入 `failed` 后变为 no-op，不再刷新 `retry_at` 或重复写 retry event outbox。
- `MarkTaskRetryScheduled` 和 `MarkTaskDeadLettered` 的 attempt 状态更新收紧为只接受 `claimed/running`。
- PostgreSQL 测试覆盖 retry duplicate no-op、DLQ duplicate no-op、event/dlq outbox 不重复。
- DispatchService 单测覆盖旧 attempt ACK、旧 attempt NACK、重复 retriable NACK。

v0.2-alpha.7：Retry Outbox-Driven Scheduling 完成状态：

- retry 调度在 `MarkTaskRetryScheduled` 的同一 PostgreSQL transaction 内同时完成 attempt `failed`、task `retry_scheduled`、`task.retry_scheduled` event outbox 和 delayed `task_publish` outbox 写入。
- retry publish outbox 使用稳定 dedupe key：`task_publish:<tenant_id>:<task_id>:<next_attempt>`。
- delayed `task_publish.next_attempt_at` 等于 task `retry_at`，由通用 outbox publisher 在到期后投递，不再依赖独立 retry stream。
- 生产 API 启动路径不再启动旧的 task-table retry scheduler；`server/internal/retry` 仅保留为 legacy/fallback 包，不作为 Core 生产调度路径。
- PostgreSQL 测试覆盖 retry outbox payload、下一 attempt、`next_attempt_at` 和重复 retriable NACK 不重复创建 retry publish outbox。

故障测试完成状态：

- NATS publish 成功，但 outbox 后置 DB transaction 失败：已覆盖。
- DB 已写 completed，但 NATS ACK 失败，随后旧 delivery redeliver：已覆盖。
- API 进程在 pull 后、ACK 前重启：已覆盖。
- lease timeout 后原 Agent 再 ACK：已覆盖。
- retry exhausted 后进入 DLQ，DLQ replay 后重新入队：已覆盖。
- 旧 attempt 的 ACK/NACK 重复提交：已覆盖。
- retry_scheduled 到下一次入队只由 delayed outbox `next_attempt_at` 驱动：已覆盖。

v0.2 Core Reliability Alpha 完成状态：

- v0.2.1、v0.2.2 和 v0.2-alpha.1 到 v0.2-alpha.7 的可靠性范围均已完成。
- `make coverage` 固化 Core 覆盖率门禁；当前合并覆盖率为 90.0%，门禁阈值为 90.0%。
- 覆盖率门禁范围是 Core 可单测包：`core`、关键 `server/internal` 服务、gateway、handler、grpc、outbox、lease、storage、`cli` 和 Go SDK。
- 生成代码、demo、外部依赖集成测试、长周期 soak/chaos、不稳定外部服务路径不计入该 90.0% 单元覆盖率门禁；这些仍由 `make test`、可靠性测试和后续 Production Beta 验证覆盖。
- v0.2 不包含 Dashboard、7 天 soak、Kubernetes 生产演练或 Enterprise 能力；这些继续保留在 v0.5、Production Beta 和 Enterprise track。

退出标准：

- 上述故障测试全部通过。
- 单任务在任意重启点不会丢失。
- ACK/NACK 重复提交不会重复结算预算、重复写结果或重复调度 retry。
- API/CLI 不把 API accepted 错判为 queued。
- Core 覆盖率门禁达到 90.0%。

---

## 7. Milestone 2：API / SDK Contract Beta

目标：把 Core 的可靠性语义稳定暴露给外部调用方。

必须完成：

- proto 中补齐 `attempt`、标准错误码、result/result_ref 语义。
- HTTP handler 与 grpc handler 与 proto 保持一致。
- 删除手写临时 grpc-gateway 方案，恢复标准生成链路。
- Go SDK 支持：
- publish/pull/start/heartbeat/ack/nack。
- attempt 传递。
- API key。
- 标准错误类型。
- Python SDK 与 Go SDK 契约对齐。
- TypeScript SDK 进入 Core。
- CLI 支持常用生产操作：
- agent register/heartbeat/status。
- task publish/status/cancel/replay。
- mailbox create/status/pause/resume。
- dlq query/replay/discard。
- api-key create/list/revoke。
- API key 默认可启用，提供 key 管理 API/CLI。
- mTLS 作为 Core 可选部署模式。

v0.2-beta.1：Standard HTTP/SDK Error Contract 完成状态：

- HTTP handler 的错误响应升级为兼容 envelope：保留 legacy `error` 字段，同时新增 `code`、`message`、`status`。
- HTTP status 到标准错误码的基础映射已固定：`INVALID_ARGUMENT`、`UNAUTHENTICATED`、`PERMISSION_DENIED`、`NOT_FOUND`、`CONFLICT`、`RESOURCE_EXHAUSTED`、`UNAVAILABLE`、`INTERNAL`、`UNKNOWN`。
- Go SDK 新增 typed `APIError`，解析标准 envelope，并兼容旧的 `{"error": "..."}` 响应。
- Python SDK 新增 `JanusAPIError`，继承 `httpx.HTTPStatusError` 以保持旧捕获路径兼容，同时暴露 `code/message/status`。
- Python SDK 测试用例已更新为 attempt-aware 的 start/heartbeat/ack/nack contract。
- TypeScript SDK 和完整 migration note 已在 v0.3.1 contract baseline 中补齐。

v0.2-beta.2：Proto/gRPC Standard Error Mapping 完成状态：

- `proto/janus/v1/common.proto` 新增 `ErrorCode` enum 和 `APIError` message，作为跨 HTTP/gRPC/SDK 的标准错误契约。
- gRPC handler 不再直接返回原始 Go error；统一通过 mapper 转换为 `status.Error(codes.X, message)`。
- 已覆盖 Agent、Task、Dispatch、Audit 四类 gRPC service handler。
- gRPC error mapper 固定常见业务错误分类：validation -> `InvalidArgument`，not found -> `NotFound`，policy denied -> `PermissionDenied`，budget/quota/concurrency -> `ResourceExhausted`，conflict/duplicate -> `AlreadyExists`，queue/Redis/NATS unavailable -> `Unavailable`，DB/transaction -> `Internal`。
- grpc-gateway 使用自定义 error handler，HTTP JSON 错误 envelope 与手写 HTTP handler 对齐：`error/code/message/status`。
- 测试覆盖 mapper 分类、已有 gRPC status 保留、gateway error envelope、Dispatch service error 到 gRPC status 的转换。
- API key 管理 API/CLI、TypeScript SDK 和完整 migration note 已在后续 v0.2-beta.3 / v0.3.1 中补齐。

v0.2-beta.3：API Key Management API/CLI 完成状态：

- `api_keys` schema 增加 `id`、`last_used_at`、`revoked_at`，并提供 `(tenant_id, id)` 唯一索引和 active `key_hash` 索引。
- 新 key prefix 从 8 位提升到 20 位；validator 改为按 `key_hash` 校验，兼容旧 prefix 长度，并拒绝 `revoked_at` 非空的 key。
- API key 校验成功后更新 `last_used_at`。
- 新增 API key manager：create/list/revoke。
- 新增 HTTP 管理接口：
- `POST /v1/tenants/{tenant_id}/api-keys`
- `GET /v1/tenants/{tenant_id}/api-keys`
- `POST /v1/tenants/{tenant_id}/api-keys/{key_id}/revoke`
- create 只在创建响应中返回一次 raw secret；list/revoke 响应不返回 secret/hash。
- auth middleware 的 missing/invalid key 和 tenant mismatch 错误改为标准 `error/code/message/status` envelope。
- Go SDK 新增 `CreateAPIKey`、`ListAPIKeys`、`RevokeAPIKey`。
- CLI 新增：
- `janus api-key create --name <name>`
- `janus api-key list`
- `janus api-key revoke <key-id>`
- CLI 新增全局 `--api-key` flag，默认读取 `JANUS_API_KEY`。
- 测试覆盖 manager 生命周期、revoked key 不可认证、handler 不泄漏 secret/hash、SDK contract、CLI create/list/revoke。
- TypeScript SDK、完整 migration note、API key gRPC/proto 管理面已在 v0.3.1 contract baseline 中纳入。

v0.3.1：Contract Baseline & Verification Gate 完成状态：

- 新增 `docs/Janus-api-contract.md`，冻结 v0.3 HTTP、gRPC gateway、Go/Python/TypeScript SDK 的字段语义。
- 新增 `docs/Janus-v0.3-migration.md`，记录 v0.2 -> v0.3 的调用方迁移步骤。
- `make verify` 扩展为 full Go test、Python syntax check、Python SDK unit tests、TypeScript SDK unit tests 和 Core 90.0% coverage gate。
- Python SDK dev 依赖安装方式明确为 `python3 -m venv .venv` 后执行 `make python-dev-install`。
- TypeScript SDK 测试纳入统一门禁，不再只靠手工执行 `npm test`。
- `.venv/` 已加入 `.gitignore`，本地 Python 测试环境不进入仓库。

v0.3.2：SDK Parity Baseline 完成状态：

- Python SDK 补齐 agent get/heartbeat、mailbox create/get/update/pause/resume、task replay、DLQ query/replay/discard。
- TypeScript SDK 补齐 agent get/heartbeat、mailbox create/get/update/pause/resume、task replay、DLQ query/replay/discard。
- Python SDK 新增对应模型：Agent、AgentCapability、Mailbox、RetryPolicy、CreateMailboxRequest、UpdateMailboxRequest、MailboxActionResponse。
- TypeScript SDK 新增对应类型定义和客户端方法。
- Python SDK 测试扩展到 24 个用例，覆盖新增管理面。
- TypeScript SDK 测试扩展到 8 个用例，覆盖新增管理面。
- `docs/Janus-api-contract.md` 已把新增 Python/TypeScript parity 标为稳定 SDK 面。

v0.3.3：Cross-SDK Conformance Fixtures 完成状态：

- 新增 `sdk/conformance/http_cases.json`，作为 Go/Python/TypeScript SDK 的共享 HTTP contract golden source。
- fixture 覆盖 tenant-scoped path、method、API key header、JSON body、query params、empty pull、标准错误 envelope。
- Go SDK 新增 conformance 测试，读取共享 fixture 校验请求和响应解码。
- Python SDK 新增 conformance 测试，读取共享 fixture 校验请求和响应解码。
- TypeScript SDK 新增 conformance 测试，读取共享 fixture 校验请求和响应解码。
- Go SDK `RegisterAgentRequest.display_name` 改为 `omitempty`，与 Python/TypeScript 的可选字段行为对齐。

v0.3.4：Proto / HTTP / SDK Drift Audit 完成状态：

- 新增 `scripts/check_api_contract.py`，自动比对 proto HTTP annotations、共享 SDK conformance fixture 和显式 HTTP-only route allowlist。
- 新增 `make contract-check`，并已接入 `make verify`。
- 新增 `docs/Janus-api-surface-audit.md`，记录 v0.3 当前 API surface、proto 覆盖、HTTP-only 路由和暂不纳入 SDK conformance 的 proto 路由。
- SDK conformance fixture 扩展到 agent、task lifecycle、mailbox、DLQ、API key 和标准错误 envelope 的稳定 HTTP 面。
- Go/Python/TypeScript SDK conformance 测试继续使用同一份 fixture，避免三套 SDK 各自漂移。

v0.3.5：Mailbox / DLQ Proto-Gateway Closure 完成状态：

- 新增 `proto/janus/v1/mailbox.proto`，覆盖 mailbox create/get/update/pause/resume。
- 新增 `proto/janus/v1/dlq.proto`，覆盖 DLQ query/replay/discard。
- 新增 mailbox 和 DLQ gRPC server 实现，并注册到 gRPC server 与 grpc-gateway。
- `scripts/check_api_contract.py` 的稳定 HTTP-only allowlist 清空；稳定 Core SDK route 必须有 proto annotation 覆盖。
- `docs/Janus-api-surface-audit.md` 更新为 28 条 proto route、24 条 conformance route、0 条 HTTP-only stable route。

v0.3.6：grpc-gateway HTTP Parity Gate 完成状态：

- grpc-gateway marshaler 固定为 proto field name 输出，HTTP JSON 使用 `snake_case`，避免 gateway 返回 `camelCase` 破坏 SDK contract。
- grpc-gateway create 类 RPC 增加 response status 映射，保持 agent/task/mailbox/API key create 与手写 HTTP 的 `201` 语义一致。
- 新增 in-process gateway HTTP parity tests，覆盖 `/grpc/v1/...` mailbox create/get、DLQ query 和标准错误 envelope。
- `sdk/conformance/http_cases.json` 的 `publish_task` status 对齐当前手写 HTTP contract：`201`。

v0.3.7：Expanded grpc-gateway Stable Surface Parity 完成状态：

- gateway parity tests 扩展到 agent register/get，覆盖 create status、`display_name` 和 `tenant_id` 的 `snake_case` 输出。
- gateway parity tests 扩展到 task create/get，覆盖 create status、`source_agent`、`target_type` 的 `snake_case` 输出。
- gateway parity tests 扩展到 API key create/list/revoke，覆盖 `api_keys`、`created_at`、`revoked_at` 的 `snake_case` 输出。
- mailbox、DLQ、agent、task、API key 和标准错误 envelope 都已经有 `/grpc/v1/...` HTTP 层 parity gate。

v0.3.8：HTTP -> grpc-gateway Migration Inventory 完成状态：

- 新增 `docs/Janus-http-gateway-migration-inventory.md`，记录公开 `/v1` 从手写 HTTP router 迁移到 grpc-gateway 前的生产级 route 清单。
- 按生产切换风险把稳定 route 分为 `canary-ready`、`needs-shim`、`needs-contract-freeze` 和 `hand-written-only`。
- 明确第一批可灰度候选：API key management、mailbox management、agent/task read、DLQ management。
- 明确不能直接切换的兼容点：agent/task create 简化响应体、agent heartbeat 响应体、task cancel 响应体、task events `limit`/`page_size` 查询参数、dispatch action empty response、pull 空队列 `204 No Content`。
- 明确 v0.3.8 没有改变公开 `/v1` route ownership；grpc-gateway 仍挂载在 `/grpc/v1/...`，用于 parity gate 和后续灰度。

v0.3.9：Final Contract Closure 完成状态：

- grpc-gateway 增加稳定 HTTP response rewriter，agent/task create 保持 `{"id","status"}`，不把 full proto resource 泄漏到 v0.3 HTTP/SDK contract。
- grpc-gateway 增加 action response shim，agent heartbeat、task cancel、dispatch start/heartbeat/ack/nack 与手写 HTTP status body 对齐。
- grpc-gateway 增加 empty pull `204 No Content` 兼容层，并抑制 no-content response body。
- grpc-gateway 增加 `limit -> page_size` query alias，task events 继续接受 v0.3 SDK 稳定查询参数。
- gateway parity tests 扩展到 agent heartbeat、task cancel/replay/events、dispatch pull/start/heartbeat/ack/nack、mailbox update/pause/resume、DLQ replay/discard。
- 新增 `sdk/conformance/worker_flow.json`，Go/Python/TypeScript SDK 使用同一份 fixture 验证 `pull -> start -> heartbeat -> ack` 和 `pull -> start -> heartbeat -> nack` 过程中 `task_id`、`lease_id`、`attempt`、`result_ref` 和结构化错误一致传递。
- `sdk/conformance/http_cases.json` 版本推进到 `v0.3.9`。
- v0.3 稳定 SDK surface 已达到 proto annotation 覆盖、gateway HTTP parity、跨 SDK conformance 和 worker lifecycle sequence 四层门禁。
- 公开 `/v1` route ownership 仍保持手写 HTTP router；后续是否逐条切 gateway 是 v0.4/ops 灰度动作，不再是 v0.3 contract 阻塞项。

v0.3 API / SDK Contract Beta 完成状态：

- Go/Python/TypeScript SDK 已覆盖同一套稳定 HTTP contract fixture。
- Go/Python/TypeScript SDK 已跑通同一套 pull-execute-ack/nack worker lifecycle fixture。
- proto、手写 HTTP、grpc-gateway、SDK 字段语义已由 drift check、gateway parity test 和 SDK conformance test 共同锁定。
- API key 管理 API/CLI、mTLS 可选部署、标准错误 envelope、attempt-aware dispatch lifecycle、result_ref ACK contract 均已纳入 v0.3 baseline。
- v0.3 不切换公开 `/v1` route ownership；这避免在 contract beta 末尾引入额外部署风险。

退出标准：

- Go/Python/TypeScript SDK 都能跑通同一套 pull-execute-ack/nack 示例：已完成，由 `sdk/conformance/worker_flow.json` 固化 ACK 和 NACK 两条 worker lifecycle。
- proto、HTTP、SDK 字段没有语义分叉：已完成，稳定 SDK surface 由 proto annotation、gateway parity tests 和 cross-SDK conformance fixtures 共同约束。
- 旧版本不兼容变更有明确 migration note：已完成，见 `docs/Janus-v0.3-migration.md`。
- `make verify` 在安装 Python dev 依赖后必须通过，并保持 Core 覆盖率不低于 90.0%。
- 新增或变更 SDK HTTP contract 时，必须先更新共享 conformance fixture。
- 新增稳定 API route 或变更 route ownership 时，必须更新 drift audit inventory 和 gateway migration inventory，并通过 `make contract-check`。

---

## 8. Milestone 3：Interop + Routing Beta

目标：让 Janus 能以 Core 形态接入真实 Agent 生态，而不是只服务内部 demo。

必须完成：

- Agent capabilities 注册、更新、查询完整落库。
- 基础 resolver 支持：
- `target.type = mailbox`：已在 v0.4.1 校验 active mailbox。
- `target.type = agent`：已在 v0.4.1 校验 online agent，并选择 active mailbox。
- `target.type = capability`：已在 v0.4.1 基于 online capability agent 和 mailbox backlog 选择目标。
- `target.type = group`：已在 v0.4.2 支持显式 `mailbox_id` 或 tenant-scoped 静态 mailbox 映射。
- `target.type = human`：已在 v0.4.2 支持显式 `mailbox_id` 或 tenant-scoped 静态 mailbox 映射。
- capability routing 已在 v0.4.9 基于 online/active/backlog foundation 叠加 policy / budget / capacity / data classification 硬约束过滤、可选语义排序、候选评分细节和 route explanation。
- A2A Gateway 完整映射：
- Agent Card。
- task/message send。
- 状态映射。
- error mapping。
- trace/context 透传。
- ACP Gateway beta 基础映射（v0.4.5 已完成）。
- MCP Gateway beta 基础映射（v0.4.6 已完成）。
- ContextRef 与 task 绑定关系可用。
- Artifact/Object Store Core interface、本地实现和 S3-compatible adapter 边界。

v0.4.1 已完成：

- Agent capability 路由 foundation：capability 候选必须来自 online agent，目标 mailbox 必须 active。
- mailbox / agent / capability 目标解析不再静默创建无可投递 mailbox 的任务。
- 多 mailbox 候选按 backlog 最低选择，mailbox ID 作为稳定 tie-break。
- 显式 `mailbox_id` 仍需通过 target type 合法性与 active mailbox 校验。
- group / human 无显式 `mailbox_id` 时返回标准 400 error envelope，避免生产中出现不可投递任务。

v0.4.2 已完成：

- `routing.group_mailboxes` 与 `routing.human_mailboxes` 支持 tenant-scoped 静态 mailbox 映射，精确 tenant 优先，`*` 作为显式 wildcard fallback。
- group / human 映射结果仍必须通过 active mailbox 校验；映射不存在或映射 mailbox 不可用时拒绝创建任务。
- 路由失败写入 `routing.failed` 审计事件，payload 包含 reason、message、target_type、target_value、mailbox_id。
- 路由失败指标 `janus_routing_failures_total{tenant_id,target_type,reason}` 可用于 dashboard 和 alert。

v0.4.3 已完成：

- capability 候选阶段叠加 data classification、`task.route` policy deny、agent/mailbox capacity、budget 并发与 task budget 上限硬约束。
- capability schema 支持 `allowed_data_classifications` / `data_classifications` / `max_data_classification`，用于 Core 数据分级过滤。
- 成功路由写入 `routing.selected` 审计事件，payload 包含 strategy、selected agent/mailbox、候选数量、rejection counts 和 selected backlog。
- 成功路由指标 `janus_routing_selected_total{tenant_id,target_type,strategy}` 可用于 dashboard 和 route health 观察。

v0.4.4 已完成：

- A2A Agent Card 注册映射增强，capability input/output schema 会进入 Janus capability schema。
- A2A HTTP send 与 JSON-RPC `message/send` 均可映射为 Janus Task Envelope。
- A2A target 支持 mailbox、capability 等 Janus target 类型，预算、策略、contextRefs、TTL、trace/context 可以透传。
- A2A `tasks/get` 与 `GET /a2a/task/{task_id}/status` 将 Janus task status/error/trace 映射回 A2A status。
- A2A HTTP endpoint 使用 `{error,message,status}`，JSON-RPC endpoint 使用标准 JSON-RPC error object。

v0.4.5 已完成：

- ACP Agent Manifest 注册映射增强，capability input_schema/output_schema 会进入 Janus capability schema。
- 新增 `POST /acp/runs`，ACP run 可映射为 Janus Task Envelope。
- ACP target 支持 body 优先、query fallback，预算、策略、contextRefs、TTL、trace/thread、parent run 和 idempotency key 可以透传。
- 新增 `GET /acp/runs/{run_id}/status`，将 Janus task status/error/result_ref/trace 映射回 ACP run status。
- ACP HTTP endpoint 使用 `{error,message,status}`，并保持 Core beta 不引入 Enterprise catalog、连接器审批或策略包。

v0.4.6 已完成：

- MCP tool call 映射增强，`tool_name` 默认进入 `mcp.tool.{tool_name}` capability target，也支持显式 mailbox/capability target。
- 新增 `POST /mcp/tools/call`，MCP tool call 可映射为 Janus Task Envelope。
- MCP tool call 支持预算、策略、contextRefs、TTL、trace/thread、parent call 和 idempotency key 透传，确保不绕过 Janus policy/budget/audit。
- 新增 `GET /mcp/tools/calls/{call_id}/status`，将 Janus task status/error/result_ref/trace 映射回 MCP tool call status。
- 新增 `POST /mcp/resources`，MCP resource 可注册为 Janus ContextRef，并在后续 tool call 中参与数据分级和策略判断。

v0.4.7 已完成：

- Task 创建时会规范化 `envelope.context_refs[]`，缺失 `tenant_id` 使用 task tenant，缺失 `id` 生成 `ctxref_*`。
- inline ContextRef 会写入或更新 `context_refs`，并持久写入 `task_context_refs` 绑定表。
- 只传 `id` 的 ContextRef 必须引用同 tenant 下已有 ContextRef；跨 tenant ContextRef 会拒绝创建任务。
- Postgres direct create 与 outbox transaction create 使用同一绑定语义，TaskService 会先规范化 ContextRef，保证 DB envelope、outbox payload 和 worker payload 一致。
- `POST /v1/tenants/{tenant_id}/tasks/{task_id}/context-refs/attach` 支持创建并绑定新 ContextRef，或通过 body `id` 绑定已有 ContextRef。

v0.4.8 已完成：

- `core.ArtifactStore` 抽象与 `LocalArtifactStore` 本地实现已接入服务端运行路径。
- 新增 `ArtifactService`，统一 artifact tenant 校验、上传、下载和可选 ContextRef 注册。
- 新增 `POST /v1/tenants/{tenant_id}/artifacts?name=...`，上传 body 原始内容并返回 `artifact://local/{tenant_id}/{name}` URI、size 和 sha256。
- 新增 `GET /v1/tenants/{tenant_id}/artifacts?uri=...`，下载 artifact 并返回 `X-Artifact-URI` / `X-Artifact-SHA256`。
- 支持 `context_ref=true&classification=...&access_scope=a,b`，上传后创建 `type=artifact` 的 ContextRef。
- 新增配置 `artifacts.store=local`、`artifacts.local_dir=data/artifacts`；S3 SigV4、KMS、WORM、retention、quota 和 per-tenant bucket isolation 不进入 Core v0.4.8。

v0.4.9 已完成：

- capability resolver 在 policy / budget / capacity / data classification 硬约束之后增加可选语义排序。
- 语义排序仅使用本地可审计规则：`payload.type`、payload token、allowed tools、model classes、ContextRef type/scope 与 capability schema / description 的显式 hint 匹配。
- 至少一个候选 semantic score 为正时，排序顺序为 `semantic_score_desc,backlog_asc,mailbox_id_asc,agent_id_asc`；所有候选为 0 时保持原有 backlog + 稳定 ID 排序。
- `routing.selected` route explanation 新增 `sort_order`、`candidate_score_count`、`candidate_scores`、`selected_semantic_score`、`selected_semantic_reasons`。

v0.4.10 已完成：

- 新增 `examples/interop/python/langgraph_worker.py`，展示 LangGraph worker 如何通过 Janus pull/start/ack/nack 生命周期运行。
- 新增 `examples/interop/python/autogen_worker.py`，展示 AutoGen team/chat runner 如何作为 Janus capability worker 接入。
- 新增 `examples/interop/python/crewai_worker.py`，展示 CrewAI crew kickoff 如何作为 Janus capability worker 接入。
- 新增 `examples/interop/github-actions/janus-pr-review.yml`，展示 GitHub Actions 如何上传 PR diff artifact、创建 ContextRef，并发布 Janus review task。
- 新增 `examples/interop/README.md`，说明示例运行方式、环境变量和 Core 生命周期边界。
- `make verify` 新增 `python-examples-compile`，确保 Python interop 示例保持语法可编译。

v0.4 代码项剩余：

- 无。

退出标准：

- LangGraph/AutoGen/CrewAI/GitHub Actions 至少以示例方式接入：已完成，见 `examples/interop/`。
- A2A agent-to-agent 链路可以通过 Janus 完成 publish、dispatch、result 和 audit 查询：已完成。
- MCP 只作为 tool/context 接入，不绕过 Janus policy/budget/audit：已完成。

---

## 9. Milestone 4：Ops + Observability RC

目标：让 Janus Core 可以被运维团队部署、观察、升级和回滚。

必须完成：

- OpenTelemetry trace provider 接入。
- Prometheus metrics 覆盖：
- publish latency。
- pull latency。
- ACK/NACK count。
- retry count。
- DLQ count。
- outbox backlog。
- mailbox backlog。
- lease timeout。
- policy deny。
- budget throttle。
- 结构化 JSON log，包含 `tenant_id`、`task_id`、`attempt`、`trace_id`。
- `/healthz`、`/readyz`、dependency readiness 分离。
- 基础 Helm chart。
- migration runbook。
- backup/restore runbook。
- rolling upgrade runbook。
- Dashboard 展示：
- agents。
- mailboxes。
- task lifecycle。
- outbox backlog。
- retry/DLQ。
- audit trace。

v0.5.1 已完成：

- 新增 `log` 与 `observability` 配置，支持环境变量覆盖：
- `JANUS_LOG_LEVEL` / `JANUS_LOG_FORMAT`
- `JANUS_METRICS_ENABLED` / `JANUS_METRICS_PATH`
- `JANUS_TRACING_ENABLED` / `JANUS_TRACING_SERVICE_NAME` / `JANUS_TRACING_ENDPOINT`
- `/metrics` 变为可配置开关和路径，默认仍启用 `/metrics`。
- HTTP handler 外层增加 observability middleware，记录结构化 JSON request log。
- request log 包含 `tenant_id`、`task_id`、`attempt`、`trace_id`、method、path、route、status、duration。
- 新增 W3C `traceparent` 兼容传播：入站有 `traceparent` 时沿用 trace id；没有时生成 trace id，并在响应头返回 `Traceparent` / `X-Trace-ID`。
- 新增 HTTP request 指标：
- `janus_http_requests_total{method,route,status}`
- `janus_http_request_duration_seconds{method,route,status}`
- middleware 使用低基数 route label，并透传 `Flusher` / `Hijacker` / `Pusher`，不破坏 WebSocket。

v0.5.2 已完成：

- gRPC unary server 增加 observability interceptor，并由 `janus-api` 启动路径传入 tracing 配置。
- gRPC 入站 metadata 支持 W3C `traceparent` 和 `x-trace-id`；启用 tracing 时通过 response header 返回 `traceparent` / `x-trace-id`。
- gRPC request log 使用结构化 JSON，字段包含 `tenant_id`、`task_id`、`attempt`、`trace_id`、method、status、duration。
- 新增 gRPC request 指标：
- `janus_grpc_requests_total{method,status}`
- `janus_grpc_request_duration_seconds{method,status}`
- gRPC 指标只使用 method/status 低基数 label；tenant/task/attempt 只进入日志，不进入 Prometheus label。
- v0.5.7 已补齐 OpenTelemetry provider、HTTP/gRPC span lifecycle 和 OTLP exporter，当前 tracing 链路不再只是 metadata propagation。

v0.5.3 已完成：

- 新增 metrics collector，`janus-api` 在 metrics enabled 时后台刷新运维 gauge。
- outbox backlog 按 tenant/kind 聚合，统计 `pending`、`retry`、`publishing` 状态：
- `janus_outbox_backlog{tenant_id,kind}`
- mailbox backlog 通过 PostgreSQL batch snapshot 采集，并同步刷新兼容指标：
- `janus_mailbox_backlog{tenant_id,mailbox_id}`
- `janus_queue_backlog{tenant_id,mailbox_id}`
- online agent 数按 tenant 聚合：
- `janus_agent_online{tenant_id}`
- collector 先生成快照再替换 gauge，避免大规模 mailbox 采集期间 `/metrics` 出现短暂空样本窗口。

v0.5.4 已完成：

- 新增 internal structured log helper，后台组件输出 JSON log，`janus-api` 的 JSON writer 会保留字段。
- outbox publisher 错误日志结构化，包含 `component`、`worker_id`、`outbox_id`、`tenant_id`、`kind`、`dedupe_key`、`status`、`attempts`，task publish 后置事务失败时额外包含 `task_id`、`mailbox_id`、`attempt`。
- event projector 的 channel full / record failure 日志结构化，包含 event、tenant、task、trace、source/target agent 和 actor 字段。
- lease scanner、expiry scanner、heartbeat sweeper、metrics collector 的错误/关键状态日志结构化。
- coverage 门禁纳入 `server/internal/logutil` 与 `server/internal/metrics`，新增生产包不绕过 90% 覆盖率要求。

v0.5.5 已完成：

- 基础 Helm chart 更新到 Core RC 形态：
- Deployment / Service / ConfigMap / Secret / ServiceAccount / PodDisruptionBudget。
- startup/liveness/readiness probe。
- pod/container securityContext。
- resource requests/limits。
- Prometheus scrape annotations。
- optional TLS secret mount。
- local artifact `emptyDir` 或 PVC 挂载。
- Helm values 覆盖 Janus Core 已支持的 log、metrics、tracing、artifact、auth、TLS、PostgreSQL、NATS、Redis、migration 配置。
- `docs/Janus-ops-runbook.md` 补充 Helm install/upgrade/rollback、controlled migration、artifact PVC、Prometheus scrape、backup/restore 和 rolling upgrade 操作。
- 已通过 `helm lint deployments/helm/janus-core` 与 `helm template` 默认 / TLS+PVC 渲染验证。

v0.5.6 已完成：

- 新增 Grafana dashboard JSON：`deployments/grafana/dashboards/janus-core.json`。
- 新增 Grafana dashboard provisioning 示例：`deployments/grafana/provisioning/dashboards/janus-core.yaml`。
- Dashboard 使用 `DS_PROMETHEUS` datasource 变量，不绑定具体 Prometheus 实例名称。
- Dashboard 覆盖 outbox backlog、mailbox backlog、online agents、DLQ、HTTP/gRPC p95 latency、publish/pull p95 latency、task lifecycle rate、API error rate、policy/budget/routing blocks、top mailbox backlog。
- Dashboard JSON 已通过 `jq empty` 校验。

v0.5.7 已完成：

- `janus-api` 启动时根据 `observability.tracing` 初始化 W3C propagator、OpenTelemetry tracer provider 和 OTLP gRPC exporter。
- `JANUS_TRACING_INSECURE` / `observability.tracing.insecure` 控制 collector 明文/TLS 连接。
- HTTP middleware 创建 request span，提取入站 `traceparent`，记录 method、route、status、url path、tenant/task/attempt 属性，并继续返回 `Traceparent` / `X-Trace-ID`。
- gRPC unary interceptor 创建 call span，提取入站 metadata trace context，记录 method/status 与 tenant/task/attempt 属性，并继续返回 tracing response headers。
- `janus-api` 退出时执行 tracer provider shutdown hook，尽量 flush batch span。
- `docs/Janus-ops-runbook.md` 补充 OTLP tracing 环境变量和 Helm 配置。

v0.5 剩余：

- v0.5 Core Ops + Observability RC 功能项已完成；后续只保留 bugfix、文档校准和部署演练反馈。

退出标准：

- 单集群 Kubernetes 部署可复现。
- 滚动重启期间不丢任务。
- 指标能定位 NATS、PostgreSQL、Redis、outbox、mailbox 的主要故障。
- Dashboard 能解释任务为什么未被执行。

---

## 10. Milestone 5：Production Beta

目标：在真实但受控的生产链路中 dogfood。

推荐 dogfood 场景：

- PR review agent。
- CI failure triage agent。
- 自动修复 agent。
- 安全扫描 agent。
- 发布审批 agent。

必须完成：

- 7 天连续运行 soak test。
- chaos test：
- API restart。
- NATS restart。
- PostgreSQL failover simulation。
- Redis restart。
- Agent crash。
- slow consumer。
- 负载基线：
- 1k active agents。
- 10k mailboxes。
- 100 task/s publish。
- 500 event/s audit。
- p95 pull latency < 100ms。
- p95 publish accepted latency < 100ms。
- 安全基线：
- API key rotation。
- mTLS deployment。
- tenant guard tests。
- secret 不进入 Task Envelope 明文。

退出标准：

- dogfood 链路可以替代点对点 agent 调用。
- 故障恢复有可审计证据。
- 没有已知 P0。
- P1 都有明确 owner 和 release 目标。

v0.6.1 已完成：

- 新增 `server/tests/productionbeta` fast profile，作为 Production Beta 前置门禁。
- fast profile 覆盖可配置 agent/mailbox/task 规模、并发 worker、agent crash 后 lease expiry/retry/requeue、slow consumer 延迟路径和 publish/pull p95 预算。
- 新增 `make beta-fast`，默认运行 `JANUS_BETA_PROFILE=fast go test ./server/tests/productionbeta`，可纳入常规开发验证。
- 新增 `make beta-soak`，显式运行 `JANUS_BETA_PROFILE=soak JANUS_RUN_LONG_SOAK=true`；默认不在 `make verify` 中执行，避免把 7 天 soak 混入快速门禁。
- 当前 v0.6.1 仍是内存驱动前置验证，不替代真实 Kubernetes、NATS、PostgreSQL、Redis、OTLP collector 的 Production Beta 演练。

v0.6 剩余：

- 构建真实集群 beta environment：Janus API、NATS JetStream、PostgreSQL、Redis、OpenTelemetry Collector、Prometheus、Grafana。
- 把 fast profile 的 agent crash / slow consumer 场景迁移到真实环境 smoke test。
- 增加 API restart、NATS restart、PostgreSQL failover simulation、Redis restart 的自动化 chaos 脚本。
- 生成 1k agent / 10k mailbox / 100 task/s publish / 500 event/s audit 的负载基线报告。
- 执行并记录 7 天连续 soak test。
- 补齐 API key rotation、mTLS deployment、tenant guard、secret redaction 的生产安全演练。

v0.6.2 已完成：

- 新增 `make smoke-deps` 和 `scripts/smoke_api_dependencies.sh`，面向当前收敛范围只验证 API / PostgreSQL / NATS / Redis。
- smoke 脚本默认关闭 metrics 和 tracing：`JANUS_METRICS_ENABLED=false`、`JANUS_TRACING_ENABLED=false`，不测试 OTLP、Prometheus、Grafana。
- smoke 脚本启动真实 `janus-api` 进程，检查 `/healthz` 和 `/readyz`，要求 `postgres`、`nats`、`redis` 三项依赖均为 `ok`。
- smoke 脚本执行一条真实 HTTP API 生命周期：创建 tenant、注册 agent、创建 mailbox、publish task、从 NATS-backed mailbox pull、start、ack，并从 PostgreSQL 验证 task completed/result_ref。
- 本地启动模式增加依赖 preflight；PostgreSQL、NATS、Redis 端口不可连时快速失败并提示需要设置 `JANUS_PG_*`、`JANUS_NATS_URL`、`JANUS_REDIS_ADDR` 或先启动 docker compose。

v0.6.2 实跑确认：

- 已通过 v0.6.3 的 `make smoke-prod` 覆盖确认：`/readyz` 返回 `postgres`、`nats`、`redis` 均为 `ok`，并完成真实 task 生命周期。

v0.6.3 已完成：

- 扩展 `deployments/smoke-deps.compose.yaml` 为生产 smoke stack，包含 PostgreSQL、NATS JetStream、Redis、Prometheus、Grafana、Tempo、OpenTelemetry Collector。
- 新增 Prometheus scrape 配置：`deployments/prometheus/janus-smoke.yml`，默认抓取 `host.containers.internal:18080/metrics`。
- 新增 Grafana datasource provisioning：`deployments/grafana/provisioning/datasources/janus-smoke.yaml`，配置 Prometheus 和 Tempo datasource，并复用 Janus Core dashboard provisioning。
- 新增 OTel Collector 配置：`deployments/otel/collector-smoke.yaml`，接收 OTLP gRPC/HTTP 并转发到 Tempo。
- 新增 Tempo smoke 配置：`deployments/tempo/tempo-smoke.yaml`。
- 新增 `make smoke-prod`，在 API/NATS/PostgreSQL/Redis 生命周期验证之外，同时验证 `/metrics`、Prometheus target/query、Grafana health/dashboard provisioning、Tempo readiness 和 OTLP collector 端口。

v0.6.3 实跑确认：

- 用户通过 Podman 启动 `deployments/smoke-deps.compose.yaml` 后，`make smoke-prod` 已通过。
- 验证结果包含：API readiness、PostgreSQL/NATS/Redis 依赖健康、真实 task publish/pull/start/ack/completed 生命周期、trace headers、`/metrics`、Prometheus target/query、Grafana health/dashboard provisioning、Tempo readiness、OTLP collector 端口可达。
- 追加 `make smoke-7-agents` 真实场景验证，覆盖 7 个 Agent、7 个 NATS-backed mailbox、artifact ContextRef、capability lookup、fan-out/fan-in、7 个 task 完整生命周期、audit trace events、metrics/Prometheus/Grafana/Tempo。
- `make smoke-7-agents` 已加入异常路径：重复 idempotency 提交不重复投递、错误 lease ACK 被拒绝、non-retriable NACK 进入 DLQ 且可查询。
- `make smoke-7-agents` 已扩展治理/数据面检查：mailbox pause/resume、artifact download + SHA-256 header、ContextRef lookup、task ContextRef binding query。
- 7-agent 场景暴露并修复 outbox ID 同毫秒冲突问题：outbox ID 生成改为毫秒前缀加随机后缀，并新增 tight-loop 唯一性回归测试。

v0.6.4 已完成：

- 新增 `make smoke-security` 和 `scripts/smoke_core_security.sh`。
- security smoke 使用同一 Podman 依赖栈，启动 auth-enabled `janus-api`，默认监听 `18081/19091`，避免与 `smoke-prod` 端口冲突。
- 覆盖 missing/invalid API key 标准 `UNAUTHENTICATED` envelope、bootstrap API key、API key create/list/revoke、API key rotation、revoked key rejection、HTTP tenant guard 和 raw API key log redaction。
- `make smoke-security` 已实跑通过，依赖健康为 PostgreSQL/NATS/Redis 全部 `ok`。

v0.6.5 已完成：

- 新增 native gRPC security interceptor，在 auth-enabled 模式下从 metadata 读取 `x-api-key` / `authorization: Bearer`，复用 API key validator，并通过 proto reflection 校验请求 `tenant_id` 与认证 tenant 一致。
- 新增 grpc-gateway auth header matcher，确保 `/grpc` 入口把 `X-API-Key` 和 `Authorization` 传递给 native gRPC，避免 gateway 绕过或被 native gRPC 误拒。
- A2A、ACP、MCP gateway 在 auth-enabled 上下文中强制 query/body tenant 与 API key tenant 一致；WebSocket 在认证模式下默认订阅认证 tenant，并拒绝跨 tenant 订阅。
- 新增 `scripts/security_grpc_probe.go`，用于真实 gRPC 进程验证 missing/invalid/revoked API key、cross-tenant deny 和 valid key success。
- 扩展 `make smoke-security`：除 HTTP API key/tenant guard 外，新增 native gRPC、A2A、ACP、MCP、WebSocket tenant guard 和 ContextRef cross-tenant deny 验证。
- 扩展 `make smoke-security`：新增 artifact upload/download + cross-tenant artifact URI deny 验证。
- 扩展 `make smoke-security`：构造含敏感字符串的 Task Envelope，并验证 payload/context ref 内容不进入 API log。
- 新增 `scripts/smoke_core_mtls.sh` 和 `make smoke-mtls`，动态生成 CA/server/client cert，验证 HTTP/gRPC TLS+mTLS、client cert required、grpc-gateway over mTLS、native gRPC over mTLS。
- 新增 `make verify-security`，串联 `smoke-security` 与 `smoke-mtls`。
- 新增 `scripts/check_ga_readiness.py`、`make ga-readiness` 和 `make verify-production` 骨架；只要 Capability Matrix 中仍有 P0 非 `Covered`，GA readiness 会 fail-fast，防止误宣称 GA。
- `make verify-security` 已实跑通过；`make ga-readiness` 当前按预期失败，剩余 P0 blocker 继续由后续 governance/protocol/reliability/ops/SDK gates 收敛。

v0.6.6 已完成：

- 新增 `scripts/protocol_grpc_probe.go` 和 `scripts/smoke_core_protocol.sh`。
- 新增 `make smoke-protocol` 和 `make verify-protocol`，并把 `verify-protocol` 接入 `make verify-production`。
- protocol smoke 使用同一 Podman 依赖栈，启动独立 `janus-api`，默认监听 `18083/19093`，覆盖 native gRPC、grpc-gateway 与 A2A Agent、Mailbox、Task、Dispatch、Audit 和 DLQ lifecycle。
- native gRPC smoke 覆盖 agent register/list/heartbeat、mailbox create/get、task create、pull/start/ack/completed、Audit task/trace query、non-retriable nack 和 DLQ query。
- grpc-gateway smoke 覆盖同一 lifecycle，包含 gateway agent/mailbox/task JSON contract、pull/start/heartbeat/ack、Audit task/trace query、non-retriable nack 和 DLQ query。
- A2A smoke 覆盖 Agent Card 注册、JSON-RPC `message/send`、REST status、JSON-RPC `tasks/get`、标准 dispatch 完成和 Audit task/trace query，并验证 policy/budget/ContextRef/trace 不被 A2A gateway 丢弃。
- protocol smoke 暴露并修复 gRPC `CreateTask` envelope 转换缺陷：proto `trace`、`policy`、`context_refs` 现在会映射到 Core envelope；Core envelope 转回 proto 时也保留这些治理和追踪字段。
- protocol smoke 暴露并修复 outbox completed event 缺少 trace/source agent 的审计缺陷；`MarkTaskCompleted` 现在生成可按 trace 查询的 completed event。
- `make smoke-protocol` 已实跑通过，依赖健康为 PostgreSQL/NATS/Redis 全部 `ok`。

v0.6.7 已完成：

- 新增 Policy Rule HTTP 管理 API：`POST/GET /v1/tenants/{tenant_id}/policy-rules`，用于 Core 基础 policy deny / approval_required 规则管理。
- 新增 Budget HTTP 管理 API：`POST/GET /v1/tenants/{tenant_id}/budgets` 和 `GET /v1/tenants/{tenant_id}/budgets/{scope_type}/{scope_id}`，用于 tenant/agent/team/model/task scope 的预算配置。
- Go/Python/TypeScript SDK 新增 Policy Rule 和 Budget 管理方法，并纳入共享 `sdk/conformance/http_cases.json`；fixture 版本推进到 `v0.3.10`。
- `scripts/check_api_contract.py` 将 5 条治理管理路由登记为显式 HTTP-only stable route，`docs/Janus-api-surface-audit.md` 和 `docs/Janus-http-gateway-migration-inventory.md` 同步记录 v0.4 proto/gateway 迁移约束。
- 新增 `scripts/smoke_core_sdk_cli.sh`、`make smoke-sdk-cli` 和 `make verify-sdk-cli`，并把 `verify-sdk-cli` 接入 `make verify-production`。
- SDK/CLI smoke 在 auth-enabled API 上验证 Go/Python/TypeScript SDK typed error、worker lifecycle、Policy Rule / Budget SDK 方法，以及 CLI agent/task/mailbox/DLQ/api-key/global `--api-key`、API key revoke 和 tenant guard。
- 新增 `scripts/smoke_core_governance.sh`、`make smoke-governance` 和 `make verify-governance`，并把 `verify-governance` 接入 `make verify-production`。
- governance smoke 使用同一 Podman 依赖栈，启动独立 `janus-api`，默认监听 `18084/19094`，覆盖 Policy Rule / Budget 管理 API、policy publish deny、policy route + data classification deny、approval_required -> approve -> dispatch -> ack、Redis-backed TPM throttle。
- governance smoke 暴露并修复 pending approval nullable 字段扫描缺陷：`approver`、`reason`、`decision` 为 NULL 时不再导致 approval list/get 500，并新增 PostgreSQL repo 回归测试。
- governance smoke 暴露并修复 approval outbox 投递竞态：task_publish 可能先到 NATS、任务状态稍后才变为 queued，消费者现在会 NACK `approval_pending` 投递等待 outbox 收敛，避免 ACK 掉有效消息。
- `make verify-governance` 已实跑通过，依赖健康为 PostgreSQL/NATS/Redis 全部 `ok`。

v0.6.8 已完成：

- 新增 `scripts/smoke_core_reliability.sh`、`make smoke-reliability` 和 `make verify-reliability`，并把 `verify-reliability` 接入 `make verify-production`。
- reliability smoke 在 PostgreSQL/NATS/Redis 真实依赖上覆盖 mailbox pause/resume/backlog、pull/start/heartbeat/ACK、重复 ACK 幂等、budget ledger dedupe、retriable NACK retry、retry exhausted DLQ、DLQ replay/discard 幂等、lease timeout recovery。
- reliability smoke 暴露并修复 retry outbox 投递竞态：下一 attempt 的 task_publish 可能先到 NATS、任务仍是 `retry_scheduled`，消费者现在会 NACK 下一 attempt 投递等待 outbox 收敛；旧 attempt delivery 仍 ACK 清理。
- reliability smoke 暴露并修复 API/DB 时钟偏移问题：heartbeat 不再把已有更晚的 lease 截短，retry outbox 的 `next_attempt_at` 改为按数据库 `now() + delay` 写入，避免 DB 时间落后时 retry 被延迟数十分钟。
- `make verify-reliability` 已实跑通过，依赖健康为 PostgreSQL/NATS/Redis 全部 `ok`。

v0.6.9 已完成：

- 扩展 `scripts/smoke_core_governance.sh`，同一真实依赖 gate 现在覆盖 approval approve/reject/expire、offline agent rejection、capacity rejection、max-cost budget rejection、model-class candidate rejection、route explanation audit、ContextRef attach/get/bind/list/detach/delete/cross-tenant deny、ContextRef-derived classification route filtering、artifact upload/download 和 API restart 后 artifact persistence。
- capability routing 新增 model-class 硬过滤：任务 budget 声明 `model_classes` 且候选 capability schema 明确声明不匹配 model class 时，该候选会被拒绝并写入 `rejected_model_class` audit evidence；未声明 model class 的既有 capability 保持兼容。
- capability routing 失败的 `routing.failed` audit payload 新增 candidate rejection counters，capacity、budget、policy、model_class 和 data_classification 拒绝原因可通过 Audit REST API 断言。
- ContextRef detach 改为 zero-row delete 返回 `NOT_FOUND`，跨 tenant detach 不再被误判为成功；新增 PostgreSQL repo 回归测试。
- `make verify-governance` 已再次实跑通过，依赖健康为 PostgreSQL/NATS/Redis 全部 `ok`，并包含 API restart artifact persistence check。
- `make ga-readiness` 当前剩余 P0 blocker 降至 29 个；治理 P0（GOV-02/03/04/06/07/09/10/11/12）已从 Partial 收敛到 Covered。

v0.6.10 已完成：

- 扩展 `scripts/smoke_core_protocol.sh`，`make verify-protocol` 现在覆盖 native gRPC、grpc-gateway、A2A、ACP、MCP 和 WebSocket 的真实 API 进程端到端验证。
- ACP smoke 覆盖 manifest 注册、run create/status、错误 envelope 和 trace preservation。
- MCP smoke 覆盖 resource -> ContextRef 注册、tool call create/status、错误 envelope 和 trace preservation。
- WebSocket smoke 新增 `scripts/protocol_ws_probe.go`，连接 `/ws?tenant=...` 并验证 NATS-backed `task.completed` event 能携带目标 task 和 trace。
- `make verify-protocol` 已实跑通过，依赖健康为 PostgreSQL/NATS/Redis 全部 `ok`。
- `make ga-readiness` 中协议 P0（PROTO-02/05/06）已从 Partial 收敛到 Covered；PROTO-11 仍等待 OTLP trace id 与 Audit REST API 关联查询证据。

v0.6.11 已完成：

- 扩展 `scripts/smoke_api_dependencies.sh` 的观测性验证，`make smoke-prod` 现在覆盖 smoke tenant 的 `janus_tasks_created_total`、`janus_pull_requests_total` 和 `janus_ack_total` Prometheus lifecycle metrics。
- `make smoke-prod` 新增 Audit REST trace correlation：同一 completed task 必须能通过 `/tasks/{task_id}/events` 和 `/traces/{trace_id}` 查询到，且 trace id 与任务 envelope/`traceparent` 一致。
- `make smoke-prod` 新增 Tempo `/api/traces/{traceID}` 查询；脚本兼容 Tempo OTLP JSON 中 base64 形式的 `traceId`，避免把编码差异误判为 trace 丢失。
- `make smoke-prod` 新增 tenant-scoped 404 异常请求的结构化 JSON 日志检查，要求日志包含 tenant 和 trace 字段。
- 修复 Audit trace handler 的 trace id path 解析，并为 EventService 增加按 tenant events 回退过滤的回归测试，避免 trace projection 查询暂时为空时误报。
- `make smoke-prod` 已实跑通过，依赖健康为 PostgreSQL/NATS/Redis 全部 `ok`，Prometheus/Grafana/Tempo/OTLP Collector 均参与验证。
- Capability Matrix 中 PROTO-11 和 OPS-03 已从 Partial 收敛到 Covered；OPS-02/OPS-04 仍等待异常/治理指标和后台异常日志的完整真实依赖验证。

v0.6.12 已完成：

- 新增 Core security audit event 类型：`security.api_key_created`、`security.api_key_revoked`、`security.auth_failed`、`security.tenant_guard_denied`。
- auth middleware 和 tenant guard 新增可插拔审计接口；auth-enabled API 在 tenant-scoped missing/invalid key 和跨 tenant deny 时会写入 Janus audit projection。
- API key service 在 create/revoke 成功后写入安全审计事件，payload 只包含 key id/name/prefix，不暴露 raw secret 或 hash。
- 扩展 `scripts/smoke_core_security.sh`：`make verify-security` 现在通过 Audit REST 断言 API key rotation、auth failure、tenant guard deny 的 security audit events。
- `make verify-security` 已实跑通过，依赖健康为 PostgreSQL/NATS/Redis 全部 `ok`，并继续覆盖 mTLS/native gRPC/security gateway 边界。
- Capability Matrix 中 REL-01 和 SEC-09 已从 Partial 收敛到 Covered；SEC-08 仍等待底层 NATS/Redis boundary negative case 和更深 PostgreSQL isolation chaos。

v0.6.13 已完成：

- 新增 `scripts/smoke_core_ops_chaos.sh`、`make smoke-ops-chaos` 和 `make verify-ops-chaos`，并接入 `make verify-production`。
- ops chaos gate 会自启动本地 `janus-api`，并通过 Podman 停启 Redis、NATS、PostgreSQL 容器验证 readiness 降级和恢复。
- Redis restart 验证确认 durable agent registry 不依赖 Redis，重启后通过 heartbeat 恢复 tenant-scoped heartbeat key，并检查其他 tenant 不出现同名 heartbeat key。
- NATS outage 验证确认 task publish 在 NATS 不可用时仍返回 accepted，任务通过 transactional outbox 留待恢复后投递。
- NATS + API restart 验证确认 Janus 从 PostgreSQL tenant 状态重新校验 NATS tenant streams；mailbox consumer 在首次 pull 时 lazy ensure，outbox retry 恢复为 published，任务最终可 pull/start/ack completed。
- PostgreSQL restart 验证确认 `/healthz` 与 `/readyz` 分离，PostgreSQL stop/start 后 API readiness 可恢复。
- 扩展 `make verify-ops-chaos`：启动第二个本地 API 进程，验证双 API / 双 outbox worker 可并发发布并 exactly-once drain 一批任务。
- 扩展 `make verify-ops-chaos`：停止一个 API 进程时另一个 API 保持 ready，可继续 publish/complete，并在主 API 启回后确认 outbox backlog 清零。
- `make verify-ops-chaos` 已实跑通过；Capability Matrix 中 REL-02、REL-04、REL-05、REL-13、REL-19、REL-20、SEC-08、OPS-01、OPS-10、OPS-11、OPS-12 已从 Partial/Missing test 收敛到 Covered。

v0.6.14 已完成：

- 新增 `server/cmd/janus-event-replay-probe`，作为发布验证工具从 NATS EVENTS stream 重放指定 task 的事件并重建 PostgreSQL audit projection。
- 扩展 `scripts/smoke_core_reliability.sh`：增加 inflight duplicate no-redelivery、old attempt rejection、outbox post-mark-failure retry recovery、event replay projection rebuild。
- outbox post-mark-failure recovery 通过强制将已发布的 `task_publish` outbox row 改回 `retry` 来模拟 “NATS publish 成功但 DB mark failure”，随后验证 NATS dedupe 下只投递一次。
- event replay probe 会删除 smoke task 的 audit projection，再从 NATS event stream 重放并写回，验证 task events 可重新查询。
- `make verify-reliability` 已实跑通过，依赖健康为 PostgreSQL/NATS/Redis 全部 `ok`。
- Capability Matrix 中 REL-12、REL-14、REL-17 已从 Partial 收敛到 Covered。

v0.6.15 已完成：

- TaskService policy deny 路径新增 `janus_policy_denied_total` 计数；BudgetService throttle 路径新增 `janus_budget_throttle_total` 计数，避免治理指标只在 DispatchService 分支中出现。
- `make verify-governance` 默认启用 metrics，并断言 policy denied、budget throttle、routing failed、routing selected 的 Prometheus samples。
- `make verify-reliability` 默认启用 metrics，并断言 pull/ACK/NACK/retry/DLQ/lease counters，以及 outbox/mailbox/queue backlog gauges 暴露。
- `make verify-ops-chaos` 新增 outbox worker 结构化异常日志断言，NATS outage 必须产生包含 component、tenant、kind、error 字段的 `outbox_publish_failed` JSON log。
- `make verify-governance`、`make verify-reliability`、`make verify-ops-chaos` 均已实跑通过。
- Capability Matrix 中 OPS-02 和 OPS-04 已从 Partial 收敛到 Covered。

v0.6.16 已完成：

- 新增 `server/cmd/janus-migration-probe`，用于受控验证 PostgreSQL migration `up -> no-change -> down one -> up one -> no-change`，并输出 JSON 证据。
- 新增 `server/cmd/janus-nats-persistence-probe`，用于验证 NATS JetStream file-storage 写入、重启后读回，以及 release-load 前缀 stream 清理。
- Helm chart 新增 `migrationJob` 模板；生产 rollout 可设置 `config.migration.auto=false`，先由 Helm pre-install/pre-upgrade Job 执行 migration，再滚动 API。
- Dockerfile 同时打包 `janus-api`、`janus-migration-probe`、`janus-nats-persistence-probe`，确保 chart Job 可复用同一镜像。
- Mailbox 创建和 API 启动 bootstrap 改为只初始化 tenant-level queue resources；mailbox DLQ stream 和 consumer 在首次 pull 时懒加载，避免 10k mailbox 控制面预置压垮 NATS。
- 新增 `scripts/smoke_core_release_ops.sh`、`make smoke-release-ops` 和 `make verify-release-ops`，并接入 `make verify-production`。
- `make verify-release-ops` 覆盖 Helm lint/template、controlled migration rollback drill、PostgreSQL dump/restore sentinel、NATS file-storage restart persistence、release load baseline、soak profile preflight。
- GA target load run 已实跑通过：1000 agents、10000 mailboxes、1000 tasks、publish p95 23ms、pull p95 75ms、publish 2048/s、estimated events 1872/s。
- Capability Matrix 中 REL-18、OPS-07、OPS-08、OPS-09、OPS-13、OPS-14 已从 Partial/Missing test 收敛到 Covered；真实 Kubernetes install/rollback 和 7 天 wall-clock soak 作为 release artifact 归档要求保留。

v0.6.17 已完成：

- 新增 `migrations/000014_mailbox_backlog_index`，为 `tasks(tenant_id, mailbox_id, status)` 的 queued/retry backlog 查询建立部分索引。
- `MailboxRepository.BacklogSnapshot` 使用单次 SQL 聚合返回所有 mailbox backlog，包括零 backlog mailbox。
- metrics collector 优先使用 batch snapshot，并在本地快照完成后一次性刷新 `janus_mailbox_backlog` / `janus_queue_backlog`，避免 10k mailbox 场景下逐邮箱查询和先 reset 后填充造成的 metrics 空窗。
- `OutboxRepo.FetchPending` 使用 PostgreSQL `now() + duration` 计算 `publishing` lease，并用 `locked_at + current_lease_duration <= now()` 回收旧版本遗留的 clock-skewed `publishing` row。
- `make verify-reliability` 的 backlog metrics 断言改为插入真实 future-scheduled outbox sentinel，再等待 collector 暴露 `janus_outbox_backlog{kind="metrics_probe"}`；mailbox/queue gauge 断言具体 mailbox 标签。
- simulation 测试 fixture 显式创建 active mailbox，以匹配 lazy mailbox consumer bootstrap 的生产前置条件。
- 新增 GOV-13 前的基线验证：`make verify-production` 已完整通过；其中 Core coverage 为 90.3%，release ops 最终 migration version 为 14，默认负载 publish p95 61ms、pull p95 104ms、estimated event rate 1766/s。将 Intent Resolver 纳入 Core GA 后，该结果只作为既有能力回归基线，不再代表扩展后 GA readiness。

---

## 11. Milestone 6：Core v1.0 GA

Core v1.0 GA 的正式发布门禁以 [Janus Core Capability Matrix](./Janus-core-capability-matrix.md) 为准。所有 `P0` Core 能力必须达到 `Covered`；任一 `P0` 仍为 `Partial`、`Missing test` 或 `Not implemented` 时，Janus Core 不能标记为生产级发布。`P1` 若未完全覆盖，必须有明确风险接受、owner 和下一版本目标。

v1.0 Core GA 标准：

- 可靠性：
- publish 不丢。
- Agent 离线不丢。
- ACK/NACK 幂等。
- retry/DLQ 可恢复。
- API 多实例可运行。
- 契约：
- Task Envelope spec 稳定。
- proto/HTTP/SDK 字段一致。
- Go/Python/TypeScript SDK 可用。
- 运维：
- Docker Compose、Helm、migration、backup/restore 文档齐全。
- metrics/traces/logs 能解释主要故障。
- 安全：
- API key 和 mTLS 可用。
- tenant_id 逻辑隔离强制执行。
- 基础 policy/audit 可用。
- 生态：
- A2A 基础互通可用。
- ACP/MCP 基础 adapter 可用。
- Natural language intent resolver：~~可将自然语言请求解析为 capability target（例如“我想审查这段代码” -> `code_review`）并通过同一 policy/budget/capacity/classification 路由链投递到合格 Agent~~。**推迟至 v1.1**——见第 12 节：代码存在但未在生产装配中接线，`AgentLookup.ListOnlineAgents` 亦无实现，因此该能力在 GA 时实际不可用。GA 退守为：MCP/ACP 网关对省略 target 的请求返回明确的 `target required` 错误。
- 至少一个真实 CI/DevOps demo 通过 Janus 运行。

---

v0.6.18 完成：

> ⚠️ **本段描述超前于代码实际状态，已于 v1.0 GA 后修订。** 下列各项原计划在 v0.6.18 落地，但 `server/internal/service/intent/` 代码在生产装配（`server/cmd/janus-api/main.go`）中从未被接线（`WithIntentResolver` 零调用），其依赖 `AgentLookup.ListOnlineAgents` 也无任何实现，因此 **GA 时这些能力实际不可用**。该需求已重新立为 **v1.1 交付项（catalog-first 方案）**，详见第 12 节。以下原文仅作历史记录保留。

- Intent Resolver 已纳入 Core GA P0 范围：支持 `target.type=intent`，可将自然语言请求解析为唯一 capability target。
- 解析依据来自 tenant 内 online Agent 声明的 capability 名称、alias、description、schema hints、payload/content tokens、ContextRef metadata 和 policy hints；输出写入 resolved capability、confidence、reason 与候选摘要。
- 低置信度、无匹配或候选歧义时拒绝创建任务，写入 `routing.failed` 并暴露 routing failure metrics；不会默认投递到任意 mailbox。
- 成功 intent 解析后复用现有 `capability` 路由：online agent、active mailbox、policy、budget、capacity、data classification、semantic score 和 backlog 排序全部照常执行。
- 验证已覆盖：service/unit 覆盖匹配、无匹配、歧义；`make smoke-7-agents` 真实依赖场景验证“我想审查这段代码”投递到 Code Review Agent mailbox，检查持久化 task 被改写为 `target_type=capability,target_value=code_review`，并覆盖 ambiguous/unmatched intent 拒绝。

## 12. v1.1：Intent Resolution（catalog-first）

> **对上文的修订**：第 11 节 GA 标准与“v0.6.18 完成”段中关于 Intent Resolver“已纳入 Core GA P0 / 已完成”的描述**超前于代码实际状态**。`server/internal/service/intent/` 代码确实存在，但 `WithIntentResolver` 在生产装配（`server/cmd/janus-api/main.go`）中从未调用，其依赖 `AgentLookup.ListOnlineAgents` 也无任何实现；MCP/ACP 网关对省略 target 的请求发出 `target_type=intent`，但这些任务实际落到“target value is required”被拒绝。本节将其重新立为 v1.1 交付项，并采用更低风险的方案。

### 背景与风险

- **为什么不直接接活现有 keyword resolver**：现有打分器（`resolver.go`）以 `payload.Content` 对 capability 描述做匹配，接受阈值低（0.3）。在多租户 + 数据分级策略 + 审计日志的系统里，弱匹配可能**静默误路由到“策略允许但错误”的 agent**——比当前的明确拒绝更糟，且属于安全/审计问题，不是 UX 问题。Router 的 data-class 过滤只能剔除“不能处理”的 agent，抓不住“能处理但答非所问”的 agent。
- **为什么不把 LLM 放进同步 `Create` 路径**：`Create` 同时承担 policy 检查与 outbox 事务，同步塞 LLM 会引入延迟、失败传导与非确定性，侵蚀该路径确定性、可审计的核心属性。`go.mod` 当前零 LLM 依赖并非偶然。

### v1.1 范围（分阶段）

1. **Catalog 端点（先做，零 Create 路径风险）**：新增只读 `GET /v1/tenants/{tenantID}/catalog`，返回租户内在线 agent 及其 capability（名称、描述、schema）。无新依赖，不改任务创建。使调用方能用自有模型自行完成 NL→capability 解析。
2. **网关语义清晰化**：将省略 target 时的默认行为从静默 `target_type=intent` 改为明确的 `400 target required; call GET /catalog`。
3. **Advisory 解析端点（仅在度量出真实需求时）**：若第 1 步后仍存在无法自解析的轻量调用方，新增无状态的 `POST /v1/tenants/{tenantID}/intents/resolve` 咨询接口（调用方传入 payload，拿回建议 capability，再正常发布 capability 任务）。LLM 依赖隔离于此，配 per-tenant 限流与成本预算，返回值须校验在当前在线 catalog 内。
4. **v1.1 不做**：同步 LLM 进 `Create`（否决）；异步 `resolving` 任务状态 + 后台 worker（否决，对本代码库的生命周期/审计模型而言过度设计）。

**Owner**：TBD。**依赖**：不依赖其他 v1.1 项。

---

## 13. Enterprise 启动条件

Janus-enterprise 不应早于 Core Reliability Alpha。建议在以下条件满足后启动 Enterprise track：

- Core 的 outbox/ACK/NACK/retry/DLQ/lease timeout 故障测试全部通过。
- API/SDK attempt 契约稳定。
- tenant_id 逻辑隔离和 audit trace 已经贯穿核心链路。
- Helm 单集群部署可用。

Enterprise 第一阶段只做最小商业闭环：

- OIDC/SSO。
- RBAC。
- per-tenant KMS key。
- audit export。
- DLP hook 实现。
- cost center UI。

不要在 Enterprise 中复制 Core 可靠性逻辑。