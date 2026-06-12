# Janus 复查：设计偏差与代码实现问题

审查日期：2026-06-12

参考设计文档：`Janus-detail-design.md`

本次复查目标：

1. 检查当前代码是否偏离详细设计。
2. 检查代码实现是否存在正确性、可靠性、安全性和可维护性问题。

优先级说明：

- P0：阻断编译、启动、核心主流程或租户隔离的严重问题。
- P1：核心设计能力缺失、行为不一致或可靠性风险。
- P2：测试、文档、可观测性和工程质量问题。

## 一、当前验证结果

执行过的命令：

```bash
go test ./...
```

结果：

- 失败，原因是仓库根目录不是 `go.work` 中列出的 module，不能用根目录 `./...` 作为测试入口。

执行过的命令：

```bash
go test ./core/... ./server/... ./cli/... ./sdk/go/... ./demo/... ./proto/...
```

结果：失败。主要失败原因如下：

- `proto/gen/janus/v1/*.pb.go` 引用了 `github.com/agentium-lab/Janus/proto/gen/google/api`，但仓库没有生成或提交该包。
- `server/internal/driver/redis/driver_test.go` 仍引用旧的 `heartbeatKey`、`extractAgentID` 函数，当前 Redis driver 已改为 sorted set 实现，测试没有同步更新。
- NATS driver 测试硬编码启动 `$HOME/go/bin/nats-server`，当前机器没有该二进制。
- Postgres repo 和 e2e 测试默认依赖本机 PostgreSQL，当前环境无法连接。

已经改善的点：

- `server/internal/config/config.go` 已使用 viper，并设置 `grpc_port` 默认值 `9090`。
- `server/cmd/janus-api/main.go` 已将鉴权包装在 combined handler 上，不再只包装手写 mux。
- grpc-gateway 不再抢占 `/v1/*` 手写 REST 路由，而是挂到 `/grpc/`。
- `TaskService.Create` 已在成功路径返回 task，不再是成功返回 nil。
- `PullTaskRequest` 已包含 `agent_id`，gRPC handler 也已传入该字段。
- `OutboxRepo` 已支持 `pending/retry` 扫描和 `next_attempt_at`。
- `EventRepo` 已实现按 tenant/task/trace 查询。
- Redis heartbeat 已从 TTL key 改为 sorted set，方向上解决了过期 key 被 Redis 删除后无法扫描的问题。

## 二、当前版本与设计文档的偏差

### P0：proto / grpc-gateway 生成链路仍然不可用

证据：

- `proto/gen/janus/v1/agent.pb.go:10`、`dispatch.pb.go:10`、`task.pb.go:10`、`audit.pb.go:10` 引用 `github.com/agentium-lab/Janus/proto/gen/google/api`。
- `proto/gen/` 目录下没有 `google/api` 包。
- `proto/buf.gen.yaml` 配置了 `protoc-gen-grpc-gateway`。
- `server/internal/grpc/gateway.go` 调用 `pb.RegisterAgentServiceHandlerFromEndpoint` 等 gateway 注册函数。
- `proto/gen/` 目录下没有任何 `.pb.gw.go` 文件。

与设计偏差：

- 设计要求 HTTP + gRPC 双协议通过 grpc-gateway 由同一份 proto 暴露。
- 当前生成产物不完整，proto module 不能编译，gateway 注册函数也没有对应生成文件。

建议：

- 修复 `buf.gen.yaml` 的 managed `go_package_prefix` 对 `google/api` 的影响。
- 明确 `google/api` 是使用外部 `google.golang.org/genproto`，还是生成并提交到 `proto/gen/google/api`。
- 重新生成并提交 `.pb.go`、`_grpc.pb.go`、`.pb.gw.go`。
- CI 增加 proto generate 校验，防止 proto 源文件和生成产物漂移。

### P1：HTTP REST、gRPC、gateway 三套入口行为不一致

证据：

- 设计文档 `30.2 gRPC Service 定义` 说明 HTTP + gRPC 通过 `grpc-gateway` 一份 proto 生成两套 API。
- 当前生产 HTTP `/v1/*` 主要由手写 handler 提供。
- grpc-gateway 被挂载到 `/grpc/`，不是 proto annotation 里声明的 `/v1/*`。
- `server/internal/handler/task_handler.go` 的 CreateTask 请求 shape 是顶层 `id/source_agent/target_type/target_value/mailbox_id/envelope`。
- `proto/janus/v1/task.proto` 的 CreateTask 请求 shape 是 `tenant_id + envelope`。
- HTTP handler 会把 `target_type=mailbox` 时的 `target_value` 映射到 `MailboxID`。
- `server/internal/grpc/convert.go` 的 `createTaskReqToCore` 不会把 envelope target mailbox 映射到 `Task.MailboxID`，所以通过 gRPC 创建 mailbox 任务不会自动入队。

影响：

- SDK、HTTP、gRPC 之间不是同一契约。
- 同一个业务请求通过不同入口会得到不同调度结果。

建议：

- 选定唯一外部 API 契约。优先以 proto + gateway 为准。
- 如果保留手写 REST，应明确它是兼容层，并加契约测试保证与 proto 语义一致。
- 修复 gRPC 创建任务时 mailbox target 不入队的问题。
- HTTP handler 应完整解析 proto envelope 字段，而不是只解析部分字段。

### P1：Task 创建 API 丢失设计字段，响应状态也不准确

证据：

- `server/internal/handler/task_handler.go` 只解析 envelope 的 `janus_version/task_id/tenant_id/source_agent/target/payload/trace.trace_id`。
- 设计 envelope 中的 `idempotency_key/priority/deadline/ttl_seconds/budget/policy/context_refs/parent_task_id/span_id` 没有完整进入 `core.TaskEnvelope`。
- 创建成功后 handler 固定返回 `{"status":"created"}`，即使数据库里已经被 `TaskService.createWithOutbox` 更新为 `queued`，或策略要求变为 `approval_pending`。
- `TaskService.createWithOutbox` 更新 DB 状态为 `queued` 后返回的 `&task` 仍是旧状态。

与设计偏差：

- 设计把 Task Envelope 作为跨 Agent 的标准任务语义，预算、策略、上下文引用、trace 都是核心字段。

建议：

- HTTP 层直接使用与 proto 一致的 TaskEnvelope DTO。
- CreateTask 返回持久化后的真实 task 状态。
- `createWithOutbox` 在状态更新后同步更新内存对象，或从 repo 重新读取后返回。

### P0：审批流不能闭环

证据：

- `server/internal/service/task_service.go` 在策略返回 `approval_required` 时只把 task 置为 `approval_pending`。
- 该流程不会自动调用 `ApprovalService.RequestApproval`，也不会创建 `approvals` 表记录。
- `server/cmd/janus-api/main.go` 只注册了 `/approvals/{id}/approve` 和 `/approvals/{id}/reject`，没有注册 `POST /v1/tenants/{tenant_id}/approvals`、查询 pending approval 等入口。
- `server/internal/service/approval_service.go` 的 `Approve` 只把 task 状态转为 `queued` 并发布事件，不会把 task 投递到 NATS，也不会写入 outbox。

影响：

- 需要审批的任务会停在 `approval_pending`，但没有 approval 记录可供审批。
- 即使手动造出 approval 并 approve，task 状态可能变成 `queued`，但队列中没有对应消息，Agent 拉不到任务。

与设计偏差：

- 设计文档中 Approval Service 职责包括创建审批请求、记录审批结果、继续或拒绝任务。
- Task 生命周期要求 `approval_pending` 只能由 Approval Service 转为 `queued` 或 `cancelled`。

建议：

- `approval_required` 时在同一事务中创建 approval 记录，并发布 `task.approval_pending` / `policy.approval_required` 事件。
- 注册 approval request/list/get API。
- `Approve` 应通过 outbox 投递 task_publish，并与状态更新处于同一事务边界。
- 增加审批端到端测试：创建任务 -> pending approval -> approve -> 入队 -> pull。

### P1：Event / Audit Plane 没有持久化闭环

证据：

- `EventRepo` 已能写入和查询 `audit_event_projection`。
- `server/cmd/janus-api/main.go` 创建了 `EventService`，但只用于 Audit Handler 查询。
- 主流程中 Task/Dispatch/DLQ 事件直接调用 `QueueEventDriver.PublishEvent`，没有通过 `EventService.Record`。
- main 订阅 NATS event stream 后只送入 WebSocket broadcaster，没有写入 PostgreSQL projection。
- 多处 `core.JanusEvent{...}` 没有设置 `EventID` 和 `Timestamp`。

影响：

- NATS 上可能有事件，但 Audit API 查询 PostgreSQL 时可能为空。
- event envelope 不稳定，缺少设计要求的不可变事件 ID 和时间戳。

与设计偏差：

- 设计要求 Event / Audit Service 统一发布 Janus events，并同步或异步投影到 PostgreSQL 查询表。

建议：

- 增加 event projector：订阅 NATS event stream，补齐/校验 envelope 后写入 `audit_event_projection`。
- 或改为所有业务服务通过 `EventService.PublishEvent`，由 EventService 同时写 NATS 和 projection。
- 事件创建统一补 `event_id/timestamp/trace_id/actor/security`。

### P0：API key 鉴权没有强制租户隔离

证据：

- `server/internal/auth/apikey.go` 验证 API key 后把 tenantID 写入 context。
- handler 普遍直接从 URL 解析 tenantID，例如 `tenantIDFromPath(r.URL.Path)`。
- `rg TenantFromContext` 显示业务 handler 没有使用 API key 绑定的 tenant。

影响：

- 如果开启 API key 鉴权，tenant A 的 key 可能访问 `/v1/tenants/tenant-b/...`。
- PostgreSQL 虽然有 tenant_id 逻辑隔离，但 API 层没有强制路径 tenant 与认证 tenant 一致。

与设计偏差：

- 设计要求第一阶段通过 PostgreSQL tenant_id 逻辑隔离、NATS subject namespace 隔离和 Policy 强制检查保证租户隔离。

建议：

- 在鉴权 middleware 或 tenant guard middleware 中校验 URL tenant 与 API key tenant 一致。
- 对 tenant 创建、跨租户 admin 操作引入明确的 system/admin 权限。
- 增加“tenant A key 访问 tenant B 路径必须 403”的测试。

### P1：Outbox 有重试，但多实例并发和覆盖范围仍不足

证据：

- `OutboxRepo.FetchPending` 使用 `SELECT ... FOR UPDATE SKIP LOCKED`，但它是在普通 pool query 中执行，没有显式事务包住 fetch + publish + mark。
- PostgreSQL 行锁会在该语句结束后释放，publish/mark 之前其他 publisher 仍可能再次取到同一 pending 行。
- `TaskService.createWithOutbox` 使用 outbox，但 `ApprovalService.Approve`、`TaskService.Replay`、`DLQServiceAdapter.ReplayDLQ` 仍直接发布 NATS 或直接更新状态。
- outbox failed/retry 没有记录 `last_error`。

影响：

- 多个 outbox publisher 实例可能重复发布同一事件或任务。
- 审批入队、DLQ replay、手动 replay 等路径仍存在 DB/NATS 双写不一致。

与设计偏差：

- 设计要求 transactional outbox 解决 PostgreSQL 和 NATS 双写不一致。

建议：

- 用单事务 claim outbox 行，例如 `UPDATE ... SET status='publishing' ... RETURNING *`，或 fetch/update 在同一事务里完成。
- 发布成功后再标记 `published`，失败记录 `last_error` 和 `next_attempt_at`。
- 所有会改变任务可调度状态的路径都统一写 outbox，而不是直接 PublishTask。

### P1：Retry 设计仍依赖 NATS redelivery，Janus 自身 retry 闭环不足

证据：

- `DispatchService.NackTask` retriable 时先对 NATS delivery 执行 `NackTask`。
- 然后更新 DB `retry_at`，事件为 `task.retry_scheduled`。
- `server/internal/retry/scheduler.go` 到期后只把 DB 状态改为 `queued`，不会重新发布 task 到 NATS。

影响：

- 业务 retry policy 和 NATS redelivery 可能互相打架。
- 如果原消息因为 NATS 行为或人工操作不再可投递，DB 状态变成 queued 也没有队列消息。

与设计偏差：

- 设计明确 Janus 不应完全依赖 NATS redelivery 表达业务重试，推荐 Janus 自己维护 retry policy。

建议：

- retriable NACK 后 ACK/term 原消息，由 Janus retry scheduler 到期后通过 outbox 重新投递。
- 或明确采用 NATS redelivery，并删除 DB retry scheduler，避免双重语义。
- 增加 NACK retry 的端到端测试，验证到期后 Agent 确实能再次 pull 到任务。

### P1：Budget / Backpressure 没有真正形成多维闭环

证据：

- `DispatchService.PullTask` 调用 `CheckConcurrency(ctx, tenantID, agentID, 0)`，当前运行数固定传 0。
- `BudgetService.Reserve` 只对 agent scope 调用 `usageRepo.ReserveTask`。
- `BudgetService.Settle` 只结算 agent scope，没有结算 tenant scope。
- tenant daily cost 检查读取 tenant usage，但当前生产路径没有写 tenant usage。
- task envelope 的 `budget.max_cost_usd` 没有看到实际成本预估或阻断逻辑。

影响：

- agent/tenant max_concurrency 基本不起作用。
- tenant 级 daily cost / task count 不会准确累积。
- 设计里的预算和背压只能部分生效。

建议：

- 使用 `budget_usage.task_count` 或 running task 查询作为 concurrency 当前值。
- Reserve/Settle 同时写 agent 和 tenant scope。
- 对 task envelope 里的 `max_tokens/max_cost_usd` 做 publish 前或 dispatch 前校验。
- 对 backpressure 增加事件和指标。

### P1：Task 状态机仍被多处绕过

证据：

- `core.CanTransition` 和 `TaskService.transition` 已存在。
- 但 `DispatchService` 直接调用 `taskRepo.UpdateStatus` 设置 `claimed/running/completed/dead_lettered`。
- `TaskService.Block` 直接 `UpdateStatus` 到 `blocked`。
- `DLQServiceAdapter.ReplayDLQ` 直接把 `dead_lettered -> created -> queued`。
- `retry.Scheduler` 直接把 `retry_scheduled -> queued`。
- `TaskRepository.UpdateStatus` 不检查 rows affected，也不强制 expected current status。

影响：

- 可以绕过 `core.ValidTransitions`。
- ACK 可能从 claimed 直接 completed，不一定要求 running。
- block/replay/DLQ 等路径可能产生设计外状态迁移。
- 更新不存在的 task 可能被当成成功。

建议：

- 所有状态变化收敛到一个 lifecycle service。
- repository 更新使用 expected status 和 rows affected，区分 not found / conflict / invalid transition。
- 对 ACK/NACK/Start/Block/Replay/DLQ/Retry 全部补状态机测试。

### P1：Task result_ref 没有写入 tasks 表

证据：

- `DispatchService.AckTask` 接收 `resultRef`。
- 该值只写入完成事件 payload。
- `TaskRepository.UpdateStatus` 在 completed 时只更新 `status/updated_at/completed_at`，没有更新 `result_ref`。

与设计偏差：

- 设计文档 `29.4 Task Result 存储` 要求 MVP 阶段 Task Result 存入 PostgreSQL 的 `tasks.result_ref` 字段。

建议：

- 增加 `CompleteTask(ctx, tenantID, taskID, resultRef, usage)` repository/service 方法。
- ACK 成功时持久化 `result_ref`。
- 增加 ACK 后 GetTask 能返回 result_ref 的测试。

### P2：Context Reference 已有局部实现，但没有接入主路由和 Task 创建

证据：

- `server/internal/handler/context_ref_handler.go`、`ContextRefService`、`context_refs` 迁移存在。
- `server/cmd/janus-api/main.go` 没有实例化或注册 ContextRefHandler。
- Task Create HTTP/gRPC 转换没有完整保存 `envelope.context_refs`。

与设计偏差：

- 设计文档把 Context Reference Service 作为独立能力，且 Task Envelope 支持 context_refs。

建议：

- 注册 context refs API。
- Task 创建时将 envelope context_refs 持久化或校验引用存在。
- Policy/DLP 钩子可以基于 context classification 执行。

### P2：Heartbeat 实现与设计文档、测试不一致

证据：

- 设计文档 `29.3 Heartbeat 存储` 写的是 Redis `SET agent:heartbeat:<tenant>:<agent_id> ... EX 60`。
- 当前 `server/internal/driver/redis/driver.go` 使用 sorted set `agent:heartbeat:<tenant>` 保存 agent 过期时间。
- `server/internal/driver/redis/driver_test.go` 仍测试旧的 `heartbeatKey` / `extractAgentID`。

判断：

- 当前 sorted set 实现比设计中的 TTL key 更适合 sweeper 扫描，方向是合理的。
- 但设计文档和测试没有同步，导致测试包编译失败。

建议：

- 同步更新设计文档 heartbeat 章节。
- 修复 Redis driver 测试，改为验证 zset score、过期扫描和 Remove。

### P2：SDK 不支持 API key，且 Python 模型与核心类型不一致

证据：

- `sdk/go/client.go` 的 Config 只有 `BaseURL/TenantID`，请求没有 `X-API-Key` 或 `Authorization`。
- `sdk/python/janus_sdk/client.py` 同样没有 API key 配置。
- Python `TargetType` 定义为 `agent/mailbox/semantic`，但 core 支持的是 `agent/mailbox/capability/group/human`。
- SDK 使用手写 REST shape，不是 proto/gateway shape。

影响：

- 开启鉴权后 SDK 默认不可用。
- Python SDK 可能发出 core 不接受的 target type。

建议：

- SDK Config 增加 APIKey，并统一注入请求头。
- SDK 类型从 proto 或共享契约生成。
- 修正 Python TargetType。

### P2：测试基础设施和测试覆盖与生产路径不一致

证据：

- NATS 测试硬编码 `$HOME/go/bin/nats-server`。
- Redis 测试依赖 `$HOME/.local/bin/redis-server`，且当前测试代码未同步新实现。
- Postgres repo 测试默认使用本机 `/tmp/.s.PGSQL.5432` 和用户 `silv`。
- `server/internal/driver/postgres/testutil_test.go` 的 `runMigration` 只跑 `000001` 和 `000002`，没有覆盖 `000003` 到 `000008`。
- e2e 测试中 `TaskService` 没接 policy/outbox，`BudgetService` 没接 usage repo，与 `janus-api` 生产启动路径不同。

建议：

- 将单元测试、集成测试、e2e 测试分层。
- 外部依赖不存在时 skip，而不是直接失败。
- 提供 docker compose 或 testcontainers 测试入口。
- e2e 应覆盖生产 wiring：policy、outbox、budget usage、event projector、auth。

### P2：可观测性指标定义较全，但业务路径未充分打点

证据：

- `server/internal/metrics/metrics.go` 定义了 pull/ack/nack/budget/policy/agent/queue 等指标。
- `rg "metrics\\."` 显示目前主要只在 `TaskService` 记录 task created/completed/failed/dead_lettered。

影响：

- 设计中的运行期观测能力无法发挥作用。

建议：

- 在 PullTask/AckTask/NackTask/Budget throttle/Policy deny/Agent heartbeat/Queue backlog 路径补指标。
- 补 task latency histogram。

## 三、建议修复顺序

### 第一阶段：恢复可编译和可验证

1. 修复 proto/google/api 生成包缺失。
2. 生成并提交 `.pb.gw.go`。
3. 修复 Redis driver 测试。
4. 调整测试入口，明确根目录不能直接 `go test ./...`。
5. 将外部依赖测试改为可配置或可跳过。

### 第二阶段：修复核心任务闭环

1. 统一 HTTP/gRPC/SDK 的 Task API 契约。
2. 修复 CreateTask envelope 字段丢失和响应状态不准确。
3. 修复 gRPC mailbox target 不入队。
4. 修复审批创建、审批通过后入队。
5. 修复 ACK result_ref 持久化。

### 第三阶段：收口可靠性和安全性

1. API key tenant guard。
2. 所有可调度状态变化统一走 outbox。
3. outbox claim 改为事务化，支持多实例安全。
4. 明确 retry 由 Janus 还是 NATS 负责，避免双重语义。
5. 引入 event projector 或统一 EventService 发布路径。

### 第四阶段：补齐控制面能力

1. Budget tenant/agent usage 双写与 concurrency 真实校验。
2. ContextRef API 与 Task envelope context_refs 接入。
3. SDK 支持 API key 并修正类型。
4. 指标全面接入业务路径。
5. 更新设计文档中 heartbeat、API 暴露方式、当前实现限制。

## 四、最低验收清单

- `go test ./core/... ./server/... ./cli/... ./sdk/go/... ./demo/... ./proto/...` 编译通过。
- proto 生成产物包含 `google/api` 依赖处理和 `.pb.gw.go`。
- HTTP 与 gRPC 创建同一 mailbox task 后，都能被 Agent pull 到。
- policy `approval_required` 创建 approval 记录，approve 后任务入队。
- API key tenant A 访问 tenant B 返回 403。
- NATS 短暂不可用后 outbox 能重试成功，且多 publisher 不重复发布。
- NACK retriable 到期后 Agent 能再次 pull 到任务。
- ACK 后 `GetTask` 返回 `result_ref`。
- Audit API 能查到 task.created/task.queued/task.claimed/task.completed 等事件。
- SDK 在开启 API key 鉴权时可以跑通 publish/pull/ack 最小流程。
