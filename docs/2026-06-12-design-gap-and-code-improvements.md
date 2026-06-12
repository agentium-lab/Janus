# Janus 提交后复查：设计偏差与代码实现问题


审查日期：2026-06-12


参考设计文档：`Janus-detail-design.md`


本次复查仍按两个要求组织：


1. 当前代码是否偏离设计文档。

2. 代码实现是否存在问题。


优先级说明：


- P0：阻断编译、启动、CI 或核心主流程。

- P1：核心设计能力未闭环，或存在一致性、安全性、可靠性风险。

- P2：测试、SDK、文档、可观测性和可维护性问题。


## 一、验证结果


执行结果：


```bash

go build ./...

```


失败：


```text

pattern ./...: directory prefix . does not contain modules listed in go.work or their selected dependencies

```


执行结果：


```bash

go vet ./...

```


失败，原因同上。


执行结果：


```bash

go test ./server/cmd/janus-api

```


失败：


```text

server/internal/grpc/gateway.go:17:15: undefined: pb.RegisterAgentServiceHandlerFromEndpoint

server/internal/grpc/gateway.go:20:15: undefined: pb.RegisterTaskServiceHandlerFromEndpoint

server/internal/grpc/gateway.go:23:15: undefined: pb.RegisterDispatchServiceHandlerFromEndpoint

server/internal/grpc/gateway.go:26:15: undefined: pb.RegisterAuditServiceHandlerFromEndpoint

```


执行结果：


```bash

go test ./core/... ./server/... ./cli/... ./sdk/go/... ./demo/... ./proto/...

```


失败，主要原因：


- `server/internal/grpc` 缺少 grpc-gateway 生成函数。

- NATS 测试默认启动 `$HOME/go/bin/nats-server`，本机不存在。

- Redis 测试默认启动 `$HOME/.local/bin/redis-server`，本机不存在。

- Postgres 测试默认连接本机 PostgreSQL，当前环境无法连接。


已改善的点：


- `proto/gen/google/api/stubs.go` 已补上，解决了上一轮 `google/api` 生成包缺失导致的直接编译错误。

- `PullTaskRequest` 已包含 `agent_id`。

- `TaskService.Create` 成功路径会重新读取并返回真实 task。

- `ApprovalService` 已接入 `TaskService`，并新增 approval request/list/get 路由。

- `TenantGuard` 已接到 API key middleware 后面，可校验 URL path 中的 tenant。

- `ContextRefHandler` 已接入 main router。

- `RetryScheduler` 已尝试在 retry 到期后重新发布任务。

- Outbox 已支持 `last_error`、`next_attempt_at` 和 `publishing` 状态。


## 二、是否偏离设计文档


### P0：grpc-gateway 设计仍未落地，服务端主包无法编译


设计要求：


- `Janus-detail-design.md` 明确 HTTP + gRPC 双协议通过 `grpc-gateway` 一份 proto 生成两套 API。


当前实现：


- `proto/buf.gen.yaml` 配置了 `protoc-gen-grpc-gateway`。

- `server/internal/grpc/gateway.go` 调用 `Register*HandlerFromEndpoint`。

- `proto/gen/janus/v1/` 下没有任何 `.pb.gw.go` 文件。


影响：


- `server/internal/grpc` 和 `server/cmd/janus-api` 无法编译。

- `janus-api` 不能启动。

- CI 中 build/vet/test 都会被阻断。


建议：


- 正确生成并提交 `.pb.gw.go`。

- 或在 gateway 未生成前移除 `RegisterGateway` 启动路径，避免主服务不可编译。

- CI 增加 proto generate 校验，避免 proto 源文件和生成产物漂移。


### P0：CI 命令与 go.work 结构不匹配


设计要求：


- 仓库是多 module workspace，`go.work` 列出了 `core/server/cli/sdk/go/demo/proto`。


当前实现：


- `.github/workflows/ci.yml` 中 build 和 vet 使用：


```bash

go build ./...

go vet ./...

```


问题：


- 当前仓库根目录不是 module，根目录执行 `./...` 会失败。


建议：


- 改为显式 module 路径，例如：


```bash

go build ./core/... ./server/... ./cli/... ./sdk/go/... ./demo/... ./proto/...

go vet ./core/... ./server/... ./cli/... ./sdk/go/... ./demo/... ./proto/...

```


### P1：HTTP/gRPC/SDK 契约仍不统一


设计要求：


- proto 是统一 API 合约。

- Go/Python SDK 共享同一份 proto 定义。


当前实现：


- proto 的 `CreateTaskRequest` 是 `tenant_id + envelope`。

- 手写 HTTP `TaskHandler.Create` 仍要求顶层 `id/source_agent/target_type/target_value/mailbox_id`。

- 如果客户端按 proto/gateway 形态只传 `envelope`，HTTP handler 会让顶层字段为空，最终被 `TaskService.Create` 拒绝。

- Go/Python SDK 仍使用手写 REST shape，不是 proto/gateway shape。


影响：


- 同一个 CreateTask 语义在 HTTP、gRPC、SDK 三处不一致。

- 设计中的“一份 proto 双协议”没有成立。


建议：


- 将外部 HTTP API 收敛到 gateway JSON mapping。

- 若保留手写 REST，必须做兼容层：从 `envelope` 自动推导 `id/source_agent/target_type/target_value/mailbox_id/priority`。

- SDK 类型从 proto 生成，或至少用契约测试锁定与 proto 一致。


### P1：Task Envelope 仍未完整接入


设计要求：


- Task Envelope 包含 `deadline`、`ttl_seconds`、`budget`、`policy`、`context_refs`、完整 trace 等字段。


当前实现：


- `server/internal/handler/task_handler.go` 已补 `idempotency_key`、`ttl_seconds` 和部分 trace。

- 但 `deadline` 字段只声明为 string，没有解析进 `core.Task.Deadline` 或 `Envelope.Deadline`。

- `budget`、`policy`、`context_refs` 仍没有解析。

- `Envelope.TenantID` 没有校验必须等于 URL path tenant。


影响：


- Budget、Policy、Context Reference、TTL/deadline 的设计能力无法通过 HTTP CreateTask 完整使用。

- 请求体内 envelope tenant 与 URL tenant 不一致时，可能形成数据语义不一致。


建议：


- HTTP 层直接复用 proto DTO 或完整 TaskEnvelope DTO。

- 创建时校验 URL tenant、task tenant、envelope tenant 一致。

- 完整解析并持久化 `deadline/budget/policy/context_refs/trace`。


### P1：审批流有进展，但仍未满足 transactional outbox 设计


设计要求：


- `approval_pending` 只能由 Approval Service 转为 `queued` 或 `cancelled`。

- Task 入队应通过 transactional outbox 保证 DB 与 NATS 最终一致。


当前实现：


- `TaskService.Create` 在 task 创建成功后才调用 `ApprovalService.RequestApproval`。

- approval 创建错误被忽略：`_, _ = s.approvalSvc.RequestApproval(...)`。

- approval 记录和 task 状态不是同一事务。

- `ApprovalService.Approve` 先更新 approval，再把 task 转为 `queued`，之后直接 `PublishTask` 到 NATS。

- `PublishTask` 的错误被忽略。


影响：


- approval 创建失败时，task 会停在 `approval_pending`，但没有 approval 记录。

- approve 后即使 NATS 发布失败，DB 也可能显示 `queued`。

- 这仍是 DB/NATS 双写不一致。


建议：


- `approval_required` 时在同一事务内创建 task 和 approval。

- `Approve` 使用 outbox 写入 `task_publish`，不要直接调用 NATS。

- 所有错误必须向上返回，不能忽略。


### P1：Event / Audit 投影仍未闭环


设计要求：


- Event / Audit Service 统一发布事件，并同步或异步投影到 PostgreSQL 查询表。


当前实现：


- `EventProjector` 已新增，但它内部自带一个 channel。

- main 中 NATS `SubscribeEvents` 的 `eventCh` 只传给 WebSocket broadcaster。

- 没有代码把 NATS event stream 转发到 `EventProjector.Record`。

- `rg` 结果显示 `EventProjector.Record` 没有生产调用点。


影响：


- Audit API 查询 PostgreSQL projection 时，主业务事件仍可能查不到。

- 设计中的 event stream -> audit projection 没有形成闭环。


建议：


- 让 projector 直接消费 `natsDrv.SubscribeEvents` 的 channel。

- 或用一个 tee channel 同时送 WebSocket 和 EventProjector。

- 事件投影失败需要重试或至少有死信/告警。


### P1：事件 envelope 不统一


设计要求：


- Event Envelope 必须包含 `event_id`、`timestamp`、`tenant_id`、`trace_id`、`task_id`、actor、payload 等。


当前实现：


- `EventService.Record` 会补 `event_id` 和 `timestamp`。

- 但主业务路径多数直接调用 `QueueEventDriver.PublishEvent`。

- `nats.Driver.PublishEvent` 只是 marshal，不补 `event_id` 和 `timestamp`。


影响：


- NATS event stream 上的事件可能缺少不可变事件 ID 和时间戳。

- 即使后续 projector 接通，projection 也可能收到不完整事件。


建议：


- 所有事件发布都走统一 EventService。

- 或在 QueueEventDriver 的 decorator 层统一补全/校验 event envelope。


### P1：Retry 语义仍可能重复投递


设计要求：


- Janus 不应完全依赖 NATS redelivery 表达业务重试，推荐由 Janus 自己维护 retry policy。


当前实现：


- `DispatchService.NackTask` 在 retriable 时先调用 `queueDriver.NackTask`。

- NATS retriable NAK 会触发底层 redelivery。

- 同时 DB 被设置为 `retry_scheduled`。

- `RetryScheduler` 到期后又会重新 `PublishTask`。


影响：


- 同一 task 可能被 NATS redelivery 和 Janus retry scheduler 双重投递。


建议：


- 明确只保留一种 retry 机制。

- 如果由 Janus 管业务 retry，retriable NACK 应 ACK/TERM 原消息，再由 scheduler/outbox 到期重新投递。

- 如果由 NATS redelivery 负责，则不要再让 scheduler 重新发布任务。


### P1：Outbox 仍有崩溃恢复缺口


设计要求：


- transactional outbox 用于补偿 NATS 不可用，保证最终一致。


当前实现：


- `OutboxRepo.FetchPending` 会把 pending/retry 行改为 `publishing` 后提交。

- publisher 随后在事务外发布 NATS。

- 如果进程在 `publishing` 后、`MarkPublished/MarkFailed` 前崩溃，该 outbox 行会永久停在 `publishing`。

- `FetchPending` 不会扫描 `publishing`，迁移索引包含 `publishing` 也没有实际效果。

- `MarkPublished` 又会 `attempts = attempts + 1`，第一次成功发布会在 claim 和 publish 各加一次 attempts。


影响：


- outbox 可能丢失补偿能力。

- attempts 计数语义不准确。


建议：


- 为 `publishing` 增加 `locked_at/locked_by`，超时后可重新 claim。

- 或采用 `UPDATE ... WHERE status IN (...) RETURNING *` 的短事务 claim 模式，并有 stale reclaim。

- attempts 只在一次投递尝试开始时递增一次。


### P1：Outbox 覆盖范围仍不完整


当前仍直接发布 NATS 的路径：


- `ApprovalService.Approve`

- `TaskService.Replay`

- `DLQServiceAdapter.ReplayDLQ`

- `RetryScheduler`

- 多个 dispatch event publish 路径


影响：


- 这些路径仍可能出现 DB 状态已更新但 NATS 消息或事件未发布的问题。


建议：


- 所有“任务进入可调度状态”的路径统一写 outbox。

- 所有状态事件统一写 outbox 或 EventService。


### P1：Budget / Backpressure 与设计仍有偏差


设计要求：


- Budget Service 支持 tenant、team、agent、model、task 等多维资源预算。

- Backpressure 应按真实运行态和预算使用量执行。


当前实现：


- `DispatchService.PullTask` 使用 `taskRepo.CountByStatus(ctx, tenantID, running)` 作为 `currentRunning`。

- 这个值是租户全局 running 数，却同时用于 agent budget 和 tenant budget 判断。

- `BudgetService.Reserve/Settle/Release` 只写 agent scope usage。

- tenant daily cost 检查读取 tenant usage，但代码没有写 tenant usage。


影响：


- agent max concurrency 可能被其他 agent 的 running task 误伤。

- tenant 级 daily cost / task_count 基本不会正确累计。


建议：


- agent concurrency 应统计该 agent 当前 running/claimed task。

- tenant concurrency 应统计 tenant 全局 running/claimed task。

- Reserve/Settle/Release 同时维护 tenant 和 agent scope usage。


### P1：租户隔离仍有绕过点


已改善：


- `TenantGuard` 可以校验 URL path 中的 tenant 和 API key tenant。


仍存在的问题：


- `ApprovalHandler.Request` 忽略 URL path tenant，直接使用 body 里的 `tenant_id`。

- `TaskHandler.Create` 没校验 envelope tenant 与 URL tenant 一致。

- `/a2a/*` 通过 query 参数传 `tenant_id`，`TenantGuard` 只看 path，无法校验 query tenant。


影响：


- 开启 API key 后，仍可能通过 body/query tenant 造成跨租户写入或语义混乱。


建议：


- 所有 handler 都从认证上下文或 path tenant 派生 tenant，禁止 body 覆盖 tenant。

- TenantGuard 扩展支持 query tenant 或 A2A gateway 内部校验。

- 增加跨租户负向测试。


### P2：Heartbeat 实现与设计文档不一致


设计文档写法：


- Redis `SET agent:heartbeat:<tenant>:<agent_id> {ts} EX 60`

- sweeper 扫描 TTL 过期。


当前实现：


- Redis driver 使用 sorted set `agent:heartbeat:<tenant>`，score 是 expire timestamp。


判断：


- sorted set 方案更适合扫描过期心跳，是合理改进。

- 但设计文档应同步，否则后续实现者会按旧 TTL key 方案开发。


建议：


- 更新 `Janus-detail-design.md` 的 heartbeat 存储章节。

- 配置项中的 heartbeat TTL 应实际传入 Redis driver，而不是硬编码 60s。


### P2：Context Reference API 已接入，但路径和实现仍有 bug


当前实现：


- main router 已接入 context refs。

- `ContextRefHandler.Detach` 使用 `lastPathSegment(r.URL.Path)` 获取 refID。


问题：


- 如果路径是 `/v1/tenants/{tenant}/context-refs/{id}/detach`，last segment 是 `detach`，不是 `{id}`。

- Task Create 仍未解析 envelope 中的 `context_refs`，因此 context refs 与 task 仍未自然关联。


建议：


- 修复 detach 路径解析，取 `context-refs` 后的 segment。

- Task 创建时校验并持久化 envelope context refs。


### P2：SDK 仍不支持鉴权，也未共享 proto 契约


当前实现：


- Go SDK `Config` 只有 `BaseURL/TenantID`，没有 API key。

- Python SDK 同样没有 API key。

- Python `TargetType` 仍是 `agent/mailbox/semantic`，而 core 是 `agent/mailbox/capability/group/human`。


影响：


- 开启 `JANUS_AUTH_ENABLED=true` 后 SDK 默认不可用。

- Python SDK 可能发出 core 不接受的 target type。


建议：


- SDK Config 增加 `APIKey`，统一设置 `X-API-Key` 或 `Authorization`。

- SDK 类型从 proto 生成，或至少与 core/proto 的枚举保持一致。


## 三、代码实现问题


### P0：`server/cmd/janus-api` 当前不可编译


原因：


- `server/internal/grpc/gateway.go` 调用的 grpc-gateway handler 注册函数不存在。


建议：


- 立即生成 `.pb.gw.go` 或移除未生成 gateway 依赖。


### P1：`TaskService.Create` 忽略审批创建失败


位置：


- `server/internal/service/task_service.go`


问题：


- approval request 创建失败被 `_` 忽略。

- task 可能已提交为 `approval_pending`，但没有对应 approval。


建议：


- approval request 必须与 task 创建处于同一事务，或至少失败时返回错误并回滚/补偿。


### P1：`ApprovalService.Approve` 忽略 NATS 发布失败


位置：


- `server/internal/service/approval_service.go`


问题：


- task 已转为 `queued` 后直接 `PublishTask`。

- `PublishTask` 错误被忽略。


建议：


- 用 outbox 入队。

- 发布失败必须可重试或向上返回。


### P1：Task 状态机仍被绕过


绕过点：


- `DispatchService.PullTask/StartTask/AckTask/NackTask`

- `TaskService.Block`

- `TaskService.Replay`

- `DLQServiceAdapter.ReplayDLQ/DiscardDLQ`

- `RetryScheduler`


问题：


- 这些路径直接 `UpdateStatus`，没有统一通过 `TaskService.transition` 或状态机。

- `TaskRepository.UpdateStatus` 不检查 rows affected，也没有 expected current status。


影响：


- 可以产生设计外状态迁移。

- 更新不存在的 task 可能被视为成功。

- 并发 ACK/NACK/timeout 时容易出现竞态。


建议：


- 引入统一 lifecycle service。

- repository 更新使用 compare-and-set：`WHERE tenant_id=? AND id=? AND status=?`。

- 检查 rows affected，返回 not found/conflict。


### P1：ACK/NACK 幂等性不足


设计要求：


- ACK 请求必须具备幂等语义，并且只有当前 claim owner 可以 ACK/NACK。


当前实现：


- 只校验 latest attempt 的 lease_id。

- `UpdateFinished` 不检查 attempt 当前状态。

- duplicate ACK/NACK 可能重复更新 attempt、重复 settle/release budget、重复发布事件。

- `SetResultRef` 错误被忽略。


建议：


- attempt finish 使用 expected status，例如 `WHERE status IN ('claimed','running')`。

- ACK/NACK 根据 rows affected 判断是否重复提交。

- `SetResultRef` 失败必须返回错误或参与同一事务。


### P1：NACK 更新顺序存在一致性风险


当前实现：


- `NackTask` 先调用 `queueDriver.NackTask`，再更新 attempt 和 task 状态。


影响：


- 如果 NATS 已 NAK 但 DB 更新失败，消息可能已重新投递，而 DB 仍停留在旧状态。


建议：


- 先用事务记录 attempt/task 状态，再 ACK/TERM 或由 outbox/scheduler 控制重投递。


### P1：Outbox migration 回滚不完整


位置：


- `migrations/000008_outbox_retry.up.sql`

- `migrations/000008_outbox_retry.down.sql`


问题：


- up 增加 `last_error`。

- down 只删除 `next_attempt_at`，没有删除 `last_error`。


建议：


- down migration 补 `ALTER TABLE outbox_events DROP COLUMN IF EXISTS last_error;`。


### P2：测试基础设施仍依赖本机固定路径


问题：


- NATS 测试硬编码 `$HOME/go/bin/nats-server`。

- Redis 测试硬编码 `$HOME/.local/bin/redis-server`。

- Postgres 测试默认用户 `silv` 和本机 socket。


建议：


- 缺少外部依赖时 `t.Skip`。

- 支持 env 指定二进制路径。

- CI 和本地统一使用 docker compose 或 testcontainers。


### P2：Postgres repo 测试迁移链不完整


位置：


- `server/internal/driver/postgres/testutil_test.go`


问题：


- `runMigration` 只执行 `000001` 和 `000002`。

- 当前已有 `000003` 到 `000008`，包含 api_keys、budget_usage、retry_at、context_refs、outbox retry。


影响：


- repo 测试不能覆盖真实 schema。


建议：


- 测试迁移使用 golang-migrate 跑完整 `migrations/`。


### P2：可观测性指标定义多，实际接入少


当前实现：


- `metrics.go` 定义了 pull/ack/nack/budget/policy/agent/queue 指标。

- 实际主要在 TaskService 里打 created/completed/failed/dead_lettered。


建议：


- 在 Dispatch、Budget、Policy、Agent Heartbeat、Outbox、Retry 路径补指标。


## 四、建议修复顺序


1. 先修复 grpc-gateway 生成产物，保证 `server/cmd/janus-api` 可编译。

2. 修复 CI build/vet 命令，避免根目录 `./...`。

3. 修复 HTTP/gRPC/SDK Task API 契约不一致。

4. 把 approval approve、retry、replay、DLQ replay 全部改为 outbox 入队。

5. 接通 EventProjector 与 NATS event stream。

6. 收敛 Task 状态机和 ACK/NACK 幂等。

7. 修复 tenant body/query 绕过、SDK 鉴权和 context-ref detach。

8. 整理测试基础设施和完整迁移链。


## 五、最低验收清单


- `go build ./core/... ./server/... ./cli/... ./sdk/go/... ./demo/... ./proto/...` 通过。

- `go test ./server/cmd/janus-api` 通过。

- `server/internal/grpc` 能编译，gateway 注册函数存在。

- HTTP 和 gRPC 使用同一 CreateTask 契约。

- 按 proto/gateway JSON 创建 mailbox task 后能被 Agent pull。

- approval_required 任务能生成 approval；approve 后通过 outbox 入队。

- NATS 短暂不可用时，approval/retry/replay/DLQ replay 都能最终补偿。

- Audit API 能查询到主流程事件。

- API key tenant A 不能通过 path/body/query 写入 tenant B。

- ACK/NACK 重复提交不会重复结算预算或重复发布状态事件。