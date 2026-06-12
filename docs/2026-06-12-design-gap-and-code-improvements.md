# Janus 当前实现与详细设计偏差及改进建议

审查日期：2026-06-12

参考设计文档：`Janus-detail-design.md`

本文件记录两类问题：

1. 当前代码实现与设计文档存在的主要偏差。
2. 代码实现层面的改进建议与优先级。

优先级说明：

- P0：会导致核心服务无法编译、无法启动、主流程不可用或数据一致性风险。
- P1：核心设计能力缺失或行为明显偏离设计，需要在近期修复。
- P2：工程质量、测试、可维护性或文档一致性改进。

## 一、当前版本与设计文档的偏差

### P0：proto 生成代码无法编译

现象：

- `proto/gen/janus/v1/agent.pb.go:10` 引用了 `github.com/agentium-lab/Janus/proto/gen/google/api`。
- 仓库中没有提交 `proto/gen/google/api` 生成代码。
- `proto/buf.gen.yaml` 的 managed `go_package_prefix` 会影响 `google/api` 相关导入路径，但实际生成产物不完整。

影响：

- `go test ./core/... ./server/... ./cli/... ./sdk/go/... ./demo/... ./proto/...` 无法通过。
- 这会阻断服务端、SDK、CLI、demo 的统一验证。

与设计偏差：

- 设计文档要求以 proto 为统一 API 合约，并通过 gRPC / grpc-gateway 对外提供 API。当前 proto 产物本身不可编译，统一 API 合约无法成立。

建议：

- 修复 `buf.gen.yaml` 的外部依赖生成策略。
- 明确 `google/api/annotations.proto`、`http.proto` 是生成到本仓库，还是通过外部模块引用。
- 将 proto 生成加入 CI，保证提交的生成代码与 proto 源文件一致。

### P0：数据库迁移存在重复建表

现象：

- `migrations/000001_initial_schema.up.sql:158` 已创建 `outbox_events`。
- `migrations/000004_outbox_events.up.sql:1` 再次创建 `outbox_events`。

影响：

- 空库按顺序执行迁移时会在 `000004` 失败。
- 新环境无法可靠初始化。

与设计偏差：

- 设计文档要求 PostgreSQL 承载控制面状态和审计投影，迁移脚本应能从空库稳定构建目标 schema。

建议：

- 合并或删除重复迁移。
- 如果历史迁移已经发布，新增修复迁移并避免修改已发布迁移；如果尚未发布，直接整理迁移序列。
- 增加一次“空库全量迁移”测试。

### P0：gRPC 端口配置未加载，服务默认监听 0 端口

现象：

- `server/internal/config/config.go` 定义了 `GRPCPort`。
- `Load()` 只设置了 `HTTPPort`，没有加载 `GRPCPort`。
- `server/cmd/janus-api/main.go:125` 使用 `cfg.GRPCPort` 启动 gRPC。
- `server/cmd/janus-api/main.go:133` grpc-gateway dial `localhost:%d`，实际可能是 `localhost:0`。

影响：

- gRPC 服务地址不可预测。
- grpc-gateway 可能无法连接后端 gRPC 服务。

与设计偏差：

- 设计文档配置示例包含 `grpc_port: 9090`。
- 设计依赖 HTTP + gRPC 双入口，当前配置会破坏该入口。

建议：

- 为 `GRPCPort` 设置默认值 `9090`。
- 支持环境变量和配置文件加载。
- 启动日志明确打印 HTTP 与 gRPC 实际监听地址。

### P0：HTTP 路由组合导致 REST 手写接口被 grpc-gateway 遮蔽

现象：

- `server/cmd/janus-api/main.go:141-144` 将 `/v1/` 全部交给 `gwMux`。
- `newRouter(...)` 中大量手写 REST 路由同样挂在 `/v1/*`，会被 `/v1/` 的 gateway 路由抢先匹配。
- 开启鉴权时，`server/cmd/janus-api/main.go:154` 包装的是 `mux`，不是 `combined`，导致 gateway 和 `/metrics` 不在鉴权后的 handler 中。

影响：

- 租户、邮箱、审批、DLQ、预算等手写 REST 接口可能无法访问。
- 鉴权开启后路由行为与关闭时不一致。

与设计偏差：

- 设计文档要求 HTTP API 覆盖 Task、Agent、Mailbox、Approval、Audit、Budget 等核心控制面。
- 当前路由组合使一部分设计 API 实际不可达。

建议：

- 统一 API 暴露方式：要么全部通过 proto + gateway，要么明确拆分 gateway 与手写 REST 的路径前缀。
- 鉴权中间件应包装最终的 combined handler。
- 增加路由级集成测试，覆盖每个公开 endpoint。

### P0：Task 创建成功路径返回 nil，HTTP/gRPC 调用可能 panic 或返回空响应

现象：

- `server/internal/service/task_service.go:89-100` 新任务创建成功后返回 `nil, nil`。
- `server/internal/handler/task_handler.go:102-110` 在非幂等命中路径读取 `result.ID`，可能触发 nil pointer panic。
- `server/internal/grpc/task_handler.go:32-36` 将 nil task 转换为 proto response。

影响：

- 普通创建任务请求会失败。
- 只有幂等键命中已有任务时才可能返回正常结果。

与设计偏差：

- 设计文档中 Task 创建是系统最核心入口之一，应该返回明确的任务 ID、状态和后续调度状态。

建议：

- `TaskService.Create` 在所有成功路径返回创建后的 `*core.Task`。
- HTTP/gRPC handler 增加 nil 防御。
- 增加“首次创建”和“幂等重复创建”两类测试。

### P0：审批策略可以被绕过

现象：

- `TaskService.Create` 在策略判定 `approval_required` 时把任务状态设置为 `approval_pending`。
- 但只要任务带有 `MailboxID`，后续仍会发布到队列，并把状态更新为 `queued`。
- `ApprovalService.Approve` 只更新状态为 `queued`，没有补充真正的入队发布逻辑。

影响：

- 需要审批的任务可能在审批前被 Agent 拉取执行。
- 审批后反而可能只改变数据库状态，不一定真正进入队列。

与设计偏差：

- 设计文档明确 `approval_pending` 只能由 Approval Service 迁移到后续状态。
- Policy / Approval 是 Task 生命周期的关键门禁，当前实现绕过了该门禁。

建议：

- 当策略结果为 `approval_required` 时，创建流程必须停止入队。
- 审批通过后由 Approval Service 或专门的调度服务执行入队，并写入 outbox。
- 用状态机校验所有 Task 状态迁移。

### P1：gRPC/gateway PullTask 缺少 agent_id，无法通过设计入口正常拉取任务

现象：

- `proto/janus/v1/dispatch.proto` 的 `PullTaskRequest` 没有 `agent_id`。
- `server/internal/grpc/dispatch_handler.go` 调用 `PullTask(ctx, tenantID, mailboxID, "")`。
- `server/internal/service/dispatch_service.go` 要求 `agentID` 非空。
- Go/Python SDK 的 REST 拉取接口会发送 `agent_id`，但默认 `/v1/` 又被 gateway 路由接管。

影响：

- 通过 gRPC/gateway 的拉取任务接口不可用。
- SDK、REST、gRPC 三套契约不一致。

与设计偏差：

- 设计文档要求 Agent 通过统一 API 拉取和 ACK/NACK 任务，且 Agent 身份是调度和并发控制的关键字段。

建议：

- 在 `PullTaskRequest` 中加入 `agent_id`。
- 同步更新 proto、生成代码、gateway、SDK、测试。
- 统一 REST JSON 字段结构与 proto gateway 结构。

### P1：审计事件查询没有真正接入持久化查询

现象：

- `server/cmd/janus-api/main.go:378-379` 的 `auditAdapter.QueryByTenant` 直接返回空数组。
- `server/internal/service/event_service.go` 的 `EventRepo` 只有 `Save`，没有按租户查询能力。
- `server/internal/grpc/audit_handler.go` 默认无过滤条件时返回空列表。

影响：

- Dashboard / Audit API 无法查看租户事件。
- 事件投影即使写入，也缺少完整读取链路。

与设计偏差：

- 设计文档将 Event/Audit Plane 作为可观测性和审计核心能力，并定义了 audit event projection。

建议：

- 为 `EventRepo` 增加 `ListByTenant`、`ListByTaskID`、分页和时间范围查询。
- REST/gRPC Audit Handler 统一使用 `EventService`。
- 增加审计事件写入后可查询的集成测试。

### P1：Outbox 失败后不会重试

现象：

- `server/internal/outbox/publisher.go:55-58` 发布失败后将事件标记为 `failed`。
- `server/internal/driver/postgres/outbox_repo.go:38-44` 只扫描 `status='pending'`。
- 因此失败事件不会再次被扫描发布。

影响：

- NATS 短暂不可用时，outbox 事件可能永久停留在 failed。
- Task 入队、事件通知等异步动作存在丢失风险。

与设计偏差：

- 设计文档明确 transactional outbox 用于补偿 NATS 不可用，要求 outbox scanner 后续重放。

建议：

- 引入 `retry_count`、`next_attempt_at`、`last_error`。
- 扫描 pending 与可重试 failed。
- 达到最大重试次数后进入 dead-letter outbox 或报警队列。

### P1：Agent 心跳过期扫描无法可靠标记离线

现象：

- `server/internal/driver/redis/driver.go:49-52` 使用 Redis TTL 保存 heartbeat。
- `ScanExpired` 扫描现存 key 并判断 `TTL < 0`。
- 但真正过期的 key 通常已经被 Redis 删除，扫描不到。

影响：

- Agent 离线状态无法可靠更新。
- 调度层可能长期认为 Agent 在线。

与设计偏差：

- 设计文档要求基于 Redis heartbeat 标记 Agent 在线状态，并由 sweeper 处理离线。

建议：

- 使用 sorted set 记录 `agent_id -> expire_at`，sweeper 按时间查询过期 Agent。
- 或启用 Redis keyspace notification，但需要明确部署依赖。
- 将 Agent 在线状态落库或写入明确事件，供 Dashboard/Audit 查询。

### P1：Budget 使用量没有在生产启动路径接入

现象：

- `server/cmd/janus-api/main.go` 创建了 `budgetRepo`。
- 启动路径使用 `NewBudgetService(budgetRepo).WithRateLimiter(redisDrv)`。
- 已存在 `BudgetUsageRepo` 和 `NewBudgetServiceWithUsage(...)`，但生产路径没有使用。

影响：

- token/cost 等累计预算可能只存在接口定义，未真正参与生产检查。
- 预算控制与任务调度的实际关联不足。

与设计偏差：

- 设计文档要求 Budget Service 支持 token、cost、concurrency 等多维限制和背压。

建议：

- 在生产路径注入 `BudgetUsageRepo`。
- 在任务创建、拉取、完成回报等关键点记录和校验预算使用。
- 明确预算超限后的 Task 状态和错误码。

### P1：SDK 与实际 API 契约不一致

现象：

- `sdk/go/client.go` 使用手写 REST JSON shape，例如顶层 `id`、`source_agent`、`target_type`、`target_value`、`mailbox_id`、`envelope`。
- Python SDK 同样使用手写 REST shape。
- 默认 `/v1/` 路由接入 grpc-gateway，而 gateway 期望 proto shape，例如 `CreateTaskRequest{tenant_id, envelope}`。

影响：

- SDK 调用实际服务时可能请求格式不匹配。
- 对外 API 难以稳定演进。

与设计偏差：

- 设计文档强调 proto 作为统一 API 合约，SDK 应围绕统一合约生成或适配。

建议：

- 明确 SDK 是调用 gateway JSON 还是手写 REST。
- 优先让 SDK 基于 proto/gateway 生成或共享契约测试。
- 为 Go/Python SDK 增加 against-server 的最小集成测试。

### P2：配置系统与设计不一致

现象：

- 设计文档提到 YAML 配置、环境变量覆盖、flags 覆盖，并列出 viper。
- 当前 `server/internal/config/config.go` 主要从环境变量读取，未看到完整 YAML/viper 加载链路。

影响：

- 部署配置方式与设计和示例配置不一致。
- 多环境部署时容易产生隐式默认值问题。

建议：

- 确定配置加载顺序：默认值 < YAML < 环境变量 < CLI flags。
- 为所有关键配置设置默认值和校验。
- 启动时打印非敏感配置摘要。

### P2：设计文档与当前迁移/模型存在漂移

现象：

- 设计文档中的 schema 与当前迁移数量、字段不完全一致。
- 例如当前迁移通过 `000002_delivery_ref.up.sql` 为 `task_attempts` 增加 `delivery_ref`，但设计文档 schema 片段未覆盖该字段。

影响：

- 后续开发者按设计文档实现时，可能忽略已落地字段和运行时约束。

建议：

- 将设计文档中的 schema 作为概念模型，另建 `docs/schema.md` 或 migration reference 作为真实数据库说明。
- 每次新增迁移时同步更新文档。

## 二、代码实现改进意见

### P0：先恢复可编译、可迁移、可启动的最小主线

建议按以下顺序修复：

1. 修复 proto 生成代码缺失问题。
2. 修复重复迁移，保证空库可全量迁移。
3. 修复 `GRPCPort` 默认值和加载逻辑。
4. 修复 Task 创建成功返回 nil 的问题。
5. 修复 HTTP/gRPC 路由组合与鉴权包装。

验收标准：

- `go test ./core/... ./server/... ./cli/... ./sdk/go/... ./demo/... ./proto/...` 至少不再因为编译错误失败。
- 空库执行所有 migration 成功。
- `janus-api` 启动后 HTTP、gRPC、metrics 三类入口均可访问。

### P0：把 Task 生命周期收敛到统一状态机

当前 `core.ValidTransitions` 已经定义了状态流转，但服务层和仓储层没有强制使用。

建议：

- 所有状态变化通过统一的 `TaskLifecycleService` 或 `TransitionTaskStatus` 方法执行。
- `TaskRepository.UpdateStatus` 应检查 rows affected，并可选带上 expected current status。
- 对审批、入队、拉取、ACK、NACK、DLQ 都使用同一套状态迁移校验。

收益：

- 防止 `approval_pending -> queued` 这类绕过门禁的问题。
- 便于审计每次状态变化。
- 后续实现 retry、timeout、cancel 更安全。

### P0：重构审批后入队流程

建议目标流程：

1. `TaskService.Create` 完成持久化和策略判断。
2. 如果策略允许执行，则写 outbox 入队事件。
3. 如果需要审批，则状态停在 `approval_pending`，不写入队事件。
4. `ApprovalService.Approve` 使用事务将状态推进到 `pending/queued`，并写 outbox 入队事件。
5. Outbox publisher 负责实际投递到 NATS。

需要补充测试：

- 需要审批的任务创建后不会被拉取。
- 审批通过后才可被拉取。
- 审批拒绝后不可入队。
- 重复审批、并发审批只生效一次。

### P1：统一 API 合约和 SDK 契约

建议：

- 以 proto 为唯一源头，重新生成 gRPC、gateway、OpenAPI 和 SDK 所需类型。
- 所有 HTTP JSON 请求都以 gateway JSON mapping 为准。
- 若保留手写 REST，需要使用不同路径前缀，例如 `/internal/v1/*`，避免和 gateway `/v1/*` 冲突。
- 增加契约测试，覆盖 Go SDK、Python SDK、HTTP gateway、gRPC server 的同一组场景。

优先修复接口：

- CreateTask
- PullTask
- AckTask
- NackTask
- ListAuditEvents
- Approval Approve/Reject

### P1：补齐 Event/Audit 持久化闭环

建议：

- 明确事件写入路径：业务服务直接写 EventService，或统一从 NATS event stream 投影到 PostgreSQL。
- 如果采用 NATS 投影，需要增加独立 projector，订阅事件并写入 `audit_event_projection`。
- Audit API 不应返回占位空数组，应接入真实 repository。
- WebSocket fanout 可以消费同一事件流，但不应替代持久化审计。

建议接口能力：

- 按 tenant 查询。
- 按 task_id 查询。
- 按 event_type 查询。
- 按时间范围查询。
- 分页。

### P1：完善 outbox 可靠性

建议：

- `outbox_events` 增加或使用重试字段：`retry_count`、`next_attempt_at`、`last_error`。
- 发布失败时不要永久排除，应转为可重试状态。
- 扫描时使用 `FOR UPDATE SKIP LOCKED` 避免多实例重复处理。
- 达到最大重试后进入终态并触发告警。

建议测试：

- NATS 首次失败、第二次恢复后事件能成功发布。
- 多 publisher 并发扫描不会重复发布同一事件。
- 失败超过上限后不会无限重试。

### P1：修复 Agent 在线状态与并发控制

建议：

- Heartbeat 用 Redis sorted set 或数据库字段记录过期时间，而不是扫描已过期 TTL key。
- Agent offline 事件应写入 audit/event 流。
- `DispatchService.PullTask` 中的并发检查不应传入常量 `0`，应读取 Agent 当前 running task 数。
- ACK/NACK/timeout 时同步减少 running 计数。

收益：

- Budget concurrency、Agent capacity、Mailbox 调度才能形成闭环。

### P1：预算系统需要进入任务关键路径

建议：

- 在生产启动路径使用带 usage repo 的 BudgetService。
- Task 创建前检查租户预算。
- PullTask 前检查并发预算和 Agent 限流。
- 任务完成后记录实际 token/cost。
- 对超预算任务返回稳定错误码，并记录审计事件。

需要明确：

- token/cost 是由调用方上报、Agent 上报，还是系统从模型调用链路采集。
- 预算超限时任务状态是拒绝、延迟，还是进入等待队列。

### P2：改进测试基础设施

当前部分测试依赖本机固定路径：

- NATS 使用 `$HOME/go/bin/nats-server`。
- Redis 使用 `$HOME/.local/bin/redis-server`。
- PostgreSQL 测试假设本地数据库和用户存在。

建议：

- 用环境变量配置外部二进制路径。
- 缺失依赖时明确 `t.Skip`，不要直接失败。
- 对集成测试提供 docker compose 或 testcontainers 方案。
- 将单元测试、集成测试、端到端测试分开运行。

### P2：补齐启动期配置校验和健康检查

建议：

- 启动时校验 PostgreSQL、Redis、NATS、HTTP port、gRPC port 等关键配置。
- `/healthz` 只表示进程可用，`/readyz` 表示依赖可用。
- 依赖不可用时返回明确错误，并写结构化日志。

### P2：增强仓储层错误语义

建议：

- `UpdateStatus`、`AckAttempt`、`NackAttempt` 等修改操作检查 rows affected。
- 区分 not found、conflict、invalid transition、dependency unavailable。
- handler 将领域错误映射为稳定 HTTP/gRPC 错误码。

### P2：文档需要拆分“目标设计”和“当前实现”

建议：

- `Janus-detail-design.md` 保持目标架构和领域模型。
- 新增或维护 `docs/current-implementation.md`，记录当前已实现能力、未实现能力、已知限制。
- 新增 `docs/api-contract.md`，记录 proto/gateway/SDK 的真实契约和兼容性策略。
- 每次完成 P0/P1 修复后同步更新文档，避免设计文档继续漂移。

## 三、建议修复路线

### 第一阶段：恢复工程主线

目标：代码可编译，数据库可初始化，服务可启动。

任务：

- 修复 proto 生成。
- 修复重复 migration。
- 修复 gRPC port 加载。
- 修复 Task 创建 nil 返回。
- 修复路由组合和鉴权包装。

### 第二阶段：修复 Task 核心生命周期

目标：任务创建、审批、入队、拉取、ACK/NACK、DLQ 的行为与设计一致。

任务：

- 引入统一状态迁移入口。
- 修复审批绕过。
- 修复 PullTask 的 agent_id 契约。
- 修复 outbox 重试。
- 补齐核心生命周期测试。

### 第三阶段：补齐控制面闭环

目标：Audit、Budget、Agent heartbeat、SDK/API 契约可真实使用。

任务：

- 接入审计事件查询。
- 接入预算 usage repo。
- 修复 Agent offline 检测。
- 统一 SDK 和 proto/gateway 契约。
- 增加端到端测试。

## 四、最低验收清单

- 空库 migration 全部成功。
- `go test ./core/... ./server/... ./cli/... ./sdk/go/... ./demo/... ./proto/...` 编译通过。
- CreateTask 首次请求返回 task id 和状态。
- 需要审批的任务在 approve 前不能被 PullTask 拉到。
- approve 后任务可以入队并被对应 agent 拉取。
- NATS 短暂不可用时 outbox 后续能重试成功。
- Audit API 可以查到任务创建、审批、入队、ACK/NACK 等事件。
- Go/Python SDK 的 create/pull/ack 最小流程可跑通。