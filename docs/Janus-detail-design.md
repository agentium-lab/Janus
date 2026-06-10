# Janus 详细设计说明书

版本：0.1 
状态：设计草案 
关联文档：[Janus.md](./Janus.md)

---

## 1. 文档目标

本文档描述 Janus 的工程详细设计，用于指导 MVP 到早期生产版本的实现。

本文档重点回答：

- Janus 的核心服务如何拆分。
- Agent、Task、Mailbox、Event、Policy、Budget 如何建模。
- 默认使用 NATS JetStream 时，stream、subject、consumer 如何设计。
- Task 生命周期、ACK、Retry、DLQ、幂等如何实现。
- A2A / ACP / MCP 与 Janus 内部模型如何映射。
- Queue/Event Driver 如何保留后期扩展能力。
- 安全、审计、观测、部署和测试如何落地。

本文档不重复产品定位和商业化论证；这些内容以 [Janus.md](./Janus.md) 为准。

---

## 2. 设计边界

### 2.1 当前阶段目标

Janus 第一阶段默认使用 **NATS JetStream** 作为统一消息与事件后端，覆盖：

- Durable Agent Mailbox
- Pull-based Task Dispatch
- ACK / NACK
- Retry / DLQ
- Task lifecycle events
- Trace events
- Audit events
- Billing events

PostgreSQL 存储控制面元数据、任务当前状态、审计查询投影和策略配置。

Valkey / Redis 作为可选缓存与限流计数器，不作为强依赖。

### 2.2 后期扩展目标

Janus 业务层不得直接依赖 NATS 细节。必须通过 `Queue/Event Driver` 隔离底层后端。

后期可以根据规模和客户场景引入：

- Pulsar
- Kafka
- AutoMQ
- RocketMQ
- 客户自带事件平台

但第一阶段不实现多后端，只保留接口边界。

### 2.3 非目标

第一阶段不做：

- 通用工作流引擎
- Agent Marketplace
- 自研向量数据库
- 自研身份系统
- 自研 LLM Gateway
- 多云 SaaS 控制台
- 复杂自动语义路由

---

## 3. 总体架构

```text
External Agents / Systems
- A2A Agents
- ACP-compatible Agents
- LangGraph / AutoGen / CrewAI
- CI / DevOps Agents
- Human Approval UI
- MCP Tool Servers

|
v

Ingress Layer
- A2A Gateway
- ACP Adapter
- SDK Gateway
- HTTP / gRPC API
- WebSocket Stream API
- MCP Tool Adapter

|
v

Core Services
- Agent Registry Service
- Task Service
- Mailbox Service
- Dispatch Service
- Policy Service
- Budget Service
- Context Reference Service
- Approval Service
- Event / Audit Service

|
v

Infrastructure Abstraction
- Queue/Event Driver
- Metadata Store
- Cache / Rate Limit Store
- Object / Artifact Store
- Vector Index

|
v

Default Runtime
- NATS JetStream
- PostgreSQL
- Valkey / Redis
- S3-compatible Object Storage
```

---

## 4. 核心领域模型

### 4.1 Tenant

`Tenant` 表示租户，是隔离边界。

职责：

- 隔离 Agent、Task、Policy、Budget。
- 对应企业客户、业务单元或开发者 workspace。
- 映射到底层 NATS account 或 subject namespace。

### 4.2 Agent

`Agent` 是可接收或发起任务的执行主体。

Agent 可以是：

- 常驻服务
- CI Runner
- 本地开发进程
- Kubernetes Job
- Serverless Worker
- 人工审批节点
- 外部 A2A Agent

### 4.3 Mailbox

`Mailbox` 是 Agent 的持久任务收件箱。

一个 Agent 可以有多个 mailbox，例如：

```text
code-reviewer.team-a.default
code-reviewer.team-a.high-priority
code-reviewer.team-a.security
```

Mailbox 支持：

- 持久化
- Pull 消费
- 优先级
- TTL
- 并发限制
- 重试策略
- DLQ

### 4.4 Task

`Task` 是 Agent 之间交接的最小工作单元。

Task 必须具备：

- 全局唯一 `task_id`
- 幂等键 `idempotency_key`
- 租户 `tenant_id`
- 来源 Agent
- 目标 Agent 或能力
- 状态机
- 策略上下文
- 预算上下文
- Trace 上下文
- Payload
- Context references

### 4.5 Event

`Event` 是 Janus 的不可变事实记录。

事件用于：

- 审计
- Trace
- Replay
- Billing
- 故障复盘
- 数据分析
- 当前状态投影

### 4.6 Policy

`Policy` 决定某个任务、工具调用、上下文引用或路由动作是否允许发生。

### 4.7 Budget

`Budget` 描述租户、团队、Agent、模型或任务级别的资源预算。

预算维度：

- RPM
- TPM
- 并发数
- 每任务最大 Token
- 每任务最大成本
- 每日 / 每月成本
- 模型供应商额度

---

## 5. 服务拆分

### 5.1 A2A Gateway

职责：

- 接收 A2A Agent Card 注册。
- 将 A2A task / message 映射为 Janus Task Envelope。
- 将 Janus task status 映射回 A2A 状态。
- 支持同步提交和异步提交。
- 支持 Server-Sent Events 或 WebSocket 状态流。

不负责：

- 任务持久化。
- 策略判断。
- 调度。

### 5.2 SDK Gateway

职责：

- 给 Go、Python、TypeScript SDK 提供统一接入。
- 封装 publish、pull、ack、heartbeat、status update。
- 支持本地开发和 CI 场景。

### 5.3 MCP Tool Adapter

职责：

- 把 MCP Tool Server 作为外部工具资源接入 Janus。
- 为工具调用生成 `tool.*` audit events。
- 在工具调用前执行 Policy 和 DLP hook。

MCP 不作为 Janus 的主 Agent 通信协议。

### 5.4 Agent Registry Service

职责：

- 注册 Agent。
- 更新 Agent 能力。
- 维护 Agent online / offline / degraded 状态。
- 维护 mailbox 绑定关系。
- 暴露 Agent discovery API。

关键接口：

```text
RegisterAgent
UpdateAgent
Heartbeat
ListAgents
ResolveCapability
GetAgentStatus
```

### 5.5 Task Service

职责：

- 创建 Task。
- 校验 Task Envelope。
- 执行幂等检查。
- 写入任务当前状态。
- 发布 task.created / task.queued 事件。
- 调用 Queue/Event Driver 写入 mailbox。

### 5.6 Mailbox Service

职责：

- 创建 mailbox。
- 查询 backlog。
- 暂停 / 恢复 mailbox。
- 更新 mailbox 并发、优先级、TTL、retry policy。
- 映射到底层 NATS stream / subject / consumer。

### 5.7 Dispatch Service

职责：

- 支持 Agent pull task。
- 执行 dispatch-time policy check。
- 执行 budget reservation。
- 维护 claim lease。
- 处理 ACK / NACK / timeout。

Dispatch Service 是 Janus 数据面核心。

### 5.8 Policy Service

职责：

- 执行访问控制。
- 执行上下文访问策略。
- 执行工具访问策略。
- 判断是否需要人工审批。
- 生成 policy decision event。

第一阶段使用内置规则模型，后续可接入 OPA / Cedar。

### 5.9 Budget Service

职责：

- Token / cost / concurrency 预算检查。
- 资源预留。
- 成本结算。
- 超预算拦截。
- 限流事件记录。

### 5.10 Context Reference Service

职责：

- 管理上下文引用。
- 存储 context metadata。
- 不默认存储完整敏感正文。
- 支持摘要、裁剪、脱敏、权限判断。

### 5.11 Approval Service

职责：

- 创建人工审批请求。
- 记录审批结果。
- 审批超时处理。
- 继续或拒绝任务。

### 5.12 Event / Audit Service

职责：

- 统一发布 Janus events。
- 保证 event envelope 格式一致。
- 写入 NATS event stream。
- 同步或异步投影到 PostgreSQL 查询表。
- 后期导出到 Kafka / AutoMQ / SIEM / 数据湖。

---

## 6. Task Envelope

### 6.1 标准 Envelope

```json
{
"janus_version": "0.1",
"task_id": "task_01HZY8Q9ZWZK3F8R0V8Y5B7M2Q",
"idempotency_key": "repo-123-pr-456-review",
"tenant_id": "acme",
"source_agent": "product-agent.team-a",
"target": {
"type": "capability",
"value": "code_review"
},
"priority": "normal",
"deadline": "2026-06-10T10:00:00Z",
"ttl_seconds": 86400,
"budget": {
"max_tokens": 120000,
"max_cost_usd": 3.0,
"model_classes": ["coding", "reasoning"]
},
"policy": {
"data_classification": "internal",
"requires_human_approval": false,
"allowed_tools": ["git.diff", "test.runner"]
},
"context_refs": [
{
"type": "git_pr",
"uri": "github://acme/repo/pull/456",
"hash": "sha256:..."
}
],
"payload": {
"type": "code_review_request",
"content": "Review this PR for correctness and security."
},
"trace": {
"trace_id": "trace_01HZY8Q9ZWZK3F8R0V8Y5B7M2Q",
"parent_task_id": null,
"span_id": "span_01HZY8Q..."
}
}
```

### 6.2 字段规则

| 字段 | 必填 | 说明 |
| :--- | :--- | :--- |
| `janus_version` | 是 | Envelope 版本 |
| `task_id` | 是 | 全局唯一 ID |
| `idempotency_key` | 否 | 幂等键，同租户内唯一 |
| `tenant_id` | 是 | 租户 ID |
| `source_agent` | 是 | 来源 Agent |
| `target` | 是 | 目标 Agent、mailbox 或 capability |
| `priority` | 否 | `low` / `normal` / `high` / `critical` |
| `deadline` | 否 | 截止时间 |
| `ttl_seconds` | 否 | 任务最大存活时间 |
| `budget` | 否 | 预算约束 |
| `policy` | 否 | 策略上下文 |
| `context_refs` | 否 | 上下文引用 |
| `payload` | 是 | 业务载荷 |
| `trace` | 是 | 链路追踪信息 |

---

## 7. Task 生命周期

### 7.1 状态机

```text
created
-> queued
-> approval_pending
-> claimed
-> running
-> completed

running
-> blocked
-> failed
-> cancelled

failed
-> retry_scheduled
-> dead_lettered

retry_scheduled
-> queued

queued
-> expired
-> cancelled
```

### 7.2 状态说明

| 状态 | 含义 |
| :--- | :--- |
| `created` | Task 已创建但未入队 |
| `queued` | Task 已进入 mailbox 等待消费 |
| `approval_pending` | Task 需要人工审批 |
| `claimed` | Agent 已拉取但未开始执行 |
| `running` | Agent 正在执行 |
| `blocked` | 等待外部条件，例如人工输入、依赖任务、资源恢复 |
| `completed` | 成功完成 |
| `failed` | 当前 attempt 失败 |
| `retry_scheduled` | 已安排重试 |
| `dead_lettered` | 超过重试或策略禁止，进入 DLQ |
| `expired` | 超过 TTL 或 deadline |
| `cancelled` | 被用户、策略或系统取消 |

### 7.3 状态转换约束

- 只有 `queued` 任务可以被 claim。
- 只有当前 claim owner 可以 ACK / NACK。
- `completed`、`dead_lettered`、`expired`、`cancelled` 是终态。
- 任意非终态任务都可以被管理员取消。
- `approval_pending` 只能由 Approval Service 转为 `queued` 或 `cancelled`。

---

## 8. PostgreSQL 数据模型

PostgreSQL 存储控制面元数据和当前状态投影。事件事实仍以 NATS event stream 为准。

### 8.1 tenants

```sql
create table tenants (
id text primary key,
name text not null,
status text not null default 'active',
created_at timestamptz not null default now(),
updated_at timestamptz not null default now()
);
```

### 8.2 agents

```sql
create table agents (
id text not null,
tenant_id text not null references tenants(id),
display_name text not null,
protocol text not null,
endpoint text,
status text not null default 'offline',
description text,
metadata jsonb not null default '{}',
created_at timestamptz not null default now(),
updated_at timestamptz not null default now(),
last_heartbeat_at timestamptz,
primary key (tenant_id, id)
);
```

### 8.3 agent_capabilities

```sql
create table agent_capabilities (
tenant_id text not null,
agent_id text not null,
capability text not null,
schema jsonb,
description text,
embedding_ref text,
created_at timestamptz not null default now(),
primary key (tenant_id, agent_id, capability)
);
```

### 8.4 mailboxes

```sql
create table mailboxes (
tenant_id text not null,
id text not null,
agent_id text not null,
status text not null default 'active',
priority text not null default 'normal',
max_concurrency integer not null default 1,
ack_wait_seconds integer not null default 300,
max_deliver integer not null default 5,
retention_seconds integer not null default 604800,
created_at timestamptz not null default now(),
updated_at timestamptz not null default now(),
primary key (tenant_id, id)
);
```

### 8.5 tasks

```sql
create table tasks (
tenant_id text not null,
id text not null,
idempotency_key text,
source_agent text not null,
target_type text not null,
target_value text not null,
mailbox_id text,
status text not null,
priority text not null default 'normal',
deadline timestamptz,
ttl_seconds integer,
envelope jsonb not null,
result_ref text,
error jsonb,
attempt_count integer not null default 0,
created_at timestamptz not null default now(),
updated_at timestamptz not null default now(),
completed_at timestamptz,
primary key (tenant_id, id)
);

create unique index tasks_idempotency_idx
on tasks (tenant_id, idempotency_key)
where idempotency_key is not null;

create index tasks_status_idx
on tasks (tenant_id, status, priority, created_at);
```

### 8.6 task_attempts

```sql
create table task_attempts (
tenant_id text not null,
task_id text not null,
attempt integer not null,
agent_id text not null,
lease_id text not null,
status text not null,
started_at timestamptz not null default now(),
heartbeat_at timestamptz,
finished_at timestamptz,
error jsonb,
token_usage jsonb,
primary key (tenant_id, task_id, attempt)
);
```

### 8.7 budgets

```sql
create table budgets (
tenant_id text not null,
scope_type text not null,
scope_id text not null,
rpm integer,
tpm integer,
max_concurrency integer,
daily_cost_usd numeric(18, 6),
monthly_cost_usd numeric(18, 6),
created_at timestamptz not null default now(),
updated_at timestamptz not null default now(),
primary key (tenant_id, scope_type, scope_id)
);
```

### 8.8 policy_rules

```sql
create table policy_rules (
tenant_id text not null,
id text not null,
name text not null,
status text not null default 'active',
priority integer not null default 100,
condition jsonb not null,
action jsonb not null,
created_at timestamptz not null default now(),
updated_at timestamptz not null default now(),
primary key (tenant_id, id)
);
```

### 8.9 approvals

```sql
create table approvals (
tenant_id text not null,
id text not null,
task_id text not null,
status text not null default 'pending',
requested_by text not null,
approver text,
reason text,
decision text,
expires_at timestamptz,
created_at timestamptz not null default now(),
decided_at timestamptz,
primary key (tenant_id, id)
);
```

### 8.10 audit_event_projection

```sql
create table audit_event_projection (
tenant_id text not null,
event_id text not null,
event_type text not null,
task_id text,
agent_id text,
trace_id text,
occurred_at timestamptz not null,
payload jsonb not null,
primary key (tenant_id, event_id)
);

create index audit_event_task_idx
on audit_event_projection (tenant_id, task_id, occurred_at);

create index audit_event_trace_idx
on audit_event_projection (tenant_id, trace_id, occurred_at);
```

---

## 9. NATS JetStream 设计

### 9.1 Subject 命名

统一 subject 命名：

```text
janus.<tenant>.tasks.<mailbox>
janus.<tenant>.tasks_retry.<mailbox>
janus.<tenant>.tasks_dlq.<mailbox>
janus.<tenant>.events.<event_type>
janus.<tenant>.agent_status.<agent_id>
janus.<tenant>.heartbeats.<agent_id>
```

示例：

```text
janus.acme.tasks.code-reviewer.team-a.default
janus.acme.events.task.completed
janus.acme.tasks_dlq.code-reviewer.team-a.default
```

### 9.2 Stream 设计

| Stream | Subject | 用途 | Retention |
| :--- | :--- | :--- | :--- |
| `JANUS_TASKS` | `janus.*.tasks.>` | 主任务投递 | Work queue |
| `JANUS_TASK_RETRY` | `janus.*.tasks_retry.>` | 延迟重试任务 | Limits / Interest |
| `JANUS_TASK_DLQ` | `janus.*.tasks_dlq.>` | 死信任务 | Limits |
| `JANUS_EVENTS` | `janus.*.events.>` | 审计与 trace 事件 | Limits |
| `JANUS_AGENT_STATUS` | `janus.*.agent_status.>` | Agent 状态变化 | Limits |
| `JANUS_HEARTBEATS` | `janus.*.heartbeats.>` | 心跳事件 | Limits / short TTL |

### 9.3 建议配置

MVP 单集群：

```yaml
nats:
jetstream:
enabled: true
storage: file
streams:
tasks:
replicas: 1
retention: workqueue
max_age: 7d
events:
replicas: 1
retention: limits
max_age: 30d
```

生产私有化：

```yaml
nats:
jetstream:
enabled: true
storage: file
cluster:
replicas: 3
streams:
tasks:
replicas: 3
retention: workqueue
max_age: 7d
dlq:
replicas: 3
retention: limits
max_age: 30d
events:
replicas: 3
retention: limits
max_age: 90d
```

### 9.4 Consumer 设计

每个 mailbox 对应一个或多个 durable pull consumer。

命名：

```text
consumer.<tenant>.<mailbox>
```

配置：

```yaml
consumer:
durable_name: consumer.acme.code-reviewer.team-a.default
ack_policy: explicit
ack_wait: 300s
max_deliver: 5
max_ack_pending: 100
deliver_policy: all
```

### 9.5 ACK / NACK 映射

| Janus 操作 | NATS 操作 |
| :--- | :--- |
| Agent pull | Consumer fetch |
| Agent claim | 写入 task_attempts，返回 lease_id |
| Agent ACK | NATS ACK + task completed |
| Agent NACK retriable | NATS NAK / Janus retry stream |
| Agent NACK non-retriable | ACK 原消息 + 写 DLQ |
| Agent timeout | NATS redelivery + task failed event |

Janus 不应完全依赖 NATS redelivery 表达业务重试。推荐由 Janus 自己维护 retry policy，NATS redelivery 作为底层兜底。

### 9.6 Retry 设计

重试策略：

```yaml
retry:
max_attempts: 5
backoff:
type: exponential
initial_seconds: 10
max_seconds: 900
jitter: true
```

流程：

```text
1. Agent NACK 或 attempt timeout
2. Task Service 判断是否可重试
3. 写 task.failed event
4. 如果可重试，写 task.retry_scheduled event
5. 到达 backoff 时间后重新写入 mailbox
6. 如果不可重试，写入 DLQ
```

### 9.7 DLQ 设计

DLQ 消息包含：

- 原始 Task Envelope
- 最后一次错误
- attempt_count
- failure_reason
- policy_decision_id
- first_failed_at
- dead_lettered_at

DLQ 支持：

- 查询
- 重放
- 丢弃
- 修改目标后重放
- 导出

---

## 10. Queue/Event Driver 接口

第一阶段只实现 `nats-jetstream`，但内部必须依赖抽象接口。

### 10.1 Go 接口草案

```go
type QueueEventDriver interface {
PublishTask(ctx context.Context, msg TaskMessage) error
FetchTasks(ctx context.Context, mailbox string, opts FetchOptions) ([]TaskDelivery, error)
AckTask(ctx context.Context, delivery DeliveryRef) error
NackTask(ctx context.Context, delivery DeliveryRef, reason NackReason) error
PublishEvent(ctx context.Context, event JanusEvent) error
ReplayEvents(ctx context.Context, filter EventReplayFilter) (EventIterator, error)
EnsureTenant(ctx context.Context, tenantID string) error
EnsureMailbox(ctx context.Context, mailbox MailboxSpec) error
EnsureConsumer(ctx context.Context, consumer ConsumerSpec) error
}
```

### 10.2 抽象对象

```go
type TaskMessage struct {
TenantID string
MailboxID string
TaskID string
Priority string
Payload []byte
Headers map[string]string
}

type TaskDelivery struct {
TaskID string
Payload []byte
DeliveryRef DeliveryRef
RedeliveryCount int
}

type JanusEvent struct {
EventID string
EventType string
TenantID string
TraceID string
TaskID string
AgentID string
OccurredAt time.Time
Payload []byte
}
```

### 10.3 设计原则

- 业务服务只理解 `mailbox`、`task`、`event`，不理解 NATS subject。
- Driver 负责 subject / topic / stream 映射。
- Driver 负责 offset / sequence / cursor 映射。
- Driver 负责底层错误分类。
- Driver 不负责业务状态机。

---

## 11. Dispatch 流程

### 11.1 Publish 流程

```text
1. Ingress 接收任务
2. Envelope validation
3. Idempotency check
4. Source agent authorization
5. Resolve target
6. Policy check
7. Budget pre-check
8. Create task row
9. Publish task.created event
10. Publish to mailbox
11. Update task status to queued
12. Publish task.queued event
13. Return ACK to caller
```

### 11.2 Pull 流程

```text
1. Agent 调用 PullTask(mailbox)
2. 校验 Agent 身份
3. 检查 mailbox 是否属于该 Agent
4. 检查 Agent concurrency
5. 从 Queue/Event Driver fetch
6. Dispatch-time policy check
7. Budget reservation
8. 创建 task_attempt
9. 更新 task status = claimed
10. 返回 Task Envelope + lease_id
```

### 11.3 Start / Heartbeat / Complete

```text
Agent Start:
claimed -> running
publish task.started

Agent Heartbeat:
update task_attempt.heartbeat_at
publish optional task.heartbeat

Agent Complete:
validate lease_id
settle budget
store result_ref
running -> completed
ack driver delivery
publish task.completed
```

### 11.4 NACK / Failure

```text
Agent NACK:
validate lease_id
release or settle partial budget
update attempt failed
if retry allowed:
schedule retry
else:
move to DLQ
ack original delivery when Janus has persisted next state
```

---

## 12. 路由设计

### 12.1 目标类型

Task target 支持：

```text
agent 明确指定某个 Agent
mailbox 明确指定某个 mailbox
capability 指定能力，由 Janus 决定目标
group 指定 Agent group
human 指定人工审批节点
```

### 12.2 路由流程

```text
1. 读取 target
2. 如果是 agent/mailbox，直接解析
3. 如果是 capability，进入候选搜索
4. 过滤 tenant / team / policy
5. 过滤 online / degraded / capacity
6. 过滤 budget / model class
7. 过滤 data classification
8. 按 capability tag / schema 评分
9. 可选 semantic ranking
10. 选择目标 mailbox
```

### 12.3 语义路由位置

语义路由只用于候选排序，不允许绕过：

- 权限
- 租户隔离
- 数据分级
- 预算限制
- 人工审批策略

---

## 13. Budget 与 Backpressure

### 13.1 预算层级

预算按层级叠加：

```text
tenant
-> team
-> agent
-> model_provider
-> model
-> task
```

任一层级不足，都应阻止 dispatch。

### 13.2 Token Reservation

Pull 阶段做预算预留：

```text
reserved_tokens = min(task.budget.max_tokens, estimated_tokens)
```

Complete 阶段做结算：

```text
actual_tokens = prompt_tokens + completion_tokens
release reserved_tokens - actual_tokens
record budget.consumed event
```

失败或取消时释放未使用额度。

### 13.3 Backpressure 策略

当预算或容量不足时：

- 不从 mailbox 分发新任务。
- 任务继续保留在 NATS / mailbox 中。
- 记录 `scheduler.throttled` event。
- 对调用方返回可解释原因。

常见原因：

```text
tenant_tpm_exceeded
agent_concurrency_exceeded
model_rpm_exceeded
daily_budget_exceeded
approval_required
```

---

## 14. Policy 设计

### 14.1 Policy 输入

```json
{
"tenant_id": "acme",
"actor": {
"type": "agent",
"id": "coding-agent.team-a"
},
"action": "task.publish",
"resource": {
"type": "capability",
"value": "production_release"
},
"context": {
"data_classification": "confidential",
"tools": ["deploy.prod"],
"cost_estimate_usd": 12.5
}
}
```

### 14.2 Policy 输出

```json
{
"decision": "approval_required",
"decision_id": "poldec_01HZY...",
"matched_rules": ["prod-release-human-approval"],
"reason": "Production release requires human approval."
}
```

### 14.3 决策类型

```text
allow
deny
approval_required
redact_context
reduce_context_scope
throttle
```

---

## 15. Event / Audit Plane

### 15.1 Event Envelope

```json
{
"event_id": "evt_01HZY8Q9ZWZK3F8R0V8Y5B7M2Q",
"event_type": "task.completed",
"timestamp": "2026-06-09T12:00:00Z",
"tenant_id": "acme",
"trace_id": "trace_01HZY...",
"task_id": "task_01HZY...",
"source_agent": "coding-agent.team-a",
"target_agent": "review-agent.team-a",
"actor": {
"type": "agent",
"id": "coding-agent.team-a"
},
"payload": {
"status": "completed",
"duration_ms": 184000,
"result_ref": "janus://artifacts/task_01HZY/result"
},
"security": {
"data_classification": "internal",
"policy_decision_id": "poldec_01HZY..."
}
}
```

### 15.2 事件类型

Task:

```text
task.created
task.queued
task.approval_pending
task.claimed
task.started
task.heartbeat
task.blocked
task.completed
task.failed
task.retry_scheduled
task.dead_lettered
task.cancelled
task.expired
```

Agent:

```text
agent.registered
agent.updated
agent.online
agent.offline
agent.heartbeat
agent.capacity_changed
agent.capability_changed
```

Policy:

```text
policy.allowed
policy.denied
policy.approval_required
policy.dlp_redacted
policy.context_scope_reduced
```

Budget:

```text
budget.reserved
budget.consumed
budget.released
budget.exceeded
```

Tool:

```text
tool.invocation_requested
tool.invocation_allowed
tool.invocation_denied
tool.invocation_started
tool.invocation_completed
tool.invocation_failed
```

System:

```text
queue.backlog_high
consumer.lag_high
scheduler.throttled
storage.error
node.unhealthy
```

### 15.3 不可变性

事件一旦写入，不允许更新。修正错误时写入补偿事件，例如：

```text
audit.corrected
task.metadata_corrected
budget.adjusted
```

如果进入合规场景，需要增加：

- event hash chain
- signed event batches
- periodic checkpoint
- WORM object storage export

---

## 16. Context Reference 设计

### 16.1 原则

Janus 默认不在 Task Envelope 中携带大段上下文正文，而是携带引用：

```text
git_pr
file_snapshot
artifact
ticket
document
conversation_summary
database_query_result
```

### 16.2 Context Ref

```json
{
"type": "git_pr",
"uri": "github://acme/repo/pull/456",
"hash": "sha256:...",
"classification": "internal",
"expires_at": "2026-06-16T00:00:00Z",
"access_scope": ["code-reviewer.team-a"]
}
```

### 16.3 Context 处理事件

```text
context.attached
context.access_allowed
context.access_denied
context.trimmed
context.summarized
context.redacted
context.expired
```

---

## 17. API 设计

### 17.1 HTTP API

Agent:

```text
POST /v1/tenants/{tenant_id}/agents
PATCH /v1/tenants/{tenant_id}/agents/{agent_id}
POST /v1/tenants/{tenant_id}/agents/{agent_id}/heartbeat
GET /v1/tenants/{tenant_id}/agents
GET /v1/tenants/{tenant_id}/agents/{agent_id}
```

Task:

```text
POST /v1/tenants/{tenant_id}/tasks
GET /v1/tenants/{tenant_id}/tasks/{task_id}
POST /v1/tenants/{tenant_id}/tasks/{task_id}/cancel
POST /v1/tenants/{tenant_id}/tasks/{task_id}/replay
```

Mailbox:

```text
POST /v1/tenants/{tenant_id}/mailboxes/{mailbox_id}/pull
POST /v1/tenants/{tenant_id}/tasks/{task_id}/start
POST /v1/tenants/{tenant_id}/tasks/{task_id}/heartbeat
POST /v1/tenants/{tenant_id}/tasks/{task_id}/ack
POST /v1/tenants/{tenant_id}/tasks/{task_id}/nack
```

Audit:

```text
GET /v1/tenants/{tenant_id}/events
GET /v1/tenants/{tenant_id}/traces/{trace_id}
GET /v1/tenants/{tenant_id}/tasks/{task_id}/events
```

### 17.2 Pull 响应示例

```json
{
"task": {
"task_id": "task_01HZY...",
"envelope": {}
},
"lease": {
"lease_id": "lease_01HZY...",
"expires_at": "2026-06-09T12:05:00Z"
}
}
```

### 17.3 ACK 请求示例

```json
{
"lease_id": "lease_01HZY...",
"result_ref": "janus://artifacts/task_01HZY/result",
"token_usage": {
"prompt_tokens": 12000,
"completion_tokens": 3000,
"total_tokens": 15000
}
}
```

### 17.4 NACK 请求示例

```json
{
"lease_id": "lease_01HZY...",
"retriable": true,
"error": {
"code": "model_rate_limited",
"message": "Model provider returned rate limit."
}
}
```

---

## 18. A2A / ACP / MCP 映射

### 18.1 A2A Agent Card 映射

| A2A 概念 | Janus 概念 |
| :--- | :--- |
| Agent Card | Agent Registry record |
| Skills / capabilities | agent_capabilities |
| Endpoint | agent.endpoint |
| Authentication | Agent identity |
| Task / message | Janus Task Envelope |
| Task status | Janus Task lifecycle |

### 18.2 ACP 映射

ACP-compatible Agent 通过 adapter 接入，映射原则和 A2A 一致：

- 能力声明进入 Registry。
- 消息进入 Task Envelope。
- 状态进入 Task lifecycle。
- 审计进入 Event / Audit Plane。

### 18.3 MCP 映射

MCP 只作为工具和上下文资源入口。

| MCP 概念 | Janus 概念 |
| :--- | :--- |
| Tool | Tool resource |
| Tool call | tool invocation event |
| Resource | Context ref |
| Prompt | Payload template |

---

## 19. 安全设计

### 19.1 身份认证

支持：

- API key
- mTLS
- OIDC token
- Workload identity

MVP 可先支持 API key + mTLS，企业版接入 OIDC / SSO。

### 19.2 授权

授权维度：

- Tenant
- Team
- Agent
- Mailbox
- Capability
- Tool
- Context classification
- Action

### 19.3 租户隔离

第一阶段使用：

- PostgreSQL tenant_id 逻辑隔离。
- NATS subject namespace 隔离。
- Janus Policy 强制检查。

企业版增强：

- NATS account 隔离。
- 独立 encryption key。
- 独立 storage bucket。
- 独立 deployment namespace。

### 19.4 Secret 管理

Janus 不应在 Task Envelope 中存储明文 secret。

Secret 应通过：

- Secret reference
- Vault
- Kubernetes Secret
- 云厂商 secret manager

传递。

### 19.5 DLP

DLP hook 位置：

- Publish 前
- Dispatch 前
- Context attach 前
- Tool call 前
- Audit projection 前

---

## 20. 可观测性

### 20.1 Metrics

核心指标：

```text
janus_tasks_created_total
janus_tasks_completed_total
janus_tasks_failed_total
janus_tasks_dead_lettered_total
janus_task_latency_seconds
janus_queue_backlog
janus_pull_requests_total
janus_ack_total
janus_nack_total
janus_budget_throttle_total
janus_policy_denied_total
janus_agent_online
janus_agent_heartbeat_lag_seconds
```

### 20.2 Tracing

每个 Task 必须带：

- trace_id
- span_id
- parent_task_id
- source_agent
- target_agent

Janus 服务内部使用 OpenTelemetry。

### 20.3 Logs

日志必须结构化：

```json
{
"level": "info",
"msg": "task dispatched",
"tenant_id": "acme",
"task_id": "task_01HZY...",
"agent_id": "code-reviewer.team-a",
"trace_id": "trace_01HZY..."
}
```

---

## 21. 部署设计

### 21.1 本地开发

```text
janus-api
janus-worker
nats
postgres
valkey
```

通过 Docker Compose 启动。

### 21.2 单集群生产

```text
Kubernetes namespace: janus

Deployments:
janus-api
janus-dispatcher
janus-event-projector
janus-scheduler

Stateful:
nats cluster
postgres
valkey
```

### 21.3 高可用

建议：

- Janus stateless 服务至少 2 副本。
- NATS JetStream 3 节点。
- PostgreSQL 使用托管 HA 或主从。
- Object storage 使用云厂商或企业对象存储。

---

## 22. 故障处理

### 22.1 Agent 离线

处理：

- 心跳超时后标记 offline。
- 不再 dispatch 新任务。
- 已 queued 任务保留在 mailbox。
- 已 running 任务按 lease timeout 处理。

### 22.2 Agent 执行超时

处理：

- attempt 标记 failed。
- 释放预算。
- 根据 retry policy 重试或 DLQ。

### 22.3 Janus API 重启

处理：

- API 无状态，可水平恢复。
- 已持久化 task 不丢失。
- event projector 可从 stream replay。

### 22.4 NATS 不可用

处理：

- Publish 失败返回 caller。
- 已写 DB 但未写 NATS 的任务通过 outbox scanner 补偿。
- Dispatch 暂停。
- 记录 storage.error。

### 22.5 PostgreSQL 不可用

处理：

- 控制面 API 降级。
- Publish 不应继续接受新任务，避免状态不一致。
- Dispatch 可按配置短时间继续，但 ACK 结算必须依赖 DB 恢复。

---

## 23. 一致性与 Outbox

Task 创建涉及 PostgreSQL 和 NATS 两个系统，需要避免双写不一致。

MVP 推荐使用 transactional outbox：

```text
1. DB transaction:
- insert tasks
- insert outbox_events
2. commit
3. outbox publisher publish to NATS
4. mark outbox_events published
```

Outbox 表：

```sql
create table outbox_events (
id text primary key,
tenant_id text not null,
kind text not null,
payload jsonb not null,
status text not null default 'pending',
attempts integer not null default 0,
created_at timestamptz not null default now(),
published_at timestamptz
);
```

对于任务入队，可以采用同样模式，确保任务当前状态和 NATS 消息最终一致。

---

## 24. 幂等设计

### 24.1 Publish 幂等

同租户内：

```text
(tenant_id, idempotency_key)
```

唯一。

重复提交时返回已有 task。

### 24.2 ACK 幂等

ACK 请求必须携带：

- task_id
- attempt
- lease_id

如果 task 已处于终态，返回当前状态，不重复结算预算。

### 24.3 Event 幂等

event_id 全局唯一。Projection 写入 PostgreSQL 时使用 upsert 或忽略重复事件。

---

## 25. 测试策略

### 25.1 单元测试

覆盖：

- Task state machine
- Policy decision
- Budget reservation
- Retry backoff
- Envelope validation
- Route resolution

### 25.2 集成测试

覆盖：

- PostgreSQL + NATS publish
- Pull / ACK
- NACK / retry
- DLQ
- Event projection
- Agent heartbeat timeout

### 25.3 故障测试

覆盖：

- Janus API 重启
- NATS 短暂不可用
- PostgreSQL 短暂不可用
- Agent pull 后崩溃
- ACK 重复提交
- DLQ 重放

### 25.4 负载测试

早期目标：

```text
1k active agents
10k mailboxes
100 task/s publish
500 event/s audit
p95 pull latency < 100ms
p95 publish latency < 100ms
```

这些不是最终容量承诺，只用于 MVP 性能基线。

---

## 26. MVP 交付拆分

### 26.1 Milestone 1：核心模型

- PostgreSQL schema
- Task Envelope
- Task lifecycle
- Agent Registry
- Mailbox model

### 26.2 Milestone 2：NATS Driver

- Stream 初始化
- Publish task
- Pull task
- ACK / NACK
- Event publish

### 26.3 Milestone 3：Dispatch 闭环

- Agent pull
- lease
- start
- heartbeat
- ack
- retry
- DLQ

### 26.4 Milestone 4：治理最小闭环

- Basic policy
- Basic budget
- Token reservation
- Audit events

### 26.5 Milestone 5：Coding / DevOps Demo

- Coding Agent
- Review Agent
- Test Agent
- Human approval
- Trace UI

---

## 27. 开放问题

需要后续验证：

- NATS subject 是否按 tenant 统一 stream，还是大客户独立 stream。
- DLQ 是统一 stream 还是 mailbox 级 stream。
- Heartbeat 是否写 NATS，还是只写 Redis / PostgreSQL。
- Task result 是否统一进入 object storage。
- Budget 估算模型如何基于历史执行数据迭代。
- 是否需要第一阶段引入 OPA。
- 是否需要支持 WebSocket streaming result。
- 是否需要对 A2A task status 做完整兼容映射。

---

## 28. 关键设计决策

当前确认的关键决策：

1. Janus 是 A2A-native durable broker，不是 MCP Broker。
2. MCP 作为工具和上下文接入层，不作为主通信协议。
3. 第一阶段默认使用 NATS JetStream 作为统一消息与事件后端。
4. 业务层通过 Queue/Event Driver 保留后端扩展接口。
5. PostgreSQL 存当前状态和查询投影，NATS event stream 存不可变事实事件。
6. Task Envelope 是 Janus 形成生产语义的核心。
7. 语义路由只做候选排序，不绕过策略、预算和权限。
8. Coding / DevOps Agent 协作是第一落地场景。

---

## 29. 设计澄清（已确认）

以下决策已确认，用于指导 MVP 实现。

### 29.1 NATS Stream 粒度

大客户使用独立 Stream，不共享统一 `JANUS_TASKS` stream。租户间通过 NATS subject namespace 隔离，大客户可请求独立 stream 以保证物理隔离、独立 retention 和独立扩容。

### 29.2 DLQ 粒度

每个 Mailbox 拥有独立的 DLQ stream，不共享统一 DLQ。命名规则：

```text
janus.<tenant>.tasks_dlq.<mailbox>
```

示例：

```text
janus.acme.tasks_dlq.code-reviewer.team-a.default
janus.acme.tasks_dlq.code-reviewer.team-a.security
```

DLQ 消息携带 `tenant_id`、`mailbox_id`、原始 Task Envelope、最后一次错误、`attempt_count`、`failure_reason` 和 `dead_lettered_at`。

### 29.3 Heartbeat 存储

Agent 心跳使用 **Redis** 存储，不使用 NATS 或 PostgreSQL。

| 场景 | 存储 |
|---|---|
| 实时 Heartbeat 写入与 TTL | Redis `SET agent:heartbeat:<tenant>:<agent_id> {ts} EX 60` |
| 离线检测 | 后台 sweeper 扫描 Redis TTL 过期，更新 PostgreSQL `agents.status = 'offline'` |
| 外部查询 | 优先读 Redis，fallback 读 PostgreSQL |

Redis 从"可选缓存"提升为**生产强依赖**。

### 29.4 Task Result 存储

MVP 阶段 Task Result 存入 **PostgreSQL**（`tasks.result_ref` 字段直接存储结果或引用）。后期根据数据规模和访问模式，按需迁移至 Object Storage。

### 29.5 Budget 估算模型

MVP 阶段使用**静态硬限制**，预算由调用方在 Task Envelope 中声明 `max_tokens` / `max_cost_usd`。后期基于 `task_attempts.token_usage` 历史数据迭代启发式或 ML 估算模型。

### 29.6 Policy 引擎

MVP 阶段使用**内置规则引擎**，基于 `policy_rules` 表的 JSON condition + action 模型。Policy Service 通过 `PolicyEngine` 接口抽象，后期可接入 OPA / Cedar。

```go
type PolicyEngine interface {
    Evaluate(ctx context.Context, input PolicyInput) (PolicyDecision, error)
}
```

### 29.7 WebSocket Streaming

MVP 阶段需要 WebSocket 支持，场景限定为 **Dashboard/UI 实时推送**（Task 状态变化、Agent 上线/下线、队列积压）。

实现方式：轻量 WebSocket Gateway，按 tenant/task_id/agent_id 过滤 NATS event stream 后推送。

Agent-to-Agent 通信继续使用 Pull + gRPC，不使用 WebSocket。

### 29.8 A2A Task Status 映射

A2A Gateway 层实现 A2A 标准状态与 Janus 内部状态机的完整映射。Janus 核心 Task Service 保持自身状态语义不变。

| A2A 状态 | Janus 状态 |
|---|---|
| `submitted` | `created` / `queued` / `claimed` / `retry_scheduled` |
| `working` | `running` |
| `input-required` | `blocked` / `approval_pending` |
| `completed` | `completed` |
| `failed` | `failed` / `dead_lettered` / `expired` |
| `canceled` | `cancelled` |

### 29.9 Agent SDK

MVP 提供 **Go** 和 **Python** SDK，覆盖以下 API：

```
PublishTask(envelope)    发布任务
PullTask(mailbox)       拉取任务
StartTask(lease_id)     声明开始执行
Heartbeat(lease_id)     发送心跳
AckTask(lease_id, result)  确认完成
NackTask(lease_id, error)  确认失败
RegisterAgent(spec)     注册 Agent
```

TypeScript SDK 放入后续迭代。

### 29.10 CLI

CLI 使用 **Go + cobra + viper**，与主服务同一仓库，二进制随 Docker Compose 分发。

核心命令：

```
Agent 管理:
  janus agent register  注册 Agent
  janus agent heartbeat 发送心跳
  janus agent status    查询状态

Task 操作:
  janus task publish    发布任务
  janus task status     查询任务
  janus task cancel     取消任务

Mailbox 操作:
  janus mailbox pull    拉取任务
  janus mailbox ack     确认完成
  janus mailbox nack    确认失败

运维:
  janus dashboard       启动本地 Web UI
```

### 29.11 多租户策略

数据模型从 Day 1 即携带 `tenant_id` 字段，所有表均以 `(tenant_id, id)` 为主键。MVP Demo 和 CLI 默认使用 `default` 租户，不暴露多租户 UI。

后期企业版实现完整多租户隔离（独立加密密钥、NATS account、storage bucket、deployment namespace）。

### 29.12 数据库 Migration

使用 **golang-migrate**，SQL 文件驱动。Migration 文件位于 `migrations/` 目录，按时间戳命名。

```text
migrations/
├── 000001_initial_schema.up.sql
├── 000001_initial_schema.down.sql
└── ...
```

`janus-api` 启动时自动执行 `migrate up`，再启动 HTTP/gRPC 服务。

---

## 30. 工程设计补充

### 30.1 Go 项目目录结构

采用 **Go Workspace（go.work）+ 5 个独立 module**，仓库地址 `github.com/agentium-lab/Janus`。

```text
Janus/
├── go.work                        # Go workspace 统一管理
│
├── core/                          # Module 1: 领域模型 + 接口
│   ├── go.mod                     # github.com/agentium-lab/Janus/core
│   ├── task.go                    # Task 状态机、Envelope 定义
│   ├── agent.go                   # Agent 模型
│   ├── mailbox.go                 # Mailbox 模型
│   ├── event.go                   # Event 类型定义
│   ├── policy.go                  # PolicyEngine 接口
│   ├── budget.go                  # Budget 模型
│   └── driver.go                  # Queue/Event Driver 接口
│
├── proto/                         # Module 2: Protobuf 定义
│   ├── go.mod                     # github.com/agentium-lab/Janus/proto
│   ├── janus/v1/
│   │   ├── agent.proto
│   │   ├── task.proto
│   │   ├── mailbox.proto
│   │   └── event.proto
│   └── gen/                       # 生成的 Go 代码
│
├── server/                        # Module 3: 服务端
│   ├── go.mod                     # github.com/agentium-lab/Janus/server
│   ├── cmd/
│   │   ├── janus-api/             # API 入口
│   │   ├── janus-dispatcher/      # Dispatcher 入口
│   │   └── janus-event-projector/ # Event 投影入口
│   ├── internal/
│   │   ├── handler/               # HTTP/gRPC handler
│   │   ├── service/               # 业务逻辑实现
│   │   │   ├── agent_registry.go
│   │   │   ├── task_service.go
│   │   │   ├── mailbox_service.go
│   │   │   ├── dispatch_service.go
│   │   │   ├── policy_service.go
│   │   │   ├── budget_service.go
│   │   │   ├── approval_service.go
│   │   │   └── audit_service.go
│   │   ├── driver/                # Driver 实现
│   │   │   ├── nats/              # NATS JetStream driver
│   │   │   ├── postgres/          # PostgreSQL repository
│   │   │   └── redis/             # Redis heartbeat + rate limit
│   │   ├── gateway/               # 协议适配
│   │   │   ├── a2a/               # A2A Gateway
│   │   │   ├── mcp/               # MCP Tool Adapter
│   │   │   └── websocket/         # WebSocket Gateway
│   │   └── config/                # 配置加载
│   └── config.example.yaml
│
├── sdk/
│   ├── go/                        # Module 4: Go SDK
│   │   ├── go.mod                 # github.com/agentium-lab/Janus/sdk/go
│   │   ├── client.go              # JanusClient
│   │   ├── publish.go
│   │   ├── pull.go
│   │   ├── agent.go
│   │   └── options.go
│   └── python/                    # Python SDK（非 Go module）
│       ├── pyproject.toml
│       └── janus_sdk/
│           ├── client.py
│           ├── publish.py
│           └── pull.py
│
├── cli/                           # Module 5: CLI
│   ├── go.mod                     # github.com/agentium-lab/Janus/cli
│   ├── main.go
│   ├── agent.go                   # janus agent register/status/heartbeat
│   ├── task.go                    # janus task publish/status/cancel
│   └── mailbox.go                 # janus mailbox pull/ack/nack
│
├── migrations/                    # SQL migration 文件
│   ├── 000001_initial_schema.up.sql
│   └── 000001_initial_schema.down.sql
│
├── deployments/
│   ├── docker-compose.yaml        # 本地开发一键启动
│   └── helm/                      # Kubernetes 部署
│
├── configs/
│   └── janus.example.yaml         # 示例配置
│
└── docs/                          # 设计文档
```

模块依赖关系：

```text
proto ← core（接口中的消息类型）
proto ← server（handler 层序列化）
proto ← sdk/go（客户端序列化）
core  ← server（实现 core 中的接口）
sdk/go ← cli（CLI 调用 SDK）
```

设计要点：

- `core` 零外部依赖，Enterprise repo 只需 import core 即可扩展接口。
- `server` 是最大模块，MVP 不拆 driver 为独立 module，通过 core 接口解耦。
- `sdk/go` 独立版本，外部 Agent import SDK 不拉服务端依赖。
- `proto` 独立，Go/Python SDK 共享同一份 proto 定义。
- `cli` 依赖 `sdk/go`，CLI 是 SDK 的薄封装。

### 30.2 gRPC Service 定义

HTTP + gRPC 双协议并存，通过 `grpc-gateway` 一份 proto 生成两套 API。

```protobuf
// Agent Service
service AgentService {
  rpc RegisterAgent(RegisterAgentRequest) returns (Agent);
  rpc UpdateAgent(UpdateAgentRequest) returns (Agent);
  rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
  rpc ListAgents(ListAgentsRequest) returns (ListAgentsResponse);
  rpc GetAgent(GetAgentRequest) returns (Agent);
}

// Task Service
service TaskService {
  rpc CreateTask(CreateTaskRequest) returns (Task);
  rpc GetTask(GetTaskRequest) returns (Task);
  rpc CancelTask(CancelTaskRequest) returns (Task);
  rpc ReplayTask(ReplayTaskRequest) returns (Task);
}

// Dispatch Service (Mailbox 操作)
service DispatchService {
  rpc PullTask(PullTaskRequest) returns (PullTaskResponse);
  rpc StartTask(StartTaskRequest) returns (StartTaskResponse);
  rpc TaskHeartbeat(TaskHeartbeatRequest) returns (TaskHeartbeatResponse);
  rpc AckTask(AckTaskRequest) returns (AckTaskResponse);
  rpc NackTask(NackTaskRequest) returns (NackTaskResponse);
}

// Audit Service
service AuditService {
  rpc ListEvents(ListEventsRequest) returns (ListEventsResponse);
  rpc GetTrace(GetTraceRequest) returns (GetTraceResponse);
  rpc ListTaskEvents(ListTaskEventsRequest) returns (ListTaskEventsResponse);
}
```

gRPC Method 与 HTTP API 对应关系：

| gRPC Method | HTTP 端点 |
|---|---|
| `RegisterAgent` | POST /v1/tenants/{tenant_id}/agents |
| `UpdateAgent` | PATCH /v1/tenants/{tenant_id}/agents/{agent_id} |
| `Heartbeat` | POST /v1/tenants/{tenant_id}/agents/{agent_id}/heartbeat |
| `ListAgents` | GET /v1/tenants/{tenant_id}/agents |
| `GetAgent` | GET /v1/tenants/{tenant_id}/agents/{agent_id} |
| `CreateTask` | POST /v1/tenants/{tenant_id}/tasks |
| `GetTask` | GET /v1/tenants/{tenant_id}/tasks/{task_id} |
| `CancelTask` | POST /v1/tenants/{tenant_id}/tasks/{task_id}/cancel |
| `ReplayTask` | POST /v1/tenants/{tenant_id}/tasks/{task_id}/replay |
| `PullTask` | POST /v1/tenants/{tenant_id}/mailboxes/{mailbox_id}/pull |
| `StartTask` | POST /v1/tenants/{tenant_id}/tasks/{task_id}/start |
| `TaskHeartbeat` | POST /v1/tenants/{tenant_id}/tasks/{task_id}/heartbeat |
| `AckTask` | POST /v1/tenants/{tenant_id}/tasks/{task_id}/ack |
| `NackTask` | POST /v1/tenants/{tenant_id}/tasks/{task_id}/nack |
| `ListEvents` | GET /v1/tenants/{tenant_id}/events |
| `GetTrace` | GET /v1/tenants/{tenant_id}/traces/{trace_id} |
| `ListTaskEvents` | GET /v1/tenants/{tenant_id}/tasks/{task_id}/events |

### 30.3 Go 依赖选型

| 模块 | 选型 | 说明 |
|---|---|---|
| HTTP 框架 | `net/http` + `grpc-gateway` | gRPC 转 HTTP，一份 proto 双协议 |
| gRPC 框架 | `google.golang.org/grpc` | 标准库 |
| Proto 生成 | `buf` | 比 protoc + 插件更现代 |
| PostgreSQL 驱动 | `pgx` | Go 生态 PG 首选 |
| 数据库操作 | `pgx` 原生 `pgxpool` | 不用 ORM，SQL 手写 |
| NATS 客户端 | `nats.go` + `nats.go/jetstream` | 官方库 |
| NATS 部署 | **单独部署** | 不嵌入 Janus 进程 |
| Cache 客户端 | `go-redis/redis` | 支持 standalone / sentinel / cluster |
| CLI 框架 | `cobra` + `viper` | |
| 配置 | `viper` | YAML + 环境变量 |
| 日志 | `slog` | Go 1.21+ 标准库结构化日志 |
| Metrics | `prometheus/client_golang` | Prometheus 生态 |
| Tracing | `otel` + `otel/trace` | OpenTelemetry |
| Migration | `golang-migrate` | SQL 文件驱动 |
| JSON | `encoding/json` | 标准库 |
| ID 生成 | `ULID` | 有序、唯一、可排序 |
| 测试 | `testify` | 断言 + mock |

### 30.4 配置模型

配置文件 `janus.yaml`，加载优先级：配置文件 < 环境变量 < 命令行 flag。

```yaml
# janus.yaml

server:
  http_port: 8080
  grpc_port: 9090

nats:
  url: "nats://localhost:4222"

postgres:
  host: "localhost"
  port: 5432
  user: "janus"
  password: "${JANUS_PG_PASSWORD}"
  database: "janus"
  max_conns: 20

cache:
  driver: "redis"
  mode: "standalone"           # standalone / sentinel / cluster
  # standalone 模式
  addr: "localhost:6379"
  # sentinel 模式
  sentinel:
    master_name: "mymaster"
    addrs:
      - "sentinel1:26379"
      - "sentinel2:26379"
      - "sentinel3:26379"
  # cluster 模式
  cluster:
    addrs:
      - "node1:6379"
      - "node2:6379"
      - "node3:6379"
  # 通用配置
  password: "${JANUS_CACHE_PASSWORD}"
  db: 0
  pool_size: 100

log:
  level: "info"                # debug / info / warn / error
  format: "json"               # json / text

migration:
  auto: true                   # 启动时自动 migrate up
  path: "migrations/"

heartbeat:
  sweeper_interval: "30s"      # 后台扫描 cache 过期的间隔
  ttl: "60s"                   # Agent 心跳 TTL

observability:
  metrics:
    enabled: true
    port: 9091                 # Prometheus scrape 端口
  tracing:
    enabled: false
    endpoint: ""               # OTLP endpoint
```

---

## 31. 仓库与版本策略

### 31.1 仓库划分

| 仓库 | 地址 | 可见性 | 内容 |
|---|---|---|---|
| Janus | `github.com/agentium-lab/Janus` | Private（成熟后 Public） | 开源核心：引擎、SDK、CLI、基础 Policy、基础审计、Demo |
| Janus-enterprise | `github.com/agentium-lab/Janus-enterprise` | Private | 商业版：多租户、SSO/RBAC、高级审计、DLP、成本中心、HA |

两个独立仓库，Janus-enterprise import Janus 作为依赖。

