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

Valkey / Redis 是 MVP 与生产部署的实时状态依赖，用于 Agent heartbeat TTL、短期状态、限流计数和调度 hint。Redis 不承载 durable task、claim lease、不可变事件或最终审计事实；这些仍由 NATS JetStream 与 PostgreSQL 负责。

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
- ACP Gateway
- SDK Gateway
- HTTP / gRPC API
- WebSocket Stream API
- MCP Gateway / Tool Adapter

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
- Local Artifact Store
```

S3-compatible Object Storage 不是 Core GA 默认运行时；Core 只要求 `ArtifactStore` 接口与本地持久化实现。S3 adapter、KMS、WORM、retention 和 per-tenant bucket isolation 属于后续扩展 / Enterprise 边界。

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

### 5.3 MCP Gateway / Tool Adapter

职责：

- 把 MCP Tool Server 作为外部工具资源接入 Janus。
- 将 MCP tool call 转换为 Janus Task，由 TaskService 统一执行 routing、policy、budget 和 audit。
- 将 MCP resource 转换为 Janus ContextRef，用于后续任务的数据分级、权限范围和审计。

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

Core GA 的 Registry 写入模型与数据库 schema 保持一致：

- `agents.protocol` 是单值协议字段；一个 Agent 需要多协议入口时，应注册多个 Agent 记录或由上层 gateway 代理，不在 Core registry 中写 `protocols[]`。
- `agents.team_id` 是 team 级治理边界，用于 policy condition、routing budget 和 route explanation；Core 不维护完整 Team / Group 层级表。
- `agent_capabilities` 是能力召回的主索引，`agents.description` / capability description 可参与 intent -> capability 解析和候选排序，但解析结果必须复用 capability routing，不能替代硬约束。
- runtime 编排字段、per-agent `allowed_callers`、`requires_approval_for` 不属于 Core registry schema；调用方限制和审批要求通过 `policy_rules` 与 approval workflow 表达，复杂组织模型属于 Enterprise 边界。
- 常见治理配置不写入 Agent Registry。Core 提供 policy template API/SDK/CLI 作为简化入口，模板最终仍生成 tenant-scoped `policy_rules`，由 `PolicyService.Evaluate` 统一执行。

### 5.5 Task Service

职责：

- 创建 Task。
- 校验 Task Envelope。
- 执行幂等检查。
- 写入任务当前状态。
- 写入 task.created / task_publish / task.queued 等 outbox event 或 command。
- 不直接调用 Queue/Event Driver 写入 mailbox；由 outbox publisher 负责跨 PostgreSQL 与 NATS 的最终一致发布。

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
- 执行 budget usage / concurrency accounting。
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
"tool_invocation": {
"id": "call_01HZY8Q9ZWZK3F8R0V8Y5B7M2Q",
"name": "git.diff",
"namespace": "git",
"source_protocol": "sdk"
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
| `tool_invocation` | 否 | Janus 可见的工具型任务元数据；适用于 Agent、SDK、A2A/ACP 和 MCP 入口，不表示 Janus 接管 Agent 或 MCP 内部执行 |
| `trace` | 是 | 链路追踪信息 |

---

## 7. Task 生命周期

### 7.1 状态机

```text
created
-> approval_pending
-> queued
-> claimed
-> running
-> completed

created
-> queued

claimed
-> running
-> failed
-> cancelled

running
-> blocked
-> failed
-> cancelled

blocked
-> running
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
- 需要人工审批的任务不得先进入 mailbox；审批通过后才允许从 `approval_pending` 转为 `queued` 并入队。
- `claimed` 和 `running` 都受 claim lease 约束；lease timeout 后由 timeout scanner 转为 `failed`，再按 retry policy 进入 `retry_scheduled` 或 `dead_lettered`。

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
team_id text,
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

MVP 不单独引入完整 Team / Group 表。`agents.team_id` 用于 policy、routing 和 budget 的 team 级过滤；复杂组织层级后续由企业版 RBAC / ABAC 模型承载。未设置 `team_id` 时，MVP Demo 可从 agent_id 命名约定或 metadata 推导，但生产实现应优先使用显式字段。

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
result jsonb,
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

`result_ref` 只存储外部或 Janus artifact URI。MVP 如需保存小结果正文，写入 `result`，并通过服务层限制大小；大结果后期迁移到 Object Storage。

### 8.6 task_attempts

```sql
create table task_attempts (
tenant_id text not null,
task_id text not null,
attempt integer not null,
agent_id text not null,
lease_id text not null,
lease_expires_at timestamptz not null,
delivery_ref jsonb not null default '{}',
status text not null,
claimed_at timestamptz not null default now(),
started_at timestamptz,
heartbeat_at timestamptz,
finished_at timestamptz,
error jsonb,
token_usage jsonb,
primary key (tenant_id, task_id, attempt)
);

create unique index task_attempts_lease_idx
on task_attempts (tenant_id, lease_id);
```

`delivery_ref` 保存 Queue/Event Driver 返回的底层投递引用，例如 NATS stream sequence、consumer sequence、stream、consumer、subject 等。Janus API 进程重启后，ACK / NACK 必须能仅凭 `tenant_id + lease_id` 找回当前 attempt 与底层 delivery ref。

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

`budgets` 只描述配置上限，不存储已消耗事实。当前周期用量和并发计数存入 `budget_usage`，结算审计事实写入 `budget_usage_ledger`，避免服务重启或 Redis 丢失后无法审计。

```sql
create table budget_usage (
tenant_id text not null,
scope_type text not null,
scope_id text not null,
period text not null,
period_key text not null,
tokens_used integer not null default 0,
cost_used numeric(18, 6) not null default 0,
task_count integer not null default 0,
primary key (tenant_id, scope_type, scope_id, period, period_key)
);

create table budget_usage_ledger (
tenant_id text not null,
id text not null,
task_id text not null,
attempt integer not null,
scope_type text not null,
scope_id text not null,
event_type text not null,
tokens integer not null default 0,
cost_usd numeric(18, 6) not null default 0,
payload jsonb not null default '{}',
occurred_at timestamptz not null default now(),
primary key (tenant_id, id)
);

create unique index budget_usage_ledger_attempt_scope_idx
on budget_usage_ledger (tenant_id, task_id, attempt, scope_type, scope_id, event_type);

create index budget_usage_scope_idx
on budget_usage_ledger (tenant_id, scope_type, scope_id, occurred_at);
```

Redis 可用于 RPM / TPM / concurrency 的短窗口快速计数，但 PostgreSQL ledger 和 audit events 是预算审计事实来源。

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
agent_id text, -- legacy compatibility alias for source_agent
source_agent text,
target_agent text,
actor_type text,
actor_id text,
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
janus.<tenant>.tasks_dlq.<mailbox>
janus.<tenant>.events.<event_type>
janus.<tenant>.agent_status.<agent_id>
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
| mailbox 级 DLQ stream | `janus.<tenant>.tasks_dlq.<mailbox>` | 死信任务 | Limits |
| `JANUS_EVENTS` | `janus.*.events.>` | 审计与 trace 事件 | Limits |
| `JANUS_AGENT_STATUS` | `janus.*.agent_status.>` | Agent 状态变化 | Limits |

Agent heartbeat 不写入 NATS stream。实时 TTL 写 Redis，durable `last_heartbeat_at` / `status` 写 PostgreSQL，状态变化事件可写入 `JANUS_AGENT_STATUS`。

`JANUS_TASKS` 是默认共享 stream 的逻辑名称。大客户或高隔离租户可以使用独立 task stream，但必须满足以下任一条件：

- 使用独立 NATS account，复用 `janus.<tenant>.tasks.<mailbox>` subject 语义。
- 仍在同一 NATS account 内时，共享 `JANUS_TASKS` 不得绑定该大客户 tenant 的 task subject，避免同一 subject 被多个 stream 捕获。

MVP 的延迟重试不使用独立 retry stream，由 transactional outbox 的 `next_attempt_at` 驱动重新入队。

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

每个 mailbox 对应一个 durable pull consumer，多个 Agent worker 可以并发从同一个 durable consumer pull。不要为同一 mailbox 创建多个 subject filter 重叠的 durable consumer，否则会破坏 Work queue retention 的消费语义。

命名：

```text
consumer_<tenant>_<mailbox_safe_name>
```

`durable_name` 不使用 `.`、`*`、`>` 等 NATS 保留或不安全字符。Mailbox ID 进入 consumer / stream 名称前必须做安全编码；subject 中仍可保留 mailbox 的点分层级。

配置：

```yaml
consumer:
durable_name: consumer_acme_code_reviewer_team_a_default
ack_policy: explicit
ack_wait: 300s
max_deliver: 5
max_ack_pending: 100
deliver_policy: all
```

Event projection 使用独立的 tenant-scoped durable consumer，不与 mailbox workqueue consumer 混用：

```yaml
event_projection_consumer:
durable_name: janus_event_projector
stream: JANUS_<tenant>_EVENTS
filter_subject: janus.<tenant>.events.>
ack_policy: explicit
deliver_policy: all
max_ack_pending: 1024
```

- API 启动时扫描现有 tenant，为每个 tenant 创建或恢复 durable event projection consumer，并周期性发现新 tenant。
- Consumer 只在事件完成 DLP projection 检查并成功写入 PostgreSQL `audit_event_projection` 后 ACK；写库失败必须 NAK 或重试，不得确认丢弃。
- DLP 明确拒绝 audit projection 时保持现有语义：记录错误并 ACK/drop 该投影，避免 poison event 阻塞 durable consumer。
- 普通 NATS `SubscribeEvents` 只用于 WebSocket / Dashboard 实时广播，不作为审计 projection 的事实来源。

### 9.5 ACK / NACK 映射

| Janus 操作 | NATS 操作 |
| :--- | :--- |
| Agent pull | Consumer fetch |
| Agent claim | 写入 task_attempts，返回 lease_id |
| Agent ACK | 先持久化 completed / budget settle / event outbox，再 NATS ACK |
| Agent NACK retriable | 先持久化 failed / retry_scheduled / retry outbox，再 ACK 原消息 |
| Agent NACK non-retriable | 先持久化 dead_lettered / DLQ outbox，再 ACK 原消息 |
| Dispatch-time policy / DLP / budget block | 不创建 claim，先持久化 `policy.denied` / `policy.approval_required` / `budget.exceeded`；DLP 拒绝使用 `policy.denied` 并在 payload 中标记 `reason=dlp_denied`；随后对 delivery 执行 NACK with delay |
| Lease timeout | timeout scanner 持久化 failed，并按 retry policy 调度重试或 DLQ |

Core GA 对 dispatch-time policy、DLP、budget、claim 前持久化失败使用 delayed NACK，默认 delay 为 5 秒。该 delay 是底层 redelivery backoff，不表示 Janus 业务 retry；业务 retry 仍由 task status、retry policy 和 outbox `next_attempt_at` 驱动。

Janus 不应完全依赖 NATS redelivery 表达业务重试。推荐由 Janus 自己维护 retry policy，NATS redelivery 作为底层兜底。

业务状态持久化成功但 NATS ACK 失败时，NATS 可能重新投递旧消息。Janus 必须在 Pull 阶段通过 task 状态、attempt 和 delivery metadata 做 redelivery reconciliation；旧 delivery 只允许被 ACK/drop，不允许重复创建 attempt 或重复执行业务副作用。

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
5. 写 task_publish outbox，attempt = failed_attempt + 1，next_attempt_at = now + backoff
6. 原 delivery 在下一状态持久化后 ACK
7. 到达 backoff 时间后 outbox publisher 重新写入 mailbox
8. 如果不可重试，写入 mailbox 级 DLQ stream
```

MVP 不创建 `tasks_retry` subject 或 retry stream。后续如引入 NATS message schedules、独立延迟队列或其他 scheduler，需要先更新 Queue/Event Driver 语义，避免 outbox scheduler 与 retry stream 同时调度同一任务。

### 9.7 DLQ 设计

每个 mailbox 拥有独立 DLQ stream，不共享统一 DLQ。Stream / subject 命名基于：

```text
janus.<tenant>.tasks_dlq.<mailbox>
```

Mailbox 创建时同步确保主任务 consumer 和对应 DLQ stream 存在。大客户独立 task stream 时，DLQ 仍按 mailbox 粒度创建。

DLQ raw stream 消息体使用结构化 JSON envelope，顶层字段包含：

- `tenant_id`
- `mailbox_id`
- `task_id`
- `attempt`
- `attempt_count`（从 task metadata / headers 提升；没有来源时可省略）
- `priority`
- `original_envelope`
- `error_payload`
- `failure_reason`
- `policy_decision_id`（有 policy decision 来源时填写）
- `first_failed_at`（有首次失败来源时填写）
- `dead_lettered_at`
- `dedupe_key`
- `headers`

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
Attempt int
Priority string
Payload []byte
Headers map[string]string
}

type TaskDelivery struct {
TaskID string
Attempt int
Payload []byte
DeliveryRef DeliveryRef
RedeliveryCount int
}

type DeliveryRef struct {
Driver string
TenantID string
MailboxID string
Stream string
Consumer string
Subject string
StreamSeq uint64
ConsumerSeq uint64
AckSubject string
AckToken string
Raw map[string]string
}

type NackReason struct {
Code string
Message string
Retryable bool
Delay time.Duration
}

type JanusEvent struct {
EventID string
EventType string
TenantID string
TraceID string
TaskID string
SourceAgent string
TargetAgent string
ActorType string
ActorID string
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
8. DB transaction:
   - insert task row with status = created
   - insert task.created outbox event
   - if policy decision = approval_required:
     - update task status = approval_pending
     - insert approval request
     - insert task.approval_pending outbox event
   - else:
     - insert task_publish outbox command with attempt = 1
9. Commit
10. Return accepted response to caller
11. Outbox publisher publishes task_publish to mailbox
12. After mailbox publish succeeds, post-publish DB transaction:
   - update task status = queued
   - mark enqueue outbox published
   - insert task.queued event_publish outbox event
```

需要人工审批的任务不得在审批前进入 mailbox。审批通过时，Approval Service 写入 `task_publish` outbox command；审批拒绝或超时时转为 `cancelled`。

每次入队消息必须携带 attempt。首次入队为 attempt 1；重试时由 retry scheduler 在同一个 DB transaction 中计算 next_attempt = tasks.attempt_count + 1，并写入 `task_publish` outbox command，使用 `next_attempt_at` 控制延迟。

### 11.2 Pull 流程

```text
1. Agent 调用 PullTask(mailbox, agent_id)
2. 校验 `agent_id` 必填
3. 加载 mailbox 并检查 mailbox owner 是否等于该 `agent_id`；不匹配时拒绝 pull，不能进入 fetch
4. 检查 Agent concurrency
5. 从 Queue/Event Driver fetch
6. Redelivery reconciliation:
   - 加载 task 当前状态和 delivery attempt
   - 如果 task 是终态，ACK delivery 并返回空结果
   - 如果 task 处于 `retry_scheduled`，ACK 旧 delivery 并记录 duplicate delivery event
   - 如果 task 仍是 `created` 但 delivery 已存在，原子推进到 `queued`，标记匹配的 task_publish outbox 为 published，并补写 task.queued event_publish outbox
   - 如果 delivery attempt 小于或等于已登记的 tasks.attempt_count，ACK 旧 delivery
7. Dispatch-time policy check
8. Budget usage / concurrency accounting
9. 创建 task_attempt，保存 lease_id、lease_expires_at 和 delivery_ref
10. 更新 tasks.attempt_count = delivery attempt，task status = claimed
11. 返回 Task Envelope + lease_id + attempt
```

如果 fetch 后的 dispatch-time policy、DLP 或 budget 检查失败，Janus 不创建 claim；必须先持久化对应的阻断审计事件，再对底层 delivery 执行 NACK with delay 或按策略终止，避免消息在 ack_wait 内无解释地卡住。如果阻断审计事件持久化失败，Janus 仍必须 delayed NACK 该 delivery，并向调用方返回事件持久化错误，防止无审计地把任务交给 Agent。Core GA 的默认 delayed NACK 为 5 秒。

如果完成、失败或 DLQ 状态已经持久化，但底层 NATS ACK 失败，后续 redelivery 必须通过上述 reconciliation ACK 掉旧 delivery，不得重复执行预算结算、结果写入或重试调度。

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
validate lease is current and not expired
settle budget
store result or result_ref
running -> completed
ack driver delivery
publish task.completed
```

### 11.4 NACK / Failure

```text
Agent NACK:
validate lease_id
validate lease is current
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
intent 指定自然语言意图，由 Janus 推断 capability
group 指定 Agent group
human 指定人工审批节点
```

Core v1.0 GA 的解析边界：

- `mailbox`：必须解析到同 tenant 下 active mailbox。
- `agent`：必须解析到 online agent，并从该 agent 的 active mailbox 中选择目标。
- `capability`：必须依赖 agent repository 查找 online 且声明该 capability 的 agent，再从候选 agent 的 active mailbox 中选择目标。
- `intent`：必须先通过 Intent Resolver 将自然语言请求解析为一个明确 capability，再复用 `capability` 路由。Intent Resolver 输入包括 `target.value`、`payload.type`、`payload.content`、ContextRef metadata 和 policy hints；输出必须包含 `resolved_capability`、`confidence`、`reason` 和候选摘要。
- Intent Resolver 只负责 intent -> capability 推断，不直接选择 Agent 或 mailbox，也不做授权决策。解析成功后，任务目标被改写为 capability，并继续执行 data classification、`task.route` policy、agent/mailbox capacity、budget 和 active mailbox 等硬约束；如果解析出的 capability 在硬约束后没有合格目标，Janus 必须拒绝创建任务并记录 `routing.failed`，不能静默投递到默认 mailbox 或绕过约束改投。
- Intent 是协议中立能力：A2A `message/send` 与 ACP `runs` 在没有显式 `target`、`capability` 或 `mailbox_id` 时，默认构造 `target.type=intent`，并将消息文本作为 `target.value`；HTTP/SDK 入口必须显式提交 `target_type=intent` 才进入该解析。Janus 不会把普通自然语言消息硬编码到 MCP `mcp.tool.*` 命名空间，也不会隐式投递到 `default` mailbox。
- 多个 active mailbox 候选时，先按 mailbox ID 做稳定排序，再选择 backlog 最低的 mailbox；backlog 相同则 mailbox ID 小者优先，保证结果可预测。
- 请求显式提供 `mailbox_id` 时，Janus 仍会先校验 `target.type` 合法性和 mailbox active 状态，然后直接投递该 mailbox。请求显式声明 `target.type=mailbox` 时，必须同时提供 `target.value` 或 `mailbox_id`；缺失时返回参数错误，不能 fallback 到 `default` mailbox。
- `group` / `human` 在 Core 中表示路由意图；Core 支持 tenant-scoped 静态 mailbox 映射，配置项为 `routing.group_mailboxes` 与 `routing.human_mailboxes`。查找顺序为精确 `tenant_id`，再到显式 wildcard tenant `*`。
- 如果 `group` / `human` 未提供显式 `mailbox_id`，也没有命中静态映射，Janus 拒绝创建任务，并写入 `routing.failed` 审计事件和 `janus_routing_failures_total` 指标。
- Core 不引入组织成员关系或 RBAC membership 表；Enterprise 可以在此基础上扩展完整 group membership / RBAC / 审批组织模型。
- capability routing 会对候选执行硬约束过滤：capability schema 的 data classification、`task.route` policy deny、agent/mailbox capacity、budget 并发与 task budget 上限。硬约束全部通过后，如果候选 capability schema / description 与 task hints 有可解释匹配，则先按 semantic score 降序排序，再按 backlog 和稳定 ID 做 tie-break；没有任何正分数时保持 backlog 和稳定 ID 排序。

### 12.2 路由流程

```text
1. 读取 task target 与可选 mailbox_id
2. 校验 target.type 属于 agent / mailbox / capability / intent / group / human
3. 如果显式提供 mailbox_id，校验 mailbox 存在且 active，然后选择该 mailbox
4. 如果 target.type = mailbox，校验 target.value 指向 active mailbox
5. 如果 target.type = agent，校验 agent online，并查找该 agent 的 active mailbox
6. 如果 target.type = intent，先用 Intent Resolver 将自然语言请求解析为唯一 capability；解析失败、低置信度或歧义时拒绝创建并记录 routing.failed
7. 如果 target.type = capability 或 intent 已解析 capability，查找 online 且声明该 capability 的候选 agent
8. capability 候选先过滤 data classification、policy、capacity、budget 硬约束
9. 在剩余候选 agent 的 active mailbox 中计算可选 semantic score；有正分数时按 score 降序、backlog 升序、ID 升序选择，否则按 backlog 最低选择
10. 如果 target.type = group/human，先查 tenant-scoped 静态映射，再校验映射 mailbox active
11. 如果 group/human 无显式 mailbox_id 且未命中映射，拒绝创建并记录 routing.failed
12. 成功路由记录 routing.selected，失败路由记录 routing.failed
13. Intent 解析只决定 capability 推断；语义排序只在 capability 候选通过硬约束后参与排序。二者都不能覆盖安全和治理决策
```

Intent Resolver 的 Core GA 要求：

- 支持自然语言请求到 capability 的可审计解析，例如 `我想审查这段代码`、`please review this diff` 能解析为 `code_review`。
- 解析依据必须来自 tenant 内已注册 Agent capability、capability description、schema hints、payload/content tokens、ContextRef metadata 和显式 policy hints。
- Core 默认实现可以使用本地确定性 token/alias/description 匹配；如接入 embedding/reranker，也必须保留候选、分数、阈值和选择原因，不得让不可解释模型直接决定投递。
- 解析结果必须写入 `routing.selected` payload；失败必须写入 `routing.failed` payload，并暴露 `janus_routing_failures_total{tenant_id,target_type,reason}`，其中 intent 解析失败 reason 包括 `intent_no_match`、`intent_ambiguous`、`intent_low_confidence`。
- 真实依赖 smoke 必须覆盖自然语言 intent -> code review capability -> Code Review Agent mailbox 的完整路径，以及歧义/无匹配拒绝路径；低置信度拒绝可由 service/unit 测试覆盖。

Capability schema 可以声明数据分级约束：

```json
{
  "allowed_data_classifications": ["internal", "confidential"],
  "max_data_classification": "confidential",
  "payload_types": ["code_review"],
  "supported_tools": ["pytest"],
  "model_classes": ["code"],
  "context_types": ["artifact"],
  "context_access_scopes": ["repo"],
  "routing_keywords": ["security", "regression"]
}
```

`allowed_data_classifications` 或 `data_classifications` 为显式允许列表；`max_data_classification` 使用 `public < internal < confidential < restricted` 顺序。未声明时不按 schema 限制数据分级。

Core v0.4.9 的语义排序只使用可审计的本地规则，不调用 embedding、LLM 或外部服务。Core v1.0 GA 新增 Intent Resolver 后，若引入 embedding、reranker 或 LLM，只能用于 capability 候选召回/评分，并且必须保留可审计 evidence。Task hints 来源包括：

- `payload.type`
- `payload.content` token
- `policy.allowed_tools`
- `budget.model_classes`
- `context_refs[].type`
- `context_refs[].access_scope`

Capability profile 来源包括 agent / capability description token，以及 capability schema 中的 `payload_types` / `input_types` / `task_types` / `content_types`、`tools` / `supported_tools`、`model_classes` / `supported_model_classes`、`context_types` / `supported_context_types`、`context_access_scopes` / `access_scopes`、`routing_keywords` / `routing_tags` / `keywords` / `tags`。

`routing.selected` payload 必须记录 route explanation：`strategy`、`sort_order`、selected agent/mailbox、candidate counts、rejection counts、selected backlog、candidate score summary、selected semantic score 和 selected semantic reasons。`strategy=capability_filtered` 表示所有候选 semantic score 为 0，按 backlog 排序；`strategy=capability_semantic` 表示至少一个候选有正分数，排序顺序为 `semantic_score_desc,backlog_asc,mailbox_id_asc,agent_id_asc`。

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

### 13.2 Budget Usage Accounting

Core GA 不实现 durable per-task token reservation，也不在数据库中保存 `reserved_tokens`。预算执行以当前实现为准：

```text
Pull / dispatch:
  validate task hard limits and model class constraints
  check Redis RPM / TPM counters, using task.budget.max_tokens as declared token estimate when present
  check budget_usage daily cost and task_count concurrency
  increment budget_usage.task_count for tenant / team / agent as the in-flight usage slot

ACK complete:
  write budget_usage_ledger once per tenant/task/attempt/scope/event_type
  if ledger insert succeeds, aggregate actual tokens/cost into budget_usage
  decrement budget_usage.task_count for tenant / team / agent

NACK / timeout / cancel:
  decrement budget_usage.task_count
  settle partial token usage only when usage is explicitly supplied
```

`budget_usage` 是当前周期的用量与并发聚合，`budget_usage_ledger` 是 ACK 结算的审计事实。Core GA 不逐次发布独立 `budget.reserved` / `budget.consumed` / `budget.released` 事件；publish / routing 阶段的预算拒绝通过 `janus_budget_throttle_total`、`routing.failed` / route explanation 和任务 / 工具审计 payload 体现。fetch 后的 dispatch-time budget 阻断必须先发布 `budget.exceeded`，再 delayed NACK；如果 `budget.exceeded` 持久化失败，Janus 不创建 claim，仍 delayed NACK，并向调用方返回事件持久化错误。未来如引入真实 token reservation，必须新增 schema、事件、幂等测试和真实依赖 smoke。

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
"actor_type": "agent",
"actor_id": "review-agent.team-a",
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

实现中的 `JanusEvent` 使用扁平字段 `actor_type` / `actor_id`，projection 表保留 legacy `agent_id` 兼容列，同时写入 `source_agent`、`target_agent`、`actor_type`、`actor_id`。Task lifecycle 事件在上下文可得时必须填充：

- `trace_id`：来自 `TaskEnvelope.trace.trace_id`。
- `source_agent`：原始发起 Agent。
- `target_agent`：路由选中的执行 Agent；显式 agent target 可直接使用 target value，mailbox/capability/intent 路由使用解析出的 mailbox owner。
- `actor_type` / `actor_id`：触发当前状态变化的主体；创建/发布通常是 source agent，claim/start/ack/nack/DLQ 通常是执行 Agent，审批放行可为 human approver，reconciliation 可为 system。
- `task.queued` 事件从 durable `TaskMessage.headers` 恢复上述字段，保证 outbox publish 后仍可审计；审批放行入队的 actor 是 human approver，retry re-enqueue 的 actor 是 `system/retry_scheduler`。

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

Agent heartbeat 默认写 Redis TTL 与 PostgreSQL `agents.last_heartbeat_at` / `status`，不逐条写入 NATS event stream。`agent.heartbeat` 仅用于调试、采样或外部审计明确要求的场景；Agent online / offline 状态变化仍写入事件流。

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

Core GA 预留上述 event type。普通预算 reserve / settle 事实主要通过 metrics、routing audit payload 与 PostgreSQL `budget_usage_ledger` 表达，不默认逐次发布独立 `budget.*` 事件；dispatch-time budget 阻断必须发布 `budget.exceeded`，用于解释 fetch 后未创建 claim 且 delayed NACK 的治理原因。

Routing:

```text
routing.failed
routing.selected
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

`tool.invocation_*` 是协议中立的 Janus 可见工具调用审计事件，不只属于 MCP。Agent、SDK、A2A/ACP 和 MCP 入口只要向 Janus 提交的是工具型任务，就必须通过 `TaskEnvelope.tool_invocation`、`payload.type=mcp_tool_call` 或 `target.value` 中的 `tool` capability 语义标识出来。Janus 在创建阶段记录 `requested`、`allowed`、`denied`，在任务生命周期阶段根据 Start / ACK / NACK / DLQ / lease expiry 记录 `started`、`completed`、`failed`。

该事件不表示 Janus 进入 Agent runtime 或 MCP Tool Server 内部，也不要求 Agent 或 MCP tool 上报其私有内部 tool call。Janus 只审计经过 Janus 的任务交接事实、策略/预算/容量决策、上下文引用和 Janus 可见的 result/error。Agent 自己或 MCP tool 内部调用模型产生的成本不由 Janus 核算；Janus 只核算 Janus 自身使用模型产生的成本，以及调用方通过 Task Envelope / ACK usage 显式交给 Janus 的可见预算事实。

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

`PullTask` 请求体必须包含 `agent_id`。Janus Core 会拒绝空 `agent_id`，并校验 `{mailbox_id}` 的 owner 是否等于该 `agent_id`；不匹配时返回权限错误，不会 fetch 底层 queue delivery。
Dispatch-time policy deny / approval required 映射为 HTTP `403` / gRPC `PERMISSION_DENIED`；budget、capacity、rate limit backpressure 映射为 HTTP `429` / gRPC `RESOURCE_EXHAUSTED`；阻断审计事件持久化失败属于服务侧持久化错误，映射为 HTTP `503` / gRPC `UNAVAILABLE`，且底层 delivery 已 delayed NACK。

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
"attempt": 1,
"expires_at": "2026-06-09T12:05:00Z"
}
}
```

### 17.3 ACK 请求示例

```json
{
"lease_id": "lease_01HZY...",
"attempt": 1,
"result_ref": "janus://artifacts/task_01HZY/result",
"token_usage": {
"prompt_tokens": 12000,
"completion_tokens": 3000,
"total_tokens": 15000
}
}
```

小结果可以通过 `result` 字段直接提交并存入 PostgreSQL；大结果应先写 artifact/object storage，再通过 `result_ref` 引用。

### 17.4 NACK 请求示例

```json
{
"lease_id": "lease_01HZY...",
"attempt": 1,
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

A2A Gateway Core v0.4.4 支持：

- `POST /a2a/agent/card`：注册 A2A Agent Card，`capabilities[].input/output` 会序列化进入 `agent_capabilities.schema`。
- `POST /a2a/task/send`：兼容 HTTP send，将 A2A message 映射为 Janus Task。
- `POST /a2a/jsonrpc`：支持 JSON-RPC `message/send` 和 `tasks/get`。
- `GET /a2a/task/{task_id}/status`：查询 Janus task 并映射为 A2A task status。

消息映射规则：

- `message.taskId` 优先作为 Janus `task_id`；缺失时使用 JSON-RPC `id`；仍缺失时由 gateway 生成。
- `message.contextId` 映射为 `trace.trace_id` / A2A `contextId`。
- `message.referenceTaskId` 映射为 `trace.parent_task_id`。
- `params.target` 或 query `target_type/target_value/capability/mailbox_id` 映射为 Janus target；未指定 target 且未显式提供 `mailbox_id` 时默认 `target.type=intent`，并用消息文本作为 `target.value`。
- `params.budget`、`params.policy`、`params.contextRefs`、`params.ttlSeconds` 透传到 Janus Task Envelope。

错误映射规则：

- HTTP A2A endpoint 返回 `{error,message,status}` JSON envelope。
- JSON-RPC endpoint 返回标准 `{jsonrpc,id,error:{code,message,data}}`。
- Janus `TaskError` 映射为 A2A status `error.code` / `error.message`。

### 18.2 ACP 映射

ACP-compatible Agent 通过 Core adapter 接入。v0.4.5 采用轻量 Janus ACP beta envelope，不引入 Enterprise catalog、连接器审批或策略包。

ACP Gateway Core v0.4.5 支持：

- `POST /acp/agent/manifest`：注册 ACP Agent Manifest，`capabilities[].input_schema/output_schema` 会序列化进入 `agent_capabilities.schema`。
- `POST /acp/runs`：创建 ACP run，并映射为 Janus Task。
- `GET /acp/runs/{run_id}/status`：查询 Janus task 并映射为 ACP run status。

Run 映射规则：

- `run.id` 映射为 Janus `task_id`；缺失时使用 `acp_{target_value}` 作为 beta 默认 ID。
- `run.thread_id` 映射为 `trace.trace_id`；缺失时使用 `task_id`。
- `run.parent_run_id` 映射为 `trace.parent_task_id`。
- body 中 `tenant_id/source_agent/target_type/target_value/mailbox_id` 优先，query 中 `tenant_id/source_agent/target_type/target_value/capability/mailbox_id` 作为 fallback；未指定 target 且未显式提供 `mailbox_id` 时默认 `target.type=intent`，并用 run message 作为 `target.value`。
- `priority`、`budget`、`policy`、`context_refs`、`ttl_seconds`、`idempotency_key` 透传到 Janus Task / Task Envelope。
- `message` 与 ACP 原始 run JSON 一起保存在 `payload.type=acp_run` 中，便于重放和审计。

状态和错误映射规则：

- Janus `created/queued/retry_scheduled` 映射为 ACP `queued`。
- Janus `claimed/running` 映射为 ACP `running`。
- Janus `blocked/approval_pending` 映射为 ACP `waiting`。
- Janus `completed` 映射为 ACP `completed`。
- Janus `failed/dead_lettered/expired` 映射为 ACP `failed`。
- Janus `cancelled` 映射为 ACP `cancelled`。
- HTTP ACP endpoint 返回 `{error,message,status}` JSON envelope。
- Janus `TaskError` 映射为 ACP status `error.code` / `error.message`。

### 18.3 MCP 映射

MCP 只作为工具和上下文资源入口，不作为绕过 Janus policy / budget / audit 的执行通道。v0.4.6 采用轻量 Janus MCP beta adapter，不实现完整 MCP session server、connector catalog 或审批流。

| MCP 概念 | Janus 概念 |
| :--- | :--- |
| Tool | Tool resource |
| Tool call | Janus Task Envelope |
| Resource | ContextRef |
| Prompt | Payload template |

MCP Gateway Core v0.4.6 支持：

- `POST /mcp/tools/call`：创建 MCP tool call，并映射为 Janus Task。
- `GET /mcp/tools/calls/{call_id}/status`：查询 Janus task 并映射为 MCP tool call status。
- `POST /mcp/resources`：将 MCP resource 注册为 Janus ContextRef。

Tool call 映射规则：

- `tool_name` 默认映射为 `target.type=capability`、`target.value=mcp.tool.{tool_name}`。
- query `target_type/target_value/capability/mailbox_id` 可显式覆盖目标；显式 mailbox target 使用 `mailbox_id` 或 `target_value`，两者都缺失时返回参数错误。
- MCP Gateway 不得在未显式提供 `mailbox_id` 或 `target_type=mailbox` 时注入 `default` mailbox；否则会让 mailbox 硬路由压过 capability / intent routing。
- MCP `tool_name -> mcp.tool.{tool_name}` 只适用于 `/mcp/tools/call` 这种显式工具调用。Agent、A2A、ACP 或 SDK 提交的自然语言请求（例如“我需要审查一下这段代码”）必须通过协议中立的 `target.type=intent` 解析为 capability，再复用 capability routing；不得因为没有 `tool_name` 而尝试匹配 MCP tool namespace。
- body 中 `tenant_id/source_agent` 优先，query 中 `tenant_id/source_agent` 作为 fallback。
- `thread_id` 映射为 `trace.trace_id`；缺失时使用 `task_id`。
- `parent_call_id` 映射为 `trace.parent_task_id`。
- `priority`、`budget`、`policy`、`context_refs`、`ttl_seconds`、`idempotency_key` 透传到 Janus Task / Task Envelope。
- MCP 原始 tool call JSON 保存在 `payload.type=mcp_tool_call` 中，便于重放和审计。
- MCP tool call 必须设置 `tool_invocation.name={tool_name}`、`tool_invocation.namespace=mcp`、`tool_invocation.source_protocol=mcp`，从而参与协议中立的 `tool.invocation_*` 审计链路。

状态和错误映射规则：

- Janus `created/queued/retry_scheduled` 映射为 MCP `queued`。
- Janus `claimed/running` 映射为 MCP `running`。
- Janus `blocked/approval_pending` 映射为 MCP `waiting`。
- Janus `completed` 映射为 MCP `completed`。
- Janus `failed/dead_lettered/expired` 映射为 MCP `failed`。
- Janus `cancelled` 映射为 MCP `cancelled`。
- HTTP MCP endpoint 返回 `{error,message,status}` JSON envelope。
- Janus `TaskError` 映射为 MCP status `error.code` / `error.message`。

Resource 映射规则：

- `type/uri/hash/classification/access_scope` 映射为 Janus ContextRef。
- ContextRef 可在后续 tool call 的 `context_refs` 中引用，从而参与 Janus 数据分级、策略、预算和审计链路。

### 18.4 ContextRef 与 Task 绑定

ContextRef 是 Janus Core 的上下文引用索引，不承载大对象内容本身。v0.4.7 后，ContextRef 与 Task 的绑定是持久关系，而不只是 Task Envelope 中的内嵌 JSON。

持久化规则：

- 创建 Task 时，`envelope.context_refs[]` 会被规范化：缺失 `tenant_id` 时使用 task tenant，缺失 `id` 时生成 `ctxref_*`。
- 含 `type` 与 `uri` 的 inline ContextRef 会写入或更新 `context_refs`，并写入 `task_context_refs` 绑定表。
- 只包含 `id` 的 ContextRef 视为引用已有 ContextRef；若同 tenant 下不存在该 ref，Task 创建失败。
- ContextRef `tenant_id` 与 Task `tenant_id` 不一致时拒绝创建，避免跨租户上下文泄漏。
- Postgres direct create 与 outbox transaction create 使用同一绑定语义；TaskService 会先规范化 ContextRef，确保 DB envelope、outbox payload 和 worker payload 一致。

HTTP 绑定规则：

- `POST /v1/tenants/{tenant_id}/context-refs/attach`：创建独立 ContextRef。
- `POST /v1/tenants/{tenant_id}/tasks/{task_id}/context-refs/attach`：创建 ContextRef 并绑定到 Task；若 body 只传 `id`，则绑定已有 ContextRef。
- `GET /v1/tenants/{tenant_id}/tasks/{task_id}/context-refs/list`：从 `task_context_refs` 查询 Task 已绑定 ContextRef。

---

## 19. 安全设计

### 19.1 身份认证

支持：

- API key
- mTLS
- OIDC token
- Workload identity

Core 必须支持 API key + mTLS 这类基础认证，保证开源部署可以独立运行生产链路。Enterprise 接入 OIDC / SSO / SAML、SCIM、IdP group 映射和企业 workload identity，并负责 tenant admin、org admin、auditor 等企业角色模型。

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

Core 提供基础授权检查和内置规则模型，覆盖 tenant、agent、mailbox、capability、tool、context classification 和 action 维度。Enterprise 在此基础上扩展完整 RBAC / ABAC、组织层级、group membership、角色审批和策略变更审计。

### 19.3 租户隔离

第一阶段使用：

- PostgreSQL tenant_id 逻辑隔离。
- NATS subject namespace 隔离。
- Janus Policy 强制检查。

这些属于 Core 能力。`tenant_id` 必须从 Day 1 进入数据模型、审计事件、预算、策略和队列 subject，不能被推迟到 Enterprise 才实现。

企业版增强：

- NATS account 隔离。
- 独立 encryption key。
- 独立 storage bucket。
- 独立 deployment namespace。
- 独立 retention / quota。
- tenant lifecycle 管理。

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

Core 保留 DLP hook 接口和调用点，确保数据治理可以插入核心链路。Enterprise 实现 PII 检测、脱敏、数据分类策略、跨 tenant / region 数据流限制、SIEM export 前过滤和审计报表。

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
janus_publish_latency_seconds
janus_pull_latency_seconds
janus_retry_total
janus_dlq_total
janus_outbox_backlog
janus_mailbox_backlog
janus_lease_timeout_total
janus_routing_failures_total
janus_routing_selected_total
janus_http_requests_total
janus_http_request_duration_seconds
janus_grpc_requests_total
janus_grpc_request_duration_seconds
```

HTTP / gRPC request 指标必须使用低基数 label。`tenant_id`、`task_id`、`attempt` 等高基数字段进入结构化日志和审计事件，不进入 request metrics label。

v0.5.3 已实现后台 gauge collector：outbox backlog 按 tenant/kind 聚合，mailbox backlog 按 tenant/mailbox 聚合，online agent 数按 tenant 聚合。collector 只读 PostgreSQL repository，在 `observability.metrics.enabled=true` 时由 `janus-api` 启动。

### 20.2 Tracing

每个 Task 必须带：

- trace_id
- span_id
- parent_task_id
- source_agent
- target_agent

当前 v0.5.7 已支持 HTTP / gRPC 入站 W3C `traceparent` 提取、缺失时生成 trace id，并在响应侧传播 `traceparent` / `x-trace-id`。HTTP 与 unary gRPC request 会创建 OpenTelemetry span，span 使用 `service.name`、HTTP/gRPC method/status、route、tenant/task/attempt 等属性补充诊断上下文。

当 `observability.tracing.enabled=true` 且 `observability.tracing.endpoint` 非空时，Janus 初始化 OTLP gRPC exporter 和 tracer provider，并在进程退出时执行 shutdown hook。`observability.tracing.insecure=true` 用于明文 collector endpoint；生产 TLS collector 应设置为 `false`。

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

HTTP request log 至少包含 `tenant_id`、`task_id`、`attempt`、`trace_id`、method、path、route、status、duration。gRPC request log 至少包含 `tenant_id`、`task_id`、`attempt`、`trace_id`、method、status、duration。worker/scanner/outbox 内部日志已通过 internal JSON log helper 覆盖关键故障和状态字段，并继续遵循同一字段规范。

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
- 已 claimed / running 任务按 lease timeout 处理。

### 22.2 Agent Claim / 执行超时

处理：

- claimed 或 running attempt 标记 failed。
- 释放预算。
- 根据 retry policy 重试或 DLQ。

### 22.3 Janus API 重启

处理：

- API 无状态，可水平恢复。
- 已持久化 task 不丢失。
- event projector 可从 stream replay。

### 22.4 NATS 不可用

处理：

- API publish 只要 PostgreSQL transaction 成功即可返回 accepted；NATS 不可用会导致 outbox backlog 增长，而不是丢失任务。
- Outbox publisher 发布失败时记录 attempts / last_error，并按 backoff 重试。
- 已写 DB 但未写 NATS 的任务通过 outbox scanner 补偿。
- Dispatch 暂停。
- 记录 storage.error。

### 22.5 PostgreSQL 不可用

处理：

- 控制面 API 降级。
- Publish 不应继续接受新任务，避免状态不一致。
- Dispatch 不应 claim 新任务，因为 claim lease、task_attempts、budget usage accounting 和 redelivery reconciliation 都依赖 PostgreSQL。
- 如果已经从 NATS fetch 但尚未完成 DB claim，应立即 NACK with delay 或让 ack_wait 触发 redelivery，不得把任务返回给 Agent。
- 已 running 的 Agent 可以继续本地执行；ACK / NACK 请求应返回可重试错误，要求 Agent / SDK 在 DB 恢复后重试，不能只在内存中结算或排队。

---

## 23. 一致性与 Outbox

Task 创建、任务入队、重试调度、DLQ 写入和审计事件发布都涉及 PostgreSQL 与 NATS 两个系统，需要避免双写不一致。

MVP 使用 transactional outbox：

```text
1. API / service 在 DB transaction 中写业务状态和 outbox_events。
2. transaction commit 后即可向调用方返回 accepted。
3. outbox publisher 按顺序发布到 NATS。
4. NATS publish 成功后，执行一个 DB transaction：
   - mark outbox_events published
   - 如 outbox kind 是 task_publish，按当前状态条件推进 task status = queued
   - 写 task.queued event_publish outbox event
5. 如果第 4 步 DB transaction 失败，outbox 仍保持 pending / retryable；下一轮使用同一 dedupe_key 重新 publish，依赖 NATS 去重避免重复入队。
```

Outbox 表：

```sql
create table outbox_events (
id text primary key,
tenant_id text not null,
kind text not null,
dedupe_key text not null,
payload jsonb not null,
status text not null default 'pending',
attempts integer not null default 0,
next_attempt_at timestamptz,
last_error text,
locked_by text,
locked_at timestamptz,
lease_expires_at timestamptz,
created_at timestamptz not null default now(),
published_at timestamptz
);

create unique index outbox_events_dedupe_idx
on outbox_events (tenant_id, dedupe_key);
```

`kind` 至少包括：

```text
event_publish
task_publish
dlq_publish
```

`status` 至少包括：

```text
pending
publishing
published
retry
dead
```

Outbox publisher 必须幂等。NATS publish 应携带 `dedupe_key` 作为去重键和消息头；PostgreSQL 状态更新使用当前状态条件保护，避免重复推进。

`dedupe_key` 必须稳定，推荐格式：

```text
task_publish:<tenant_id>:<task_id>:<attempt>
event_publish:<tenant_id>:<event_id>
task.dlq_enqueue:<tenant_id>:<task_id>:<attempt>
```

NATS publish 必须使用 `dedupe_key` 作为 `Nats-Msg-Id`。Outbox publisher 只有在 NATS publish 成功且后置 DB transaction 成功后，才允许把 outbox 标记为 `published`。

### 23.1 Outbox Publisher 生产调度

Outbox publisher 是可配置后台 worker，不允许在代码中硬编码固定轮询参数。默认配置：

```yaml
outbox:
  enabled: true
  poll_interval: 500ms
  idle_backoff_max: 5s
  batch_size: 100
  lease_duration: 60s
  max_retries: 5
  listen_notify: true
```

语义：

- `enabled=false` 时当前 API 实例不启动 outbox worker。多副本部署可以只让部分实例启用 worker，避免所有 API replica 都持续扫描 PostgreSQL。
- `listen_notify=true` 时，PostgreSQL 在 `outbox_events` insert commit 后通过 `pg_notify('janus_outbox_ready', tenant_id)` 唤醒 worker。
- `poll_interval` 是 fallback 扫描间隔，也是 worker 从空闲退避恢复后的最小间隔。
- `idle_backoff_max` 是空扫描指数退避上限。空闲时按 `poll_interval -> 2x -> ... -> idle_backoff_max` 退避。
- `batch_size` 是每次 claim 的最大 outbox row 数。
- `lease_duration` 是 `publishing` claim lease。worker 崩溃后，租约过期的 row 可被其他 worker 重新 claim。
- `max_retries` 是 outbox 发布失败上限，超过后 row 进入 `dead`，需要告警和人工恢复。

调度要求：

1. 有消息时，worker 立即连续 drain，不等待下一次 tick。
2. 空扫描时指数退避，降低 PostgreSQL 空轮询压力。
3. PostgreSQL `LISTEN/NOTIFY` 是主唤醒路径，polling 是兜底路径。
4. polling 必须保留，用于 notify 丢失、延迟重试到期、`publishing` lease 过期恢复。
5. 多 worker 并发必须继续使用 leaderless claim，依赖 `FOR UPDATE SKIP LOCKED` 和稳定 `dedupe_key` 保证安全。
6. claim 应尽量用单条 `UPDATE ... RETURNING` 或等价批量语句完成，避免每批 select 后逐行 update。

告警要求：

- `janus_outbox_backlog{tenant_id,kind}` 持续增长必须告警。
- publish latency 升高必须告警。
- `janus_outbox_status_rows{tenant_id,kind,status="retry"}` 持续增长必须告警。
- `janus_outbox_status_rows{tenant_id,kind,status="dead"}` 大于 0 必须告警。
- NATS 不可用时允许 API 返回 accepted，但 outbox backlog 和 retry 告警必须能定位问题。

任务当前状态和 NATS 消息采用最终一致：API 提交成功后任务可能短暂处于 `created`，只有 mailbox publish 成功后才进入 `queued`。Dashboard 和 CLI 需要展示该中间状态，而不是把 API accepted 解释为已入队。

---

## 24. 幂等设计

### 24.1 Publish 幂等

同租户内：

```text
(tenant_id, idempotency_key)
```

唯一。

重复提交时返回已有 task。

### 24.2 ACK / NACK 幂等

ACK / NACK 请求必须携带：

- `task_id`：来自 HTTP path 或 gRPC request 字段
- `attempt`
- `lease_id`

Janus 根据 `(tenant_id, task_id, attempt, lease_id)` 校验当前 claim owner。若 task 已处于终态，返回当前状态，不重复结算预算、结果写入、retry scheduling 或 DLQ 写入。

如果请求 attempt 小于当前 `tasks.attempt_count`，说明这是旧 attempt 的重复 ACK / NACK，应返回当前状态并拒绝产生副作用。如果请求 attempt 大于当前 `tasks.attempt_count + 1`，应返回 invalid attempt。

### 24.3 Event 幂等

event_id 全局唯一。Projection 写入 PostgreSQL 时使用 upsert 或忽略重复事件。

---

## 25. 测试策略

### 25.1 单元测试

覆盖：

- Task state machine
- Policy decision
- Budget usage / concurrency accounting
- Retry backoff
- Envelope validation
- Route resolution

### 25.2 集成测试

覆盖：

- PostgreSQL + NATS publish
- Pull / ACK
- NACK / retry
- Outbox publish + queued state projection
- Redelivery reconciliation
- DLQ
- Event projection
- Agent heartbeat timeout

### 25.3 故障测试

覆盖：

- Janus API 重启
- NATS 短暂不可用
- PostgreSQL 短暂不可用
- NATS publish 成功但 outbox 后置 DB transaction 失败
- DB 已完成 ACK / NACK 状态提交但 NATS ACK 失败
- Agent pull 后崩溃
- Agent pull 后未 Start 直到 claimed lease timeout
- ACK 重复提交
- 旧 attempt ACK / NACK 重复提交
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

## 26. 生产级 Core 交付拆分

详细路线图见 [Janus Core 生产级路线图](./docs/Janus-production-roadmap.md)。Core v1.0 GA 的完整能力覆盖与发布门禁见 [Janus Core Capability Matrix](./docs/Janus-core-capability-matrix.md)。本节只保留工程实现拆分和退出标准。

### 26.1 Milestone 0：基线冻结与工程卫生

- 固化 Core / Enterprise 边界。
- 清理主设计文档 CRLF / trailing whitespace。
- 建立统一测试命令。
- Docker Compose 稳定启动 PostgreSQL、NATS、Redis、Janus API。
- 当前 P0/P1/P2 backlog 明确化。

退出标准：

- Core、server、cli、sdk-go、demo、proto 测试可重复运行。
- Python SDK 有明确测试依赖和运行方式。
- 文档不存在 Core / Enterprise 边界冲突。

### 26.2 Milestone 1：Core Reliability Alpha

- Transactional outbox 使用稳定 `dedupe_key`。
- NATS publish 使用 `Nats-Msg-Id` 或等价去重机制。
- API accepted 后 task 保持 `created`；mailbox publish 成功后才进入 `queued`。
- Outbox publisher 后置 transaction 同时标记 outbox published、推进 task 状态、补写 `task.queued` event。
- `TaskMessage` / `TaskDelivery` 携带 `attempt`。
- ACK / NACK / Start / Heartbeat 校验 `(tenant_id, task_id, attempt, lease_id)`。
- Redelivery reconciliation 覆盖旧 delivery、终态 task、retry_scheduled task、created-but-published task。
- Lease timeout、retry、DLQ、DLQ replay 全链路幂等。
- API 启动时从 PostgreSQL tenants/mailboxes 自动 ensure NATS streams/consumers。

退出标准：

- NATS publish 成功但 outbox 后置 DB transaction 失败可恢复。
- DB 已写 completed 但 NATS ACK 失败后，旧 delivery redelivery 不重复执行。
- API 进程在 pull 后、ACK 前重启不会丢失任务。
- 旧 attempt ACK / NACK 重复提交不产生副作用。
- lease timeout 后原 Agent 再 ACK 不会覆盖新 attempt。
- retry exhausted 后进入 DLQ，DLQ replay 后可重新入队。

### 26.3 Milestone 2：API / SDK Contract Beta

- proto 中补齐 `attempt`、标准错误码、result/result_ref 语义。
- HTTP handler、gRPC handler 与 proto 保持一致。
- 恢复标准 grpc-gateway 生成链路。
- Go SDK、Python SDK 补齐 attempt、API key、标准错误类型。
- TypeScript SDK 进入 Core。
- CLI 支持 agent、task、mailbox、DLQ、api-key 常用操作。
- API key 管理 API/CLI。
- mTLS 作为 Core 可选部署模式。

退出标准：

- Go/Python/TypeScript SDK 跑通同一套 pull-execute-ack/nack 示例。
- proto、HTTP、SDK 字段没有语义分叉。
- 不兼容变更有 migration note。

### 26.4 Milestone 3：Interop + Routing Beta

- Agent capabilities 注册、更新、查询完整落库。
- Resolver foundation 支持：
  - `mailbox`：校验 active mailbox。
  - `agent`：校验 online agent，并选择 active mailbox。
  - `capability`：从 online capability agent 的 active mailbox 中按 backlog 选择目标。
  - `group` / `human`：Core 支持 tenant-scoped 静态 mailbox 映射；无显式 `mailbox_id` 且未命中映射时拒绝创建。
- Resolver 失败写入 `routing.failed` 审计事件，并通过 `janus_routing_failures_total{tenant_id,target_type,reason}` 暴露指标。
- capability routing 已叠加 data classification、policy、capacity、budget 硬约束过滤、可选语义排序和候选评分细节，并通过 `routing.selected` 记录成功路由解释。
- A2A Gateway 完整映射 Agent Card、task/message、状态、错误、trace/context。
- ACP Gateway beta 映射 Agent Manifest、run、状态、错误、trace/context。
- MCP Gateway beta 映射 tool call、resource、状态、错误、trace/context。
- ContextRef 与 task 绑定关系可用。
- Artifact/Object Store Core interface、本地实现和 S3-compatible adapter 边界。
- LangGraph / AutoGen / CrewAI / GitHub Actions 示例接入见 `examples/interop/`。

退出标准：

- A2A agent-to-agent 链路可以通过 Janus 完成 publish、dispatch、result 和 audit 查询：已完成。
- MCP 只作为 tool/context 接入，不绕过 Janus policy/budget/audit：已完成。
- LangGraph/AutoGen/CrewAI/GitHub Actions 至少以示例方式接入：已完成，见 `examples/interop/`。

### 26.5 Milestone 4：Ops + Observability RC

- OpenTelemetry trace provider 接入。
- Prometheus metrics 覆盖 publish、pull、ACK/NACK、retry、DLQ、outbox backlog、mailbox backlog、lease timeout、policy deny、budget throttle。
- 结构化 JSON log，包含 `tenant_id`、`task_id`、`attempt`、`trace_id`。
- `/healthz`、`/readyz`、dependency readiness 分离。
- 基础 Helm chart。
- migration、backup/restore、rolling upgrade runbook。
- Dashboard 展示 agents、mailboxes、task lifecycle、outbox backlog、retry/DLQ、audit trace。

v0.5.1 已实现 HTTP observability foundation：

- `log` 与 `observability` 配置可通过环境变量覆盖。
- `/metrics` 支持开关和路径配置。
- HTTP request middleware 记录结构化 JSON log，字段包含 `tenant_id`、`task_id`、`attempt`、`trace_id`。
- HTTP request 指标覆盖 `janus_http_requests_total` 与 `janus_http_request_duration_seconds`。
- 支持 W3C `traceparent` 入站提取和响应传播；v0.5.7 已补齐 OTLP exporter 初始化与 shutdown hook。

v0.5.2 已实现 gRPC observability foundation：

- gRPC unary server interceptor 记录 request 指标和结构化 JSON log。
- gRPC request 指标覆盖 `janus_grpc_requests_total` 与 `janus_grpc_request_duration_seconds`。
- gRPC 入站 metadata 支持 `traceparent` / `x-trace-id`，启用 tracing 时在 response header 返回 `traceparent` / `x-trace-id`。
- gRPC 指标 label 限定为 method/status；tenant/task/attempt 只进入日志，避免 Prometheus 高基数风险。
- v0.5.7 已补齐 OpenTelemetry provider、HTTP/gRPC span lifecycle 和 OTLP exporter，当前 tracing 链路不再只是 metadata propagation。

v0.5.3 已实现 backlog / online gauge collector：

- `janus_outbox_backlog{tenant_id,kind}` 统计 outbox `pending`、`retry`、`publishing`。
- `janus_mailbox_backlog{tenant_id,mailbox_id}` 和兼容指标 `janus_queue_backlog{tenant_id,mailbox_id}` 统计 mailbox 当前积压。
- `janus_agent_online{tenant_id}` 统计 online agent 数。
- collector 支持 partial error，单个 mailbox backlog 采集失败不会阻断其他 gauge 刷新。

v0.5.4 已实现 worker/scanner/outbox 结构化日志：

- `server/internal/logutil` 提供 internal JSON log helper，供后台组件复用。
- outbox publisher 的 fetch、publish、mark published、mark task published 失败日志结构化，包含 worker、outbox、tenant、kind、dedupe key、attempt 等字段。
- event projector、lease scanner、expiry scanner、heartbeat sweeper、metrics collector 的错误和关键状态日志结构化。
- coverage 门禁纳入 `server/internal/logutil` 与 `server/internal/metrics`，防止新生产包游离在覆盖率统计之外。

v0.5.5 已实现基础 Helm chart 与 runbook：

- `deployments/helm/janus-core` 提供 Deployment、Service、ConfigMap、Secret、ServiceAccount、PodDisruptionBudget 和可选 artifact PVC。
- chart values 覆盖 Core runtime 配置：PostgreSQL、NATS、Redis、migration、auth、TLS、artifact、log、metrics、tracing、resources、probes、securityContext。
- chart 默认提供 startup/liveness/readiness probe、Prometheus scrape annotations、resource requests/limits、non-root securityContext 和 artifact writable volume。
- `docs/Janus-ops-runbook.md` 覆盖 Helm install/upgrade/rollback、controlled migration、artifact PVC、Prometheus scrape、backup/restore 和 rolling upgrade。

v0.5.6 已实现 Dashboard JSON / provisioning 示例：

- `deployments/grafana/dashboards/janus-core.json` 提供 Janus Core Grafana dashboard。
- `deployments/grafana/provisioning/dashboards/janus-core.yaml` 提供 dashboard provisioning provider。
- dashboard 使用 `DS_PROMETHEUS` datasource 变量，并覆盖 outbox/mailbox backlog、online agents、DLQ、HTTP/gRPC latency/error、publish/pull latency、task lifecycle、policy/budget/routing blocks。
- `docs/Janus-ops-runbook.md` 说明 Grafana provisioning mount 和 datasource 使用方式。

v0.5.7 已实现 OpenTelemetry / OTLP tracing：

- `janus-api` 启动时根据 `observability.tracing` 初始化 W3C propagator、OpenTelemetry tracer provider 和 OTLP gRPC exporter。
- `observability.tracing.endpoint` 非空时导出 span；为空时保留 trace metadata propagation 但不导出。
- `observability.tracing.insecure` 控制 collector 明文/TLS 连接，默认明文以匹配集群内 collector 常见部署。
- HTTP middleware 为每个 request 创建 span，记录 method、route、status、url path、tenant/task/attempt 属性，并继续返回 `Traceparent` / `X-Trace-ID`。
- gRPC unary interceptor 为每个 unary call 创建 span，提取入站 metadata trace context，记录 method/status 与 tenant/task/attempt 属性，并继续通过 response header 返回 tracing headers。
- `janus-api` 退出时执行 tracer provider shutdown hook，尽量 flush batch span。

v0.5 剩余：

- v0.5 Core Ops + Observability RC 功能项已完成；后续只保留 bugfix、文档校准和部署演练反馈。

退出标准：

- 单集群 Kubernetes 部署可复现。
- 滚动重启期间不丢任务。
- 指标能定位 NATS、PostgreSQL、Redis、outbox、mailbox 的主要故障。
- Dashboard 能解释任务为什么未被执行。

### 26.6 Milestone 5：Production Beta

- PR review、CI failure triage、自动修复、安全扫描、发布审批 dogfood 场景。
- 7 天 soak test。
- API/NATS/PostgreSQL/Redis/Agent crash chaos test。
- 1k active agents、10k mailboxes、100 task/s publish、500 event/s audit 负载基线。
- API key rotation、mTLS deployment、tenant guard、secret handling 安全基线。

退出标准：

- dogfood 链路可以替代点对点 Agent 调用。
- 故障恢复有可审计证据。
- 没有已知 P0。
- P1 都有明确 owner 和 release 目标。

v0.6.1 已实现 Production Beta 前置验证骨架：

- `server/tests/productionbeta` 提供 fast profile，覆盖可配置 agent/mailbox/task 规模、并发 worker、agent crash 后 lease expiry/retry/requeue、slow consumer 和 publish/pull p95 预算。
- `make beta-fast` 运行快速门禁；`make beta-soak` 运行显式长周期 profile，默认需要 `JANUS_RUN_LONG_SOAK=true`。
- v0.6.1 不宣称完成 Production Beta 退出标准；真实 Kubernetes 集群、依赖重启/failover chaos、OTLP/Grafana 联调、1k/10k/100 task/s/500 event/s 基线和 7 天 soak 仍在 v0.6 后续工作中完成。

v0.6.2 已实现 API / NATS / PostgreSQL / Redis smoke 工具：

- `make smoke-deps` 运行 `scripts/smoke_api_dependencies.sh`，默认关闭 metrics/tracing，不测试 OTLP、Prometheus、Grafana。
- smoke 启动真实 `janus-api` 进程，检查 `/healthz` 与 `/readyz`，要求 `postgres`、`nats`、`redis` 全部 ok。
- smoke 执行真实 HTTP API 生命周期：tenant、agent、mailbox、publish、pull、start、ack，并验证 task completed/result_ref。
- 本地启动模式增加依赖 preflight；缺少 PostgreSQL/NATS/Redis 时快速失败。当前开发环境未安装 Docker/PostgreSQL/NATS，因此完整 smoke 需要在具备这些依赖的环境实跑确认。

v0.6.3 已实现完整 production smoke stack 与验证入口：

- `deployments/smoke-deps.compose.yaml` 包含 PostgreSQL、NATS JetStream、Redis、Prometheus、Grafana、Tempo、OpenTelemetry Collector。
- `make smoke-prod` 开启 metrics/tracing，执行 API/NATS/PostgreSQL/Redis 生命周期验证，并额外验证 `/metrics`、Prometheus target/query、Grafana health/dashboard provisioning、Tempo readiness 和 OTLP collector 端口。
- Prometheus 默认抓取 `host.containers.internal:18080/metrics`，Grafana provisioning 复用 Janus Core dashboard 并配置 Prometheus/Tempo datasources。
- 完整 production smoke 需要先由用户通过 Podman 启动 compose stack，随后运行 `make smoke-prod`。

### 26.7 Milestone 6：Core v1.0 GA

Core v1.0 GA 要求 Capability Matrix 中所有 `P0` Core 能力达到 `Covered`；仍为 `Partial`、`Missing test` 或 `Not implemented` 的 `P0` 项均视为生产级发布阻塞项。

- publish 不丢。
- Agent 离线不丢。
- ACK / NACK 幂等。
- retry / DLQ 可恢复。
- API 多实例可运行。
- Task Envelope spec 稳定。
- Go / Python / TypeScript SDK 可用。
- Docker Compose、Helm、migration、backup/restore 文档齐全。
- metrics / traces / logs 能解释主要故障。
- API key、mTLS、tenant_id 逻辑隔离、基础 policy、基础 audit 可用。

---

## 27. 开放问题

需要后续验证：

- Budget 估算模型如何基于历史执行数据迭代。
- 是否需要第一阶段引入 OPA。
- 是否需要支持 Agent result streaming；Dashboard 状态推送 WebSocket 已纳入 MVP。
- 大客户独立 stream 的自动化创建、配额和迁移策略。

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

大客户使用独立 Stream，不共享统一 `JANUS_TASKS` stream。推荐使用独立 NATS account 保持 subject 语义不变；如果仍在同一 account 内，共享 `JANUS_TASKS` 必须排除该 tenant 的 task subject，避免同一 subject 同时归属多个 stream。租户间通过 NATS subject namespace 隔离，大客户可请求独立 stream 以保证物理隔离、独立 retention 和独立扩容。

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

DLQ raw stream 消息体携带 `tenant_id`、`mailbox_id`、`task_id`、`attempt`、`attempt_count`、`original_envelope`、`error_payload`、`failure_reason`、`policy_decision_id`、`first_failed_at`、`dead_lettered_at`、`dedupe_key` 和 `headers`；没有来源的可选字段可省略。NATS headers 仍保留 `JANUS-Task-ID`、`JANUS-Tenant-ID`、`JANUS-Mailbox-ID`、`JANUS-Attempt`、`Nats-Msg-Id` 和 `JANUS-DLQ-Error`，用于兼容运维与低层排障。

### 29.3 Heartbeat 存储

Agent heartbeat 使用 **Redis + PostgreSQL** 协作存储，不使用 NATS 承载逐条 heartbeat event。

| 场景 | 存储 |
|---|---|
| 实时 Heartbeat TTL | Redis sorted set `agent:heartbeat:<tenant>`，member 为 `agent_id`，score 为 `expire_at_ms` |
| durable heartbeat fact | PostgreSQL `agents.last_heartbeat_at` |
| 离线检测 | 后台 sweeper 通过 `ZRANGEBYSCORE agent:heartbeat:<tenant> -inf now_ms` 找到过期 Agent，更新 PostgreSQL `agents.status = 'offline'`，再从 sorted set 删除 member |
| 外部查询 | `GetAgent` 优先读 Redis sorted set 并由 `expire_at_ms - ttl` 推导最近心跳；Redis 无记录时保留 PostgreSQL `last_heartbeat_at` |

Redis 是 MVP 与生产部署的**生产强依赖**，但不承担 durable task / event / audit fact 存储。

### 29.4 Task Result 存储

小型 Task Result 可通过 ACK 的 `result_ref` 引用外部结果；Janus 不在 `tasks` 表中保存大结果正文。生产结果应先写入 artifact/object store，再在 ACK 中提交 URI。

### 29.5 Artifact / Object Store

Artifact/Object Store 是 Core 的大结果和上下文对象入口。v0.4.8 提供 Core beta 能力：

- `core.ArtifactStore` 抽象：`Put(ctx, tenantID, reader, opts)` 与 `Get(ctx, uri)`。
- 本地实现 `LocalArtifactStore`：按 tenant 目录隔离，生成 `artifact://local/{tenant_id}/{name}` URI，计算 `sha256` 与 size。
- `ArtifactService`：统一做 tenant 校验、上传、下载，以及可选 ContextRef 注册。
- `POST /v1/tenants/{tenant_id}/artifacts?name=...`：上传 artifact，body 为原始对象内容，`Content-Type` 写入 metadata。
- `GET /v1/tenants/{tenant_id}/artifacts?uri=artifact://local/...`：下载 artifact，并返回 `X-Artifact-URI` / `X-Artifact-SHA256`。
- `POST /v1/tenants/{tenant_id}/artifacts?context_ref=true&classification=...&access_scope=a,b`：上传后创建 `type=artifact` 的 ContextRef，供后续 Task `context_refs` 引用。
- 配置项：`artifacts.store=local`、`artifacts.local_dir=data/artifacts`。

边界：

- Core v0.4.8 不实现 S3 SigV4、KMS、WORM、retention、quota 或 per-tenant bucket isolation。
- S3-compatible adapter 可在后续版本通过同一 `ArtifactStore` 接口接入。
- KMS、WORM、retention、per-tenant bucket isolation 和合规导出属于 Enterprise 边界。

### 29.6 Budget 估算模型

MVP 阶段使用**静态硬限制**，预算由调用方在 Task Envelope 中声明 `max_tokens` / `max_cost_usd`。后期基于 `task_attempts.token_usage` 历史数据迭代启发式或 ML 估算模型。

### 29.7 Policy 引擎

MVP 阶段使用**内置规则引擎**，基于 `policy_rules` 表的 JSON condition + action 模型。Policy Service 通过 `PolicyEngine` 接口抽象，后期可接入 OPA / Cedar。

Core GA 额外提供 policy template 配置入口，解决常见 Agent 协作治理场景下手写 JSON rule 过重的问题。模板是控制面编译器，不是第二套策略引擎：

- API：`POST /v1/tenants/{tenant_id}/policy-rules/templates`
- CLI：`janus policy allow-agent --agent coding-agent --capability code_review`
- SDK：`CreatePolicyRuleFromTemplate` / `create_policy_rule_from_template` / `createPolicyRuleFromTemplate`
- 输出：稳定 `id`、`name`、`status`、`priority`、`condition`、`action`
- 持久化：写入标准 `policy_rules`
- 查询：继续通过 `GET /v1/tenants/{tenant_id}/policy-rules`
- 执行：继续由 `PolicyService.Evaluate` 在 task publish、tool invoke、task route 等检查点统一判断

支持的 Core 模板：

| 模板 | 作用域 | 生成的 policy condition 语义 |
|---|---|---|
| `allow_agent_capability` / `deny_agent_capability` | source agent -> capability | `actor.id` + `action=task.publish` + `resource.type=capability` |
| `allow_team_capability` / `deny_team_capability` | source team -> capability | `actor.team_id` + `action=task.publish` + `resource.type=capability` |
| `require_approval_capability` | capability | `action=task.publish` + `resource.type=capability` |
| `require_approval_tool` | tool | `action=tool.invoke` + `resource.type=tool` |
| `allow_agent_data_classification` / `deny_agent_data_classification` | target agent receiving classified data | `action=task.route` + `context.target_agent_id` + `context.data_classification` |
| `allow_team_data_classification` / `deny_team_data_classification` | target team receiving classified data | `action=task.route` + `context.target_team_id` + `context.data_classification` |
| `allow_agent_tool` / `deny_agent_tool` | agent invoking tool at current policy checkpoint | `actor.id` + `action=tool.invoke` + `resource.type=tool` |
| `allow_team_tool` / `deny_team_tool` | team invoking tool at current policy checkpoint | `actor.team_id` + `action=tool.invoke` + `resource.type=tool` |

优先级仍按 `policy_rules.priority` 升序执行，数字越小越先匹配。`allow` 模板主要用于用更高优先级覆盖更宽泛的 deny 规则；没有匹配规则时 Core 的默认行为仍是 allow。

`tool.invoke` policy 在两个检查点可能执行：publish/create 阶段的 actor 是 `source_agent`，用于限制谁可以请求某个 Janus-visible tool；dispatch 阶段的 actor 是实际 claim mailbox 的执行 Agent，配合 mailbox owner check 用于限制谁可以执行或接收该 tool-like task。两次检查都调用同一个 `PolicyService.Evaluate`，不会引入第二套策略模型。

```go
type PolicyEngine interface {
    Evaluate(ctx context.Context, input PolicyInput) (PolicyDecision, error)
}
```

### 29.8 WebSocket Streaming

MVP 阶段需要 WebSocket 支持，场景限定为 **Dashboard/UI 实时推送**（Task 状态变化、Agent 上线/下线、队列积压）。

实现方式：轻量 WebSocket Gateway，按 tenant/task_id/agent_id 过滤 NATS event stream 后推送。

Agent-to-Agent 通信继续使用 Pull + gRPC，不使用 WebSocket。

### 29.9 A2A Task Status 映射

A2A Gateway 层实现 A2A 标准状态与 Janus 内部状态机的完整映射。Janus 核心 Task Service 保持自身状态语义不变。

| A2A 状态 | Janus 状态 |
|---|---|
| `submitted` | `created` / `queued` / `claimed` / `retry_scheduled` |
| `working` | `running` |
| `input-required` | `blocked` / `approval_pending` |
| `completed` | `completed` |
| `failed` | `failed` / `dead_lettered` / `expired` |
| `canceled` | `cancelled` |

### 29.10 Agent SDK

MVP 提供 **Go** 和 **Python** SDK，覆盖以下 API：

```
PublishTask(envelope)    发布任务
PullTask(mailbox, agent_id)       拉取任务
StartTask(task_id, attempt, lease_id)     声明开始执行
Heartbeat(task_id, attempt, lease_id)     发送心跳
AckTask(task_id, attempt, lease_id, result)  确认完成
NackTask(task_id, attempt, lease_id, error)  确认失败
RegisterAgent(spec)     注册 Agent
```

TypeScript SDK 属于 Core 的开发者采用能力，已进入 v0.3 Core 稳定 SDK 面，不归入 Enterprise。Go / Python / TypeScript SDK 必须共享 HTTP conformance fixture，并通过同一套 `pull -> start -> heartbeat -> ack/nack` worker flow fixture。

SDK 的 `TaskEnvelope` 类型必须覆盖 Core 协议字段，不能只保留最小发布字段。Go / Python / TypeScript 至少需要提供：

- `idempotency_key`
- `ttl_seconds`
- `deadline`
- `budget.max_tokens` / `budget.max_cost_usd` / `budget.model_classes`
- `policy.data_classification` / `policy.requires_human_approval` / `policy.allowed_tools`
- `context_refs[]`
- `tool_invocation`
- `trace.trace_id` / `trace.parent_task_id` / `trace.span_id`

### 29.11 CLI

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

### 29.12 多租户策略

数据模型从 Day 1 即携带 `tenant_id` 字段。除 `tenants` 这类全局根表外，租户内资源表均以 `(tenant_id, id)` 或等价复合键作为主键 / 唯一边界。MVP Demo 和 CLI 默认使用 `default` 租户，不暴露多租户 UI。

后期 Enterprise 实现完整多租户物理隔离（独立加密密钥、NATS account、storage bucket、deployment namespace、retention / quota 和 tenant lifecycle）。Core 仍必须保留 `tenant_id` 逻辑隔离、策略检查和审计归属。

### 29.13 数据库 Migration

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
│   │   │   ├── acp/               # ACP Gateway beta
│   │   │   ├── mcp/               # MCP Gateway beta
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
server -> core       # 实现 core 中的领域接口
server -> proto      # handler 层序列化
sdk/go -> proto      # 客户端序列化
cli -> sdk/go        # CLI 调用 SDK
```

`core` 不 import `proto`。领域模型和接口使用 core 自身类型；handler / SDK 负责在 proto DTO 与 core domain object 之间转换。

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
| 日志 | `log` + JSON writer | 当前实现使用标准库 `log` 输出结构化 JSON；后续可迁移到 `slog` |
| Metrics | `prometheus/client_golang` | Prometheus 生态 |
| Tracing | W3C `traceparent` propagation；OpenTelemetry OTLP exporter | 当前已实现 HTTP/gRPC trace metadata 传播、HTTP/gRPC span lifecycle、OTLP gRPC exporter 初始化和 shutdown hook |
| Migration | `golang-migrate` | SQL 文件驱动 |
| JSON | `encoding/json` | 标准库 |
| ID 生成 | `ULID` | 有序、唯一、可排序 |
| 测试 | `testify` | 断言 + mock |

### 30.4 配置模型

运行时配置文件推荐命名为 `janus-server.yaml`。默认查找顺序是当前目录、`/etc/janus/`、`$HOME/.janus/` 中的 `janus-server.yaml`；如果不存在，再兼容读取旧 `janus.yaml`。`JANUS_CONFIG_FILE` 可以显式指定文件路径。环境变量覆盖配置文件。

```yaml
# janus-server.yaml

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

redis:
  addr: "localhost:6379"
  password: "${JANUS_REDIS_PASSWORD}"
  db: 0

log:
  level: "info"                # debug / info / warn / error
  format: "json"               # json / text

migration:
  auto: true                   # 启动时自动 migrate up
  path: "migrations/"

heartbeat:
  sweeper_interval: "30s"      # 后台扫描 Redis heartbeat 过期的间隔
  ttl: "60s"                   # Agent 心跳 TTL

outbox:
  enabled: true
  poll_interval: "500ms"
  idle_backoff_max: "5s"
  batch_size: 100
  lease_duration: "60s"
  max_retries: 5
  listen_notify: true

observability:
  metrics:
    enabled: true
    path: "/metrics"           # Prometheus scrape path
  tracing:
    enabled: true
    service_name: "janus-api"
    endpoint: ""               # OTLP gRPC endpoint，例如 otel-collector:4317；为空时只做传播、不导出
    insecure: true             # collector 明文连接；TLS collector 设置为 false
```

`cache.*` 作为历史兼容别名仍可被归一化到 `redis.*`，但新配置应使用 `redis.*`。

生产级配置拆成两类文件：

| 文件 | 作用 | 是否进入运行时治理模型 |
| --- | --- | --- |
| `janus-server.yaml` | Janus API 进程配置：PostgreSQL、NATS、Redis、auth、TLS、artifact、log、metrics、tracing。 | 否 |
| `janus.project.yaml` | 租户内资源配置：tenant、agent、mailbox、budget、常见 policy template。 | 否，apply 后编译成现有 Core 资源 |

`janus.project.yaml` 支持多 tenant，但 `janus project apply` 默认只操作 `default_tenant` 或 `--tenant` 指定的 tenant；`--all-tenants` 才批量 apply。`janus apply` 保留为短别名。Tenant boundary 仍由 API key、auth guard 和 tenant-scoped storage 强制执行，不由项目配置放宽。

项目配置示例：

```yaml
version: v1
default_tenant: acme

tenants:
  acme:
    name: Acme Engineering
    agents:
      code-review:
        team: engineering
        capabilities:
          - id: code_review
            data_classifications: [public, internal, confidential]
        concurrency: 4
    budgets:
      tenant:
        tpm: 2000000
        daily_usd: 500
    policies:
      approve:
        capabilities: [prod_deploy]
      allow:
        - agent: code-review
          capability: code_review
```

CLI 默认查找当前目录或父目录中的 `janus.project.yaml`，`--file` 只作为高级覆盖项。动态操作成功后必须固化回项目文件：

```sh
janus tenant add acme --name "Acme Engineering"
janus agent add code-review --tenant acme --team engineering --capability code_review --concurrency 4
```

`janus project sync` 用于把通过 SDK 显式注册 API、CLI 或其他控制面动态创建到 Janus 的资源合并回本地 project 文件。`JanusWorker` 只负责 heartbeat、pull、start、ack/nack，不隐式注册 Agent 或 mailbox。配置文件不是新的策略引擎；policy 配置仍通过 policy template 生成标准 `policy_rules`，运行时继续由 `PolicyService.Evaluate` 统一执行。

---

## 31. 仓库与版本策略

### 31.1 仓库划分

| 仓库 | 地址 | 可见性 | 内容 |
|---|---|---|---|
| Janus | `github.com/agentium-lab/Janus` | Private（成熟后 Public） | Core：引擎、Task Envelope、durable mailbox、ACK / NACK / retry / DLQ、lease timeout、transactional outbox、基础 tenant_id 逻辑隔离、基础 Policy、基础 Audit、基础 Metrics / Trace、A2A / ACP / MCP 基础 adapter、Artifact/Object Store 抽象、Go / Python / TypeScript SDK、CLI、基础 Dashboard、Demo |
| Janus-enterprise | `github.com/agentium-lab/Janus-enterprise` | Private | Enterprise：OIDC / SSO / SAML、SCIM、RBAC / ABAC、完整多租户物理隔离、KMS / per-tenant key、独立 NATS account / stream、独立 artifact bucket、高级 DLP / PII 检测引擎、高级审计、WORM / SIEM export、合规报表、成本中心、企业策略引擎、HA Helm / Operator、backup / restore、SLO dashboard、air-gapped / 私有化交付、高级拓扑分析、incident replay、受控商业集成包 |

两个独立仓库，Janus-enterprise import Janus 作为依赖。

### 31.2 Core / Enterprise 接口边界

Enterprise 只能扩展 Core 的接口和服务编排，不应替换 Core 的可靠性语义。以下能力必须保留在 Janus Core：

- durable task / event delivery。
- ACK / NACK / retry / DLQ。
- lease timeout 和 redelivery reconciliation。
- transactional outbox。
- 基础 tenant_id 逻辑隔离。
- 基础 API key / mTLS。
- 基础 Policy 和 Audit。
- 基础 OpenTelemetry / Prometheus metrics export。
- A2A / ACP / MCP 基础协议映射。
- Artifact/Object Store 接口与基础实现。
- Go / Python / TypeScript SDK、CLI 和基础 Dashboard。

以下能力由 Janus-enterprise 实现：

- 企业身份：OIDC / SSO / SAML、SCIM、IdP group mapping、workload identity federation。
- 企业授权：RBAC / ABAC、组织层级、group membership、角色审批和权限审计。
- 完整多租户物理隔离：per-tenant encryption key、NATS account / stream、artifact bucket、deployment namespace、retention、quota 和 tenant lifecycle。
- 高级治理：DLP、PII 检测、脱敏、数据分类、跨 tenant / region 数据流控制。
- 高级合规：签名审计、WORM、SIEM export、retention policy、合规报表、incident review。
- 成本中心：org / team / project budget、chargeback / showback、usage export、预算审批。
- 企业策略：OPA / Cedar、policy bundle、versioning、dry-run、策略审批流。
- 企业交付：HA Helm / Operator、backup / restore、upgrade runbook、SLO dashboard、alerting package、air-gapped 部署。
- 高级分析：agent topology、dependency graph、bottleneck analysis、topology drift、incident replay。
- 受控集成：LangGraph / AutoGen / CrewAI / GitHub Actions 的企业连接器、治理预设和最佳实践模板。

灰区处理规则：

- A2A / ACP / MCP 基础互通在 Core；catalog、连接器审批、企业审计和策略包在 Enterprise。
- Artifact/Object Store 抽象在 Core；KMS、WORM、retention 和 per-tenant bucket isolation 在 Enterprise。
- OpenTelemetry / Prometheus 基础导出在 Core；SLO dashboard、告警包、SIEM / DWH 集成在 Enterprise。
- 语义路由的基础候选排序在 Core；受治理的私有索引、模型策略和高级拓扑分析在 Enterprise。
- SDK、CLI、Task Envelope spec 和基础 adapter 示例在 Core；商业连接器包和企业模板在 Enterprise。