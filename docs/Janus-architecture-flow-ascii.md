# Janus 生产级 ASCII 架构与流程图

版本：0.1
状态：设计补充
关联文档：

- [Janus.md](../Janus.md)
- [Janus-detail-design.md](../Janus-detail-design.md)
- [Janus-architecture-flow.md](./Janus-architecture-flow.md)

---

## 1. 图例

本文档用 ASCII 图补充说明 Janus 生产级架构和核心流程。所有图均按生产目标形态表达，以可靠性、治理、审计、隔离和恢复语义为准。

图中缩写：

| 缩写 | 含义 |
|---|---|
| A2A GW | A2A Gateway |
| SDK GW | SDK Gateway |
| WS GW | Dashboard WebSocket Gateway |
| TaskSvc | Task Service |
| DispSvc | Dispatch Service |
| PolSvc | Policy Service |
| BudSvc | Budget Service |
| AprSvc | Approval Service |
| AudSvc | Event / Audit Service |
| OutboxPub | Outbox Publisher |
| Q/Event Driver | Queue/Event Driver |
| PG | PostgreSQL |
| JS | NATS JetStream |
| Redis | Redis / Valkey |
| DLQ | Dead Letter Queue |

---

## 2. 生产级总体架构

```text
+------------------------------- External Network -------------------------------+
| |
| A2A Agents ACP Agents SDK Agents CI/CD Human UI MCP Servers |
| Enterprise export consumers such as SIEM / DWH are optional extensions. |
| |
+------------------------------------+-------------------------------------------+
|
v
+------------------------------- Edge / Security --------------------------------+
| |
| +---------+ +-----------+ +-------------+ +--------------------------+ |
| | DNS/GSLB|-->| WAF / API |-->| mTLS / OIDC |-->| Rate Limit / Tenant Gate | |
| +---------+ | Gateway | | AuthN | +------------+-------------+ |
| +-----------+ +-------------+ | |
+---------------------------------------------------------------+----------------+
|
v
+--------------------------- Janus Ingress Plane -------------------------------+
| |
| +-----------+ +-----------+ +-----------+ +-----------+ +----------+ |
| | A2A GW xN | | ACP Adp xN| | SDK GW xN | | HTTP/gRPC | | WS GW xN | |
| +-----+-----+ +-----+-----+ +-----+-----+ | API xN | +----+-----+ |
| | | | +-----+-----+ | |
| +---------------+---------------+---------------+--------------+ |
+----------------------------------------+---------------------------------------+
|
v
+-------------------------- Janus Service Plane --------------------------------+
| |
| Stateless API services, horizontally replicated: |
| |
| +----------+ +----------+ +----------+ +----------+ +----------+ |
| |Registry | |TaskSvc | |Mailbox | |DispSvc | |Policy | |
| |xN | |xN | |xN | |xN | |xN | |
| +----+-----+ +----+-----+ +----+-----+ +----+-----+ +----+-----+ |
| | | | | | |
| +----+-----+ +----+-----+ +----+-----+ +----+-----+ +----+-----+ |
| |Budget | |Context | |Approval | |Audit | |A2A Map | |
| |xN | |xN | |xN | |xN | |xN | |
| +----------+ +----------+ +----------+ +----------+ +---------+ |
| |
| Background workers, independently scaled and leader-safe: |
| |
| +-------------+ +-------------+ +-------------+ +-------------+ |
| |OutboxPub xN | |EventProj xN | |LeaseScan xN | |HbSweep xN | |
| +------+------+ +------+------+ +------+------+ +------+------+ |
+---------+-------------+-------------+-------------+-------------+-------------+
| | | | |
v v v v v
+--------------------------- Infrastructure Plane --------------------------------+
| |
| +----------------+ +----------------+ +----------------+ +-----------+ |
| | Q/Event Driver | | PG Repository | | Redis Client | | Artifact | |
| | NATS adapter | | pgx / pool | | cluster aware | | Adapter | |
| +-------+--------+ +-------+--------+ +-------+--------+ +-----+-----+ |
+----------|--------------------|--------------------|------------------|-------+
| | | |
v v v v
+------------------+ +------------------+ +------------------+ +-------------+
| NATS JS Cluster | | PostgreSQL HA | | Redis Cluster / | | Object / |
| 3+ nodes | | primary/standby | | Sentinel | | Artifact |
| tasks/events/DLQ | | state/outbox | | heartbeat/rate | | Storage |
+--------+---------+ +--------+---------+ +--------+---------+ +------+------+
| | | |
v v v v
+------------------+ +------------------+ +------------------+ +-------------+
| OTel / Metrics | | Core Audit API | | Alerting hooks | | Enterprise |
| traces/logs | | projection query | | optional | | SIEM / DWH |
+------------------+ +------------------+ +------------------+ +-------------+
```

关键点：

- Ingress 只做协议适配和入口处理。
- Core Services 负责业务状态机、策略、预算、审批、审计和调度。
- Queue/Event Driver 隔离 NATS 细节。
- PostgreSQL 是状态、outbox、ledger 和投影事实来源。
- Redis 只做 heartbeat TTL、短窗口计数和辅助实时状态。
- 生产部署中 API 服务无状态横向扩展，后台 worker 通过数据库锁、租约或幂等键保证可并行运行。
- NATS、PostgreSQL、Redis 都按 HA 形态部署；大客户可使用独立 NATS account / stream、独立 bucket 或独立 namespace。

---

## 3. 控制面与数据面

```text
Control Plane
=============

+----------+ +----------+ +----------+
| Registry | | Policy | | Budget |
+-----+----+ +-----+----+ +-----+----+
| | |
+----------------+----------------+
|
v
+-------------+
| PostgreSQL |
| metadata |
| rules |
| projection |
+-------------+


Data Plane
==========

+---------+ +---------+ +---------+ +---------+
| Publish | -> | Outbox | -> | Mailbox | -> | Pull |
+---------+ +----+----+ +----+----+ +----+----+
| | |
v v v
+---------+ +---------+ +---------+
| PG | | NATS JS | | Claim |
| outbox | | tasks | | lease |
+---------+ +---------+ +----+----+
|
v
+-----------+
| ACK/NACK |
| Retry/DLQ |
+-----------+
```

控制面是慢路径，面向配置、查询和治理。数据面是热路径，面向任务流转、投递、ACK/NACK、重试和 DLQ。

---

## 4. 生产运行时资源

```text
+------------------------------ NATS JetStream Cluster -------------------------+
| |
| account: tenant-shared or tenant-dedicated |
| |
| +-----------------------+ +-----------------------------------------+ |
| | Stream: JANUS_TASKS | ----> | Durable pull consumer per mailbox | |
| | subject tasks.> | | consumer_<tenant>_<mailbox_safe_name> | |
| | retention WorkQueue | +-----------------------------------------+ |
| +-----------------------+ |
| |
| +-----------------------+ +-----------------------------------------+ |
| | Stream: JANUS_EVENTS | ----> | Event projector / WebSocket gateway | |
| | subject events.> | | Audit export / trace readers | |
| +-----------------------+ +-----------------------------------------+ |
| |
| +-----------------------+ |
| | Mailbox DLQ streams | |
| | tasks_dlq.<mailbox> | |
| +-----------------------+ |
+------------------------------------------------------------------------------+

+------------------------------- PostgreSQL HA --------------------------------+
| |
| primary + standby / managed HA / PITR backup |
| |
| metadata: tenants, agents, capabilities, mailboxes |
| state: tasks, task_attempts, approvals |
| outbox: outbox_events with dedupe_key and next_attempt_at |
| budget: budgets, budget_usage, budget_usage_ledger |
| audit: audit_event_projection with source/target/actor columns |
| |
| Required for: publish, claim, ACK/NACK, redelivery reconciliation |
+------------------------------------------------------------------------------+

+------------------------------ Redis / Valkey HA -----------------------------+
| |
| cluster / sentinel / managed HA |
| |
| heartbeat: sorted set agent:heartbeat:<tenant>, member=agent_id, |
| score=expire_at_ms |
| counters: rpm/tpm/concurrency short-window counters |
| helper: transient throttling and scheduling hints |
| |
| Not used for durable task, durable event, or audit fact storage |
+------------------------------------------------------------------------------+

+--------------------------- Object / Compliance Storage -----------------------+
| |
| Core: result artifacts and large outputs through ArtifactStore interface |
| Enterprise: signed audit export, WORM, SIEM / DWH / data lake export |
+------------------------------------------------------------------------------+
```

生产基线仍以 PostgreSQL outbox `next_attempt_at` 驱动延迟重试，避免 retry stream 与 outbox scheduler 双重调度。同一部署如果引入独立调度器，必须先替换并重新定义 Queue/Event Driver 的 retry 语义。

---

## 5. Task 状态机

```text
+---------+
| created |
+----+----+
|
+---------------+---------------+
| |
v v
+------------------+ +----------+
| approval_pending | | queued |
+--------+---------+ +----+-----+
| |
approved | | pull + claim lease
v v
+---------+ +---------+
| queued | | claimed |
+----+----+ +----+----+
| |
| start | start
v v
+---------+ +---------+
| running |<------------------| claimed |
+----+----+ +----+----+
| |
+--------+---------+ |
| | | |
v v v v
+---------+ +-------+ +-----------+ +---------+
|completed| |blocked| | failed |<---| failed |
+---------+ +---+---+ +-----+-----+ +----+----+
| | |
| resume | retry allowed |
v v v
+---------+ +----------------+ +-------------+
| running | | retry_scheduled| | dead_letter |
+---------+ +-------+--------+ +-------------+
|
| next_attempt_at reached
v
+--------+
| queued |
+--------+

Terminal states:
completed, dead_lettered, expired, cancelled
```

规则：

- `approval_pending` 不进入 mailbox。
- 只有 `queued` 可以被 claim。
- `claimed` 和 `running` 都会 lease timeout。
- 终态任务被 NATS redelivery 时只能 ACK/drop。

---

## 6. Publish + Outbox 入队流程

```text
Caller
|
| PublishTask(envelope)
v
Ingress
|
v
TaskSvc
|
+-- validate envelope
+-- idempotency check
+-- source auth
+-- resolve target
+-- policy check
+-- budget pre-check
+-- if tool task: prepare tool.invocation_requested
|
v
+---------------------------------------------------+
| PostgreSQL TX |
| |
| insert tasks(status=created) |
| insert outbox event_publish task.created |
| if tool task and allowed: |
| insert outbox event_publish |
| tool.invocation_requested |
| tool.invocation_allowed |
| |
| if approval_required: |
| update tasks(status=approval_pending) |
| insert approvals |
| insert outbox event_publish task.approval |
| else: |
| insert outbox task_publish(attempt=1) |
+---------------------------------------------------+
|
| commit
v
Caller receives accepted
|
v
OutboxPub
|
| claim pending outbox rows
| publish to NATS with Nats-Msg-Id=dedupe_key
v
NATS JetStream
|
| publish ack
v
+---------------------------------------------------+
| PostgreSQL post-publish TX |
| |
| mark enqueue outbox published |
| update tasks(status=queued) |
| insert outbox event_publish task.queued |
+---------------------------------------------------+
```

重要语义：

- `accepted` 只表示 PostgreSQL transaction 成功。
- `queued` 表示 NATS mailbox publish 成功并完成后置 DB transaction。
- NATS 不可用时 outbox backlog 增长，不丢 task。
- 如果 policy / budget 拒绝工具型任务，Core 记录拒绝审计 payload，并写入
`tool.invocation_requested` + `tool.invocation_denied`；普通任务不依赖单独
的 `policy.denied` 事件。

---

## 7. Approval 流程

```text
Task status = approval_pending
|
v
+--------------+
| Approval UI |
+------+-------+
|
+-----+------+
| |
approve reject / timeout
| |
v v
+-------------------------------+ +------------------------------+
| PostgreSQL TX | | PostgreSQL TX |
| approval=approved | | approval=rejected/expired |
| outbox task_publish attempt=1 | | task status=cancelled |
+---------------+---------------+ | outbox task.cancelled event |
| +------------------------------+
v
OutboxPub
|
v
NATS mailbox
|
v
task status = queued
```

审批任务只有在 approve 后才会写入 mailbox。

---

## 8. Pull Dispatch 与 Redelivery Reconciliation

```text
Agent
|
| PullTask(mailbox, agent_id)
v
DispSvc
|
+-- validate agent identity
+-- validate mailbox binding
+-- check concurrency
|
v
Q/Event Driver
|
| consumer fetch
v
NATS durable consumer
|
v
TaskDelivery(task_id, attempt, delivery_ref)
|
v
+---------------------------------------------------+
| Redelivery reconciliation |
| |
| load task |
| if terminal: ACK delivery, return empty |
| if retry_scheduled: ACK stale delivery |
| if created + delivery exists: |
| TX set queued, mark outbox reconciled/published |
| if delivery attempt <= tasks.attempt_count: |
| ACK stale delivery |
+-------------------------+-------------------------+
|
v
dispatch-time policy
|
+---------------+---------------+
| |
deny / DLP allow policy
| |
policy.denied + NACK delay v
(default 5s)
budget usage accounting
|
+---------------+---------------+
| |
throttle allow
| |
budget.exceeded + NACK delay v
(default 5s)
+---------------------------------------------------+
| PostgreSQL TX |
| |
| insert task_attempts |
| lease_id |
| lease_expires_at |
| delivery_ref |
| update tasks |
| status=claimed |
| attempt_count=delivery attempt |
+---------------------------------------------------+
|
v
Agent receives envelope + lease + attempt
```

关键点：

- PG 不可用时不能 claim 新任务。
- `delivery_ref` 必须保存 NATS ACK 所需信息。
- 如果 DB 已完成状态提交但 NATS ACK 失败，下一次 redelivery 由 reconciliation 安全 ACK/drop。

---

## 9. ACK Complete 流程

```text
Agent
|
| AckTask(task_id, attempt, lease_id, result)
v
DispSvc
|
+-- validate current claim owner
+-- validate lease not expired
|
v
BudgetSvc
|
+-- update budget_usage
+-- write budget_usage_ledger
|
v
+---------------------------------------------------+
| PostgreSQL TX |
| |
| store result or result_ref |
| update tasks(status=completed) |
| update task_attempts(status=completed) |
| insert outbox event_publish task.completed |
+-------------------------+-------------------------+
|
v
Q/Event Driver
|
| ACK delivery_ref
v
NATS durable consumer

If NATS ACK fails:

NATS redelivery
|
v
Pull redelivery reconciliation
|
v
task already completed -> ACK/drop old delivery
```

ACK 是先落 PostgreSQL，再 ACK NATS。重复 ACK 不重复结算预算。

---

## 10. NACK / Retry / DLQ 流程

```text
Agent
|
| NackTask(task_id, attempt, lease_id, error)
v
DispSvc
|
+-- validate current claim owner
+-- release or partially settle budget
|
v
+---------------------------------------------------+
| PostgreSQL TX |
| |
| update task_attempts(status=failed) |
| update tasks(status=failed) |
+-------------------------+-------------------------+
|
v
retry decision
|
+---------------+----------------+
| |
v v
retry allowed retry exhausted
| |
v v
+-----------------------------+ +------------------------------+
| PostgreSQL TX | | PostgreSQL TX |
| status=retry_scheduled | | status=dead_lettered |
| outbox task_publish | | outbox dlq_publish |
| attempt=failed+1 | | outbox task.dead_lettered |
| next_attempt_at=now+backoff | +---------------+--------------+
+--------------+--------------+ |
| v
v mailbox DLQ stream
ACK original delivery
|
v
Outbox waits next_attempt_at
|
v
publish retry task to mailbox
|
v
task status = queued
```

生产基线不使用独立 retry stream。所有延迟重试都由 outbox `next_attempt_at` 调度，避免同一任务同时被两个调度源驱动。

---

## 11. Lease Timeout 流程

```text
LeaseTimeoutScanner
|
| find claimed/running attempts where lease_expires_at < now
v
+---------------------------------------------------+
| PostgreSQL TX |
| |
| lock task + attempt |
| verify lease is still current |
| mark attempt failed |
| release budget usage slot |
+-------------------------+-------------------------+
|
v
retry decision
|
+---------------+----------------+
| |
v v
retry_scheduled dead_lettered
| |
v v
outbox task_publish outbox dlq_publish
attempt+1 next_attempt_at DLQ message
```

`claimed` 和 `running` 都受 lease timeout 约束。

---

## 12. Agent Heartbeat 流程

```text
Agent
|
| Heartbeat
v
Janus API
|
| ZADD agent:heartbeat:<tenant> expire_at_ms agent_id
v
Redis


HeartbeatSweeper
|
| ZRANGEBYSCORE agent:heartbeat:<tenant> -inf now_ms
v
Redis
|
+-- exists ------------------> keep online
|
+-- expired -----------------+
|
v
+------------------+
| PostgreSQL TX |
| agent=offline |
| outbox event |
+--------+---------+
|
v
NATS event stream
```

实时 heartbeat 不写 NATS。只有 online/offline 状态变化写事件流。

---

## 13. Policy 与 Budget 决策链路

```text
Request
|
v
Identity / tenant / agent auth
|
v
Resolve target
|
+-- agent
+-- mailbox
+-- capability
+-- group
+-- human
|
v
Policy evaluation
|
+-- deny ---------------> reject + task/routing audit payload
| if tool task: tool.invocation_denied
|
+-- approval_required --> approval_pending
|
+-- redact_context ----> reduce payload/context
|
+-- throttle ----------> keep queued / NACK delay
|
+-- allow
|
v
Budget check / usage accounting
|
+-- insufficient --> throttle/reject + audit payload
| if tool task: tool.invocation_denied
|
+-- available ----> continue
```

Policy 和 Budget 是硬约束。语义路由只能在硬约束过滤后做候选排序。
`policy.denied` 和 `budget.exceeded` 是 Core GA 的 dispatch-time 阻断审计事件：fetch 后
policy / DLP / budget 拒绝必须先写事件，再 delayed NACK，解释为什么没有创建 claim。
Publish-time 拒绝仍可通过 task/routing/tool audit payload、`tool.invocation_denied` 和
metrics 表达；普通预算 reserve / settle 事实以 `budget_usage` / `budget_usage_ledger` 为准，
不逐次发布独立 `budget.*` 事件。

---

## 14. Dashboard WebSocket 流程

```text
Dashboard UI
|
| subscribe tenant/task/agent filters
v
WS Gateway
|
| consume filtered events
v
NATS JANUS_EVENTS / JANUS_AGENT_STATUS
|
v
WS Gateway
|
| push realtime update
v
Dashboard UI
|
| query history/detail
v
PostgreSQL audit projection
```

WebSocket 只服务 Dashboard/UI 实时状态推送，不用于 Agent-to-Agent 通信。

---

## 15. A2A / ACP / MCP 映射

```text
A2A / ACP Janus
+---------------+ +------------------+
| Agent Card | -----------> | agents |
| Skills | -----------> | capabilities |
| Task/Message | -----------> | Task Envelope |
| Task Status | -----------> | Task Lifecycle |
+---------------+ +------------------+

MCP Janus
+---------------+ +------------------+
| Tool Def | -----------> | capability hint |
| Tool Call | -----------> | TaskEnvelope |
| | | .tool_invocation |
| Tool Call JSON| -----------> | payload.type |
| | | =mcp_tool_call |
| Resource | -----------> | context_refs |
| Prompt | -----------> | payload template |
+---------------+ +------------------+
```

A2A / ACP 是 Agent-to-Agent 请求和状态互操作入口。MCP 只作为工具和上下文资源入口。MCP tool call 不直接绕过 Janus 执行，而是归一化为 `TaskEnvelope.tool_invocation` 和 `payload.type=mcp_tool_call`，再进入 Task Lifecycle、Mailbox、Policy、Budget、Audit 和 Result 引用语义。

### 15.1 Agent-to-Agent 数据流时序图

```text
Source Agent A2A GW / Auth TaskSvc Policy/Budget PG + Outbox NATS JS Target Agent Audit/Event
| | | | | | | |
| A2A send | | | | | | |
| task + context_ref | | | | | | |
+------------------->| | | | | | |
| | verify identity | | | | | |
| | tenant + agent | | | | | |
| +------------------>| | | | | |
| | | normalize | | | | |
| | | Task Envelope | | | | |
| | +------------------>| | | | |
| | | | policy allow? | | | |
| | | | approval needed? | | | |
| | | | budget pre-check | | | |
| | | +------------------>| | | |
| | | | | TX: tasks row | | |
| | | | | status=created | | |
| | | | | tool audit if any| | |
| | | | | outbox enqueue | | |
| | | | | task.created | | |
| | | | | tool.allowed? | | |
| | accepted task_id | | | | | |
|<-------------------+ | | | | | |
| | | | | OutboxPub polls | | |
| | | | +---------------->| publish task | |
| | | | | post-publish TX | | |
| | | | | status=queued | | |
| | | | +-------------------------------------------->| task.queued |
| | | | | | mailbox pull | |
| | | | | |<-----------------+ |
| | | | | | delivery | |
| | | | | +----------------->| |
| | | claim request | | | | |
| | |<--------------------------------------------------------------------------+ |
| | | | | TX: lease row | | |
| | | | | delivery_ref | | |
| | | | | status=claimed | | |
| | | claim ok | | | | |
| | +--------------------------------------------------------------------------->| |
| | | | | | | start execution |
| | | | |<-------------------------------------------+ |
| | | | | TX: running | | |
| | | | | heartbeat lease | | |
| | | | +-------------------------------------------->| task.running |
| | | | | | | work + tool use |
| | | | | | | result_ref |
| | | | |<-------------------------------------------+ |
| | | | | TX: completed | | |
| | | | | budget settle | | |
| | | | | task.completed | | |
| | | | | tool.completed? | | |
| | | | | | ACK delivery | |
| | | | | |<-----------------+ |
| | | | +-------------------------------------------->| task.completed |
| status/result_ref | | | | | | |
|<----------------------------------------------------------------------------------------------------------------------+ |
| | | | | | | |
```

失败和恢复分支：

- Policy deny：A2A GW 返回拒绝，TaskSvc 记录 task/routing 审计 payload；如果是工具型任务，同时记录 `tool.invocation_denied`，不进入 mailbox。
- Approval required：任务进入 `approval_pending`，审批完成前不发布到 mailbox。
- Budget insufficient：不创建可执行 claim，记录 throttle/reject 审计 payload；如果是工具型任务，同时记录 `tool.invocation_denied`。
- Target Agent NACK：ACK 原始 delivery，写入 `retry_scheduled`，由 outbox `next_attempt_at` 重新入队。
- Target Agent lost / no ACK：lease timeout scanner 标记 attempt failed，再按 retry / DLQ 规则处理。
- NATS ACK failed after DB commit：redelivery reconciliation 通过 PG 中的当前状态、attempt 和 `delivery_ref` 消除重复执行。

### 15.2 Tool Invocation 审计时序图

```text
Caller/Agent/SDK/MCP Gateway TaskSvc Policy/Budget PG+Outbox Dispatch Target Agent EventProj
| | | | | | | |
| tool task / call | | | | | | |
+------------------->| | | | | | |
| | normalize | | | | | |
| | tool_invocation| | | | | |
| +--------------->| | | | | |
| | | write requested | | | | |
| | +-------------------------------->| outbox event | | |
| | | hard checks | | | | |
| | +---------------->| policy/budget | | | |
| | | | | | | |
| | | denied? | | | | |
| | +-------------------------------->| tool.denied | | |
|<-------------------+ rejected | | | | | |
| | | | | | | |
| | | allowed? | | | | |
| | +-------------------------------->| tool.allowed | | |
| | | enqueue task | | | | |
|<-------------------+ accepted | | | | | |
| | | | | | pull/claim | |
| | | | |<---------------+ | |
| | | | | lease row | | |
| | | | |------------------------------->| task envelope |
| | | | | | StartTask | |
| | | | |<-------------------------------+ |
| | | | | tool.started | | |
| | | | | | ACK/NACK | |
| | | | |<-------------------------------+ |
| | | | | tool.completed | | |
| | | | | or tool.failed | | |
| | | | +----------------------------------------------->|
| | | | | | | project |
| | | | | | | source/target |
| | | | | | | actor fields |
```

`tool.invocation_*` 是协议中立审计链路，不只属于 MCP。Janus 只记录经过 Janus 的工具型任务交接、策略/预算/容量决策、上下文引用和 Janus 可见 result/error，不进入 Agent runtime 或 MCP Tool Server 的私有内部执行。

### 15.3 Audit Projection / Replay 流程

```text
TaskSvc / Dispatch / Registry / Approval
|
| write idempotent outbox_events
v
+-------------------+
| PostgreSQL Outbox |
+---------+---------+
|
| OutboxPub claims rows
v
+-------------------+ +-------------------+
| NATS JANUS_EVENTS | -----> | Event Projector |
+---------+---------+ +---------+---------+
| |
| replay / rebuild probe | DLP hook
+--------------------------->| Core no-op
| Enterprise impl
v
+--------------------------+
| audit_event_projection |
| legacy agent_id |
| source_agent |
| target_agent |
| actor_type / actor_id |
+------------+-------------+
|
v
Audit API / gRPC / Dashboard
```

事件流用于实时消费和重放；`audit_event_projection` 用于查询、Dashboard 和 GA 验证。Enterprise 可以在 DLP hook、签名审计、WORM、SIEM export 和合规报表处扩展，但不能改变 Core 的投影语义。

---

## 16. Coding / DevOps 端到端流程

```text
Product Agent
|
| publish requirement
v
Janus mailbox / outbox / audit
|
v
Coding Agent
|
| ACK result_ref
v
Review Agent
|
| ACK or NACK retry
v
Test Agent
|
| test result
v
Security Agent
|
v
+----------------+
| high risk ? |
+-------+--------+
|
+----+----+
| |
yes no
| |
v v
Human Release Agent
Approver |
| v
+------> Release suggestion / deployment

Every step:
policy check
budget usage accounting/settlement
trace event
audit projection
retry/DLQ if needed
```

---

## 17. 关键不变量

```text
+---------------------------------------------------------------+
| Janus Invariants |
+---------------------------------------------------------------+
| 1. Business services do not write mailbox directly. |
| Task enqueue must go through transactional outbox. |
| |
| 2. accepted != queued. |
| queued means mailbox publish + post-publish DB TX done. |
| |
| 3. One mailbox has one durable pull consumer. |
| Multiple workers share that consumer. |
| |
| 4. Retry uses outbox next_attempt_at in the production baseline.|
| No independent retry stream drives the same task lifecycle. |
| |
| 5. ACK/NACK commits DB first, then ACKs NATS. |
| NATS ACK failure is handled by redelivery reconciliation. |
| |
| 6. Claim requires durable lease and delivery_ref in PG. |
| PG unavailable means no new claim. |
| |
| 7. Redis stores no durable facts. |
| Redis is heartbeat/counter/realtime helper only. |
| |
| 8. Dedicated customer streams must not overlap subjects. |
| Prefer separate NATS account. |
| |
| 9. Policy / Budget cannot be bypassed by semantic routing. |
| Semantic routing only ranks already valid candidates. |
+---------------------------------------------------------------+
```

---

## 18. 生产级可靠闭环

```text
Agent Registry + Mailbox
|
v
Publish Task
|
v
Transactional Outbox
|
v
Task queued in mailbox
|
v
Agent Pull + Claim Lease
|
v
Start + Heartbeat + Execute
|
v
+--------------+
| result type? |
+------+-------+
|
+------+------------------+
| |
v v
ACK complete NACK / timeout
| |
v v
completed retry_scheduled
| |
v v
audit + budget queued again
|
v
retry exhausted
|
v
DLQ
|
v
audit + replay
```

生产级可靠闭环成立的标准：

- 发布不丢。
- Agent 离线不丢。
- ACK 失败不重复执行。
- Claim 超时可恢复。
- Retry 和 DLQ 可审计。
- Dashboard 能看到状态、事件和 backlog。

---

## 19. Core / Enterprise 边界

```text
+--------------------------------------------------------------------------+
| Janus Core |
|--------------------------------------------------------------------------|
| Task Envelope / durable mailbox / Queue+Event Driver |
| ACK / NACK / retry / DLQ / lease timeout / redelivery reconciliation |
| Transactional outbox / audit projection / tool.invocation_* / budget |
| tenant_id logical isolation / API key / mTLS |
| A2A / ACP / MCP basic adapters / Artifact interface / SDK / CLI |
| OpenTelemetry + Prometheus metrics export / local dashboard / demos |
+--------------------------------------------------------------------------+
|
| imported and extended by
v
+--------------------------------------------------------------------------+
| Janus Enterprise |
|--------------------------------------------------------------------------|
| OIDC / SSO / SAML / SCIM / RBAC / ABAC / IdP group mapping |
| Per-tenant KMS key / NATS account+stream / artifact bucket / namespace |
| DLP / PII detection / data classification / cross-region controls |
| Signed audit / WORM / SIEM export / compliance reports / incident review |
| Cost center / chargeback / showback / budget approval |
| OPA / Cedar bundles / policy versioning / dry-run / approval workflow |
| HA Helm+Operator / backup+restore / SLO dashboard / air-gapped delivery |
| Advanced topology / incident replay / governed commercial integrations |
+--------------------------------------------------------------------------+
```

边界规则：

- Core 负责 Janus 作为生产级 Agent Broker 必须成立的可靠性语义，Enterprise 不替换这些语义，只扩展治理、隔离、合规和运营能力。
- A2A / ACP / MCP 基础互通在 Core；catalog、连接器审批、企业审计和策略包在 Enterprise。
- Artifact/Object Store 抽象在 Core；KMS、WORM、retention 和 per-tenant bucket isolation 在 Enterprise。
- OpenTelemetry / Prometheus 基础导出在 Core；SLO dashboard、告警包、SIEM / DWH 集成在 Enterprise。
- 语义路由的基础候选排序在 Core；受治理的私有索引、模型策略和高级拓扑分析在 Enterprise。