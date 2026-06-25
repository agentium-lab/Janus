# Janus: A2A-native Durable Agent Broker 产品与概要设计

## 1. 一句话定位

**Janus 是面向企业 Agent 网络的 A2A 原生可靠消息代理与治理控制面。**

它不重新发明 Agent 通信协议，也不试图替代 LangGraph、AutoGen、CrewAI、Temporal、Dapr 等 Agent Runtime 或工作流引擎。Janus 的核心职责是：

> 让不同框架、不同团队、不同运行环境中的 Agent 能够可靠、安全、可审计、可控成本地交接任务。

更具体地说，A2A / ACP 解决的是 **Agent 如何互相说话**，Janus 解决的是 **Agent 通信进入生产环境后如何不丢、不乱、不越权、不爆预算、可追踪、可恢复**。

---

## 2. 背景与机会

### 2.1 背景

大语言模型正在推动企业应用从单一 Agent 走向多 Agent 协作。一个真实的企业 Agent 网络通常会包含：

- Coding Agent
- Review Agent
- Test Agent
- Security Agent
- Data Analyst Agent
- Product Agent
- Customer Support Agent
- Release / Ops Agent
- Human approval node

这些 Agent 可能来自不同框架、不同模型供应商、不同云平台，也可能运行在本地、CI Runner、Kubernetes、Serverless 或开发者桌面中。

点对点的 Agent-to-Agent 调用在早期 Demo 中可行，但进入生产后会暴露明显问题：

- Agent 不在线时任务容易丢失。
- 长任务容易中断，状态难以恢复。
- 调用链路缺少统一审计。
- 下游 Agent 被并发、RPM、TPM 或上下文窗口限制打爆。
- 谁能调用谁、能携带哪些上下文、能访问哪些工具缺少统一策略。
- 企业无法统一观察 Agent 拓扑、Janus 可见预算事实、失败率和安全风险。

### 2.2 Janus 的机会

Janus 的机会不在于做一个通用消息队列，也不在于做一个泛化 Agent 平台，而在于成为：

> **A2A 生态里的可靠投递层、治理层和调度层。**

协议会标准化，模型会变化，Agent 框架会持续迭代，但企业始终需要一层中立基础设施来管理 Agent 协作的生产运行语义。

Janus 应优先切入 **Coding / DevOps Agent 协作** 场景。这个场景具备几个优势：

- 任务链路清晰：需求、开发、审查、测试、安全、发布。
- 异步协作天然存在：Agent 经常离线、排队、重试、等待审批。
- 价值容易衡量：交付效率、失败恢复、审计可见性、Janus 可见预算与容量控制。
- 用户技术能力强：便于开源传播和早期集成。
- 私有化诉求明确：代码、凭据、业务逻辑不能随意出域。

---

## 3. 设计原则

### 3.1 A2A / ACP 优先，MCP 辅助

Janus 的协议优先级应为：

1. **A2A / ACP**：作为 Agent-to-Agent 的主协议，承担 Agent 发现、能力声明、任务委派、状态回传和异步协作。
2. **MCP**：作为工具、数据源、上下文资源的接入协议，用于连接外部能力，而不是 Janus 的主通信协议。
3. **自定义 SDK / Adapter**：用于接入现有 Agent 框架、内部系统和遗留自动化平台。

因此，Janus 不是 MCP Broker，而是 **A2A-native Agent Broker**，同时兼容 MCP 作为工具与上下文生态入口。

### 3.2 可靠性优先于智能性

语义路由是亮点，但不是地基。生产系统中，路由决策必须先满足：

- 权限约束
- 租户隔离
- Agent 能力声明
- SLA 与优先级
- 成本与预算
- 可用性与负载
- 合规策略

在这些硬约束过滤之后，语义匹配才用于候选排序。

### 3.3 Broker 不拥有业务逻辑

Janus 不应把自己变成工作流引擎或业务编排平台。它只负责 Agent 任务交接的运行语义：

- 收件箱
- 投递
- ACK
- 重试
- 死信队列
- 状态追踪
- 策略校验
- 成本与容量调度

业务流程可以由 LangGraph、Temporal、Dapr、企业内部流程引擎或业务服务承载。

### 3.4 治理是一等公民

Agent 网络一旦进入企业生产环境，治理不是附加功能，而是核心价值。

Janus Core 必须默认支持治理基础能力；完整企业 IAM 能力属于 Janus Enterprise：

- Agent 身份
- 基础授权边界与策略执行
- RBAC / ABAC 扩展点
- 租户隔离
- 审计日志
- Trace ID
- 策略决策
- DLP Hook
- 人工审批 Hook
- 成本预算
- 失败重放

---

## 4. 目标用户与核心场景

### 4.1 目标用户

Janus 的早期目标用户不是普通终端用户，而是构建企业 Agent 基础设施的人：

- 企业 AI 平台团队
- 研发效能团队
- 平台工程团队
- DevOps / SRE 团队
- 安全与合规团队
- Agent 平台创业公司
- 需要私有化部署的受监管行业客户

### 4.2 第一场景：Coding / DevOps Agent 协作

典型链路：

1. Product Agent 发布需求。
2. Coding Agent 拉取任务并修改代码。
3. Review Agent 审查代码质量。
4. Test Agent 运行单元测试、集成测试和回归测试。
5. Security Agent 扫描依赖、凭据和高风险变更。
6. Human Approver 审批关键发布节点。
7. Release Agent 执行发布或生成发布建议。

Janus 在该链路中的价值：

- 每个 Agent 有持久收件箱。
- 每个任务有生命周期状态。
- 失败任务可以重试或进入 DLQ。
- 每一步经过 Janus 的任务交接、执行状态和调用方显式回传的 usage 可追踪。
- 高风险任务可以进入人工审批。
- 企业可以审计谁触发了什么操作、传递了什么上下文、产出了什么结果。

---

## 5. 核心产品形态

### 5.1 Durable Agent Mailbox

每个 Agent 在 Janus 中拥有一个或多个持久收件箱。

能力：

- 离线投递
- Pull-based dispatch
- ACK / NACK
- Retry policy
- TTL
- Priority
- Dead Letter Queue
- Idempotency key
- Backlog visibility

这是 Janus 的最小可用核心。没有 durable mailbox，Janus 就只是一个网关；有 durable mailbox，Janus 才是可靠 Agent 协作基础设施。

### 5.2 Agent Registry

Agent 注册表负责维护 Agent 元数据。

核心字段：

```yaml
agent_id: code-reviewer.team-a
tenant_id: acme
team_id: team-a
protocol: a2a
capabilities:
- capability: code.review
description: Reviews Python code for correctness, security, and maintainability.
schema: {"language":"python"}
description: Reviews Python code for correctness, security, and maintainability.
endpoint: https://reviewer.internal/a2a
limits:
max_concurrency: 4
rpm: 60
tpm: 200000
status: online
```

Core GA 的 Agent Registry 契约以当前实现为准：每个 Agent 使用单一 `protocol` 字段，能力存入 `agent_capabilities`，`team_id` 是 team 级 policy / routing / budget 的显式边界，`description` 可参与 intent -> capability 解析和候选排序。多协议数组、runtime 编排元数据、per-agent `allowed_callers` / `requires_approval_for` 不写入 Core registry schema；这些约束必须通过 `policy_rules`、approval workflow 或未来 Janus Enterprise 的组织 / RBAC / ABAC 层表达。

为了降低常见治理配置成本，Core 提供 policy template 入口，但模板不改变策略事实来源。`janus policy allow-agent --agent coding-agent --capability code_review`、`janus policy require-approval --capability prod_deploy`、`janus policy deny-tool --agent intern-agent --tool deploy.prod` 这类命令最终都会生成标准 `policy_rules`，并继续通过 `PolicyService.Evaluate` 执行。

为了进一步降低配置成本，Core CLI 提供 `janus.project.yaml` 作为统一项目配置入口，支持多 tenant、声明式 agent/mailbox/budget/policy、`janus apply/diff/validate`，以及 `janus tenant add` / `janus agent add` 成功后固化回项目文件。项目配置只是控制面编译入口，最终仍写入 Agent Registry、Mailbox、Budget 和标准 `policy_rules`。

### 5.3 Task Lifecycle

Janus 应定义清晰的任务生命周期：

```text
created -> approval_pending -> queued -> claimed -> running -> completed
created -> queued
claimed -> failed -> retry_scheduled -> queued
running -> blocked -> running
running -> failed -> retry_scheduled -> queued
running -> failed -> dead_lettered
queued -> expired
任意非终态 -> cancelled
```

这些状态必须是可查询、可审计、可重放的。

### 5.4 Policy Layer

策略层决定任务是否可以被投递。

策略维度：

- Caller identity
- Target identity
- Tenant
- Team
- Capability
- Data classification
- Tool permission
- Context scope
- Cost budget
- Human approval requirement

策略引擎可以先内置轻量规则，后续支持 OPA / Cedar / 自定义策略插件。

### 5.5 Token and Capacity Scheduler

传统消息队列不理解 LLM 的资源模型。Janus 应内置 AI 原生容量调度：

- RPM 限制
- TPM 限制
- 并发限制
- 模型预算
- 团队预算
- 租户预算
- 任务优先级
- Deadline
- 下游 Agent 可用性

当 Agent 或模型配额不足时，Janus 不应继续推送任务，而应让任务在队列中安全等待。

### 5.6 Trace, Audit and Replay

每个任务都必须形成完整链路：

- 谁创建了任务
- 谁接收了任务
- 任务经过了哪些 Agent
- 每一步经过 Janus 的任务交接、策略决策和可见工具型任务
- 调用方通过 Task Envelope / ACK usage 显式回传的 Token 和费用
- 输入输出摘要是什么
- 哪些上下文被引用
- 哪些策略被命中
- 哪里失败，是否重试

Replay 能力用于事故复盘、调试和回归验证。

Janus 不侵入 Agent runtime 或 MCP Tool Server 内部。Agent 或 MCP tool 私有调用模型产生的成本、工具链细节和中间步骤不由 Janus 核算；Janus 只审计和治理经过 Janus 的任务、上下文引用、策略/预算/容量决策、result/error，以及 Janus 自身使用模型产生的成本。

### 5.7 Semantic Routing

语义路由不是第一优先级，但可以形成产品差异。

正确设计应为：

```text
候选 Agent = Registry 中满足硬约束的 Agent
候选 Agent = 按权限、租户、SLA、预算、负载过滤
候选 Agent = 按能力标签和 schema 过滤
候选 Agent = 用 embedding / reranker 做语义排序
最终选择 = 策略 + 语义 + 成本 + 可用性共同决策
```

语义路由适合做：

- 候选召回
- 能力模糊匹配
- Agent 推荐
- 相似任务复用
- 自动分派建议

不适合单独决定生产投递。

---

## 6. 系统架构

### 6.1 分层架构

```text
External Agents / Runtimes
- A2A Agents
- ACP-compatible Agents
- LangGraph / AutoGen / CrewAI
- CI / DevOps Agents
- Human Approval Nodes
- MCP Tool Servers

|
v

Janus Ingress Layer
- A2A Gateway
- ACP Gateway
- SDK Gateway
- WebSocket / gRPC Gateway
- MCP Gateway / Tool & Context Adapter

|
v

Janus Control Plane
- Agent Registry
- Policy Engine
- Tenant / Team Management
- Token Budget Manager
- Topology Manager
- Billing and Audit

|
v

Janus Data Plane
- Durable Mailbox
- Task Queue
- ACK / Retry / DLQ
- Pull Dispatcher
- Backpressure Controller
- Trace Event Stream

|
v

Storage and Runtime
- NATS JetStream
- PostgreSQL
- Redis
- Object Storage
- Vector Index
```

### 6.2 控制面

控制面负责慢路径决策：

- Agent 注册与发现
- 能力声明管理
- 租户与团队管理
- 策略管理
- Token 预算
- 成本统计
- 拓扑观察
- 审计查询

### 6.3 数据面

数据面负责高频任务流转：

- 任务写入
- 队列持久化
- Pull dispatch
- ACK / NACK
- 重试
- DLQ
- Trace event
- Backpressure

控制面与数据面应解耦，避免策略、审计、UI 查询影响任务投递性能。

### 6.4 队列与事件后端抽象

Janus 默认使用 **NATS JetStream** 作为统一消息与事件后端，覆盖：

- Task mailbox
- Pull dispatch
- ACK / Retry / DLQ
- Trace event stream
- Audit event stream
- Billing event stream
- Agent status event stream

第一阶段不应过早引入 Pulsar、Kafka、AutoMQ 等重型组件。Janus 当前最重要的是验证 Agent 可靠交接、治理和成本控制的产品价值，而不是提前承担多后端运维复杂度。

但 Janus 的业务层不能直接绑定 NATS 细节，应保留清晰的后端扩展接口：

```text
Janus Queue/Event Driver
- nats-jetstream # default
- pulsar # future
- kafka # future
- automq # future
- rocketmq # future
```

该接口至少应抽象：

- Stream / subject / topic 创建
- 消息发布
- Pull 消费
- ACK / NACK
- 重试策略
- DLQ 写入
- Consumer offset / cursor
- Retention policy
- Tenant namespace
- Event replay

后期只有在以下需求明确出现后，才引入新的 Event / Audit Plane：

- Janus Cloud 出现大规模多租户部署。
- Trace、Audit、Billing 事件需要长期保留。
- 客户要求接入 Kafka / Pulsar 生态。
- 需要对接 Flink、ClickHouse、SIEM、数据湖等数据平台。
- NATS 在事件保留、分析生态或跨区域复制上成为明确瓶颈。

---

## 7. 消息模型

### 7.1 Janus Task Envelope

Janus 应定义稳定的任务信封，而不是直接转发裸 prompt。

```json
{
"janus_version": "0.1",
"task_id": "task_01HZY...",
"idempotency_key": "repo-123-pr-456-review",
"tenant_id": "acme",
"source_agent": "product-agent.team-a",
"target": {
"type": "capability",
"value": "code_review"
},
"priority": "normal",
"deadline": "2026-06-10T10:00:00Z",
"budget": {
"max_tokens": 120000,
"max_cost_usd": 3.0
},
"policy": {
"data_classification": "internal",
"requires_human_approval": false
},
"context_refs": [
{
"type": "git_pr",
"uri": "github://acme/repo/pull/456"
}
],
"payload": {
"type": "code_review_request",
"content": "Review this PR for correctness and security."
},
"trace": {
"trace_id": "trace_01HZY...",
"parent_task_id": null
}
}
```

### 7.2 为什么需要 Envelope

Envelope 是 Janus 形成优势的关键。它让任务具备生产语义：

- 可幂等
- 可审计
- 可重试
- 可限流
- 可计费
- 可策略判断
- 可跨协议适配
- 可跨 Agent Runtime 传递

---

## 8. MVP 范围

### 8.1 MVP 必须做

第一版 Janus 应聚焦以下能力：

1. A2A-compatible Agent Registry
2. Durable Mailbox
3. Pull-based Task Dispatch
4. ACK / Retry / DLQ
5. Task Lifecycle API
6. Token / Concurrency Budget
7. Basic Policy Layer
8. Trace and Audit Log
9. CLI and local dashboard
10. Coding / DevOps Agent demo

### 8.2 MVP 不应做

第一版不建议做：

- Agent Marketplace
- 全自动复杂语义路由
- 大而全工作流编排
- 多云 SaaS 控制台
- 通用企业 Agent Mesh
- 自研向量数据库
- 自研 LLM Gateway
- 自研身份系统

这些能力可以后续演进，但不应进入第一个可验证版本。

---

## 9. 技术选型建议

| 模块 | MVP 推荐 | 后续可选 | 说明 |
| :--- | :--- | :--- | :--- |
| 开发语言 | Go | Rust | Broker、网关和队列适配层优先选择 Go，生态成熟、并发简单 |
| 队列与事件后端 | NATS JetStream | Pulsar / Kafka / AutoMQ / RocketMQ | 默认用 NATS 统一覆盖任务投递与事件审计；后期通过 Queue/Event Driver 按规模和客户场景扩展 |
| 元数据存储 | PostgreSQL | CockroachDB | 存储 Agent、任务、审计、租户、策略 |
| 实时状态与限流 | Redis | KeyDB / Dragonfly | MVP / 生产部署保留 Redis，用于 Agent heartbeat TTL、短期状态、限流计数和调度 hint；不承载 durable task / event / claim lease |
| 策略引擎 | 内置规则 | OPA / Cedar | 初期不要过度复杂 |
| 向量检索 | 内存索引 / Qdrant | Milvus / pgvector | 语义路由后置，不应阻塞 MVP |
| API | gRPC + HTTP + Dashboard WebSocket | WebSocket result streaming | gRPC 用于 Agent 通信，HTTP 用于管理面，轻量 WebSocket 只用于 Dashboard 实时状态推送 |
| 部署 | Docker Compose / Helm | Operator | 先保证本地和单集群易部署 |
| 可观测性 | OpenTelemetry | Prometheus / Grafana | Trace 语义应从第一天设计 |

---

## 10. 商业化路径

### 10.1 Open-core 策略

Janus 适合采用 open-core：

边界原则：

- **Core 必须包含生产可靠性底座**：durable mailbox、ACK / NACK、retry、DLQ、lease timeout、redelivery reconciliation、transactional outbox、基础审计和基础策略都属于 Janus 能否成立的核心能力，不放入商业版。
- **Enterprise 聚焦企业治理、隔离、合规和运营**：SSO / RBAC / ABAC、完整多租户物理隔离、高级 DLP / PII 检测、合规报表、成本中心、HA 运维包和高级策略管理属于商业化增强。
- **协议互通优先开源**：A2A / ACP / MCP 的基础 adapter 和 Task Envelope 映射属于生态入口，应保留在 Core；Enterprise 只增强 catalog、审批、治理、审计和受控连接器。
- **开发者采用能力优先开源**：Go / Python / TypeScript SDK、CLI、基础 Dashboard、基础 Helm chart、示例 adapter 和 Task Envelope spec 不应作为商业版门槛。

开源核心：

- A2A / ACP adapter
- MCP Tool / Context adapter 基础能力
- Durable mailbox
- Task lifecycle
- ACK / NACK / Retry / DLQ
- Lease timeout / redelivery reconciliation
- Transactional outbox
- Basic tenant_id logical isolation
- API key / mTLS 基础认证
- Go / Python / TypeScript SDK
- CLI
- 本地 dashboard
- 基础策略
- 基础审计
- 基础 metrics / trace / OpenTelemetry export
- Artifact/Object Store 抽象与基础实现
- 基础语义路由候选排序
- 自然语言 intent 到 capability target 的可审计解析，例如“我想审查这段代码”解析为 `code_review`
- LangGraph / AutoGen / CrewAI / GitHub Actions 示例

商业版：

- 完整多租户隔离：独立 encryption key、NATS account / stream、artifact bucket、deployment namespace、tenant lifecycle 和 quota。
- 企业身份与权限：OIDC / SSO / SAML、RBAC / ABAC、SCIM、IdP group 映射、tenant admin / auditor 角色。
- 高级审计与合规：签名审计、WORM 存储、SIEM export、retention policy、合规报表、incident review。
- DLP 与数据治理：PII 检测、脱敏策略、跨 tenant / region 数据流控制、SIEM export 前过滤和合规报表；Core 只提供 DLP hook 接口与关键调用点。
- 成本中心：org / team / project 级预算、chargeback / showback、usage export、预算审批流。
- 企业策略引擎：OPA / Cedar 集成、policy bundle、versioning、dry-run、审批流程。
- HA 与私有化交付：HA Helm / Operator、backup / restore、upgrade runbook、SLO dashboard、alerting package、air-gapped 部署。
- 高级拓扑分析：agent dependency graph、bottleneck analysis、topology drift、incident replay。
- 受控商业集成包：LangGraph / AutoGen / CrewAI / GitHub Actions 的企业连接器、治理预设和最佳实践模板。
- SLA 与优先级调度。

### 10.2 付费客户

优先客户：

- 企业 AI 平台团队
- 研发效能平台团队
- 安全合规要求高的研发组织
- 私有化部署客户
- 构建 Agent 平台的创业公司

不优先：

- 只做个人效率工具的开发者
- 只需要单 Agent 应用的团队
- 不需要审计、权限、可靠性的 Demo 项目

---

## 11. 护城河

Janus 的优势应围绕五类护城河构建。

### 11.1 兼容护城河

优先兼容：

- A2A
- ACP
- MCP
- LangGraph
- AutoGen
- CrewAI
- Claude Code / Codex 类 Coding Agent
- GitHub Actions
- Jenkins
- Kubernetes Jobs

谁能连接更多 Agent Runtime，谁就更接近基础设施。

### 11.2 运行语义护城河

Janus 应定义并稳定输出一套 Agent 生产运行语义：

- Durable mailbox
- Task envelope
- Task lifecycle
- Budget headers
- Policy claims
- Trace context
- Replay semantics

用户一旦依赖这套语义，就会产生切换成本。

### 11.3 企业信任护城河

Janus 应主打：

- 中立
- 私有化
- 可审计
- 可控成本
- 跨云
- 跨框架
- 不绑定单一模型供应商

这是区别于云厂商内置 Agent 平台的关键。

### 11.4 数据护城河

Janus 会积累 Agent 执行数据：

- 任务耗时
- 失败模式
- 重试原因
- 成本分布
- 路由决策
- Agent 能力表现
- 策略命中情况

这些数据可以反过来优化调度、预算、路由和风险控制。

### 11.5 工作流嵌入护城河

Janus 应尽快嵌入高频生产链路：

- PR review
- CI failure triage
- 自动修复
- 安全扫描
- 发布审批
- 数据分析任务交接

当 Janus 成为生产链路的一部分，它就不再只是可替换的中间件。

---

## 12. 路线图

详细执行路线见 [Janus Core 生产级路线图](./docs/Janus-production-roadmap.md)。

路线图不再按"先 MVP、再生态、再企业控制面"简单推进，而是按 Janus Core 是否满足生产级 broker 语义推进。

### 12.1 Milestone 0：基线冻结与工程卫生

目标：让当前仓库、测试、文档和部署基线可控。

交付：

- 固化 Core / Enterprise 边界。
- 清理主设计文档换行和行尾空白，减少无关 diff。
- 建立统一测试命令。
- Docker Compose 可稳定启动 PostgreSQL、NATS、Redis、Janus API。
- 当前 P0/P1/P2 backlog 明确化。

### 12.2 Milestone 1：Core Reliability Alpha

目标：补齐 Janus 作为生产级 durable broker 的可靠性闭环。

交付：

- Transactional outbox 稳定 `dedupe_key`。
- NATS publish 使用去重键。
- `created` 只有在 mailbox publish 成功后才推进到 `queued`。
- `TaskMessage` / `TaskDelivery` 携带 `attempt`。
- ACK / NACK / Start / Heartbeat 校验 `(tenant_id, task_id, attempt, lease_id)`。
- Redelivery reconciliation 覆盖旧 delivery、终态 task、retry_scheduled task。
- Lease timeout、retry、DLQ、DLQ replay 全链路幂等。
- API 启动时从 PostgreSQL tenants/mailboxes 自动 ensure NATS streams/consumers。

退出标准：

- NATS publish 成功但 DB 后置 transaction 失败可恢复。
- DB completed 但 NATS ACK 失败后，旧 delivery redelivery 不重复执行。
- API 重启不导致任务丢失或 ACK/NACK 失效。
- retry exhausted 后进入 DLQ，DLQ replay 后可重新入队。

### 12.3 Milestone 2：API / SDK Contract Beta

目标：把 Core 可靠性语义稳定暴露给调用方。

交付：

- proto / HTTP / SDK 字段一致。
- 标准 grpc-gateway 生成链路。
- Go / Python SDK 补齐 attempt、API key、标准错误类型。
- TypeScript SDK 进入 Core。
- CLI 支持 task、mailbox、agent、DLQ、api-key 常用操作。
- API key 管理 API/CLI。
- mTLS 可选部署模式。

### 12.4 Milestone 3：Interop + Routing Beta

目标：接入真实 Agent 生态，而不是只服务内部 demo。

交付：

- Agent capabilities 完整注册、更新、查询和落库。
- 基础 resolver 支持 mailbox、agent、capability、group、human。
- A2A Gateway 完整映射 Agent Card、task/message、状态、错误、trace/context。
- ACP Gateway beta 映射 Agent Manifest、run、状态、错误、trace/context。
- MCP Gateway beta 映射 tool call、resource、状态、错误、trace/context。
- Artifact/Object Store Core interface 和基础实现。
- LangGraph / AutoGen / CrewAI / GitHub Actions 示例。

### 12.5 Milestone 4：Ops + Observability RC

目标：让 Janus Core 可以被部署、观察、升级和回滚。

交付：

- OpenTelemetry trace provider。
- Prometheus metrics 覆盖 publish、pull、ACK/NACK、retry、DLQ、outbox backlog、mailbox backlog、lease timeout、policy deny、budget throttle。
- 结构化 JSON log。
- `/healthz`、`/readyz`、dependency readiness 分离。
- 基础 Helm chart。
- migration、backup/restore、rolling upgrade runbook。
- Dashboard 展示 agent、mailbox、task lifecycle、outbox backlog、retry/DLQ、audit trace。

### 12.6 Milestone 5：Production Beta

目标：在真实但受控的生产链路中 dogfood。

交付：

- PR review、CI failure triage、自动修复、安全扫描、发布审批等 dogfood 场景。
- 7 天 soak test。
- API/NATS/PostgreSQL/Redis/Agent crash chaos test。
- 1k active agents、10k mailboxes、100 task/s publish、500 event/s audit 负载基线。
- API key rotation、mTLS deployment、tenant guard、secret handling 安全基线。

### 12.7 Milestone 6：Core v1.0 GA

目标：发布可以被外部用户部署到生产链路中的 Janus Core。

GA 标准：

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

### 12.8 Enterprise 启动条件

Janus-enterprise 不应早于 Core Reliability Alpha。

启动条件：

- Core outbox / ACK / NACK / retry / DLQ / lease timeout 故障测试通过。
- API / SDK attempt 契约稳定。
- tenant_id 逻辑隔离和 audit trace 贯穿核心链路。
- Helm 单集群部署可用。

Enterprise 第一阶段只做 OIDC/SSO、RBAC、per-tenant KMS key、audit export、高级 DLP / PII 检测引擎和 cost center UI，不复制 Core 可靠性逻辑。

---

## 13. 主要风险

### 13.1 大厂内置化风险

云厂商可能把 A2A 通信、Agent 网关、审计、成本控制内置进自己的平台。

应对：

- 坚持中立与私有化。
- 做好跨云、跨框架、跨模型。
- 优先服务不愿被单一云锁定的客户。

### 13.2 被通用工作流引擎吞噬

Temporal、Dapr、LangGraph 等可能覆盖一部分状态、任务和恢复能力。

应对：

- 不正面竞争工作流引擎。
- 专注 Agent-to-Agent 的可靠交接层。
- 与工作流引擎集成，而不是替代。

### 13.3 语义路由价值被高估

生产环境不会信任纯向量距离决定任务投递。

应对：

- 把语义路由作为候选排序和推荐。
- 核心卖点放在可靠性、治理、审计和成本控制。

### 13.4 场景过宽导致产品失焦

如果一开始做 Enterprise Agent Mesh，容易变成平台大饼。

应对：

- 从 Coding / DevOps Agent 协作切入。
- 用具体任务链路验证价值。
- 后续再横向扩展。

---

## 14. 成功标准

Janus 早期不应以功能数量衡量，而应以生产问题是否被解决衡量。

关键指标：

- 任务投递成功率
- 失败恢复率
- 平均任务等待时间
- DLQ 处理率
- Agent 离线期间任务保留率
- Token 超预算拦截率
- 审计链路完整率
- 人工审批命中准确率
- 单个团队接入时间
- 每个 Agent 的平均集成成本

---

## 15. 最终判断

Janus 的方向成立，但不能按“通用 Agent 平台”去做。

它最有机会的路径是：

> **从 Coding / DevOps Agent 协作切入，用 open-core 获得开发者分发，用企业控制面变现，最终扩展为 A2A 生态中的中立可靠运行层。**

Janus 的核心优势不应是“更聪明地匹配 Agent”，而应是：

> **让 Agent 协作进入生产后依然可靠、可控、可审计、可恢复。**
