<div align="center">

# Janus

**A2A 原生的持久化智能体任务总线**

在 AI 智能体之间可靠、安全、可审计地交接任务——
跨框架、跨团队、跨组织。

[English](README.md) · **简体中文**

[![Release](https://img.shields.io/github/v/release/agentium-lab/Janus?color=brightgreen&label=release)](https://github.com/agentium-lab/Janus/releases)
[![Go Version](https://img.shields.io/badge/go-1.25-00ADD8?logo=go&logoColor=white)](https://go.dev)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![CI](https://github.com/agentium-lab/Janus/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/agentium-lab/Janus/actions/workflows/ci.yml)
[![Website](https://img.shields.io/badge/website-janusa2a.com-ff5c8a)](https://janusa2a.com)

`♻️ 持久化` `⚖️ 治理` `🔍 可审计` `🔌 多协议` `🧩 三语 SDK`

[文档](https://janusa2a.com/zh/docs/quickstart.html) · [快速开始](#快速开始) · [核心能力](#核心能力) · [架构](#架构) · [已知限制](#已知限制) · [版本发布](https://github.com/agentium-lab/Janus/releases)

</div>

---

Janus 是一个生产级任务总线，位于你的智能体**之间**——LangGraph、AutoGen、CrewAI、MCP 工具、自定义运行时——让智能体之间的协作**不丢任务、不超预算、不绕过策略，并留下持久化审计轨迹**。

它不替代你的智能体运行时；它让运行时之间的协作变得生产安全。

> **v1.11.0 — 正式 GA** · 外部评测 9/10 并建议 GA，无开放 P1。
> 投递语义为**至少一次（at-least-once）**——哪些输入真正生效、哪些仅被接收，
> 以[执行语义表](#已知限制)为准。

## 为什么需要 Janus？

点对点的智能体调用在演示里没问题，在生产环境会出事：

| 没有总线 | 有 Janus |
|---|---|
| 😮‍💨 智能体执行中途崩溃，任务丢失 | ♻️ 持久化邮箱 + 租约恢复——任务自动回到队列 |
| 📝 跨智能体没有统一审计轨迹 | 🔍 审计事件事务性写入（Outbox 模式）、带重试投影、可经 REST 回放——按租户/任务/链路 |
| 🌊 下游智能体被冲垮 | ⚖️ 按租户与智能体强制 RPM / TPM / 并发预算*（team 范围：已保存未生效——见[已知限制](#已知限制)）* |
| 🔓 任何人都能调用任何人 | 🛡️ 策略引擎：允许 / 拒绝 / 需审批 |
| 💸 Token 花销不可见 | 📊 按范围的用量账本*（可信成本计量：路线图）* |
| 🎯 任务落到了错误的智能体 | 🧭 能力 + 分组 + 意图路由，对照实时目录校验 |

## 快速开始

**前置条件：** Go 1.25+ · PostgreSQL 13+ · NATS 2.10+（JetStream）· Redis 7+

```bash
# 1. 启动依赖
docker compose -f deployments/smoke-deps.compose.yaml up -d postgres nats redis

# 2. 执行迁移
for f in migrations/*.up.sql; do psql -d janus -f "$f"; done

# 3. 构建并运行——绑定 localhost 时可安全关闭鉴权（本地开发）
cd server && go build -o janus-api ./cmd/janus-api/
JANUS_PG_HOST=localhost JANUS_PG_USER=janus JANUS_PG_DATABASE=janus \
JANUS_PG_SSLMODE=disable \
JANUS_NATS_URL=nats://localhost:4222 JANUS_REDIS_ADDR=localhost:6379 \
JANUS_HTTP_HOST=localhost JANUS_AUTH_ENABLED=false ./janus-api
```

<details>
<summary><strong>⚙️ 单依赖模式</strong>——用 PostgreSQL 本身做任务队列（无需 NATS）</summary>

```bash
JANUS_QUEUE_DRIVER=pg ./janus-api   # 加上你常用的 PG/Redis 配置
```

认领使用 `FOR UPDATE SKIP LOCKED`，worker 并发语义与 NATS 路径一致。需要更大吞吐与事件扇出时再加回 NATS（`--profile with-nats`）。

> **全栈替代方案：** `docker compose up -d` 会开启 API Key 鉴权并为租户 `acme` 种入公共开发密钥（凭据见 [deployments/dev/seed-dev-key.sql](deployments/dev/seed-dev-key.sql)）。导出为 `JANUS_API_KEY` 并在 SDK 客户端传 `api_key=`。

</details>

**发送你的第一个任务**——注册智能体、按能力发布、拉取 → 带租约保护的 ACK：

```python
from janus_broker import (
    AckRequest, AgentCapability, JanusClient, Payload,
    PublishTaskRequest, RegisterAgentRequest, Target, TaskEnvelope,
)

client = JanusClient(base_url="http://localhost:8080", tenant_id="acme")

client.register_agent(RegisterAgentRequest(
    id="reviewer",
    display_name="Code Reviewer",
    capabilities=[AgentCapability(capability="code_review", description="reviews source code")],
))

client.publish_task(PublishTaskRequest(
    id="task-001",
    source_agent="product",
    target_type="capability",
    target_value="code_review",
    envelope=TaskEnvelope(
        task_id="task-001", tenant_id="acme", source_agent="product",
        target=Target(type="capability", value="code_review"),
        payload=Payload(type="json", content="{}"),
    ),
))

result = client.pull_task("review-mb", "reviewer")
client.ack_task(result.task.id, AckRequest(
    lease_id=result.lease.lease_id,
    result_ref="s3://review-001.json",
))
```

<details>
<summary><strong>📈 上报实时进度</strong>——Worker 注入的 <code>(task, progress) → (result_ref, usage)</code> 合同</summary>

```python
def process(task, progress):
    progress("正在分析代码...", percent=20)
    progress("正在修复问题...", percent=60)
    return "s3://review-001.json", None  # (result_ref, token_usage)

from janus_broker import JanusWorker
worker = JanusWorker(client, agent_id="reviewer", mailbox_id="review-mb")
worker.run(process)

# 从任意客户端读取任务事件轨迹（进度 + 生命周期）：
for evt in client.get_task_events("task-001"):
    print(evt["event_type"], evt.get("payload"))
```
</details>

> 📖 **完整演练：** [智能客服教程](https://janusa2a.com/zh/docs/quickstart.html) 构建了一个完整的多智能体协作流——意图路由、崩溃恢复、并行扇出与智能体间的数据流转。

## 核心能力

### ♻️ 可靠性

| | 保证 |
|---|---|
| **持久化邮箱** | 任务在智能体崩溃与 API 重启后依然存在 |
| **事务性 Outbox** | `accepted ≠ queued`——PostgreSQL 是事实源 |
| **ACK/NACK 幂等** | 重复 ACK 永远不会重复结算预算 |
| **带退避的重试** | Outbox 驱动的延迟重试，而非 NATS 重投 |
| **死信队列（DLQ）** | 每邮箱独立 DLQ，支持重放 / 丢弃 |
| **租约超时** | 崩溃智能体的任务自动恢复 |
| **故障场景套件** | 7 个场景在真实 PG/NATS 上验证：失败、崩溃、过期、重复 |

### ⚖️ 治理

| | 强制内容 |
|---|---|
| **策略引擎** | 按智能体 / 能力 / 工具 / 密级的允许 / 拒绝 / 需审批 |
| **预算控制** | 按租户与智能体的 RPM / TPM / 并发*（team 范围已保存未生效——见[已知限制](#已知限制)）* |
| **审批工作流** | 高风险任务的人工介入（经策略 `ApprovalRequired` 触发；`requires_human_approval` 单独不拦 Create） |
| **工具审计事件** | 提交时的工具事件；贴合真实执行阶段的事件在路线图中 |

### 🧭 路由

| 类型 | 工作方式 |
|---|---|
| `mailbox` / `agent` | 直接投递 |
| `capability` | 找到声明该能力的在线智能体——硬约束（在线 / 密级 / 策略 / 容量 / 预算）+ 打分 |
| `group` | 基于智能体 `team` 字段的团队投递 |
| `intent` | 自然语言 → 最佳匹配能力。配置后 LLM 驱动（`JANUS_LLM_*`），否则关键字回退 |

另有实时**能力目录**：`GET /v1/tenants/{tenant}/catalog` 供客户端侧匹配。

### 🔌 协议

**SSE 流**（`GET /tasks/{id}/stream`）——实时任务进度。任务状态始终持久化于 PostgreSQL；实时通知是尽力而为。客户端应把流当作优化，以 `GET /tasks/{id}` 轮询为权威终态。

**HTTP REST**（`/v1/tenants/{tenant}/...`）· **gRPC**（proto 驱动双协议）· **WebSocket**（仪表盘事件流）· **MCP 网关**（`/mcp/tools/call`、`/mcp/resources`）

<details>
<summary><strong>A2A v1 子集网关</strong> · CI 中对照官方 a2a-go 客户端验证</summary>

`POST /a2a/message:send`、`POST /a2a/message:stream`、`GET /a2a/tasks/{id}`、`GET /a2a/tasks/{id}:subscribe`、`POST /a2a/tasks/{id}:cancel`、游标分页 `GET /a2a/tasks`、`/.well-known/agent-card.json` 的 Agent Card（含 `supportedInterfaces`）、`A2A-Version` 协商、带 `ErrorInfo` 的类型化错误。

多轮续接、历史与 List 过滤暂不支持——完整子集表见 [ADR-0005](docs/adr/0005-a2a-v1-conformance.md)。

**ACP 网关**（`/acp/*`）已弃用——并入 A2A，Sunset 2026-12-31。
</details>

### 📊 可观测 & 🔐 安全

| 可观测 | 安全 |
|---|---|
| OpenTelemetry——W3C traceparent + OTLP | API Key 鉴权——SHA-256 哈希、前缀查找、可选 scope |
| Prometheus——16+ 指标 | mTLS——TLS 1.2+ 客户端证书校验 |
| 结构化 JSON 日志——每行含租户/任务/链路 | 租户隔离——每个查询、主题、密钥、路径都按租户限定 |
| Grafana 仪表盘——积压/时延/错误预置 | 智能体身份绑定——绑定 Key 只能以该智能体行事（中间件 + gRPC 拦截器 + 服务层权威） |
| | 官方客户端互操作——a2a-go SDK 在 CI 中驱动我们的网关 |

## SDK

| 语言 | 包 | 亮点 |
|---|---|---|
| **Go** | `github.com/agentium-lab/Janus/sdk/go` | 39 个方法 + Worker 助手 |
| **Python** | `janus-broker`（httpx + pydantic） | 34 个方法 + JanusWorker |
| **TypeScript** | `@agentium-lab/janus-sdk` | 30 个方法 + Worker 助手 |

```bash
pip install janus-broker        # Python
npm i @agentium-lab/janus-sdk   # TypeScript
go get github.com/agentium-lab/Janus/sdk/go   # Go
```

三个 SDK 共享同一套一致性夹具（`sdk/conformance/`），并通过同一套六故障混沌矩阵（eof · 网络切断 · 租约丢失 · ACK 失败 · 90 秒长任务 · 优雅停止）。

> 命名说明：发布名在注册中心需要时带有消歧后缀——PyPI 上的通用名 `janus-sdk` 属于一个无关项目。

<details>
<summary><strong>CLI</strong>——在终端管理一切</summary>

```bash
janus agent register --id reviewer --name "Code Reviewer"
janus task publish --id task-001 --source product --target-value review-mb
janus mailbox pull review-mb
janus mailbox ack task-001 --lease <lease-id>

janus api-key create --name ci-bot --scope admin
janus api-key list
janus api-key revoke <key-id>

janus project init    # 声明式项目配置
janus project apply
janus project diff
```
</details>

## 架构

```
 外部智能体（A2A / ACP* / MCP / SDK / CI-CD）
                      │
 ┌────────────────────▼────────────────────┐
 │                接入层                    │
 │   A2A 网关 · ACP 网关* · MCP 网关 · HTTP/gRPC │
 └────────────────────┬────────────────────┘
                      │
 ┌────────────────────▼────────────────────┐
 │                核心服务                  │
 │  Task · Dispatch · Policy · Budget ·     │
 │  Approval · Audit · Routing · Intent     │
 └────────────────────┬────────────────────┘
                      │
 ┌────────────────────▼────────────────────┐
 │                后台 Worker               │
 │  Outbox 发布器 · 事件投影 ·               │
 │  租约扫描 · 心跳清扫                      │
 └────────────────────┬────────────────────┘
                      │
 ┌────────────────────▼────────────────────┐
 │                基础设施                  │
 │  NATS JetStream（队列）                  │
 │  PostgreSQL（状态）                      │
 │  Redis（心跳 / 限流）                    │
 │  本地 Artifact 存储                      │
 └─────────────────────────────────────────┘
```

## 关键设计不变量

以下每一条都有测试：

1. 业务服务绝不直接写邮箱——所有入队经事务性 Outbox
2. `accepted ≠ queued`——任务仅在 DB 提交后才发布到 NATS
3. 一个邮箱 = 一个持久化拉取消费者
4. 重试由 Outbox `next_attempt_at` 驱动，而非 NATS 重投
5. ACK/NACK 先提交 DB，再确认 NATS
6. 认领必须在 PostgreSQL 持有持久租约
7. Redis 不存任何持久事实——仅心跳与限流
8. 策略与预算检查不可被路由绕过

## 部署

```bash
# Docker Compose（本地 / 开发）
docker compose -f deployments/smoke-deps.compose.yaml up -d

# Helm（生产）——探针、迁移 Job、HPA、PDB、监控齐备
helm install janus deployments/helm/janus-core/
```

<details>
<summary><strong>探针工具</strong></summary>

```bash
janus-migration-probe         # 迁移是否最新？
janus-nats-persistence-probe  # JetStream 持久化完好？
janus-event-replay-probe      # 审计投影一致？
```
</details>

## 验证

**28 个测试包 · 1,380+ race 测试（0 FAIL）** · 7 个真实 PG/NATS 上的可靠性故障场景 · CI 中的 GA 证据演练：PITR 灾备、Redis 失败关闭、NATS 流丢失重建、三语言 × 六故障 SDK 混沌矩阵。

```bash
make verify                    # vet + staticcheck + 测试 + 覆盖率 ≥ 90%
make contract-check            # API 契约漂移检查
make beta-fast                 # 内存并发流水线仿真
go test ./server/tests/reliability/   # PG 故障场景
make verify-production         # 全部门禁 + GA 就绪
```

## 已知限制

### 执行语义——什么真正生效

API *接收*的输入不等于 Broker *强制*的输入。权威对照：

| 输入 | 语义 |
|---|---|
| 预算 `tenant` / `agent` 范围 | ✅ **生效**——派发准入（RPM/TPM/并发）与结算（持久 PG 账本） |
| 预算 `team` 范围 | ⚠️ **接收未生效**——保存不报错；调度与结算只读 tenant/agent 范围 |
| `requires_human_approval`（策略/模板） | ⚠️ **接收未生效**——审批流经策略 `ApprovalRequired` 决策触发 |
| 任务 `ttl_seconds` / `deadline` / `priority` | ✅ **以 `tasks` 列为准**——同名字段在 envelope 中仅为建议投影 |
| `mailbox.max_concurrency` | ⚠️ **接收未生效**——认领事务强制的是租户/智能体预算并发 |
| `tool.invoke` 策略 / 工具审计事件 | ⚠️ **仅审计面**——工具 `started` 事件在提交时发出；`tool.invoke` 不拦截工具执行 |
| Helm TLS `certFile`/`keyFile` | ⚠️ **未接线**——Secret 模式 TLS（`existingSecret`）为支持方式 |

### 投递语义

投递为**至少一次（at-least-once）**：租约中途过期的 worker 在恢复后会看到同一投递被重发。Broker 状态由幂等 ACK/NACK 与租约校验保护，但 worker 产生的任何*外部*副作用必须幂等。长任务应通过心跳续租。

---

<div align="center">

**[Janus](https://janusa2a.com)** —— 让智能体协作在生产环境中保持可靠、受治理、可审计、可恢复。

[文档](https://janusa2a.com/zh/docs/quickstart.html) · [English](README.md) · [报告问题](https://github.com/agentium-lab/Janus/issues) · Apache-2.0

</div>
