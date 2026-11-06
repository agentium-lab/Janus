<div align="center">

# Janus

**A2A 原生的多智能体持久化任务总线**

在 AI 智能体之间可靠、安全、可审计地交接任务——
跨框架、跨团队、跨组织。

[English](README.md) · **简体中文**

[![Release](https://img.shields.io/github/v/release/agentium-lab/Janus?color=brightgreen&label=release)](https://github.com/agentium-lab/Janus/releases)
[![Go Version](https://img.shields.io/badge/go-1.25-00ADD8?logo=go&logoColor=white)](https://go.dev)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![CI](https://github.com/agentium-lab/Janus/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/agentium-lab/Janus/actions/workflows/ci.yml)
[![Website](https://img.shields.io/badge/website-janusa2a.com-ff5c8a)](https://janusa2a.com)

`♻️ 持久化` `⚖️ 治理` `🔍 可审计` `🔌 多协议` `🧩 SDK×3`

[文档](https://janusa2a.com/zh/docs/quickstart.html) · [快速开始](#快速开始) · [核心能力](#核心能力) · [架构](#架构) · [已知限制](#已知限制) · [版本发布](https://github.com/agentium-lab/Janus/releases)

</div>

---

Janus 是位于智能体**之间**的持久化任务总线。它为 LangGraph、AutoGen、CrewAI、MCP 工具及自定义运行时提供**可恢复的任务交接、租户隔离、任务准入控制和持久审计记录**。

你的框架负责规划与执行；Janus 负责保存交接状态、协调投递与恢复，并记录结果。

> **v1.11.0 — GA** · [CI](https://github.com/agentium-lab/Janus/actions/workflows/ci.yml) · [Nightly 验收](https://github.com/agentium-lab/Janus/actions/workflows/nightly.yml)
>
> 投递语义为**至少一次（at-least-once）**——哪些输入真正生效、哪些仅被接收，
> 以[执行语义表](#已知限制)为准。

## 为什么需要 Janus？

点对点的智能体调用在演示里没问题，在生产环境会出事：

| 没有总线 | 有 Janus |
|---|---|
| 😮‍💨 智能体执行中途崩溃，任务丢失 | ♻️ 持久化邮箱 + 租约恢复——任务自动回到队列 |
| 📝 跨智能体没有统一审计轨迹 | 🔍 审计事件事务性写入（Outbox 模式）、带重试投影、可经 REST 回放——按租户/任务/链路 |
| 🌊 下游智能体被冲垮 | ⚖️ 按租户与智能体强制 RPM / TPM / 并发预算 *（team 范围：已保存未生效——见[已知限制](#已知限制)）* |
| 🔓 任何人都能调用任何人 | 🛡️ 策略引擎：允许 / 拒绝 / 需审批 |
| 💸 Token 花销不可见 | 📊 按范围的用量账本 *（可信成本计量：路线图）* |
| 🎯 任务落到了错误的智能体 | 🧭 能力 + 分组 + 意图路由，对照实时目录校验 |

## 快速开始

在本地完成一次“发布 → 处理 → `completed`”。本例使用根目录 Compose、PostgreSQL 队列模式，以及预置的 `acme` 租户。

**前置条件：** Git、带 Compose v2 的 Docker，以及 Python 3.10+。以下命令使用 macOS/Linux 的 Bash；服务端由 Docker 构建，无需本机安装 Go。

### 1. 启动开发环境

```bash
git clone https://github.com/agentium-lab/Janus.git
cd Janus

docker compose up -d --build

python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ./sdk/python

export JANUS_API_KEY="janus_e12ebdab09a71c856666756ab26fd12db630041a96559b74066f66820c1d6d48"
```

上面的密钥是仓库中的**公共开发密钥**，由 [api-key-seed](deployments/dev/seed-dev-key.sql) 种入。生产环境使用你自己的凭据和部署配置。

等待携带密钥的租户查询成功，确保 API 和密钥初始化均已完成：

```bash
python - <<'PY'
import os
import time
import httpx
from janus_broker import JanusClient

with JanusClient(tenant_id="acme", api_key=os.environ["JANUS_API_KEY"], timeout=2) as client:
    for _ in range(60):
        try:
            client.get_tenant("acme")
            break
        except httpx.HTTPError:
            time.sleep(1)
    else:
        raise SystemExit("Startup incomplete: check docker compose logs janus-api api-key-seed")
print("Janus is ready; the acme tenant and dev key are available")
PY
```

### 2. 发布并完成任务

将代码保存为 `first_task.py`，在同一个已激活的环境中执行 `python first_task.py`。示例会注册发布方与处理方、创建有明确 owner 的邮箱，并填入必需的 trace ID。每次生成不同 ID，可在本地重复执行。

<details>
<summary><strong>📨 first_task.py：完整的注册、发布、领取与结算示例</strong></summary>

```python
import os
from time import monotonic, sleep
from uuid import uuid4

from janus_broker import (
    AckRequest, CreateMailboxRequest, JanusClient, Payload,
    PublishTaskRequest, RegisterAgentRequest, Target, TaskEnvelope,
    TraceContext,
)

suffix = uuid4().hex[:8]
producer, reviewer = f"product-{suffix}", f"reviewer-{suffix}"
mailbox, task_id = f"reviews-{suffix}", f"task-{suffix}"

with JanusClient(
    base_url="http://localhost:8080", tenant_id="acme",
    api_key=os.environ["JANUS_API_KEY"], timeout=5.0,
) as client:
    for agent_id in (producer, reviewer):
        client.register_agent(RegisterAgentRequest(
            id=agent_id, display_name=agent_id,
        ))
    client.create_mailbox(CreateMailboxRequest(
        id=mailbox, agent_id=reviewer,
    ))
    client.publish_task(PublishTaskRequest(
        id=task_id, source_agent=producer,
        target_type="mailbox", target_value=mailbox,
        envelope=TaskEnvelope(
            task_id=task_id, tenant_id="acme", source_agent=producer,
            target=Target(type="mailbox", value=mailbox),
            payload=Payload(type="json", content='{"file":"app.py"}'),
            trace=TraceContext(trace_id=uuid4().hex),
        ),
    ))

    deadline = monotonic() + 30
    while monotonic() < deadline:
        delivery = client.pull_task(mailbox, reviewer)
        if delivery and delivery.task:
            break
        sleep(0.2)
    else:
        raise TimeoutError("Task was not delivered; check the API logs")

    assert delivery.task.id == task_id
    lease_id = delivery.lease.lease_id
    client.start_task(task_id, lease_id, agent_id=reviewer)
    client.ack_task(task_id, AckRequest(
        lease_id=lease_id, agent_id=reviewer,
        result_ref="demo://review/approved",
    ))
    task = client.get_task(task_id)
    assert task.status == "completed", task.status
    print(f"{task.id}: {task.status}")
```

</details>

预期输出：`task-<id>: completed`。`demo://` 只是结果引用示意，本例不会上传文件。

### 3. 接入你的 worker

在应用中使用 worker helper 持续领取任务、续租和结算：[Go](sdk/go/worker.go)、[Python](sdk/python/janus_broker/worker.py)、[TypeScript](sdk/typescript/src/worker.ts)。你实现任务处理函数，结果数据由应用自行存储。

<details>
<summary><strong>使用 NATS 投递任务</strong></summary>

根目录 Compose 默认使用 `JANUS_QUEUE_DRIVER=pg`。若选择 JetStream 投递，保留 PostgreSQL 并启动 NATS profile：

```bash
JANUS_QUEUE_DRIVER=nats docker compose --profile with-nats up -d --build
```

SDK 调用方式保持一致。这份 Compose 用于本地开发；生产环境需要配置持久化依赖。

</details>

<details>
<summary><strong>📈 上报实时进度</strong> — Worker handler 合同</summary>

将这样的 handler 传给已配置 Agent 和邮箱的 `JanusWorker`。Worker 负责持续领取、续租与结算：

```python
def process(task, progress):
    progress("Analyzing code...", percent=20)
    progress("Fixing issues...", percent=60)
    return "demo://review/approved", None  # (result_ref, token_usage)
```

结果数据由应用自行存储；返回值是引用，不会自动上传文件。持久事件可通过已配置客户端的 `get_task_events(task_id)` 查询。

</details>

> 📖 **完整教程：** [智能客服示例](https://janusa2a.com/zh/docs/quickstart.html#example-smart-service)介绍更完整的多智能体工作流。

## 核心能力

### ♻️ 可靠性

| 能力 | 保证 |
|---|---|
| **持久化邮箱** | 任务在智能体崩溃与 API 重启后依然存在 |
| **事务性 Outbox** | `accepted ≠ queued`——PostgreSQL 是事实源 |
| **ACK/NACK 幂等** | 重复 ACK 永远不会重复结算预算 |
| **带退避的重试** | 持久 retry_at 调度，经事务 Outbox 重发布 |
| **死信队列（DLQ）** | 每邮箱独立 DLQ，支持重放 / 丢弃 |
| **租约超时** | 崩溃智能体的任务自动恢复 |
| **故障场景套件** | 7 项真实 PostgreSQL＋注入队列故障测试；真实 NATS 由 Nightly 演练覆盖 |

### ⚖️ 治理

| 能力 | 执行语义 |
|---|---|
| **策略引擎** | 任务准入按 Agent、能力与密级允许 / 拒绝 / 需审批；工具执行边界见下文 |
| **预算控制** | 按租户与智能体的 RPM / TPM / 并发 *（team 范围已保存未生效——见[已知限制](#已知限制)）* |
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

**SSE 流* *（`GET /v1/tenants/{tenant}/tasks/{id}/stream`）——实时任务进度。任务状态始终持久化于 PostgreSQL；实时通知是尽力而为。客户端应把流当作优化，以 `GET /v1/tenants/{tenant}/tasks/{id}` 轮询为权威终态。

**HTTP REST* *（`/v1/tenants/{tenant}/...`）· **gRPC* *（proto 驱动双协议）· **WebSocket* *（仪表盘事件流）· **MCP 网关* *（`/mcp/tools/call`、`/mcp/resources`）

<details>
<summary><strong>A2A v1 子集网关</strong> · CI 使用官方 a2a-go 客户端验证</summary>

| 操作 | 端点 |
|---|---|
| Agent Card | `GET /.well-known/agent-card.json` |
| 发送 / 流式发送 | `POST /a2a/message:send`、`POST /a2a/message:stream` |
| 获取 / 列表 | `GET /a2a/tasks/{id}`、带游标的 `GET /a2a/tasks` |
| 订阅 / 取消 | `GET /a2a/tasks/{id}:subscribe`、`POST /a2a/tasks/{id}:cancel` |

v1 任务/消息端点需要 `A2A-Version: 1.0`；已定义 A2A reason 的协议错误携带 `ErrorInfo`。多轮任务续接、history 和 List filters 尚不支持。

**ACP 网关* *（`/acp/*`）已弃用，迁移方向为 A2A，Sunset 为 2026-12-31。
</details>

### 📊 可观测 & 🔐 安全

| 可观测 | 安全 |
|---|---|
| OpenTelemetry——服务 Span，可选 OTLP 导出 | API Key 鉴权——SHA-256 哈希、前缀查找、可选 scope |
| Prometheus——任务生命周期、Outbox、重试、路由、审计与 gRPC 指标 | mTLS——TLS 1.2+ 客户端证书校验 |
| 结构化 JSON 日志——任务上下文与运行事件 | 租户隔离——按认证租户授权和访问数据 |
| Grafana 仪表盘——积压/时延/错误预置 | 智能体身份绑定——绑定 Key 只能以该智能体行事（中间件 + gRPC 拦截器 + 服务层权威） |
| | 官方客户端互操作——a2a-go SDK 在 CI 中驱动我们的网关 |

## SDK

| 语言 | 包 | 亮点 |
|---|---|---|
| **Go** | `github.com/agentium-lab/Janus/sdk/go` | Client + Worker helper |
| **Python** | `janus-broker`（httpx + pydantic） | Client + JanusWorker |
| **TypeScript** | `@agentium-lab/janus-sdk` | Client + Worker helper |

Go 命令在你的应用 Go module 中执行；当前需同时显式解析 `core` 与 SDK。

```bash
pip install janus-broker        # Python
npm i @agentium-lab/janus-sdk   # TypeScript
go get github.com/agentium-lab/Janus/core@latest \
  github.com/agentium-lab/Janus/sdk/go@latest  # Go
```

三个 SDK 共享一致性夹具（`sdk/conformance/`）与六故障混沌矩阵（eof · 网络切断 · 租约丢失 · ACK 失败 · 90 秒长任务 · 优雅停止）。

> 命名说明：发布名在注册中心需要时带有消歧后缀——PyPI 上的通用名 `janus-sdk` 属于一个无关项目。

<details>
<summary><strong>CLI</strong> — 管理 Agent、任务与项目配置</summary>

使用 `go.work` 指定的工具链构建：`mkdir -p bin && go build -o bin/janus ./cli`。以下是已有资源的命令参考，请替换大写 ID；CLI 会读取环境变量 `JANUS_API_KEY`。

```bash
bin/janus --tenant acme task status TASK_ID
bin/janus --tenant acme mailbox pull MAILBOX_ID --agent AGENT_ID
bin/janus --tenant acme mailbox ack TASK_ID --lease LEASE_ID
bin/janus --tenant acme api-key list
bin/janus project --help
```
</details>

## 架构

```text
 External Agents / SDKs / Frameworks
                       │
                       ▼
 ┌────────────────────────────────────────────┐
 │ Ingress                                    │
 │                                            │
 │ A2A · MCP · REST / gRPC · WebSocket        │
 └────────────────────────────────────────────┘
                       │
                       ▼
 ┌────────────────────────────────────────────┐
 │ Services                                   │
 │                                            │
 │ Task · Dispatch · Policy · Budget          │
 │ Approval · Audit · Routing · Intent        │
 └────────────────────────────────────────────┘
                       │
                       ▼
 ┌────────────────────────────────────────────┐
 │ Background workers                         │
 │                                            │
 │ Outbox Publisher · Event Projector         │
 │ Retry Scheduler · Lease Scanner            │
 │ Heartbeat Sweeper                          │
 └────────────────────────────────────────────┘
                       │
                       ▼
 ┌────────────────────────────────────────────┐
 │ Infrastructure                             │
 │                                            │
 │ PostgreSQL: state / leases / PG queue      │
 │ NATS JetStream: optional task queue        │
 │ Redis: presence / RPM / TPM limits         │
 └────────────────────────────────────────────┘
```

PostgreSQL 是状态、租约、Outbox 与结算的权威来源，可同时承担任务队列；NATS 是另一种投递驱动。Redis 用于在线状态与限流。ContextRef API 已接入；Artifact 存储目前仅为库接口，未提供公开上传/下载 API。

## 关键设计不变量

以下约束定义生产路径，并由生命周期与恢复测试覆盖：

1. 任务状态与投递/审计意图在事务中持久化，NATS 发布由 Outbox 驱动。
2. `accepted ≠ queued`——接收与队列投递是不同阶段，NATS 发布发生在 DB 提交后。
3. NATS 模式下，一个邮箱对应一个持久化拉取消费者；PG 模式使用数据库队列。
4. 任务重试由持久 `retry_at` 调度，并通过事务 Outbox 重发布；消息重投用于投递恢复。
5. ACK/NACK 先提交 PostgreSQL，再确认队列。
6. 派发认领依赖 PostgreSQL 中的持久租约。
7. Redis 不存持久事实，仅提供在线状态与限流支持。
8. 路由按受支持的策略和预算约束进行检查，准确范围见已知限制。

## 部署

**本地开发：** 在仓库根目录执行 `docker compose up -d --build`。

**生产：** [Helm Chart](deployments/helm/janus-core/) 使用你提供的外部依赖部署 Janus。先参考 [values 文件](deployments/helm/janus-core/values.yaml)，配置已确认可拉取的 `image.repository`/`image.tag`、PostgreSQL 地址与凭据、PG/NATS 队列选择、TLS 和 Redis 故障策略。租户/API Key 初始化与备份由部署方安排。

```bash
cp deployments/helm/janus-core/values.yaml values.production.yaml
```

按实际基础设施编辑 `values.production.yaml`，完成后安装：

```bash
helm upgrade --install janus deployments/helm/janus-core/ \
  --namespace janus --create-namespace \
  --values values.production.yaml
```

Helm TLS 使用 `config.tls.existingSecret` 和 gateway CA 配置；mTLS 还需匹配的客户端/探针凭据。Chart 包含迁移 hook、探针及可选 HPA/PDB/监控配置，不会代为创建 PostgreSQL、NATS 或 Redis。

生产环境使用私有依赖凭据、明确的 API Key scopes，以及自己的持久化与备份策略。[SECURITY.md](SECURITY.md) 说明安全问题反馈方式，[values 文件](deployments/helm/janus-core/values.yaml) 列出部署选项。

## 验证

**真实依赖场景** · **三语言 × 六故障 SDK 矩阵** · **部署与灾备演练**

| 证据入口 | 验证范围 |
|---|---|
| [CI](https://github.com/agentium-lab/Janus/actions/workflows/ci.yml) | 构建、Go/SDK 测试、合同与 Helm 检查 |
| [Nightly](https://github.com/agentium-lab/Janus/actions/workflows/nightly.yml) | 鉴权、PG/NATS 部署格、Redis fail-closed、主备/PITR、Helm 升级回滚与 SDK 故障矩阵 |
| [CodeQL](https://github.com/agentium-lab/Janus/actions/workflows/codeql.yml) | Go 静态安全分析 |

[参考功能运行](https://github.com/agentium-lab/Janus/actions/runs/42140317423)对应 `ce11be3`（2026-11-06）。持续负载属于独立 profile；容量与延迟判断需查看实测配置、指标和 `WAIVED` 项，不能由功能绿灯推断。

源码开发使用 `go.work` 指定的工具链：

```bash
make verify                    # local coverage floor: 90%
make contract-check            # API contract drift check
make beta-fast                 # in-memory pipeline simulation
go test ./server/tests/reliability/  # configure real dependencies
make verify-production         # aggregate local gates
```

CI 当前覆盖率门槛为 85%。实际执行范围以 workflow、运行记录及产物中的 PASS/FAIL/SKIP 为准。

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
| Tracing | 服务 Span 与可选 OTLP 导出已接入；生产 HTTP 链尚未接入 W3C traceparent 提取/注入 |
| Artifact 存储 | ⚠️ **仅库接口**——尚无公开上传/下载 API；结果数据由应用自行保存并返回引用 |
| Helm TLS `certFile`/`keyFile` | ⚠️ **未接线**——Secret 模式 TLS（`existingSecret`）为支持方式 |

### 预算与费用口径

- TPM 使用任务声明的 Token 估算；实际用量由 Agent 上报，费用按估算单价计算，并非可信计费。
- 日费准入以已结算花费为准，在制任务可能超过阈值；月费上限尚未强制。
- Redis 限流故障默认 fail-open，可配置 fail-closed；并发与持久结算依据 PostgreSQL。

### 投递语义

投递为**至少一次（at-least-once）**：租约中途过期的 worker 在恢复后会看到同一投递被重发。Broker 状态由幂等 ACK/NACK 与租约校验保护，但 worker 产生的任何*外部*副作用必须幂等。长任务应通过心跳续租。

---

<div align="center">

**[Janus](https://janusa2a.com)** —— 让智能体协作在生产环境中保持可靠、受治理、可审计、可恢复。

[文档](https://janusa2a.com/zh/docs/quickstart.html) · [English](README.md) · [报告问题](https://github.com/agentium-lab/Janus/issues) · Apache-2.0

</div>
