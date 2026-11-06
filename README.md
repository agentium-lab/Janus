<div align="center">

# Janus

**A2A 原生的持久化智能体任务总线 / A2A-native durable agent broker**

在 AI 智能体之间可靠、安全、可审计地交接任务——
跨框架、跨团队、跨组织。

**English** · [简体中文](README.zh-CN.md)

[![Release](https://img.shields.io/github/v/release/agentium-lab/Janus?color=brightgreen&label=release)](https://github.com/agentium-lab/Janus/releases)
[![Go Version](https://img.shields.io/badge/go-1.25-00ADD8?logo=go&logoColor=white)](https://go.dev)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![CI](https://github.com/agentium-lab/Janus/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/agentium-lab/Janus/actions/workflows/ci.yml)
[![Website](https://img.shields.io/badge/website-janusa2a.com-ff5c8a)](https://janusa2a.com)

`♻️ Durable` `⚖️ Governed` `🔍 Auditable` `🔌 Multi-protocol` `🧩 SDK × 3`

[Documentation](https://janusa2a.com/docs/quickstart.html) · [Quick Start](#quick-start) · [Capabilities](#core-capabilities) · [Architecture](#architecture) · [Limitations](#known-limitations) · [Releases](https://github.com/agentium-lab/Janus/releases)

</div>

---

Janus is a production-grade task broker that sits **between** your agents — LangGraph, AutoGen, CrewAI, MCP tools, custom runtimes — so that agent-to-agent collaboration **doesn't lose tasks, doesn't overrun budgets, doesn't bypass policies, and provides durable audit trails**.

It doesn't replace your agent runtime. It makes collaboration between runtimes production-safe.

> **v1.11.0 — GA** · 9/10 external review with GA recommendation, no open P1.
> Delivery semantics are **at-least-once** — see [Enforcement semantics](#known-limitations)
> for the authoritative map of what is enforced vs. merely accepted.

## Why Janus?

Point-to-point agent calls work in demos. In production they break:

| Without a broker | With Janus |
|---|---|
| 😮‍💨 Tasks lost when an agent crashes mid-task | ♻️ Durable mailboxes + lease recovery — tasks automatically return to the queue |
| 📝 No unified audit trail across agents | 🔍 Audit events written transactionally (outbox pattern), projected with retry, replayable via REST — per tenant, task, and trace |
| 🌊 Downstream agents overwhelmed | ⚖️ RPM / TPM / concurrency budgets enforced per tenant and agent *(team scope: saved, not yet enforced — see [Limitations](#known-limitations))* |
| 🔓 Anyone can call anyone | 🛡️ Policy engine with allow / deny / approval-required rules |
| 💸 Token spend invisible | 📊 Usage ledger with per-scope accounting *(trusted cost metering: roadmap)* |
| 🎯 Wrong agent gets the task | 🧭 Capability + group + intent routing, validated against the live catalog |

## Quick Start

**Prerequisites:** Go 1.25+ · PostgreSQL 13+ · NATS 2.10+ (JetStream) · Redis 7+

```bash
# 1. Start dependencies
docker compose -f deployments/smoke-deps.compose.yaml up -d postgres nats redis

# 2. Run migrations
for f in migrations/*.up.sql; do psql -d janus -f "$f"; done

# 3. Build and run — binding localhost keeps auth-off safe for local dev
cd server && go build -o janus-api ./cmd/janus-api/
JANUS_PG_HOST=localhost JANUS_PG_USER=janus JANUS_PG_DATABASE=janus \
JANUS_PG_SSLMODE=disable \
JANUS_NATS_URL=nats://localhost:4222 JANUS_REDIS_ADDR=localhost:6379 \
JANUS_HTTP_HOST=localhost JANUS_AUTH_ENABLED=false ./janus-api
```

<details>
<summary><strong>⚙️ Single-dependency mode</strong> — PostgreSQL as the task queue itself (no NATS)</summary>

```bash
JANUS_QUEUE_DRIVER=pg ./janus-api   # plus your usual PG/Redis settings
```

Claims use `FOR UPDATE SKIP LOCKED`, so worker concurrency semantics match the NATS path. Add NATS back (`--profile with-nats`) when you want its throughput and event fan-out at scale.

> **Full-stack alternative:** `docker compose up -d` runs everything with API-key auth enabled and seeds a public dev key for tenant `acme` (credential in [deployments/dev/seed-dev-key.sql](deployments/dev/seed-dev-key.sql)). Export it as `JANUS_API_KEY` and pass `api_key=` to the SDK client.

</details>

**Send your first task** — register an agent, publish by capability, pull → ack with lease protection:

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
<summary><strong>📈 Report live progress</strong> — the Worker-injected <code>(task, progress) → (result_ref, usage)</code> contract</summary>

```python
def process(task, progress):
    progress("Analyzing code...", percent=20)
    progress("Fixing issues...", percent=60)
    return "s3://review-001.json", None  # (result_ref, token_usage)

from janus_broker import JanusWorker
worker = JanusWorker(client, agent_id="reviewer", mailbox_id="review-mb")
worker.run(process)

# Read the task's event trail from any client (progress + lifecycle):
for evt in client.get_task_events("task-001"):
    print(evt["event_type"], evt.get("payload"))
```
</details>

> 📖 **Full walkthrough:** the [Smart Customer Service tutorial](https://janusa2a.com/docs/quickstart.html#example-smart-service) builds a complete multi-agent exchange workflow — intent routing, crash recovery, parallel fan-out, and data flow between agents.

## Core Capabilities

### ♻️ Reliability

| | Guarantee |
|---|---|
| **Durable mailboxes** | Tasks survive agent crashes and API restarts |
| **Transactional outbox** | `accepted ≠ queued` — PostgreSQL is the source of truth |
| **ACK/NACK idempotency** | Duplicate ACKs never double-settle budgets |
| **Retry with backoff** | Outbox-driven delayed retry, not NATS redelivery |
| **Dead Letter Queue** | Per-mailbox DLQ with replay / discard |
| **Lease timeout** | Crashed agents' tasks automatically recovered |
| **Fault-scenario suite** | 7 scenarios tested against real PG/NATS: fail, crash, expiry, duplicates |

### ⚖️ Governance

| | What it enforces |
|---|---|
| **Policy engine** | Allow / deny / approval-required per agent, capability, tool, classification |
| **Budget control** | RPM / TPM / concurrency per tenant and agent *(team scope saved, not enforced — see [Limitations](#known-limitations))* |
| **Approval workflow** | Human-in-the-loop for high-risk tasks (engages via policy `ApprovalRequired`; `requires_human_approval` alone does not gate Create) |
| **Tool audit events** | Submit-time tool events; lifecycle-accurate execution stages are on the roadmap |

### 🧭 Routing

| Type | How it works |
|---|---|
| `mailbox` / `agent` | Direct delivery |
| `capability` | Find online agents that declare the capability — hard constraints (online / classification / policy / capacity / budget) + scoring |
| `group` | Team-based delivery via the agent `team` field |
| `intent` | Natural language → best matching capability. LLM-powered when configured (`JANUS_LLM_*`), keyword fallback otherwise |

Plus a live **capability catalog**: `GET /v1/tenants/{tenant}/catalog` for client-side matching.

### 🔌 Protocols

**SSE Streaming** (`GET /tasks/{id}/stream`) — real-time task progress. Task state is always durable in PostgreSQL; live notifications are best-effort. Clients should treat the stream as an optimization and poll `GET /tasks/{id}` for the authoritative terminal state.

**HTTP REST** (`/v1/tenants/{tenant}/...`) · **gRPC** (proto-driven dual protocol) · **WebSocket** (dashboard event stream) · **MCP Gateway** (`/mcp/tools/call`, `/mcp/resources`)

<details>
<summary><strong>A2A v1 subset Gateway</strong> · verified against the official a2a-go client in CI</summary>

`POST /a2a/message:send`, `POST /a2a/message:stream`, `GET /a2a/tasks/{id}`, `GET /a2a/tasks/{id}:subscribe`, `POST /a2a/tasks/{id}:cancel`, cursor-paginated `GET /a2a/tasks`, Agent Card at `/.well-known/agent-card.json` with `supportedInterfaces`, `A2A-Version` negotiation, typed errors with `ErrorInfo`.

Multi-turn continuation, history, and List filters are not yet supported — see [ADR-0005](docs/adr/0005-a2a-v1-conformance.md) for the full subset table.

**ACP Gateway** (`/acp/*`) is deprecated — merged into A2A, Sunset 2026-12-31.
</details>

### 📊 Observability & 🔐 Security

| Observability | Security |
|---|---|
| OpenTelemetry — W3C traceparent + OTLP | API-key auth — SHA-256 hashed, prefix lookup, optional scopes |
| Prometheus — 16+ metrics | mTLS — TLS 1.2+ with client cert verification |
| Structured JSON logs — tenant/task/trace in every line | Tenant isolation — every query, subject, key, path scoped |
| Grafana dashboard — backlog, latency, errors pre-built | Agent identity binding — bound keys may only act as their agent (middleware + gRPC interceptor + service-layer authority) |
| | Official-client interop — the a2a-go SDK drives our gateway in CI |

## SDKs

| Language | Package | Highlights |
|---|---|---|
| **Go** | `github.com/agentium-lab/Janus/sdk/go` | 39 methods + Worker helper |
| **Python** | `janus-broker` (httpx + pydantic) | 34 methods + JanusWorker |
| **TypeScript** | `@agentium-lab/janus-sdk` | 30 methods + Worker helper |

```bash
pip install janus-broker        # Python
npm i @agentium-lab/janus-sdk   # TypeScript
go get github.com/agentium-lab/Janus/sdk/go   # Go
```

All three SDKs share the same conformance fixtures (`sdk/conformance/`) and pass the same six-case chaos matrix (eof · network-cut · lease-lost · ack-failure · 90s · stop).

> Naming note: distribution names carry a disambiguator where registries require it — the generic `janus-sdk` name on PyPI belongs to an unrelated project.

<details>
<summary><strong>CLI</strong> — manage everything from the terminal</summary>

```bash
janus agent register --id reviewer --name "Code Reviewer"
janus task publish --id task-001 --source product --target-value review-mb
janus mailbox pull review-mb
janus mailbox ack task-001 --lease <lease-id>

janus api-key create --name ci-bot --scope admin
janus api-key list
janus api-key revoke <key-id>

janus project init    # declarative project config
janus project apply
janus project diff
```
</details>

## Architecture

```
 External Agents (A2A / ACP* / MCP / SDK / CI-CD)
                      │
 ┌────────────────────▼────────────────────┐
 │                Ingress                  │
 │   A2A GW · ACP GW* · MCP GW · HTTP/gRPC │
 └────────────────────┬────────────────────┘
                      │
 ┌────────────────────▼────────────────────┐
 │                Services                 │
 │  Task · Dispatch · Policy · Budget ·     │
 │  Approval · Audit · Routing · Intent     │
 └────────────────────┬────────────────────┘
                      │
 ┌────────────────────▼────────────────────┐
 │                Workers                  │
 │  Outbox Publisher · Event Projector ·    │
 │  Lease Scanner · Heartbeat Sweeper       │
 └────────────────────┬────────────────────┘
                      │
 ┌────────────────────▼────────────────────┐
 │             Infrastructure              │
 │  NATS JetStream (queue)                 │
 │  PostgreSQL (state)                     │
 │  Redis (heartbeat / rate limiting)      │
 │  Local Artifact Store                   │
 └─────────────────────────────────────────┘
```

## Key Design Invariants

Every one of these is tested:

1. Business services never write to mailboxes directly — all enqueue goes through the transactional outbox
2. `accepted ≠ queued` — a task publishes to NATS only after the DB commit
3. One mailbox = one durable pull consumer
4. Retry is driven by outbox `next_attempt_at`, not NATS redelivery
5. ACK/NACK commits to DB first, then NATS
6. Claiming requires a durable lease in PostgreSQL
7. Redis stores no durable facts — heartbeat and rate-limiting only
8. Policy and budget checks cannot be bypassed by routing

## Deployment

```bash
# Docker Compose (local / dev)
docker compose -f deployments/smoke-deps.compose.yaml up -d

# Helm (production) — probes, migration job, HPA, PDB, monitoring included
helm install janus deployments/helm/janus-core/
```

<details>
<summary><strong>Probe tools</strong></summary>

```bash
janus-migration-probe         # migrations up-to-date?
janus-nats-persistence-probe  # JetStream persistence intact?
janus-event-replay-probe      # audit projection consistent?
```
</details>

## Verification

**28 test packages · 1,380+ race-enabled tests (0 FAIL)** · 7 reliability fault scenarios on real PG/NATS · GA evidence drills in CI: PITR disaster recovery, Redis fail-closed runtime, NATS stream-loss rebuild, and a 6-case × 3-language SDK chaos matrix.

```bash
make verify                    # vet + staticcheck + tests + coverage ≥ 90%
make contract-check            # API contract drift check
make beta-fast                 # in-memory concurrent pipeline simulation
go test ./server/tests/reliability/   # PG-backed fault scenarios
make verify-production         # all gates + GA readiness
```

## Known Limitations

### Enforcement semantics — what actually gates behavior

Inputs the API *accepts* are not all inputs the broker *enforces*. The authoritative map:

| Input | Semantics |
|---|---|
| Budget `tenant` / `agent` scope | ✅ **Enforced** at dispatch admission (RPM/TPM/concurrency) and settlement (durable PG ledger) |
| Budget `team` scope | ⚠️ **Accepted, not enforced** — saved without error; scheduler and settlement read only tenant/agent scopes |
| `requires_human_approval` (policy/template) | ⚠️ **Accepted, not wired into Create admission** — the approval flow engages via policy `ApprovalRequired` decisions |
| Task `ttl_seconds` / `deadline` / `priority` | ✅ **Enforced from the `tasks` columns** — same-named envelope fields are advisory projections |
| `mailbox.max_concurrency` | ⚠️ **Accepted, not in the dispatch gate** — the claim transaction enforces tenant/agent budget concurrency |
| `tool.invoke` policy / tool audit events | ⚠️ **Audit-plane only** — tool `started` events are emitted at submit time; `tool.invoke` does not gate tool execution |
| Helm TLS `certFile`/`keyFile` values | ⚠️ **Not wired** — Secret-based TLS (`existingSecret`) is the supported mode |

### Delivery semantics

Delivery is **at-least-once**: a worker whose lease expires mid-task will see the same delivery redelivered after recovery. Broker state is protected by idempotent ACK/NACK and lease checks, but any *external* side effects your worker performs must be idempotent. Long-running tasks should heartbeat to renew the lease.

---

<div align="center">

**[Janus](https://janusa2a.com)** — let agent collaboration stay reliable, governed, auditable, and recoverable in production.

[Documentation](https://janusa2a.com/docs/quickstart.html) · [简体中文](README.zh-CN.md) · [Report an Issue](https://github.com/agentium-lab/Janus/issues) · Apache-2.0

</div>
