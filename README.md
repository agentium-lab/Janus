<div align="center">

# Janus

**A2A-native durable agent broker for enterprise agent networks**

Reliably, securely, and auditably hand off tasks between AI agents —
across frameworks, teams, and organizations.

[![Release](https://img.shields.io/github/v/release/agentium-lab/Janus?color=brightgreen&label=release)](https://github.com/agentium-lab/Janus/releases)
[![Go Version](https://img.shields.io/badge/go-1.25-00ADD8?logo=go&logoColor=white)](https://go.dev)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-988%E2%9C%93-brightgreen)](#verification)
[![Website](https://img.shields.io/badge/website-janusa2a.com-ff5c8a)](https://janusa2a.com)

[Documentation](https://janusa2a.com/docs/quickstart.html) · [Quick Start](#quick-start) · [Architecture](#architecture) · [Releases](https://github.com/agentium-lab/Janus/releases)

</div>

---

Janus is a production-grade task broker that sits **between** your agents — LangGraph, AutoGen, CrewAI, MCP tools, custom runtimes — so that agent-to-agent collaboration **doesn't lose tasks, doesn't overrun budgets, doesn't bypass policies, and is always traceable**.

It doesn't replace your agent runtime. It makes collaboration between runtimes production-safe.

## Why Janus?

Point-to-point agent calls work in demos. In production they break:

| Without a broker | With Janus |
|---|---|
| Tasks lost when an agent crashes mid-task | Durable mailboxes + lease recovery — tasks automatically return to the queue |
| No unified audit trail across agents | Every publish / pull / start / ack / nack recorded per tenant, task, and trace |
| Downstream agents overwhelmed | RPM / TPM / concurrency budgets enforced per tenant, team, and agent |
| Anyone can call anyone | Policy engine with allow / deny / approval-required rules |
| Token spend is invisible | Usage ledger with per-scope accounting *(trusted cost metering: roadmap)* |
| Wrong agent gets the task | Capability + group + intent routing, validated against the live catalog |

## Quick Start

**Prerequisites:** Go 1.25+, PostgreSQL 13+, NATS 2.10+ (JetStream), Redis 7+

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

> **Full-stack alternative:** `docker compose up -d` runs everything with API-key auth enabled and seeds a public dev key for tenant `acme` (credential in [deployments/dev/seed-dev-key.sql](deployments/dev/seed-dev-key.sql)). Export it as `JANUS_API_KEY` and pass `api_key=` to the SDK client.

**Send your first task** — natural language in, routed to the right agent:

```python
from janus_broker import JanusClient

client = JanusClient(base_url="http://localhost:8080", tenant_id="acme")

# Register an agent with its capabilities
client.register_agent({
    "id": "reviewer",
    "display_name": "Code Reviewer",
    "team": "eng",
    "capabilities": [{"capability": "code_review", "description": "reviews source code"}],
})

# Publish with intent routing — Janus resolves "review my PR" → code_review
client.publish_task({
    "id": "task-001",
    "source_agent": "product",
    "target_type": "intent",
    "target_value": "review my PR for race conditions",
    "envelope": { ... },
})

# Pull → process → ack (with lease protection)
result = client.pull_task("review-mb", "reviewer")
client.ack_task(result.task.id, {
    "lease_id": result.lease.lease_id,
    "result_ref": "s3://review-001.json",
})
```

> 📖 **Full walkthrough:** the [Smart Customer Service tutorial](https://janusa2a.com/docs/quickstart.html#example-smart-service) builds a complete multi-agent exchange workflow — intent routing, crash recovery, parallel fan-out, and data flow between agents.

## Core Capabilities

### ♻️ Reliability

| Feature | What it guarantees |
|---|---|
| Durable mailboxes | Tasks survive agent crashes and API restarts |
| Transactional outbox | `accepted ≠ queued` — PostgreSQL is the source of truth |
| ACK/NACK idempotency | Duplicate ACKs never double-settle budgets |
| Retry with backoff | Outbox-driven delayed retry, not NATS redelivery |
| Dead Letter Queue | Per-mailbox DLQ with replay / discard |
| Lease timeout | Crashed agents' tasks automatically recovered |
| Fault-scenario suite | 7 scenarios tested against real PG/NATS: fail, crash, expiry, duplicates |

### ⚖️ Governance

| Feature | What it enforces |
|---|---|
| Policy engine | Allow / deny / approval-required per agent, capability, tool, classification |
| Budget control | RPM / TPM / concurrency per tenant, team, and agent |
| Approval workflow | Human-in-the-loop for high-risk tasks |
| Full tool audit chain | `tool.invocation_requested → allowed/denied → started → completed/failed` |

### 🧭 Routing

| Type | How it works |
|---|---|
| `mailbox` / `agent` | Direct delivery |
| `capability` | Find online agents that declare the capability — hard constraints (online / classification / policy / capacity / budget) + scoring |
| `group` | Team-based delivery via the agent `team` field |
| `intent` | Natural language → best matching capability. LLM-powered when configured (`JANUS_LLM_*`), keyword fallback otherwise |

Plus a live **capability catalog**: `GET /v1/tenants/{tenant}/catalog` for client-side matching.

### 🔌 Protocols

**HTTP REST** (`/v1/tenants/{tenant}/...`) · **gRPC** (proto-driven dual protocol) · **A2A Gateway** (`/a2a/agent/card`, `/a2a/task/send`) · **ACP Gateway** (`/acp/agent/manifest`, `/acp/runs`) · **MCP Gateway** (`/mcp/tools/call`, `/mcp/resources`) · **WebSocket** (dashboard event stream)

### 📊 Observability & 🔐 Security

| Observability | Security |
|---|---|
| OpenTelemetry — W3C traceparent + OTLP | API-key auth — SHA-256 hashed, prefix lookup, optional scopes (`admin` / `task:write` / `task:read` / `audit:read`) |
| Prometheus — 16+ metrics | mTLS — TLS 1.2+ with client cert verification |
| Structured JSON logs — tenant/task/trace in every line | Tenant isolation — every query, subject, key, path scoped |
| Grafana dashboard — backlog, latency, errors pre-built | TenantGuard — path tenant must match authenticated tenant |

## SDKs

| Language | Package | Coverage |
|---|---|---|
| **Go** | `github.com/agentium-lab/Janus/sdk/go` | 39 methods + Worker helper |
| **Python** | `janus_broker` (httpx + pydantic) | 34 methods + JanusWorker |
| **TypeScript** | `@agentium-lab/janus-sdk` (`sdk/typescript/`) | 30 methods + Worker helper |

**Install:** `pip install janus-broker` · `npm i @agentium-lab/janus-sdk` · `go get github.com/agentium-lab/Janus/sdk/go`

> Naming note: distribution names carry a disambiguator where registries require it — the generic `janus-sdk` name on PyPI belongs to an unrelated project.

All three SDKs share the same conformance fixtures (`sdk/conformance/`).

<details>
<summary><strong>CLI</strong> — manage everything from the terminal</summary>

```bash
janus agent register --id reviewer --name "Code Reviewer"
janus task publish --id task-001 --source product --target-value review-mb
janus mailbox pull review-mb
janus mailbox ack task-001 --lease <lease-id>

janus api-key create --name ci-bot
janus api-key list
janus api-key revoke <key-id>

janus project init    # declarative project config
janus project apply
janus project diff
```
</details>

## Architecture

```
 External Agents (A2A / ACP / MCP / SDK / CI-CD)
                      │
 ┌────────────────────▼────────────────────┐
 │                Ingress                  │
 │   A2A GW · ACP GW · MCP GW · HTTP/gRPC  │
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

These rules are what make Janus trustworthy — every one of them is tested:

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

**988 test functions** · 7 fault scenarios · 11 verification gates

```bash
make verify                    # vet + staticcheck + tests + coverage ≥ 90%
make contract-check            # API contract drift check
make beta-fast                 # in-memory concurrent pipeline simulation
go test ./server/tests/reliability/   # PG-backed fault scenarios
make verify-production         # all 11 gates + GA readiness
```

---

<div align="center">

**[Janus](https://janusa2a.com)** — let agent collaboration stay reliable, governed, auditable, and recoverable in production.

[Documentation](https://janusa2a.com/docs/quickstart.html) · [Report an Issue](https://github.com/agentium-lab/Janus/issues) · Apache-2.0

</div>

## Known Limitations

- **At-least-once delivery.** A worker whose lease expires mid-task (default 300s) will see the same delivery redelivered after recovery. Broker state is protected by idempotent ACK/NACK and lease checks, but any *external* side effects your worker performs must be idempotent. Long-running tasks should call the task heartbeat endpoint to renew the lease before expiry.
- **Cost accounting is estimated.** `CostUSD` is derived from reported token counts at a flat rate (`EstimatedCostPerTokenUSD`); authoritative per-model metering arrives with the enterprise llm-proxy.
