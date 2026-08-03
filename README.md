# Janus

**A2A-native Durable Agent Broker for Enterprise Agent Networks**

Janus is a production-grade task broker that lets different AI agent frameworks (LangGraph, AutoGen, CrewAI, custom) reliably, securely, and auditably hand off tasks to each other.

It doesn't replace your agent runtime — it makes agent-to-agent collaboration **not lose tasks, not overrun budgets, not bypass policies, and always be traceable**.

## Why Janus?

Point-to-point agent calls work in demos. In production they break:

- Tasks lost when agents go offline
- No unified audit trail across agents
- Downstream agents overwhelmed by concurrency/token limits
- No policy enforcement on who-can-call-whom
- No budget visibility or cost control

Janus solves these by sitting between agents as a **durable, governed, observable message broker**.

## Quick Start

### Prerequisites

- Go 1.21+
- PostgreSQL 13+
- NATS 2.10+ (with JetStream)
- Redis 7+

### 1. Start dependencies

```bash
# Start PostgreSQL, NATS, Redis
docker compose -f deployments/smoke-deps.compose.yaml up -d postgres nats redis

# Or install locally:
# PostgreSQL: initdb + pg_ctl start
# NATS: nats-server -js -p 4222 -m 8222
# Redis: redis-server --daemonize yes
```

### 2. Run migrations

```bash
for f in migrations/*.up.sql; do psql -d janus -f "$f"; done
```

### 3. Build and run

```bash
cd server && go build -o janus-api ./cmd/janus-api/

JANUS_PG_HOST=localhost JANUS_PG_USER=janus JANUS_PG_DATABASE=janus \
JANUS_NATS_URL=nats://localhost:4222 JANUS_REDIS_ADDR=localhost:6379 \
JANUS_AUTH_ENABLED=false ./janus-api
```

### 4. Send your first task

```python
from janus_sdk import JanusClient

client = JanusClient(base_url="http://localhost:8080", tenant_id="acme")

# Register an agent
client.register_agent({"id": "reviewer", "display_name": "Code Reviewer", "protocol": "a2a"})

# Create a mailbox
client.create_mailbox({"id": "review-mb", "agent_id": "reviewer"})

# Publish a task
client.publish_task({
    "id": "task-001",
    "source_agent": "product",
    "target_type": "mailbox",
    "target_value": "review-mb",
    "envelope": {
        "janus_version": "0.3",
        "task_id": "task-001",
        "tenant_id": "acme",
        "source_agent": "product",
        "target": {"type": "mailbox", "value": "review-mb"},
        "payload": {"type": "review", "content": "Review PR #42"},
        "trace": {"trace_id": "trace-001"}
    }
})

# Pull and process
result = client.pull_task("review-mb", "reviewer")
if result:
    client.start_task(result.task.id, result.lease.lease_id)
    client.ack_task(result.task.id, {"lease_id": result.lease.lease_id, "result_ref": "s3://review-001.json"})
```

## Core Capabilities

### Reliability

- **Durable mailboxes** — tasks survive agent crashes and API restarts
- **Transactional outbox** — accepted != queued; PostgreSQL is source of truth
- **ACK/NACK idempotency** — duplicate ACKs don't double-settle budgets
- **Retry with backoff** — outbox-driven delayed retry, not NATS redelivery
- **Dead Letter Queue** — per-mailbox DLQ with replay/discard
- **Lease timeout** — crashed agents' tasks automatically recovered
- **7 fault scenarios** — tested: NATS fail, DB fail, crash recovery, lease expiry, duplicate ACK, retry exhaustion, delayed outbox

### Governance

- **Policy engine** — allow/deny/approval_required per agent/capability/tool/classification
- **Budget control** — RPM/TPM/concurrency per tenant/team/agent (cost limits planned — awaiting trusted token metering)
- **Approval workflow** — human-in-the-loop for high-risk tasks
- **Capability routing** — hard constraints (online/classification/policy/capacity/budget) + semantic scoring
- **Intent resolver** — natural language to capability target
- **12 policy templates** — one-line rules for common governance patterns

### Protocols

- **HTTP REST API** — `/v1/tenants/{tenant}/...`
- **gRPC + grpc-gateway** — proto-driven dual protocol
- **A2A Gateway** — `/a2a/agent/card`, `/a2a/task/send`, `/a2a/jsonrpc`
- **ACP Gateway** — `/acp/agent/manifest`, `/acp/runs`
- **MCP Gateway** — `/mcp/tools/call`, `/mcp/resources`
- **WebSocket** — dashboard event stream

### Observability

- **OpenTelemetry** — W3C traceparent propagation + OTLP exporter
- **Prometheus** — 16+ metrics (task lifecycle, budget, policy, routing, HTTP/gRPC)
- **Structured JSON logs** — tenant/task/attempt/trace in every log line
- **Grafana dashboard** — pre-built panels for backlog, latency, errors

### Security

- **API key authentication** — SHA-256 hashed, prefix lookup
- **mTLS** — optional TLS 1.2+ with client certificate verification
- **Tenant isolation** — every query, NATS subject, Redis key, and artifact path is tenant-scoped
- **TenantGuard** — path tenant must match authenticated tenant

## SDKs

| Language | Package | Status |
|---|---|---|
| **Go** | `github.com/agentium-lab/Janus/sdk/go` | 39 methods + Worker helper |
| **Python** | `janus_sdk` (httpx + pydantic) | 34 methods + JanusWorker |
| **TypeScript** | `sdk/typescript/` | 30 methods + Worker helper |

All three SDKs share the same conformance fixtures (`sdk/conformance/`).

## CLI

```bash
# Register an agent
janus agent register --id reviewer --name "Code Reviewer"

# Publish a task
janus task publish --id task-001 --source product --target-value review-mb

# Pull and ACK
janus mailbox pull review-mb
janus mailbox ack task-001 --lease <lease-id>

# Manage API keys
janus api-key create --name ci-bot
janus api-key list
janus api-key revoke <key-id>

# Project config (declarative)
janus project init
janus project apply
janus project diff
```

## Deployment

### Docker Compose (local/dev)

```bash
docker compose -f deployments/smoke-deps.compose.yaml up -d
```

### Helm (production)

```bash
helm install janus deployments/helm/janus-core/
```

The Helm chart includes:
- Deployment with liveness/readiness probes
- Migration Job (pre-install hook)
- ConfigMap + Secret for configuration
- HPA, PDB, PVC for artifact storage
- Prometheus scrape annotations

### Probe tools

```bash
janus-migration-probe         # Check migrations are up-to-date
janus-nats-persistence-probe  # Verify NATS JetStream persistence
janus-event-replay-probe      # Verify audit event projection
```

## Architecture

```
External Agents (A2A/ACP/SDK/MCP/CI-CD)
         |
    +----+----+
    | Ingress |  A2A GW - ACP GW - MCP GW - HTTP/gRPC - WebSocket
    +----+----+
         |
    +----+----+
    | Services |  Task - Dispatch - Policy - Budget - Approval - Audit - Routing - Intent
    +----+----+
         |
    +----+----+
    | Workers  |  Outbox Publisher - Event Projector - Lease Scanner - Heartbeat Sweeper
    +----+----+
         |
    +----+------------------------+
    | Infrastructure             |
    |  NATS JetStream (queue)    |
    |  PostgreSQL (state)        |
    |  Redis (heartbeat/rate)    |
    |  Local Artifact Store      |
    +----------------------------+
```

## Key Design Invariants

1. Business services never write mailbox directly — all enqueue via transactional outbox
2. `accepted != queued` — task publish to NATS only after DB commit
3. One mailbox = one durable pull consumer
4. Retry driven by outbox `next_attempt_at`, not NATS redelivery
5. ACK/NACK commits DB first, then NATS
6. Claim requires durable lease in PostgreSQL
7. Redis stores no durable facts — heartbeat/rate-limit only
8. Policy/Budget cannot be bypassed by routing

## Verification

```bash
# Full local gate
make verify                    # vet + staticcheck + test + coverage >= 90%

# API contract drift check
make contract-check

# Reliability simulation
make beta-fast                 # in-memory concurrent pipeline test

# PG-backed fault scenarios
go test ./server/tests/reliability/  # 7 fault scenarios

# Total GA gate
make verify-production         # runs all 11 gates + ga-readiness
```

---

**Janus** — Let agent collaboration remain reliable, governed, auditable, and recoverable in production.