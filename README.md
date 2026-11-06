<div align="center">

# Janus

**A2A-native durable task broker for multi-agent systems**

Reliably, securely, and auditably hand off tasks between AI agents —
across frameworks, teams, and organizations.

**English** · [简体中文](README.zh-CN.md)

[![Release](https://img.shields.io/github/v/release/agentium-lab/Janus?color=brightgreen&label=release)](https://github.com/agentium-lab/Janus/releases)
[![Go Version](https://img.shields.io/badge/go-1.25-00ADD8?logo=go&logoColor=white)](https://go.dev)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![CI](https://github.com/agentium-lab/Janus/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/agentium-lab/Janus/actions/workflows/ci.yml)
[![Website](https://img.shields.io/badge/website-janusa2a.com-ff5c8a)](https://janusa2a.com)

`♻️ Durable` `⚖️ Governed` `🔍 Auditable` `🔌 Multi-protocol` `🧩 SDK×3`

[Documentation](https://janusa2a.com/docs/quickstart.html) · [Quick Start](#quick-start) · [Capabilities](#core-capabilities) · [Architecture](#architecture) · [Limitations](#known-limitations) · [Releases](https://github.com/agentium-lab/Janus/releases)

</div>

---

Janus is a durable task broker that sits **between** your agents — LangGraph, AutoGen, CrewAI, MCP tools and custom runtimes — providing **recoverable handoffs, tenant isolation, task admission controls and durable audit trails**.

Your framework handles planning and execution; Janus persists the handoff, coordinates delivery and recovery, and records the outcome.

> **v1.11.0 — GA** · [CI](https://github.com/agentium-lab/Janus/actions/workflows/ci.yml) · [Nightly verification](https://github.com/agentium-lab/Janus/actions/workflows/nightly.yml)
>
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

Complete one local task from publication to `completed`. This recipe uses the root Compose stack, PostgreSQL queue mode and the bundled `acme` tenant.

**Requirements:** Git, Docker with Compose v2, and Python 3.10+. Commands below use a macOS/Linux Bash shell. Docker builds the server; a local Go installation is not required.

### 1. Start the development stack

```bash
git clone https://github.com/agentium-lab/Janus.git
cd Janus

docker compose up -d --build

python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ./sdk/python

export JANUS_API_KEY="janus_e12ebdab09a71c856666756ab26fd12db630041a96559b74066f66820c1d6d48"
```

The key above is the repository's **public development key**, seeded by [api-key-seed](deployments/dev/seed-dev-key.sql). Production uses your own credentials and deployment configuration.

Wait for an authenticated tenant query to succeed. This checks both API startup and completion of the seed job:

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

### 2. Send and complete a task

Save the code as `first_task.py`, then run `python first_task.py` in the same activated environment. It registers both agents, creates an owned mailbox and includes the required trace ID. Unique IDs allow repeat runs against the local stack.

<details>
<summary><strong>📨 first_task.py: complete registration, publication, delivery and settlement</strong></summary>

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

Expected output: `task-<id>: completed`. The `demo://` result reference is an illustrative URI; the example does not upload a file.

### 3. Connect your worker

Use the worker helper for the continuous pull/heartbeat/settlement loop in your application: [Go](sdk/go/worker.go), [Python](sdk/python/janus_broker/worker.py), or [TypeScript](sdk/typescript/src/worker.ts). You implement the handler; result storage remains your application's responsibility.

<details>
<summary><strong>Use NATS for task delivery</strong></summary>

The root Compose stack defaults to `JANUS_QUEUE_DRIVER=pg`. To select JetStream delivery, keep PostgreSQL and start the NATS profile:

```bash
JANUS_QUEUE_DRIVER=nats docker compose --profile with-nats up -d --build
```

The SDK calls stay the same. This Compose setup is for local development; configure persistent dependencies for production.

</details>

<details>
<summary><strong>📈 Report live progress</strong> — the Worker handler contract</summary>

Pass a handler like this to `JanusWorker` for your configured agent and mailbox. The worker handles continuous pulls, lease renewal and settlement:

```python
def process(task, progress):
    progress("Analyzing code...", percent=20)
    progress("Fixing issues...", percent=60)
    return "demo://review/approved", None  # (result_ref, token_usage)
```

Your application stores the result; returning a reference does not upload a file. Query persisted events through `get_task_events(task_id)` on a configured client.

</details>

> 📖 **Full walkthrough:** the [Smart Customer Service tutorial](https://janusa2a.com/docs/quickstart.html#example-smart-service) describes a more complete multi-agent workflow.

## Core Capabilities

### ♻️ Reliability

| Feature | Guarantee |
|---|---|
| **Durable mailboxes** | Tasks survive agent crashes and API restarts |
| **Transactional outbox** | `accepted ≠ queued` — PostgreSQL is the source of truth |
| **ACK/NACK idempotency** | Duplicate ACKs never double-settle budgets |
| **Retry with backoff** | Persisted retry_at scheduling, republished through the Outbox |
| **Dead Letter Queue** | Per-mailbox DLQ with replay / discard |
| **Lease timeout** | Crashed agents' tasks automatically recovered |
| **Fault-scenario suite** | 7 cases with real PostgreSQL and an injected queue driver; live NATS is covered by Nightly drills |

### ⚖️ Governance

| Feature | Enforcement |
|---|---|
| **Policy engine** | Task admission: allow / deny / approval-required by agent, capability and classification; tool-execution limits below |
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

**SSE Streaming** (`GET /v1/tenants/{tenant}/tasks/{id}/stream`) — real-time task progress. Task state is always durable in PostgreSQL; live notifications are best-effort. Clients should treat the stream as an optimization and poll `GET /v1/tenants/{tenant}/tasks/{id}` for the authoritative terminal state.

**HTTP REST** (`/v1/tenants/{tenant}/...`) · **gRPC** (proto-driven dual protocol) · **WebSocket** (dashboard event stream) · **MCP Gateway** (`/mcp/tools/call`, `/mcp/resources`)

<details>
<summary><strong>A2A v1 subset Gateway</strong> · verified against the official a2a-go client in CI</summary>

| Operation | Endpoint |
|---|---|
| Agent Card | `GET /.well-known/agent-card.json` |
| Send / stream | `POST /a2a/message:send`, `POST /a2a/message:stream` |
| Get / list | `GET /a2a/tasks/{id}`, cursor-paginated `GET /a2a/tasks` |
| Subscribe / cancel | `GET /a2a/tasks/{id}:subscribe`, `POST /a2a/tasks/{id}:cancel` |

The v1 task/message endpoints require `A2A-Version: 1.0`. Protocol errors with a defined A2A reason include `ErrorInfo`. Multi-turn continuation, history and List filters are not supported.

**ACP Gateway** (`/acp/*`) is deprecated — merged into A2A, Sunset 2026-12-31.
</details>

### 📊 Observability & 🔐 Security

| Observability | Security |
|---|---|
| OpenTelemetry — service spans, optional OTLP export | API-key auth — SHA-256 hashed, prefix lookup, optional scopes |
| Prometheus — task lifecycle, Outbox, retry, routing, audit and gRPC metrics | mTLS — TLS 1.2+ with client cert verification |
| Structured JSON logs — task context and runtime events | Tenant isolation — authenticated-tenant authorization and data access |
| Grafana dashboard — backlog, latency, errors pre-built | Agent identity binding — bound keys may only act as their agent (middleware + gRPC interceptor + service-layer authority) |
| | Official-client interop — the a2a-go SDK drives our gateway in CI |

## SDKs

| Language | Package | Highlights |
|---|---|---|
| **Go** | `github.com/agentium-lab/Janus/sdk/go` | Client + Worker helper |
| **Python** | `janus-broker` (httpx + pydantic) | Client + JanusWorker |
| **TypeScript** | `@agentium-lab/janus-sdk` | Client + Worker helper |

Run the Go command inside your application module; currently resolve both `core` and the SDK explicitly.

```bash
pip install janus-broker        # Python
npm i @agentium-lab/janus-sdk   # TypeScript
go get github.com/agentium-lab/Janus/core@latest \
  github.com/agentium-lab/Janus/sdk/go@latest  # Go
```

All three SDKs share conformance fixtures (`sdk/conformance/`) and a six-case chaos matrix (eof · network-cut · lease-lost · ack-failure · 90s · stop).

> Naming note: distribution names carry a disambiguator where registries require it — the generic `janus-sdk` name on PyPI belongs to an unrelated project.

<details>
<summary><strong>CLI</strong> — manage agents, tasks and project configuration</summary>

Build the CLI with the Go toolchain in `go.work` (`mkdir -p bin && go build -o bin/janus ./cli`). The following are command references for existing resources; replace the uppercase IDs. The CLI reads `JANUS_API_KEY` from the environment.

```bash
bin/janus --tenant acme task status TASK_ID
bin/janus --tenant acme mailbox pull MAILBOX_ID --agent AGENT_ID
bin/janus --tenant acme mailbox ack TASK_ID --lease LEASE_ID
bin/janus --tenant acme api-key list
bin/janus project --help
```
</details>

## Architecture

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

PostgreSQL is the authority for state, leases, Outbox and settlement, and can also serve as the task queue. NATS is an alternative delivery driver; Redis supports presence and rate limiting. ContextRef APIs are available. Artifact storage is currently a library interface, not a public upload/download API.

## Key Design Invariants

These constraints define the production path and are covered by lifecycle and recovery tests:

1. Task state and delivery/audit intents are persisted transactionally; NATS publication is driven by the Outbox.
2. `accepted ≠ queued` — acceptance and queue delivery are separate stages; NATS publishes after the DB commit.
3. In NATS mode, one mailbox has one durable pull consumer; PG mode uses the database queue.
4. Task retry is scheduled from persisted `retry_at` and republished through a transactional Outbox; message redelivery handles delivery recovery.
5. ACK/NACK commits to PostgreSQL before the queue is acknowledged.
6. Dispatch claims require a durable lease in PostgreSQL.
7. Redis holds no durable facts; it supports presence and rate limiting.
8. Routing checks the supported policy and budget constraints; their exact scope is listed under Known Limitations.

## Deployment

**Local development:** run `docker compose up -d --build` from the repository root.

**Production:** The [Helm chart](deployments/helm/janus-core/) deploys Janus against dependencies you provide. Start from its [values file](deployments/helm/janus-core/values.yaml), set `image.repository`/`image.tag` to an image you have verified is available, configure PostgreSQL credentials, select PG or NATS delivery, and configure TLS and Redis policy. Tenant/API-key provisioning and backups are part of your deployment setup.

```bash
cp deployments/helm/janus-core/values.yaml values.production.yaml
```

Edit `values.production.yaml` for your infrastructure, then install:

```bash
helm upgrade --install janus deployments/helm/janus-core/ \
  --namespace janus --create-namespace \
  --values values.production.yaml
```

For Helm TLS, use `config.tls.existingSecret` and the gateway CA settings. mTLS also requires matching client/probe credentials. The chart includes migration hooks, probes and optional HPA/PDB/monitoring configuration; it does not provision PostgreSQL, NATS or Redis.

Use private dependency credentials, explicit API-key scopes and your own persistence/backup policy. [SECURITY.md](SECURITY.md) describes security reporting; the [configuration values](deployments/helm/janus-core/values.yaml) define the deployment options.

## Verification

**Real-dependency scenarios** · **six faults × three SDK languages** · **deployment and recovery drills**

| Evidence | Scope |
|---|---|
| [CI](https://github.com/agentium-lab/Janus/actions/workflows/ci.yml) | Build, Go/SDK tests, contracts and Helm checks |
| [Nightly](https://github.com/agentium-lab/Janus/actions/workflows/nightly.yml) | Auth, PG/NATS deployment cells, Redis fail-closed, failover/PITR, Helm upgrade/rollback and SDK fault scenarios |
| [CodeQL](https://github.com/agentium-lab/Janus/actions/workflows/codeql.yml) | Go static security analysis |

A [reference functional run](https://github.com/agentium-lab/Janus/actions/runs/42140317423) covers `ce11be3` (2026-11-06). Sustained load uses a separate profile; capacity and latency depend on the measured configuration, metrics and any `WAIVED` results, not a functional green run.

For source development, use the toolchain in `go.work`:

```bash
make verify                    # local coverage floor: 90%
make contract-check            # API contract drift check
make beta-fast                 # in-memory pipeline simulation
go test ./server/tests/reliability/  # configure real dependencies
make verify-production         # aggregate local gates
```

CI currently uses an 85% coverage floor. Workflow definitions, runs and their PASS/FAIL/SKIP artifacts establish what actually executed.

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
| Tracing | Service spans and optional OTLP export are wired; W3C traceparent extraction/injection is not connected in the production HTTP chain |
| Artifact storage | ⚠️ **Library interface only** — no public upload/download API; applications store result data and return references |
| Helm TLS `certFile`/`keyFile` values | ⚠️ **Not wired** — Secret-based TLS (`existingSecret`) is the supported mode |

### Budget and cost semantics

- TPM uses task-declared token estimates. Actual usage is agent-reported and cost uses an estimated unit price; this is not trusted billing.
- Daily-cost admission checks use settled spend. In-flight tasks can exceed the threshold; monthly cost caps are not enforced.
- Redis rate-check failures default to fail-open; fail-closed is configurable. Concurrency and durable settlement rely on PostgreSQL.

### Delivery semantics

Delivery is **at-least-once**: a worker whose lease expires mid-task will see the same delivery redelivered after recovery. Broker state is protected by idempotent ACK/NACK and lease checks, but any *external* side effects your worker performs must be idempotent. Long-running tasks should heartbeat to renew the lease.

---

<div align="center">

**[Janus](https://janusa2a.com)** — let agent collaboration stay reliable, governed, auditable, and recoverable in production.

[Documentation](https://janusa2a.com/docs/quickstart.html) · [简体中文](README.zh-CN.md) · [Report an Issue](https://github.com/agentium-lab/Janus/issues) · Apache-2.0

</div>
