# Janus MVP Completion Roadmap

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Complete all remaining MVP features per design doc §12.1 and §26, ship a production-ready Janus v1.0.

**Architecture:** Go multi-module workspace. PostgreSQL for state, NATS JetStream for messaging, Redis for heartbeat/caching. gRPC+HTTP dual protocol via grpc-gateway. Viper config.

**Tech Stack:** Go 1.23, pgx/v5, nats.go, redis/go-redis, grpc-gateway, viper, testify

---

## Phase 1: Production Hardening (P1 bugs)

### Task 1.1: Event Envelope — unify event_id/timestamp population

**Problem:** Most PublishEvent calls bypass EventService, producing events without event_id/timestamp.

**Files:**
- Modify: `server/internal/service/task_service.go`
- Modify: `server/internal/service/dispatch_service.go`
- Modify: `server/internal/service/approval_service.go`
- Modify: `server/internal/service/event_service.go`

**Approach:** Add a helper `enrichEvent(event *core.JanusEvent)` that sets event_id (if empty) and timestamp (if zero). Call it in `TaskService.publishEvent` and at every direct `PublishEvent` call site. This is a decorator on the existing publish path, not a refactor.

**Test:** Unit test that `publishEvent` always sets event_id and timestamp on outgoing events.

### Task 1.2: Task Envelope — full HTTP field parsing

**Problem:** HTTP TaskHandler.Create parses envelope but ignores deadline/budget/policy/context_refs.

**Files:**
- Modify: `server/internal/handler/task_handler.go:27-80`
- Modify: `server/internal/handler/task_handler.go:82-165`

**Approach:** Parse `deadline` (RFC3339 string → `*time.Time`), `budget` (max_tokens + max_cost_usd), `policy` (data_classification + requires_approval), `context_refs` (array of type/uri/hash/classification). Map them to `core.Task` fields before calling `TaskService.Create`.

**Test:** HTTP test sending full envelope with all fields, verify task in repo has deadline, budget, and context_refs populated.

### Task 1.3: State machine — AddStatusGuard in TaskRepo

**Problem:** `UpdateStatus` doesn't check current status, allowing invalid transitions from concurrent callers.

**Files:**
- Modify: `server/internal/service/interfaces.go` — add `UpdateStatusWithCheck(ctx, tenantID, taskID string, from, to core.TaskStatus, attemptInc int) (bool, error)`
- Modify: `server/internal/driver/postgres/task_repository.go` — implement with `WHERE status = $from`
- Modify: `server/internal/service/task_service.go` — `transition()` uses `UpdateStatusWithCheck`
- Modify: `server/internal/service/dispatch_service.go` — use `UpdateStatusWithCheck` for claimed/running/completed/dead_lettered
- Test: `server/internal/service/service_test.go` — test concurrent ACK/NACK race

**Approach:** Add a new repo method that does CAS (compare-and-swap). The service layer checks rows affected. If 0 rows, return conflict error. This is additive — old `UpdateStatus` still exists for trusted internal paths (retry scheduler, replay).

### Task 1.4: ACK/NACK idempotency — attempt expected status guard

**Problem:** Duplicate ACK can double-settle budget and double-publish events.

**Files:**
- Modify: `server/internal/service/interfaces.go` — add expected status to `UpdateFinished`
- Modify: `server/internal/driver/postgres/task_attempt_repo.go` — `WHERE status IN ('claimed','running')`
- Modify: `server/internal/service/dispatch_service.go` — check rows affected, return idempotent success if 0

**Test:** Send ACK twice for same task, verify budget settled only once, second ACK returns success (not error).

### Task 1.5: Outbox coverage — Approve/Replay/Retry via outbox

**Problem:** ApprovalService.Approve, TaskService.Replay, RetryScheduler all publish to NATS directly, bypassing outbox.

**Files:**
- Modify: `server/internal/service/approval_service.go` — write outbox event instead of direct PublishTask
- Modify: `server/internal/service/task_service.go` — Replay writes outbox event
- Modify: `server/internal/retry/scheduler.go` — re-publish writes outbox event

**Approach:** Each of these paths writes an `outbox_events` row with status `pending` + the NATS subject/message. The existing outbox publisher goroutine picks it up and publishes. If NATS is down, it retries.

**Test:** Mock NATS returning error on PublishTask, verify outbox row exists and retry succeeds.

---

## Phase 2: Docker & Deployment

### Task 2.1: Dockerfile for janus-api

**Files:**
- Create: `Dockerfile`

**Approach:** Multi-stage build. Stage 1: Go builder with GOPROXY. Stage 2: distroless or alpine runtime. Expose 8080 (HTTP) and 9090 (gRPC). Entry point: `/janus-api`.

### Task 2.2: docker-compose.yml

**Files:**
- Create: `docker-compose.yml`

**Approach:**
```yaml
services:
  janus-api:
    build: .
    ports: [8080:8080, 9090:9090]
    depends_on: [postgres, nats, redis]
    env: JANUS_PG_HOST, JANUS_NATS_URL, JANUS_REDIS_ADDR
  postgres:
    image: postgres:16
    volumes: ./migrations:/docker-entrypoint-initdb.d
  nats:
    image: nats:2-alpine
    command: -js
  redis:
    image: redis:7-alpine
```

Migration init: Use a startup script or init container that runs all 8 migrations.

**Test:** `docker compose up -d && curl localhost:8080/readyz` returns 200.

### Task 2.3: janus.example.yaml improvements

**Files:**
- Modify: `server/janus.example.yaml`

**Approach:** Add all config options with comments: db_host, db_port, nats_url, redis_addr, grpc_port, http_port, auth_enabled, heartbeat_ttl, outbox_poll_interval, retry_poll_interval.

---

## Phase 3: Demo & Integration

### Task 3.1: Coding/DevOps Pipeline Demo

**Files:**
- Modify: `demo/main.go`
- Modify: `demo/pipeline/main.go`
- Create: `demo/pipeline/coding_devops_demo.go`

**Approach:** A self-contained demo that:
1. Starts by connecting to Janus API
2. Registers 7 agents: product, review, code, test, security, human-approver, deploy
3. Creates mailboxes for each
4. Runs the full pipeline with real HTTP calls:
   - product-agent sends review request
   - review-agent pulls, reviews, sends to code-agent
   - code-agent writes code, sends to test-agent
   - test-agent runs tests (fails first time), sends fix-request back to code-agent
   - code-agent fixes, sends back to test-agent
   - test-agent passes, sends to security-agent
   - security-agent scans, sends to human-approver (blocked)
   - human-approver approves, sends to deploy-agent
   - deploy-agent deploys
5. Prints trace/audit chain at the end

**Test:** `go test ./demo/pipeline/...` with mock HTTP server (reuse simulation pattern).

### Task 3.2: SDK API Key Integration Test

**Files:**
- Create: `sdk/go/client_test.go`

**Approach:** Test that APIKey is sent as X-API-Key header. Test with httptest.Server that validates the header.

---

## Phase 4: Observability & Trace

### Task 4.1: Prometheus metrics for all paths

**Files:**
- Modify: `server/internal/metrics/metrics.go`
- Modify: `server/internal/service/dispatch_service.go`
- Modify: `server/internal/retry/scheduler.go`
- Modify: `server/internal/outbox/publisher.go`

**Approach:** Add metrics for: dispatch_pull_total, dispatch_ack_total, dispatch_nack_total, retry_scheduled_total, outbox_published_total, outbox_failed_total, budget_check_total, policy_eval_total. Use existing Prometheus patterns.

### Task 4.2: Trace context propagation

**Files:**
- Modify: `server/internal/service/task_service.go`
- Modify: `server/internal/service/dispatch_service.go`
- Modify: `core/event.go`

**Approach:** Every event published includes the trace_id from the task envelope. Events chain parent_task_id for multi-agent flows. The audit query API already returns events by trace_id. Add a `/v1/tenants/{tenant}/traces/{trace_id}` endpoint that aggregates all events for a trace.

**Test:** Create task with trace_id, complete lifecycle, query trace endpoint, verify all events present.

---

## Phase 5: Advanced Features

### Task 5.1: Semantic Routing (capability-based matching)

**Files:**
- Create: `server/internal/service/routing_service.go`
- Modify: `server/internal/service/task_service.go`

**Approach:** When target type is `capability`, query agent_capabilities table for agents with matching capability. If multiple candidates, pick by: (1) least running tasks, (2) highest priority agent. This is rule-based, not embedding-based. Semantic embedding routing is explicitly deferred.

**Test:** Register 2 agents with same capability, create task targeting that capability, verify only one agent receives it.

### Task 5.2: Agent Topology API

**Files:**
- Create: `server/internal/handler/topology_handler.go`
- Create: `server/internal/service/topology_service.go`

**Approach:** Query API that returns:
- All agents for a tenant with their status (online/offline)
- Task flow graph: which agents send to which (derived from task source_agent → target_agent history)
- Current backlog per mailbox

### Task 5.3: WebSocket task streaming

**Files:**
- Modify: `server/internal/handler/ws_handler.go`

**Approach:** Extend WebSocket to support subscribing to a specific task_id. When task state changes, push the update to the WebSocket subscriber. This enables real-time dashboards.

---

## Phase 6: Python SDK & Adapters

### Task 6.1: Python SDK fix

**Files:**
- Modify: `sdk/python/janus/client.py`
- Modify: `sdk/python/janus/models.py`

**Approach:** Fix TargetType enum to match core (agent/mailbox/capability/group/human). Add APIKey to Config. Set X-API-Key header on all requests.

### Task 6.2: A2A Agent Card serving

**Files:**
- Modify: `server/internal/gateway/a2a/gateway.go`

**Approach:** Serve `.well-known/agent.json` for registered agents. Map Janus agent metadata to A2A Agent Card format.

---

## Execution Order

| Phase | Tasks | Est. Effort | Depends On |
|-------|-------|-------------|------------|
| Phase 1 | 1.1–1.5 | 2–3 days | None |
| Phase 2 | 2.1–2.3 | 0.5 day | None |
| Phase 3 | 3.1–3.2 | 1 day | Phase 2 |
| Phase 4 | 4.1–4.2 | 1 day | Phase 1 |
| Phase 5 | 5.1–5.3 | 2 days | Phase 1, 4 |
| Phase 6 | 6.1–6.2 | 1 day | Phase 3 |

**Recommended sequence:** Phase 1 → Phase 2 → Phase 3 → Phase 4 → Phase 5 → Phase 6

**MVP minimum for v1.0:** Phase 1 + Phase 2 + Phase 3 = ship a production-hardened Janus with Docker and demo.

**Post-MVP (v1.1+):** Phase 4, 5, 6 for observability, routing, and ecosystem.

---

## Acceptance Criteria (v1.0)

- [ ] All events have event_id and timestamp
- [ ] HTTP CreateTask parses full envelope (deadline, budget, policy, context_refs)
- [ ] State machine uses compare-and-swap for all transitions
- [ ] ACK/NACK idempotent — duplicate ACK doesn't double-settle
- [ ] Approve/Replay/Retry all go through outbox
- [ ] `docker compose up -d` starts Janus + PG + NATS + Redis
- [ ] `/readyz` returns 200 after startup
- [ ] Demo runs full 7-agent pipeline via Docker
- [ ] All 10+ test packages pass with `-race`
- [ ] Agent-to-agent simulation test covers feedback loop
