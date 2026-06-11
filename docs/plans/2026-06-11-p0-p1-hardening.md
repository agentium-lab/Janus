# P0/P1 Hardening Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add API key authentication, transactional outbox, gRPC+proto, A2A gateway, budget execution closure, and retry with backoff to Janus v0.3.0.

**Architecture:** Each feature is a vertical slice touching core → service → driver → handler → test. Features are ordered by dependency: auth first (everything else needs it), then outbox (data safety), then gRPC (builds on existing HTTP handlers), then A2A (builds on gRPC), then budget closure and retry backoff (service-layer only).

**Tech Stack:** Go 1.25, pgx/v5, NATS JetStream, Redis, gRPC/grpc-gateway, protobuf, golang-migrate

---

## Feature Order & Dependencies

```
Task 1: API Key Auth (P0)        ← no deps, standalone
Task 2: Transactional Outbox (P0) ← no deps, standalone
Task 3: gRPC + Protobuf (P1)     ← depends on Task 1 (auth middleware)
Task 4: A2A Gateway (P1)         ← depends on Task 3 (gRPC)
Task 5: Budget Closure (P1)      ← depends on Task 2 (outbox for budget events)
Task 6: Retry with Backoff (P1)  ← depends on Task 2 (outbox for retry events)
```

Tasks 1 and 2 can be developed in parallel. Tasks 3-6 are sequential.

---

## Task 1: API Key Authentication (P0)

**Goal:** Every API request must carry a valid API key. Keys are per-tenant. The middleware extracts tenant_id from the key and injects it into the request context.

**Files:**
- Create: `server/internal/auth/apikey.go`
- Create: `server/internal/auth/apikey_test.go`
- Modify: `server/cmd/janus-api/main.go` — wrap mux with auth middleware
- Modify: `server/internal/config/config.go` — add auth config
- Modify: `migrations/000003_api_keys.up.sql`
- Modify: `migrations/000003_api_keys.down.sql`

### Step 1: Write migration for api_keys table

Create `migrations/000003_api_keys.up.sql`:

```sql
create table api_keys (
    tenant_id text not null,
    key_hash  text not null,
    name      text not null,
    prefix    text not null,
    created_at timestamptz not null default now(),
    primary key (tenant_id, key_hash)
);

create index api_keys_prefix_idx on api_keys (prefix);
```

Create `migrations/000003_api_keys.down.sql`:

```sql
drop table if exists api_keys;
```

### Step 2: Write the auth package

Create `server/internal/auth/apikey.go`:

```go
package auth

import (
    "context"
    "crypto/sha256"
    "database/sql"
    "encoding/hex"
    "fmt"
    "net/http"
    "strings"
)

type contextKey string

const (
    TenantCtxKey contextKey = "janus_tenant_id"
    APIKeyCtxKey contextKey = "janus_api_key"
)

func TenantFromContext(ctx context.Context) string {
    if v, ok := ctx.Value(TenantCtxKey).(string); ok {
        return v
    }
    return ""
}

type APIKeyValidator struct {
    db *sql.DB
}

func NewAPIKeyValidator(db *sql.DB) *APIKeyValidator {
    return &APIKeyValidator{db: db}
}

func hashKey(key string) string {
    h := sha256.Sum256([]byte(key))
    return hex.EncodeToString(h[:])
}

func (v *APIKeyValidator) Validate(ctx context.Context, apiKey string) (tenantID string, err error) {
    if len(apiKey) < 8 {
        return "", fmt.Errorf("invalid api key format")
    }
    prefix := apiKey[:8]
    keyHash := hashKey(apiKey)

    var tid string
    err = v.db.QueryRowContext(ctx,
        "SELECT tenant_id FROM api_keys WHERE prefix = $1 AND key_hash = $2",
        prefix, keyHash,
    ).Scan(&tid)
    if err == sql.ErrNoRows {
        return "", fmt.Errorf("invalid api key")
    }
    if err != nil {
        return "", fmt.Errorf("lookup api key: %w", err)
    }
    return tid, nil
}

func Middleware(validator *APIKeyValidator) func(http.Handler) http.Handler {
    return func(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            apiKey := extractAPIKey(r)
            if apiKey == "" {
                http.Error(w, `{"error":"missing api key"}`, http.StatusUnauthorized)
                return
            }

            tenantID, err := validator.Validate(r.Context(), apiKey)
            if err != nil {
                http.Error(w, `{"error":"invalid api key"}`, http.StatusUnauthorized)
                return
            }

            ctx := context.WithValue(r.Context(), TenantCtxKey, tenantID)
            ctx = context.WithValue(ctx, APIKeyCtxKey, apiKey[:8]+"...")
            next.ServeHTTP(w, r.WithContext(ctx))
        })
    }
}

func extractAPIKey(r *http.Request) string {
    // Header: X-API-Key or Authorization: Bearer <key>
    if key := r.Header.Get("X-API-Key"); key != "" {
        return strings.TrimSpace(key)
    }
    auth := r.Header.Get("Authorization")
    if strings.HasPrefix(auth, "Bearer ") {
        return strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
    }
    return ""
}

// GenerateKey creates a new API key with prefix for storage.
// Returns (rawKey, prefix, hash) for the caller to store.
func GenerateKey(tenantID string) (rawKey, prefix, keyHash string) {
    // Use crypto/rand to generate a 32-byte key, hex-encoded
    raw := make([]byte, 32)
    // caller must fill with crypto/rand
    rawKey = hex.EncodeToString(raw)
    prefix = rawKey[:8]
    keyHash = hashKey(rawKey)
    return
}
```

### Step 3: Write tests for auth package

Create `server/internal/auth/apikey_test.go`:

```go
package auth

import (
    "testing"
    "net/http"
    "net/http/httptest"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

func TestHashKey(t *testing.T) {
    h1 := hashKey("janus_test_key_123")
    h2 := hashKey("janus_test_key_123")
    assert.Equal(t, h1, h2, "same input must produce same hash")

    h3 := hashKey("different_key")
    assert.NotEqual(t, h1, h3, "different inputs must produce different hashes")
}

func TestExtractAPIKey(t *testing.T) {
    tests := []struct {
        name   string
        header string
        value  string
        want   string
    }{
        {"X-API-Key header", "X-API-Key", "my-api-key", "my-api-key"},
        {"Bearer token", "Authorization", "Bearer my-token", "my-token"},
        {"empty", "", "", ""},
        {"wrong scheme", "Authorization", "Basic abc", ""},
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            r := httptest.NewRequest(http.MethodGet, "/", nil)
            if tt.header != "" {
                r.Header.Set(tt.header, tt.value)
            }
            got := extractAPIKey(r)
            assert.Equal(t, tt.want, got)
        })
    }
}

func TestMiddleware_MissingKey(t *testing.T) {
    // validator is nil here — we only test the missing key path
    handler := Middleware(nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        t.Fatal("should not reach handler")
    }))

    r := httptest.NewRequest(http.MethodGet, "/v1/tenants", nil)
    w := httptest.NewRecorder()
    handler.ServeHTTP(w, r)

    assert.Equal(t, http.StatusUnauthorized, w.Code)
}
```

Run: `cd server && go test -count=1 -v ./internal/auth/`
Expected: PASS (3 tests)

### Step 4: Integrate auth middleware into main.go

In `server/cmd/janus-api/main.go`:

1. Add import for `auth` package and `database/sql`
2. Open a `*sql.DB` (or reuse pool's underlying connection) for auth validator
3. Wrap the mux: `auth.Middleware(validator)(mux)`
4. Add `/ws` to bypass list (WebSocket doesn't carry API key in header)
5. Add a CLI command or admin endpoint to generate API keys

Config change in `server/internal/config/config.go`:

```go
type AuthConfig struct {
    Enabled bool
}

// In Load():
Auth: AuthConfig{
    Enabled: getEnvBool("JANUS_AUTH_ENABLED", false),
},
```

When `JANUS_AUTH_ENABLED=false`, skip auth middleware (backward compatible for existing tests).

### Step 5: Commit

```bash
git add migrations/000003_api_keys.* server/internal/auth/ server/cmd/janus-api/main.go server/internal/config/config.go
git commit -m "feat: add API key authentication middleware"
```

---

## Task 2: Transactional Outbox (P0)

**Goal:** Task creation writes to PG + outbox table in one transaction. A background publisher reads pending outbox entries and publishes to NATS. This guarantees PG and NATS are eventually consistent.

**Files:**
- Create: `migrations/000004_outbox_events.up.sql`
- Create: `migrations/000004_outbox_events.down.sql`
- Create: `server/internal/outbox/publisher.go`
- Create: `server/internal/outbox/publisher_test.go`
- Modify: `server/internal/service/task_service.go` — use outbox instead of direct NATS publish
- Modify: `server/internal/service/dispatch_service.go` — use outbox for events
- Modify: `server/internal/driver/postgres/outbox_repo.go`
- Modify: `server/internal/service/interfaces.go` — add OutboxRepo
- Modify: `server/cmd/janus-api/main.go` — start outbox publisher

### Step 1: Write migration for outbox_events table

Create `migrations/000004_outbox_events.up.sql`:

```sql
create table outbox_events (
    id          text primary key,
    tenant_id   text not null,
    kind        text not null,
    payload     jsonb not null,
    status      text not null default 'pending',
    attempts    integer not null default 0,
    created_at  timestamptz not null default now(),
    published_at timestamptz
);

create index outbox_pending_idx on outbox_events (status, created_at) where status = 'pending';
```

Create `migrations/000004_outbox_events.down.sql`:

```sql
drop table if exists outbox_events;
```

### Step 2: Write OutboxRepo

Create `server/internal/driver/postgres/outbox_repo.go`:

```go
package postgres

import (
    "context"
    "encoding/json"
    "fmt"
    "time"

    "github.com/jackc/pgx/v5"
    "github.com/jackc/pgx/v5/pgxpool"

    "github.com/agentium-lab/Janus/core"
)

type OutboxRepo struct {
    pool *pgxpool.Pool
}

func NewOutboxRepo(pool *pgxpool.Pool) *OutboxRepo {
    return &OutboxRepo{pool: pool}
}

type OutboxEntry struct {
    ID        string
    TenantID  string
    Kind      string
    Payload   json.RawMessage
    Status    string
    Attempts  int
    CreatedAt time.Time
}

func (r *OutboxRepo) Insert(ctx context.Context, tx pgx.Tx, id, tenantID, kind string, payload json.RawMessage) error {
    _, err := tx.Exec(ctx,
        `INSERT INTO outbox_events (id, tenant_id, kind, payload) VALUES ($1, $2, $3, $4)`,
        id, tenantID, kind, payload,
    )
    return err
}

func (r *OutboxRepo) FetchPending(ctx context.Context, limit int) ([]OutboxEntry, error) {
    rows, err := r.pool.Query(ctx,
        `SELECT id, tenant_id, kind, payload, status, attempts, created_at
         FROM outbox_events
         WHERE status = 'pending'
         ORDER BY created_at ASC
         LIMIT $1 FOR UPDATE SKIP LOCKED`, limit,
    )
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var entries []OutboxEntry
    for rows.Next() {
        var e OutboxEntry
        if err := rows.Scan(&e.ID, &e.TenantID, &e.Kind, &e.Payload, &e.Status, &e.Attempts, &e.CreatedAt); err != nil {
            return nil, err
        }
        entries = append(entries, e)
    }
    return entries, rows.Err()
}

func (r *OutboxRepo) MarkPublished(ctx context.Context, id string) error {
    _, err := r.pool.Exec(ctx,
        `UPDATE outbox_events SET status = 'published', published_at = now(), attempts = attempts + 1 WHERE id = $1`,
        id,
    )
    return err
}

func (r *OutboxRepo) MarkFailed(ctx context.Context, id string) error {
    _, err := r.pool.Exec(ctx,
        `UPDATE outbox_events SET status = 'failed', attempts = attempts + 1 WHERE id = $1`,
        id,
    )
    return err
}
```

### Step 3: Write OutboxPublisher

Create `server/internal/outbox/publisher.go`:

```go
package outbox

import (
    "context"
    "encoding/json"
    "log"
    "time"

    "github.com/agentium-lab/Janus/core"
    "github.com/agentium-lab/Janus/server/internal/driver/postgres"
)

type Publisher struct {
    repo   *postgres.OutboxRepo
    driver core.QueueEventDriver
    done   chan struct{}
}

func NewPublisher(repo *postgres.OutboxRepo, driver core.QueueEventDriver) *Publisher {
    return &Publisher{
        repo:   repo,
        driver: driver,
        done:   make(chan struct{}),
    }
}

func (p *Publisher) Start(ctx context.Context, interval time.Duration) {
    ticker := time.NewTicker(interval)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-p.done:
            return
        case <-ticker.C:
            p.publishBatch(ctx)
        }
    }
}

func (p *Publisher) Stop() {
    close(p.done)
}

func (p *Publisher) publishBatch(ctx context.Context) {
    entries, err := p.repo.FetchPending(ctx, 100)
    if err != nil {
        log.Printf("outbox fetch: %v", err)
        return
    }

    for _, e := range entries {
        if err := p.publishOne(ctx, e); err != nil {
            log.Printf("outbox publish %s: %v", e.ID, err)
            _ = p.repo.MarkFailed(ctx, e.ID)
            continue
        }
        _ = p.repo.MarkPublished(ctx, e.ID)
    }
}

func (p *Publisher) publishOne(ctx context.Context, e postgres.OutboxEntry) error {
    switch e.Kind {
    case "task_publish":
        var msg core.TaskMessage
        if err := json.Unmarshal(e.Payload, &msg); err != nil {
            return err
        }
        return p.driver.PublishTask(ctx, msg)
    case "event_publish":
        var event core.JanusEvent
        if err := json.Unmarshal(e.Payload, &event); err != nil {
            return err
        }
        return p.driver.PublishEvent(ctx, event)
    default:
        return nil
    }
}
```

### Step 4: Refactor TaskService to use outbox

The key change: `TaskService.Create()` opens a PG transaction, inserts the task row AND outbox entries in the same tx, then commits. The outbox publisher publishes to NATS asynchronously.

In `task_service.go`, replace direct `queueDriver.PublishTask/PublishEvent` calls with `outboxRepo.Insert` calls inside the same transaction.

Add `OutboxRepo` to `TaskService` struct. The `TaskRepo.Create` must accept a `pgx.Tx` (or we use the pool directly with `pgx.BeginTx`).

**Key pattern:**
```go
func (s *TaskService) Create(ctx context.Context, task core.Task) error {
    tx, err := s.pool.Begin(ctx)
    if err != nil {
        return err
    }
    defer tx.Rollback(ctx)

    // Insert task
    if err := s.taskRepo.CreateTx(ctx, tx, task); err != nil {
        return err
    }

    // Insert outbox entries for events + queue publish
    outboxID := ulid.Generate()
    eventPayload, _ := json.Marshal(core.JanusEvent{...})
    if err := s.outboxRepo.Insert(ctx, tx, outboxID, task.TenantID, "event_publish", eventPayload); err != nil {
        return err
    }

    if task.MailboxID != "" {
        queuePayload, _ := json.Marshal(core.TaskMessage{...})
        if err := s.outboxRepo.Insert(ctx, tx, ulid.Generate(), task.TenantID, "task_publish", queuePayload); err != nil {
            return err
        }
    }

    return tx.Commit(ctx)
}
```

### Step 5: Start outbox publisher in main.go

In `server/cmd/janus-api/main.go`:

```go
outboxRepo := pgdriver.NewOutboxRepo(pool)
outboxPub := outbox.NewPublisher(outboxRepo, natsDrv)
go outboxPub.Start(context.Background(), 500*time.Millisecond)
defer outboxPub.Stop()
```

### Step 6: Write outbox tests

Create `server/internal/outbox/publisher_test.go` with tests for:
- OutboxRepo Insert + FetchPending + MarkPublished roundtrip
- Publisher.publishOne with mock driver

### Step 7: Commit

```bash
git add migrations/000004_outbox_events.* server/internal/driver/postgres/outbox_repo.go server/internal/outbox/ server/internal/service/task_service.go server/internal/service/interfaces.go server/cmd/janus-api/main.go
git commit -m "feat: add transactional outbox for PG+NATS consistency"
```

---

## Task 3: gRPC + Protobuf (P1)

**Goal:** Add gRPC service definitions matching §30.2, generate Go code with buf, serve gRPC alongside HTTP via grpc-gateway.

**Files:**
- Create: `proto/janus/v1/agent.proto`
- Create: `proto/janus/v1/task.proto`
- Create: `proto/janus/v1/dispatch.proto`
- Create: `proto/janus/v1/audit.proto`
- Create: `proto/buf.yaml`
- Create: `proto/buf.gen.yaml`
- Create: `server/internal/grpcserver/` — gRPC server implementations
- Modify: `server/cmd/janus-api/main.go` — start gRPC listener

### Step 1: Define protobuf files

Per §30.2, define 4 services: AgentService, TaskService, DispatchService, AuditService.

Each proto file mirrors the existing HTTP API. The gRPC handlers delegate to the same service layer as HTTP handlers.

### Step 2: Generate Go code

Use `buf generate` to produce Go code into `proto/gen/janus/v1/`.

### Step 3: Implement gRPC servers

Create thin adapters in `server/internal/grpcserver/` that call the existing service interfaces.

### Step 4: Add grpc-gateway

Register grpc-gateway reverse proxy on the HTTP mux so gRPC methods are also accessible via HTTP/JSON (replacing the manual routing in `newRouter`).

### Step 5: Update config

Add `GRPCPort` to config, default 9090.

### Step 6: Write tests

gRPC server tests using in-memory connections.

### Step 7: Commit

```bash
git commit -m "feat: add gRPC + protobuf service definitions with grpc-gateway"
```

---

## Task 4: A2A Gateway (P1)

**Goal:** Implement A2A Agent Card registration and A2A task/message mapping per §18.1.

**Files:**
- Create: `server/internal/gateway/a2a/gateway.go`
- Create: `server/internal/gateway/a2a/gateway_test.go`
- Create: `server/internal/gateway/a2a/types.go` — A2A Agent Card, Task types

### Step 1: Define A2A types

Map A2A Agent Card to Janus Agent model (§18.1 table):
- Agent Card → Agent Registry record
- Skills/capabilities → agent_capabilities
- A2A Task → Janus Task Envelope
- A2A status → Janus Task lifecycle

### Step 2: Implement A2A handler

Handle A2A protocol requests:
- POST `/a2a/agent/card` — register/update Agent Card
- POST `/a2a/task/send` — send A2A task, map to Janus Envelope, publish
- GET `/a2a/task/{id}/status` — map Janus status back to A2A status (§29.8 table)

### Step 3: Write tests

Test the mapping logic (Agent Card → Agent, A2A Task → Envelope, status mapping).

### Step 4: Commit

```bash
git commit -m "feat: add A2A gateway for Agent Card and task mapping"
```

---

## Task 5: Budget Execution Closure (P1)

**Goal:** Implement full budget lifecycle: reservation at Pull, settlement at Complete, release on failure/cancel. Add backpressure when budget exceeded.

**Files:**
- Modify: `server/internal/service/budget_service.go` — add Reserve, Settle, Release
- Modify: `server/internal/service/dispatch_service.go` — call Reserve at Pull, Settle at Ack, Release at Nack
- Create: `server/internal/driver/postgres/budget_usage_repo.go` — track actual usage
- Create: `migrations/000005_budget_usage.up.sql`

### Step 1: Add budget_usage table

```sql
create table budget_usage (
    tenant_id   text not null,
    scope_type  text not null,
    scope_id    text not null,
    period      text not null,  -- 'daily' or 'monthly'
    period_key  text not null,  -- e.g., '2026-06-11' or '2026-06'
    tokens_used  integer not null default 0,
    cost_used    numeric(18,6) not null default 0,
    task_count   integer not null default 0,
    primary key (tenant_id, scope_type, scope_id, period, period_key)
);
```

### Step 2: Add Reserve/Settle/Release to BudgetService

```go
func (s *BudgetService) Reserve(ctx context.Context, tenantID, agentID string, budget *core.Budget) error
    // Check daily/monthly spend against limits
    // Check concurrent tasks against limits
    // Increment task_count atomically

func (s *BudgetService) Settle(ctx context.Context, tenantID, agentID string, usage *core.TokenUsage) error
    // Record actual token usage
    // Decrement task_count

func (s *BudgetService) Release(ctx context.Context, tenantID, agentID string) error
    // Decrement task_count (no usage recorded)
```

### Step 3: Wire into dispatch_service.go

- `PullTask`: call `budgetSvc.Reserve()`
- `AckTask`: call `budgetSvc.Settle()` with token usage
- `NackTask`: call `budgetSvc.Release()`

### Step 4: Write tests

BudgetService unit tests for Reserve/Settle/Release with mock repos.

### Step 5: Commit

```bash
git commit -m "feat: implement budget reservation, settlement, and backpressure"
```

---

## Task 6: Retry with Exponential Backoff (P1)

**Goal:** When NackTask is retriable, schedule retry after exponential backoff delay instead of immediately re-queueing. Use the NATS retry stream + a timer mechanism.

**Files:**
- Modify: `server/internal/service/dispatch_service.go` — schedule delayed retry
- Create: `server/internal/retry/scheduler.go` — manages delayed retry timers
- Create: `server/internal/retry/scheduler_test.go`
- Modify: `server/cmd/janus-api/main.go` — start retry scheduler

### Step 1: Implement RetryScheduler

```go
type Scheduler struct {
    taskRepo    TaskRepo
    queueDriver QueueDriver
    done        chan struct{}
}

func (s *Scheduler) Start(ctx context.Context, checkInterval time.Duration)
    // Every checkInterval, query tasks in 'retry_scheduled' status
    // For each, check if backoff duration has elapsed (using RetryPolicy.BackoffDuration)
    // If yes, re-publish to NATS and update status to 'queued'

func (s *Scheduler) ScheduleRetry(ctx context.Context, tenantID, taskID string, attemptCount int, policy core.RetryPolicy)
    // Calculate backoff duration
    // Set task retry_at = now + backoff
    // Update status to 'retry_scheduled'
```

### Step 2: Refactor NackTask

In `dispatch_service.go`, replace the immediate re-queue logic with:
```go
if retriable {
    // Calculate retry_at from RetryPolicy.BackoffDuration
    retryAt := time.Now().Add(mailbox.RetryPolicy.BackoffDuration(task.AttemptCount))
    // Update task with retry_at
    if err := s.taskRepo.UpdateRetryAt(ctx, tenantID, taskID, retryAt); err != nil { ... }
    // Update status to retry_scheduled
    // The retry scheduler will pick it up after the delay
}
```

### Step 3: Add retry_at column to tasks table

Migration `000006_retry_at.up.sql`:
```sql
alter table tasks add column retry_at timestamptz;
create index tasks_retry_at_idx on tasks (status, retry_at) where status = 'retry_scheduled';
```

### Step 4: Start retry scheduler in main.go

```go
retrySched := retry.NewScheduler(taskRepo, natsDrv)
go retrySched.Start(context.Background(), 1*time.Second)
defer retrySched.Stop()
```

### Step 5: Write tests

- Unit test BackoffDuration calculations (already in core/mailbox.go)
- Integration test: Nack retriable → status retry_scheduled → scheduler picks up → status queued

### Step 6: Commit

```bash
git commit -m "feat: implement exponential backoff retry with scheduler"
```

---

## Final Verification

After all 6 tasks:

1. `go vet ./...` — clean
2. `staticcheck ./...` — clean
3. All tests pass: `go test -count=1 ./...`
4. Coverage ≥90% on all modules
5. Run `scripts/bench.sh` — no regression
6. `git tag v0.3.0`

---

## Risk & Mitigation

| Risk | Mitigation |
|------|-----------|
| Outbox adds latency to task publish | Outbox publisher polls every 500ms; acceptable for MVP. Can reduce to 100ms if needed. |
| gRPC + buf setup is complex | Keep proto definitions minimal; only 4 services matching existing HTTP API |
| A2A spec may change | Isolate A2A gateway in own package; easy to update |
| Budget reservation needs atomicity | Use PG UPDATE ... WHERE for atomic counter increments |
| Retry scheduler may miss tasks | Use FOR UPDATE SKIP LOCKED; scheduler is idempotent |
