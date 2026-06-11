# v0.8.0 P1+P2 修复 & 7-Agent 模拟测试 实施计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 修复设计规格中所有 P1/P2 偏差，然后用 7 个 Agent 端到端模拟真实多 Agent 协作场景。

**Architecture:** 在现有 Go workspace 上增量开发。新增 expiry scanner（仿照 heartbeat/sweeper 模式）、DLQ query API、mailbox update API、blocked 状态转换、Replay 实现、event 类型补全。最后在 `demo/` 模块中实现 7-Agent 模拟场景。

**Tech Stack:** Go 1.22, pgx/v5, NATS JetStream, Redis, gRPC, Prometheus, testify

---

## Task 1: Task Expiry Scanner (P1)

**Files:**
- Create: `server/internal/expiry/scanner.go`
- Create: `server/internal/expiry/scanner_test.go`
- Modify: `server/cmd/janus-api/main.go` — 启动 expiry scanner

**Context:** 仿照 `heartbeat/sweeper.go` 和 `retry/scheduler.go` 模式。扫描 `deadline < now()` 或 `created_at + ttl_seconds < now()` 的非终态任务，转换为 `expired` 并发布 `task.expired` 事件。

### Step 1: 写 scanner.go

```go
// server/internal/expiry/scanner.go
package expiry

import (
	"context"
	"log"
	"time"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/metrics"
)

type TaskExpirer interface {
	ExpireTasks(ctx context.Context) (int64, error)
}

type Scanner struct {
	expirer  TaskExpirer
	interval time.Duration
	stopCh   chan struct{}
}

func NewScanner(expirer TaskExpirer, interval time.Duration) *Scanner {
	return &Scanner{
		expirer:  expirer,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

func (s *Scanner) Start(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.scan(ctx)
		}
	}
}

func (s *Scanner) Stop() {
	close(s.stopCh)
}

func (s *Scanner) scan(ctx context.Context) {
	n, err := s.expirer.ExpireTasks(ctx)
	if err != nil {
		log.Printf("expiry scanner: %v", err)
		return
	}
	if n > 0 {
		log.Printf("expiry scanner: expired %d tasks", n)
	}
}
```

`TaskExpirer` 接口隔离，方便测试。

### Step 2: 在 postgres task_repository.go 添加 ExpireTasks 方法

在 `TaskRepository` 上添加：

```go
func (r *TaskRepository) ExpireTasks(ctx context.Context) (int64, error) {
	tag, err := r.pool.Exec(ctx,
		`UPDATE tasks SET status = 'expired', updated_at = now()
		 WHERE status NOT IN ('completed','dead_lettered','expired','cancelled')
		   AND (
		     (deadline IS NOT NULL AND deadline < now())
		     OR (ttl_seconds IS NOT NULL AND ttl_seconds > 0
		         AND created_at + (ttl_seconds || ' seconds')::interval < now())
		   )`)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
```

在 `service/interfaces.go` 的 `TaskRepo` interface 中不需要添加，因为 expiry scanner 直接依赖 `TaskExpirer` 接口，`TaskRepository` 会隐式实现。

### Step 3: 写 scanner_test.go

```go
// server/internal/expiry/scanner_test.go
package expiry

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockExpirer struct {
	count int64
	err   error
	calls int
}

func (m *mockExpirer) ExpireTasks(_ context.Context) (int64, error) {
	m.calls++
	return m.count, m.err
}

func TestScanner_StartStop(t *testing.T) {
	exp := &mockExpirer{count: 3}
	s := NewScanner(exp, 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	go s.Start(ctx)
	time.Sleep(50 * time.Millisecond)
	cancel()

	assert.GreaterOrEqual(t, exp.calls, 2)
}

func TestScanner_ExpireError(t *testing.T) {
	exp := &mockExpirer{err: fmt.Errorf("db error")}
	s := NewScanner(exp, 10*time.Millisecond)
	// Should not panic
	ctx, cancel := context.WithCancel(context.Background())
	go s.Start(ctx)
	time.Sleep(30 * time.Millisecond)
	cancel()
}
```

### Step 4: 在 main.go 启动 expiry scanner

```go
// 在 outbox publisher 启动之后添加:
expiryScanner := expiry.NewScanner(taskRepo, 30*time.Second)
go expiryScanner.Start(context.Background())
defer expiryScanner.Stop()
```

### Step 5: 运行测试

```bash
GOPROXY=https://goproxy.cn,direct go test -count=1 -race ./server/internal/expiry/...
```

---

## Task 2: Task Blocked 状态转换 API (P2)

**Files:**
- Modify: `server/internal/service/task_service.go` — 添加 Block/Unblock 方法
- Modify: `server/internal/handler/task_handler.go` — 添加 Block/Unblock handler + 更新 TaskService interface
- Modify: `server/cmd/janus-api/main.go` — 路由注册

### Step 1: 在 task_service.go 添加 Block/Unblock

```go
func (s *TaskService) Block(ctx context.Context, tenantID, taskID string, reason string) error {
	if err := s.transition(ctx, tenantID, taskID, core.TaskStatusBlocked, core.EventTaskBlocked, 0); err != nil {
		return err
	}
	if reason != "" {
		_ = s.publishEvent(ctx, core.JanusEvent{
			EventType: core.EventTaskBlocked,
			TenantID:  tenantID,
			TaskID:    taskID,
			Payload:   mustMarshal(map[string]string{"reason": reason}),
		})
	}
	return nil
}

func (s *TaskService) Unblock(ctx context.Context, tenantID, taskID string) error {
	return s.transition(ctx, tenantID, taskID, core.TaskStatusRunning, core.EventTaskStarted, 0)
}
```

### Step 2: 在 task_handler.go 添加 Block/Unblock

更新 `TaskService` interface 加入 Block/Unblock。添加 `Block` 和 `Unblock` handler 方法。

### Step 3: 在 main.go 路由注册

添加 `/block` 和 `/unblock` 路由。

### Step 4: 测试

---

## Task 3: Task Replay 实现 (P2)

**Files:**
- Modify: `server/internal/service/task_service.go` — 添加 Replay 方法
- Modify: `server/internal/handler/task_handler.go` — 更新 Replay handler
- Modify: `server/internal/grpc/task_handler.go` — 更新 gRPC ReplayTask

### Step 1: 在 task_service.go 添加 Replay

```go
func (s *TaskService) Replay(ctx context.Context, tenantID, taskID string) (*core.Task, error) {
	task, err := s.taskRepo.Get(ctx, tenantID, taskID)
	if err != nil {
		return nil, fmt.Errorf("get task: %w", err)
	}
	if !task.Status.IsTerminal() {
		return nil, fmt.Errorf("only terminal tasks can be replayed, current status: %s", task.Status)
	}

	// Reset status and republish
	task.Status = core.TaskStatusCreated
	task.AttemptCount = 0
	task.Error = nil
	task.ResultRef = ""

	if err := s.taskRepo.UpdateStatus(ctx, tenantID, taskID, core.TaskStatusCreated, -task.AttemptCount); err != nil {
		return nil, fmt.Errorf("reset task: %w", err)
	}

	// Re-queue if has mailbox
	if task.MailboxID != "" {
		payload, _ := json.Marshal(task.Envelope)
		if err := s.queueDriver.PublishTask(ctx, core.TaskMessage{
			TenantID:  tenantID,
			MailboxID: task.MailboxID,
			TaskID:    taskID,
			Priority:  task.Priority,
			Payload:   payload,
		}); err != nil {
			return nil, fmt.Errorf("re-publish to queue: %w", err)
		}
		_ = s.taskRepo.UpdateStatus(ctx, tenantID, taskID, core.TaskStatusQueued, 0)
	}

	return s.taskRepo.Get(ctx, tenantID, taskID)
}
```

### Step 2: 更新 handler

`TaskService` interface 加入 `Replay`。更新 HTTP handler 调用 `Replay` 而非返回 501。

---

## Task 4: Mailbox Update API (P2)

**Files:**
- Modify: `server/internal/service/mailbox_service.go` — 添加 Update 方法
- Modify: `server/internal/service/interfaces.go` — MailboxRepo 加 UpdateConfig
- Modify: `server/internal/driver/postgres/mailbox_repository.go` — 添加 UpdateConfig
- Modify: `server/internal/handler/mailbox_handler.go` — 添加 Update handler + 更新 interface
- Modify: `server/cmd/janus-api/main.go` — 路由注册 PATCH

### Step 1: MailboxRepo interface 加 UpdateConfig

```go
type MailboxRepo interface {
	// ... existing methods
	UpdateConfig(ctx context.Context, tenantID, mailboxID string, maxConcurrency int, ackWaitSeconds int, maxDeliver int, retentionSeconds int) error
}
```

### Step 2: postgres 实现

```go
func (r *MailboxRepository) UpdateConfig(ctx context.Context, tenantID, mailboxID string, maxConcurrency, ackWaitSeconds, maxDeliver, retentionSeconds int) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE mailboxes SET max_concurrency = $3, ack_wait_seconds = $4,
		        max_deliver = $5, retention_seconds = $6, updated_at = now()
		 WHERE tenant_id = $1 AND id = $2`,
		tenantID, mailboxID, maxConcurrency, ackWaitSeconds, maxDeliver, retentionSeconds,
	)
	return err
}
```

### Step 3: MailboxService 加 Update 方法

### Step 4: handler 加 Update，main.go 路由注册 PATCH `/mailboxes/{id}`

---

## Task 5: DLQ Query/Replay/Discard API (P2)

**Files:**
- Create: `server/internal/handler/dlq_handler.go`
- Modify: `server/internal/driver/nats/driver.go` — 添加 DLQ fetch/query 方法
- Modify: `core/driver.go` — DLQ 相关接口
- Modify: `server/cmd/janus-api/main.go` — 路由

### Step 1: core/driver.go 加 DLQ 接口

```go
type DLQEntry struct {
	TaskID        string          `json:"task_id"`
	TenantID      string          `json:"tenant_id"`
	MailboxID     string          `json:"mailbox_id"`
	Envelope      json.RawMessage `json:"envelope"`
	AttemptCount  int             `json:"attempt_count"`
	Error         json.RawMessage `json:"error"`
	DeadLetteredAt time.Time      `json:"dead_lettered_at"`
}

type DLQDriver interface {
	QueryDLQ(ctx context.Context, tenantID, mailboxID string, limit int) ([]DLQEntry, error)
	ReplayDLQ(ctx context.Context, tenantID, taskID string) error
	DiscardDLQ(ctx context.Context, tenantID, taskID string) error
}
```

### Step 2: NATS driver 实现

### Step 3: handler + 路由

DLQ 端点:
- `GET /v1/tenants/{tenant_id}/dlq` — 查询所有 DLQ 消息
- `POST /v1/tenants/{tenant_id}/dlq/{task_id}/replay` — 重放
- `POST /v1/tenants/{tenant_id}/dlq/{task_id}/discard` — 丢弃

---

## Task 6: 补全 Metrics (P2)

**Files:**
- Modify: `server/internal/metrics/metrics.go` — 添加设计规格中的缺失指标

### Step 1: 添加缺失指标

```go
QueueBacklog = promauto.NewGaugeVec(prometheus.GaugeOpts{
    Name: "janus_queue_backlog",
}, []string{"tenant_id", "mailbox_id"})

AgentHeartbeatLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
    Name: "janus_agent_heartbeat_lag_seconds",
}, []string{"tenant_id", "agent_id"})
```

### Step 2: 在相关位置埋点（dispatch pull 时更新 QueueBacklog，heartbeat sweeper 时更新 AgentHeartbeatLag）

---

## Task 7: 7-Agent 模拟测试

**Files:**
- Create: `demo/simulation/simulation_test.go` — 7 Agent 端到端模拟
- Modify: `demo/go.mod` — 添加依赖

### 场景设计

7 个 Agent 模拟 "Coding → Review → Test → Deploy" 流水线：

1. **product-agent** (产品经理) — 发布 code_review 任务
2. **code-agent** (编码 Agent) — 拉取任务、执行编码、完成后触发 review
3. **review-agent** (代码审查) — 审查代码，可能需要人工审批
4. **test-agent** (测试 Agent) — 运行测试
5. **deploy-agent** (部署 Agent) — 部署到 staging
6. **monitor-agent** (监控 Agent) — 通过 WebSocket 监控所有事件
7. **human-approver** (人工审批) — 模拟审批流程

### 测试流程

```
product-agent 发布 code_review_request 任务
  → review-agent 拉取并执行，发现需要人工审批
    → human-approver 审批通过
  → review-agent 完成审查
  → code-agent 被触发编写代码
  → test-agent 拉取测试任务
  → 部分测试失败，NACK → retry
  → test-agent 再次拉取并通过
  → deploy-agent 拉取部署任务
  → monitor-agent 验证收到所有事件
```

### Step 1: 写 simulation 测试框架

使用 httptest.Server + 真实 service 层（mock repos），7 个 goroutine 分别扮演不同 Agent。

### Step 2: 实现每个 Agent 的行为

### Step 3: 断言所有状态转换和事件

### Step 4: 运行测试

```bash
GOPROXY=https://goproxy.cn,direct go test -count=1 -race -v ./demo/simulation/...
```

---

## 实施顺序

1. **Task 1** — Expiry Scanner (P1，最重要)
2. **Task 2** — Blocked 状态 (P2，简单)
3. **Task 3** — Replay (P2，简单)
4. **Task 4** — Mailbox Update (P2，简单)
5. **Task 5** — DLQ API (P2，中等)
6. **Task 6** — Metrics 补全 (P2，简单)
7. **Task 7** — 7-Agent 模拟测试 (依赖 1-6 全部完成)

每个 Task 完成后：
1. `go vet ./...`
2. `go test -race ./...`
3. 覆盖率检查
4. git commit
