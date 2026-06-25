# LifecycleService 设计：task 状态转换的事务封装

创建日期：2026-06-18
状态：已通过设计评审
关联：[2026-06-18-ga-convergence-plan.md](../../plans/2026-06-18-ga-convergence-plan.md) Milestone 1（T9+T1+T2+T3）
覆盖任务：M1-T9（NACK 顺序）、M1-T1（ACK 进 outbox）、M1-T2（DLQ outbox）、M1-T3（生命周期事件 outbox）

---

## 1. 目标

让 DispatchService（PullTask/StartTask/AckTask/NackTask）和 TaskService（管理事件 block/unblock/cancel/replay）的所有 task 状态转换满足不变量：

- **先提交 DB（含 outbox 行）再 ACK/TERM NATS**
- **生产路径的所有 task.* 事件写 outbox 行，不再直接发 NATS**（无 outbox 的测试场景保留 fallback，见第 8 节）
- **重复 ACK/NACK 幂等**（不重复结算、不重复写结果、不重复发事件）
- **CAS 校验**（所有状态转换检查"原本是什么状态" + 影响行数）
- **NATS 失败容错**（记日志 + 审计，不回滚已提交的 DB，redelivery 兜底）

---

## 2. 方案选择

采用**方案 A：薄事务封装层**（已评审通过）。

| 方案 | 说明 | 选择 |
| --- | --- | --- |
| A. 薄事务封装层 | LifecycleService 只提供 `ApplyTx(ctx, fn)`，开事务执行 fn、commit、回滚。业务逻辑留在调用方 | ✅ 选定 |
| B. 声明式 DSL | Transition 结构描述多写，内部展开 | 否决（结构会膨胀，耦合增加） |
| C. 专用方法 | 每类转换一个方法（CompleteTaskTx/FailTaskTx） | 否决（偏离薄封装，积累业务知识） |

**理由**：最符合"薄封装"，与现有 `TaskService.createWithOutbox` 模式一致，业务逻辑可独立测试。

---

## 3. LifecycleService 接口

极薄的事务封装层（~50 行），只做一件事：开 PG 事务、执行调用方提供的写操作、commit、处理回滚。**不含任何业务逻辑**。

```go
package service

import (
    "context"
    "fmt"

    "github.com/jackc/pgx/v5"
    "github.com/jackc/pgx/v5/pgxpool"
)

type LifecycleService struct {
    pool *pgxpool.Pool
}

func NewLifecycleService(pool *pgxpool.Pool) *LifecycleService {
    return &LifecycleService{pool: pool}
}

// ApplyTx 在一个 PG 事务内执行 fn。fn 内的所有 repo 写入要么全部提交，要么全部回滚。
// 调用方负责在 ApplyTx 成功返回后再执行非事务副作用（如 ACK NATS）。
func (s *LifecycleService) ApplyTx(ctx context.Context, fn func(tx pgx.Tx) error) error {
    tx, err := s.pool.Begin(ctx)
    if err != nil {
        return fmt.Errorf("begin lifecycle tx: %w", err)
    }
    defer tx.Rollback(ctx)

    if err := fn(tx); err != nil {
        return err
    }
    return tx.Commit(ctx)
}
```

**依赖**：`pgxpool.Pool`（已有）

---

## 4. budget_usage_ledger 幂等账本

### 解决的问题

当前 `BudgetService.Settle` 直接累加 `budget_usage` 聚合表，重复 ACK 会重复计费。

### Migration `000010_budget_usage_ledger`

```sql
CREATE TABLE IF NOT EXISTS budget_usage_ledger (
    tenant_id         text        NOT NULL,
    task_id           text        NOT NULL,
    attempt           integer     NOT NULL,
    scope_type        text        NOT NULL,   -- 'tenant' | 'agent'
    scope_id          text        NOT NULL,   -- tenant_id 或 agent_id
    prompt_tokens     bigint      NOT NULL DEFAULT 0,
    completion_tokens bigint      NOT NULL DEFAULT 0,
    total_tokens      bigint      NOT NULL DEFAULT 0,
    cost_usd          numeric(18,6) NOT NULL DEFAULT 0,
    recorded_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, task_id, attempt, scope_type, scope_id)
);

CREATE INDEX IF NOT EXISTS budget_usage_ledger_scope_idx
    ON budget_usage_ledger (tenant_id, scope_type, scope_id, recorded_at);
```

**主键**：`(tenant_id, task_id, attempt, scope_type, scope_id)` 保证同一次结算不重复入账。

### core.LedgerEntry 类型（新增到 core 包）

```go
type LedgerEntry struct {
    TenantID         string
    TaskID           string
    Attempt          int
    ScopeType        string  // "tenant" | "agent"
    ScopeID          string  // tenant_id 或 agent_id
    PromptTokens     int64
    CompletionTokens int64
    TotalTokens      int64
    CostUSD          float64
}
```

### 写入流程

ACK 时为**两个 scope 各写一条**（tenant + agent）。token/cost 数据来自调用方 ACK 时返回的 `token_usage`（core.TokenUsage），Janus 不自行估算成本（遵循 §29.5：MVP 用调用方声明的静态预算，cost 由 ACK 的 token_usage 带回）：

```go
// usage 是 AckTask 入参 *core.TokenUsage（调用方带回）
var prompt, completion, total int64
var cost float64
if usage != nil {
    prompt = int64(usage.PromptTokens)
    completion = int64(usage.CompletionTokens)
    total = int64(usage.TotalTokens)
    // TokenUsage 当前无 CostUSD 字段；MVP 记 0，后期 cost center 扩展时补
}

for _, scope := range []struct{ Type, ID string }{
    {"tenant", tenantID},
    {"agent", attempt.AgentID},
} {
    inserted, err := budgetRepo.InsertLedgerTx(tx, core.LedgerEntry{
        TenantID: tenantID, TaskID: taskID, Attempt: attempt.Attempt,
        ScopeType: scope.Type, ScopeID: scope.ID,
        PromptTokens: prompt, CompletionTokens: completion,
        TotalTokens: total, CostUSD: cost,
    })  // INSERT ... ON CONFLICT (pk) DO NOTHING; RETURNING 是否插入
    if err != nil { return err }
    if inserted {
        budgetRepo.IncrementUsageTx(tx, scope.Type, scope.ID, prompt, completion, total, cost)
    }
}
```

**规则**：ledger 是事实，budget_usage 是聚合投影。重复 ACK → ledger ON CONFLICT → 不插入 → 不累加 budget_usage。

### 与三个 budget 操作的关系

| 操作 | 时机 | 是否走 ledger |
| --- | --- | --- |
| Reserve（Pull 时） | Agent 拉到 task | 否（并发占用计数，非结算） |
| Settle（ACK 时） | Agent 完成 | **是**（写 ledger 幂等 → 成功才累加 budget_usage） |
| Release（NACK/超时） | Agent 失败 | 否（释放并发占用） |

### ledger 用途
- 按 ledger 重建 budget_usage（若聚合损坏）
- 后期 cost center / billing 读明细
- 审计查"task 消耗多少 token"

---

## 5. repo 事务版方法

照搬 TaskRepository 已有的 `CreateTx`/`UpdateStatusTx` 模式，给相关 repo 加事务版方法（旧的非事务版保留）。

### 清单

**TaskAttemptRepository**
- `UpdateFinishedWithCheckTx(tx, ctx, tenantID, taskID, attempt, status, errJSON, usageJSON) (bool, error)`

**TaskRepository**（部分已有）
- 已有：`CreateTx`、`UpdateStatusTx`
- 新增：`UpdateStatusWithCheckTx(tx, ctx, tenantID, taskID, expected, new, attemptInc) (bool, error)`
- 新增：`SetResultRefTx(tx, ctx, tenantID, taskID, resultRef) error`
- 新增：`UpdateRetryAtTx(tx, ctx, tenantID, taskID, retryAt) error`

**BudgetRepository / BudgetUsageRepository**
- 新增：`InsertLedgerTx(tx, ctx, entry core.LedgerEntry) (bool, error)` — INSERT ... ON CONFLICT (pk) DO NOTHING，返回是否插入（用 `RETURNING` 或检查 RowsAffected）
- 新增：`IncrementUsageTx(tx, ctx, scopeType, scopeID string, prompt, completion, total int64, cost float64) error` — 累加 budget_usage 聚合表的 token/cost

**OutboxRepository**
- 已有：`Insert(tx, ...)` ✅、`InsertDirect(...)` ✅，无需新增

---

## 6. 四个业务路径改造

### 路径 1：AckTask（Agent 完成任务）

```
1. 参数校验 + lease 校验（非事务）
2. lifecycle.ApplyTx：
   - attemptRepo.UpdateFinishedWithCheckTx → completed（CAS：必须 claimed/running）
   - taskRepo.UpdateStatusWithCheckTx → completed（CAS）
   - taskRepo.SetResultRefTx（若有 resultRef）
   - budgetRepo.InsertLedgerTx（两 scope，幂等）+ IncrementUsageTx（仅插入成功）
   - outboxRepo.Insert("event_publish", task.completed 事件)
3. 事务 commit 后，ACK NATS
   - 失败：log.Error + outboxRepo.InsertDirect(audit 事件带 ack_error)，不返回 error
```

### 路径 2：NackTask（Agent 失败任务，T9 修正）

```
1. 参数校验 + lease 校验（非事务）
2. lifecycle.ApplyTx：
   - attemptRepo.UpdateFinishedWithCheckTx → failed（CAS）
   - budgetSvc 释放并发预算
   - 若可重试：
     - taskRepo.UpdateStatusWithCheckTx → retry_scheduled
     - 写 retry 事件 + delayed task_publish 到 outbox
   - 若不可重试/重试耗尽：
     - taskRepo.UpdateStatusWithCheckTx → dead_lettered
     - 写 dlq_publish + dead_lettered 事件到 outbox
3. 事务 commit 后，ACK/TERM NATS（失败同 AckTask 容错）
```

**关键**：NATS 操作全部挪到事务之后。先 DB 后 NATS（不变量 #5）。

### 路径 3：PullTask + StartTask

```
PullTask:
1. 参数校验 + policy + budget + NATS fetch（非事务）
2. lifecycle.ApplyTx：
   - attemptRepo.Create（claimed）
   - taskRepo.UpdateStatusWithCheckTx → claimed（CAS）
   - outboxRepo.Insert("event_publish", task.claimed 事件)
3. commit 后返回 task + lease

StartTask:
1. lease 校验（非事务）
2. lifecycle.ApplyTx：
   - taskRepo.UpdateStatusWithCheckTx → running（CAS）
   - outboxRepo.Insert("event_publish", task.started 事件)
3. commit 后返回
```

NATS fetch 放事务之前（读取，非副作用）。

### 路径 4：管理事件（cancel/block/unblock/replay）

```
cancel/block/unblock:
1. lifecycle.ApplyTx：
   - taskRepo.UpdateStatusWithCheckTx（CAS，复用 transition 校验）
   - outboxRepo.Insert(对应事件)
2. commit 后返回

replay:
1. lifecycle.ApplyTx：
   - taskRepo.ResetForReplay
   - outboxRepo.Insert("task_publish", 重投递消息)
   - outboxRepo.Insert("event_publish", task.created 事件)
2. commit 后返回
```

---

## 7. NATS ACK 失败处理

事务 commit 后 ACK/TERM NATS 失败时：

- **不返回 error**：task 业务语义已完成，返回 error 会让 Agent 误重试，造成重复执行。
- **记录结构化日志**：含 tenant/task/attempt/delivery_ref/err（M1 暂用 `log` 包，M4 升级 logutil）。
- **写 audit 事件**：`outboxRepo.InsertDirect`（非事务，发生在主事务 commit 之后）写一条带 `ack_error` 的事件进 audit projection。
- **兜底**：redelivery reconciliation（M1-T5）清理再次投递的 completed task delivery。

---

## 8. fallback 路径

**保留**生产走 outbox、测试走直接发的双路径（与现有 `TaskService.Create` 一致）。

判断条件：`if outboxRepo != nil && lifecycle != nil` → 走事务+outbox；否则走原有直接发逻辑。

- 生产/集成测试（有 PG）：走 LifecycleService.ApplyTx + outbox
- 单元测试（内存驱动/mock）：走直接发（fallback）

两套都留，不破坏现有测试。

---

## 9. 测试策略

### 第一层：单元测试（不需要 PG/NATS）

用 mock repo 验证业务逻辑：
- 重复 ACK → ledger 不重复插、budget 不重复结算
- 旧 lease ACK → 拒绝
- 重复 NACK → no-op
- 非法状态转换 → 报错

### 第二层：集成测试（需要真实 PG）

用 `openTestDB`（Phase 0 已改优雅 skip）验证事务一致性：
- ACK 成功 → 同事务 attempt/task/ledger/outbox 都写
- 事务中途失败 → 全部回滚
- 重复 ACK → ledger ON CONFLICT，budget_usage 只加一次
- NATS ACK 失败 → DB completed + 日志 + audit 事件
- NACK → 先 DB commit 再 ACK NATS（顺序验证）

### 本次覆盖的 7 类故障（M1 退出标准）

本次 T9+T1+T2+T3 重点覆盖（直接相关）：**2、4、6、7**。
其余（1、3、5）依赖 T5/T8/T12，随对应任务补。

| # | 故障 | 本次 |
| --- | --- | --- |
| 1 | NATS publish 成功但 MarkPublished 失败 | 随 T15 |
| 2 | DB completed 但 NATS ACK 失败 | ✅ |
| 3 | Pull 后 ACK 前重启 | 随 T5 |
| 4 | lease timeout 后旧 lease ACK | ✅ |
| 5 | retry 耗尽 → DLQ → replay | 随 T7 |
| 6 | 旧 attempt 重复 ACK/NACK | ✅ |
| 7 | 重复 ACK | ✅ |

---

## 10. 依赖关系与执行顺序

```
1. Migration 000010（budget_usage_ledger）
2. repo Tx 方法（第 5 节）
3. LifecycleService（第 3 节）
4. 改造 4 路径（第 6 节）—— 依赖 1/2/3
5. 测试（第 9 节）—— 依赖 4
```

---

## 11. 遗留事项（移交后续 M1 任务）

| 事项 | 归属 |
| --- | --- |
| outbox worker lease（locked_by/locked_at） | T8 |
| outbox 后置事务故障恢复（NATS 成功+MarkPublished 失败） | T15 |
| redelivery reconciliation（created/terminal/old attempt） | T5 |
| retry outbox 驱动（delayed task_publish） | T12 |
| budget 维度修正（agent concurrency 误用全局） | T14 |
| 结构化日志（logutil） | M4 |

---

## 12. 关键不变量（本次强化）

1. `accepted ≠ queued`
2. task enqueue 必经 outbox（本次扩展到 ACK/NACK/replay 事件）
3. **ACK/NACK 先 DB 后 NATS**（本次核心修正）
4. CAS 校验所有状态转换
5. 重复操作幂等（ledger / CAS / no-op）
6. NATS 失败容错（日志 + 审计，不回滚 DB）
