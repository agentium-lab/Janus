# Phase 0：工程基线与编译修复 — 执行成果

执行日期：2026-06-18
状态：✅ 完成
关联计划：[2026-06-18-ga-convergence-plan.md](./2026-06-18-ga-convergence-plan.md)

本文档记录 Phase 0 的**实际执行结果**，包括与 `docs/2026-06-12-design-gap-and-code-improvements.md` 复查报告的偏差。后续 Milestone 以本文件为代码基线的真实参照。

---

## 一、复查报告（6/12）与代码现状的偏差

`docs/2026-06-12-design-gap-and-code-improvements.md` 是 2026-06-12 的复查报告。Phase 0 实际执行时（2026-06-18）发现该报告**已大面积过时**：报告声称的多个 P0 编译问题已经修复。逐项核实结论：

### 复查报告声称的问题 vs 实际状态

| 复查报告（6/12）声称 | 实际状态（6/18） | 结论 |
| --- | --- | --- |
| grpc-gateway `.pb.gw.go` 未生成，`janus-api` 无法编译（P0） | `.pb.gw.go` 已生成，`janus-api` 可正常构建（40MB 二进制） | ✅ 已修复，报告过时 |
| CI 用 `./...` 与 go.work 不匹配（P0） | CI 已用显式 module 列表 `./core/... ./server/... ...` | ✅ 已修复，报告过时 |
| `PullTaskRequest` 缺 `agent_id` | 已包含 | ✅ 已修复 |
| `TaskService.Create` 不返回真实 task | 已返回 | ✅ 已修复 |
| `TenantGuard` 未接 API key 后面 | 已接入 | ✅ 已修复 |
| `ContextRefHandler` 未接 main router | 已接入 | ✅ 已修复 |
| `RetryScheduler` 不重新发布 | 已重新发布 | ✅ 已修复 |
| Outbox 缺 `last_error`/`next_attempt_at`/`publishing` | 已支持 | ✅ 已修复 |
| `server/internal/grpcserver/` 空目录（冗余） | 本次删除 | ✅ Phase 0 清理 |

### 复查报告中仍然成立的问题（本次确认/修复）

| 复查报告问题 | Phase 0 处理 |
| --- | --- |
| `sdk/go` 无法编译 | 实际原因不是契约不一致，而是 `client.go` 391 行 U+2002 全角空格损坏 → 已修复 |
| HTTP/gRPC/SDK 契约不一致（CreateTask） | `attempt` 维度已对齐；envelope tenant 校验已补 → 其余契约收敛归 Milestone 2 |
| `TaskHandler.Create` 不校验 envelope tenant 与 URL tenant 一致 | ✅ 已修复（envelope tenant 必须等于 path tenant，envelope 一律用 path tenant） |
| `ApprovalHandler.Request` 忽略 URL path tenant | 实际已用 `tenantIDFromPath`，未忽略 → ✅ 无需操作 |
| NACK 先调 NATS 再改 DB（一致性风险） | 现状确实如此，但属于可靠性硬化核心 → 归 **Milestone 1**（需配合 outbox/CAS 改造） |
| 测试硬编码本机路径（NATS/Redis/PG） | ✅ 已修复（e2e/postgres 优雅 skip，env 可配） |
| Postgres repo 测试迁移链不完整 | ✅ 已修复（改为自动发现 migrations/） |
| Outbox migration down 不完整 | 实际 `000008 down` 已正确删除 `last_error` → ✅ 无需操作 |

---

## 二、Phase 0 实际修复的内容

### 1. 编译/构建类（P0）

#### 1.1 `sdk/go/client.go` U+2002 全角空格损坏
- **现象**：601 行中 391 行的缩进是 U+2002（EN SPACE，`e2 80 82`）而非正常空格/tab，导致 `illegal character U+2002`，sdk/go 无法编译。
- **修复**：`sed 's/\xe2\x80\x82/ /g'` 替换为普通空格，再 `gofmt -w` 重排为 tab 缩进。
- **影响文件**：`sdk/go/client.go`

#### 1.2 `core.Agent` 缺 `TeamID` 字段
- **现象**：`cli/project_config.go`、`cli/commands.go`、`sdk/go` 引用 `agent.TeamID`，但 `core.Agent` 无此字段，导致 cli/sdk 无法编译。合约（`Janus-api-contract.md` §A.5）支持 team-scoped policy rules，故 Agent 需要 team 归属。
- **修复（全栈）**：
  - `core/agent.go`：新增 `TeamID string` 字段（`json:"team_id,omitempty"`）
  - `migrations/000009_agent_team_id.{up,down}.sql`：agents 表加 `team_id` 列 + `(tenant_id, team_id)` 索引
  - `server/internal/driver/postgres/agent_repository.go`：Register/Get/List/ListByStatus/ListAllByStatus/FindByCapability/scanAgents 共 5 处查询 + scan 补 `team_id`
  - `proto/janus/v1/agent.proto`：`Agent`（field 15）+ `RegisterAgentRequest`（field 11）加 `team_id`
  - `buf generate` 重新生成 `proto/gen/`（需安装 buf + 3 插件）
  - `server/internal/grpc/convert.go`：`agentToProto`/`registerReqToAgent` 双向映射
  - `server/internal/handler/agent_handler.go`：Register 接收 body `team_id`

#### 1.3 proto genproto 命名空间冲突
- **现象**：`proto/gen/google/api/httpbody.pb.go` 与 `google.golang.org/genproto/googleapis/api/httpbody` 都注册 `google/api/httpbody.proto`，导致 `server/internal/grpc` 测试 panic（`proto: file already registered`）。
- **修复**：删除 `proto/gen/google/api/httpbody.pb.go`（保留 annotations/http/stubs；genproto 官方包提供 httpbody）。符合 roadmap v0.2.1 记录的修复方式。
- **注意**：后续 `make proto` 重新生成时会再生 httpbody.pb.go，需在 buf 配置或生成后脚本中排除，或改为直接依赖 genproto。**此为遗留事项**。

#### 1.4 demo/sdk 测试 attempt-aware 签名不匹配
- **现象**：SDK `StartTask`/`Heartbeat` 已升级为 attempt-aware 4 参数签名（`(ctx, taskID, attempt, leaseID)`），但 `demo/main.go`、`sdk/go/client_test.go` 仍用旧 3 参数签名。
- **修复**：
  - `demo/main.go`、`demo/pipeline/main.go`、`demo/pipeline/pipeline_test.go`：从 `result.Lease.Attempt` 取 attempt 传入
  - `sdk/go/client_test.go`：4 处 StartTask/Heartbeat + 2 处 AckRequest/NackRequest 补 `Attempt: 1`

#### 1.5 buf 工具链
- **新增**：通过 `go install` 安装 buf v1.59.0 + protoc-gen-go v1.36.0 + protoc-gen-go-grpc v1.5.1 + protoc-gen-grpc-gateway v2.29.0（用 `GOPROXY=https://goproxy.cn,direct` 解决网络）。
- 机器无 `proxy.golang.org` 访问，所有 go install 必须用 goproxy.cn。

### 2. 测试卫生

#### 2.1 stale CLI 测试（3 个）
- **现象**：CLI 已升级（`agent status` 实现化、`agent heartbeat` 返回 err、`mailbox pull` 要求 `--agent`），但测试仍断言旧行为（"not yet implemented"、"status: 500"、不带 --agent）。
- **修复**：`cli/commands_test.go` 重写 `TestAgentStatus`（真实 server + JSON）、`TestAgentHeartbeat_ServerError`（assert.Error）、`TestMailboxPull_Empty`/`TestMailboxPull_ServerError`（补 `--agent a1`）。

#### 2.2 e2e TestMain 优雅 skip
- **修复**：`server/tests/e2e/e2e_test.go` 的 `TestMain` 在 PG/NATS/Redis 不可达时 `os.Exit(0)` + 打印 skip 原因，而非 `os.Exit(1)`。

#### 2.3 postgres 测试优雅 skip + 迁移链修正
- **修复**：
  - `server/internal/driver/postgres/testutil_test.go` 的 `openTestDB`：PG 不可达时 `t.Skipf`
  - `runMigration`：从硬编码 8 个迁移文件名（且与实际文件名不符，如 `000004_budget_usage` 实际是 `000004_outbox_events`）改为**自动发现** `migrations/*.up.sql`/`.down.sql`，down 按逆序执行

### 3. 代码质量（staticcheck）

#### 3.1 `core/driver.go` 拼写错误
- `failure_reason,omitempy` → `failure_reason,omitempty`
- `PllicyDecisionID` → `PolicyDecisionID`（字段未被引用，重命名安全）

#### 3.2 redis driver 废弃 API
- `ZRangeByScore` → `ZRangeArgs`（Redis 6.2.0+ 推荐）

#### 3.3 测试 mock nil context
- `service_test.go` 两处 `ListByStatus(nil, ...)` → `ListByStatus(context.Background(), ...)`

#### 3.4 staticcheck 排除项（暂定，后续 milestone 清理）
- 排除：`U1000`（未用代码，有遗留 test helper）、`ST1000/ST1020/ST1021`（文档注释风格）
- 正确性检查（SA*）全部保留

### 4. 安全（P1）

#### 4.1 envelope tenant 校验
- **修复**：`server/internal/handler/task_handler.go` 的 `Create`：
  - 解码后校验 `req.Envelope.TenantID != "" && != tenantID` → 400
  - Envelope 的 `TenantID` 一律用 path tenant（不再用 body 值），保证 task 记录与 envelope 一致

### 5. 工程基础设施

#### 5.1 新建 `Makefile`
- target：`build`/`vet`/`staticcheck`/`test`/`coverage`/`verify`/`proto`/`python-compile`/`clean`
- 处理 go.work 多 module（显式 `GO_MODULES` 列表）
- `GO_INTERNAL_PKGS`：12 个不需外部设施的内部包
- `coverage`：可配置阈值（`COVERAGE_THRESHOLD`，默认 0 即 report-only；M1 设 90）；用纯 shell 整数比较（不依赖 `bc`）
- 工具链安装用 `GOPROXY=${GOPROXY:-https://goproxy.cn,direct}` fallback

#### 5.2 CI 强化（`.github/workflows/ci.yml`）
- unit test job 补全 12 个内部子包 + simulation
- 新增 "E2E + integration tests" 步骤（带 JANUS_PG_DSN/NATS/REDIS/INTEGRATION env）
- staticcheck 排除项与 Makefile 一致

#### 5.3 仓库卫生
- 删除空目录 `server/internal/grpcserver/`（实际 grpc 代码在 `server/internal/grpc/`，main.go 用别名 import）
- 删除 `deployments/`（仅含旧版重复的 Dockerfile + docker-compose.yaml，根目录版更完整）

---

## 三、验证结果（Phase 0 退出标准）

全部通过：

```
go build ./core/... ./server/... ./cli/... ./sdk/go/... ./demo/... ./proto/...   ✅
go vet  （同上）                                                                  ✅
go test （所有内部包，含 nats/redis driver；e2e/postgres 优雅 skip）              ✅
make verify （vet + staticcheck + test + python-compile + coverage）              ✅
janus-api 二进制可构建（40MB），migrations 共 9 个（000001-000009）               ✅
```

**覆盖率基线**：44.2%（report-only）。90% 硬门槛是 **Milestone 1** 退出标准。

---

## 四、遗留事项（移交后续 Milestone）

| 事项 | 归属 Milestone | 说明 |
| --- | --- | --- |
| NACK 先调 NATS 再改 DB | M1 | 需配合 outbox/CAS 改造，属可靠性硬化核心 |
| HTTP/gRPC/SDK CreateTask 契约完整收敛 | M2 | envelope 其余字段（budget/policy/context_refs）HTTP 完整解析 |
| `make proto` 重新生成会再生 httpbody.pb.go | M1+ | 需在 buf 配置排除或改依赖 genproto |
| staticcheck 排除项 U1000/ST* | 后续 | 清理遗留 test helper + 补包注释 |
| `dispatchAdapter`（simulation test 未用）等 dead code | 后续 | U1000 清理 |
| Coverage 从 44.2% → 90% | M1 | 补 task_service/agent_service/mailbox_service/outbox/retry 单测 |

---

## 五、关键经验

1. **复查报告会过时**：6/12 的报告到 6/18 已大面积失真。后续应以**实际 build/vet/test**为基线，而非旧报告。
2. **机器网络限制**：`proxy.golang.org` 不可达，所有 `go install` 必须用 `goproxy.cn`。Makefile/CI/Dockerfile 已统一处理。
3. **U+2002 损坏**：疑为跨机器复制（Windows ↔ WSL）引入。建议 CI 加 `grep -rP "[\x{2002}\x{2003}\x{00A0}]"` 检查。
4. **proto 重新生成依赖 buf 工具链**：本机已安装到 `~/.local/bin`，但 CI 需补 buf 安装步骤（当前 CI 无 proto generate 校验）。
