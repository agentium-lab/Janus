# Janus Core GA 收敛计划（当前 MVP → v1.0 GA）

创建日期：2026-06-18
状态：已批准，执行中（Phase 0 ✅ 完成）
配套文档：[2026-06-18-phase0-baseline.md](./2026-06-18-phase0-baseline.md)（Phase 0 实际成果与偏差）

---

## 0. 背景与方法

### 定位澄清

`docs/Janus-production-roadmap.md`（含"v0.6.18/Covered"等进度信息）来自另一台**已 GA** 的机器，**不反映当前仓库状态**。本计划：

- **忽略** roadmap/matrix 中的进度声明（"v0.6.x 已完成"/"Covered"）
- **遵循** 其中的**设计规范**（Milestone 定义、`Janus-detail-design.md`、`Janus-api-contract.md`、`Janus-core-capability-matrix.md`、`Janus-architecture-flow-ascii.md`）

### 当前代码真实状态（Phase 0 核实，2026-06-18）

详见 [phase0-baseline.md](./2026-06-18-phase0-baseline.md)。要点：

- ✅ 已有：9 核心服务、PG/NATS/Redis 三驱动、outbox/状态机/retry/expiry/heartbeat/DLQ、REST+gRPC+WebSocket、A2A 最小网关、API Key+TenantGuard、Go+Python SDK、CLI、**9 个 migration**（含 000009 team_id）、Prometheus 14 指标、healthz/readyz、Makefile、44 测试文件。
- 整体约 30–40% 完成，可靠性最强（70–80%），运维（10–15%）与生态（10–15%）最弱。
- Phase 0 后：所有模块 build/vet/test 全绿，`make verify` 通过，覆盖率基线 44.2%。

### 方法

按 roadmap 6 Milestone 推进。每个 Milestone 含"代码对齐设计规范 + 对应验证门禁脚本"两部分（满足"门禁基础设施一并补齐"）。

---

## 关键设计不变量（全程遵守，来自 architecture-flow）

1. `accepted ≠ queued`（publish 仅 PG 事务提交）。
2. 任何 task enqueue 必须经 transactional outbox，业务服务不直接写 mailbox。
3. 一 mailbox 一 durable pull consumer。
4. Retry 由 outbox `next_attempt_at` 驱动，不用独立 retry stream。
5. ACK/NACK 先提交 DB 再 ACK NATS。
6. Claim 需 durable lease + delivery_ref 在 PG；PG 不可用则无新 claim。
7. Redis 不存 durable 事实（仅 heartbeat/counter/realtime）。
8. semantic routing 不绕过 policy/budget/tenant/classification。
9. 所有过渡经 `TaskService.transition` + repo CAS（`WHERE status=?` + rows affected）。
10. 新 HTTP stable route 必进 conformance fixture + contract-check；新协议入口必保留 trace propagation + 标准 error envelope + 接入同一 auth boundary。

---

## Phase 0：工程基线与编译修复 ✅ 已完成

**目标**：让 `go build`/`go test` 全绿，建立 Makefile + coverage 门禁骨架，消除环境噪声。

**实际成果**：见 [phase0-baseline.md](./2026-06-18-phase0-baseline.md)。核心交付：
- 修复 sdk/go U+2002 损坏、core.Agent.TeamID 全栈接入、proto genproto 冲突、attempt-aware 签名
- stale CLI 测试、e2e/postgres 优雅 skip、迁移链自动发现
- staticcheck 拼写错误/废弃 API/nil context
- envelope tenant 校验（P1 安全）
- 新建 Makefile、CI 强化、仓库卫生

**退出标准**：`make verify` 全绿 ✅；coverage 44.2%（report-only）；janus-api 可构建 ✅。

---

## Milestone 1：Core Reliability Alpha（~2–3 周）

**目标**：补齐作为生产级 durable broker 的可靠性闭环。

**设计依据**：`Janus-detail-design.md` §7（状态机）、§9（NATS/ACK/Retry/DLQ）、§13（Budget/Backpressure）、§22–24（故障/一致性/幂等）、§29；`Janus-architecture-flow-ascii.md` 的 dispatch/policy/budget 流程与不变量。roadmap §6 的 7 类故障场景为验收设计。

### 代码任务

1. **ACK completed 进 outbox（事务一致）**：同 PG 事务内更新 attempt/tasks=completed+result_ref、写 `budget_usage_ledger`（按 task+attempt+scope 幂等，仅 ledger 成功才累加 budget_usage）、并发释放、`task.completed` event outbox（dedupe `task.completed:<t>:<task>:<attempt>`）。DB 失败不 ACK delivery。新增 migration: `budget_usage_ledger`。
2. **DLQ 路径 outbox 化**：final NACK/retry exhausted 同事务；`dlq_publish`+`task.dead_lettered` 走 outbox（dedupe `task.dlq_enqueue:<t>:<task>:<attempt>`）。
3. **DispatchService 生命周期事件进 outbox**：`claimed`/`started`/`retry_scheduled` 不直接 publish；与 task/attempt 状态同事务写 event outbox。保留无 outbox 测试 fallback。
4. **TaskService 管理事件进 outbox**：block/unblock/cancel/replay。
5. **Redelivery reconciliation**：`created` delivery→NACK 保留；terminal/retry_scheduled/old attempt→ACK 清理；同 attempt in-flight→ACK；更新 attempt in-flight→NACK。
6. **ACK/NACK/lease 幂等硬化**：`UpdateFinishedWithCheck` 用 `WHERE status IN ('claimed','running')`；lease timeout 后旧 lease ACK/NACK 拒绝；重复 retriable NACK no-op；`MarkTaskRetryScheduled`/`MarkTaskDeadLettered` 只接受 `claimed/running`。**所有 transition 走 `TaskService.transition` + repo CAS（`WHERE tenant_id=? AND id=? AND status=?`）+ 检查 rows affected**。
7. **DLQ replay/discard 幂等**：`dead_lettered→created→queued` 不重复 publish；已 queued+ 再 replay 返回当前状态；`created` 残留再 replay 重新确保 publish；discard 已 cancelled 无副作用；discard 非 DLQ 报错。新增 PG DLQ replay 事务方法。
8. **Outbox worker lease**：fetch 写 `locked_by`/`locked_at`/`lease_expires_at`(60s)；`publishing` 仅 lease 过期可接管；继续 `FOR UPDATE SKIP LOCKED`；`attempts` 每次投递只 +1。migration: outbox lease 列。
9. **NACK 顺序修正**（Phase 0 遗留）：`DispatchService.NackTask` 改为先提交 DB（attempt failed + task 状态 + event outbox）再 ACK/TERM NATS，满足不变量 #5。
10. **Pull-Ack crash recovery**：claimed/running 同 attempt redelivery 归 `inflight_delivery` 并 ACK；覆盖 Pull 后/ACK 前重启。
11. **Lease timeout scanner**：claimed/running 超时→retry 或 DLQ；旧 lease 拒绝。
12. **Retry outbox 驱动**：`MarkTaskRetryScheduled` 同事务写 attempt failed+task retry_scheduled+event outbox+delayed `task_publish` outbox（`next_attempt_at=task.retry_at`，dedupe `task_publish:<t>:<task>:<next>`）；停旧 retry scheduler 为生产路径（留 legacy fallback）。
13. **NATS tenant stream bootstrap**：API 启动从 PG tenants ensure tenant-level streams；mailbox DLQ stream+consumer 首次 pull lazy ensure。
14. **Budget 维度修正**：agent concurrency 统计该 agent；tenant concurrency 统计全局；Reserve/Settle/Release 同维护 tenant+agent scope usage；写 tenant daily cost。
15. **Outbox 后置事务故障恢复**：覆盖 NATS 成功+MarkTaskPublished 失败。

### 验证门禁
- `make beta-fast`（内存驱动：可配置规模、并发、agent crash→lease expiry/retry/requeue、slow consumer、p95 预算）
- `make coverage COVERAGE_THRESHOLD=90`（**M1 开始强制 90%**）
- PG-backed 可靠性测试覆盖 7 类故障

### 退出标准
单任务任意重启点不丢；重复 ACK/NACK 不重复结算/写结果/调度 retry；accepted≠queued；coverage ≥ 90%。

---

## Milestone 2：API / SDK Contract Beta（~2–3 周）

**目标**：稳定暴露 Core 可靠性语义。

**设计依据**：`Janus-api-contract.md` 全文；detail-design §6/§17/§30.2。

### 代码任务
1. **标准错误契约**：`common.proto` 加 `ErrorCode`+`APIError`；HTTP 错误升级 `{error,code,message,status}`（留 legacy `error`）；HTTP→错误码映射表（contract §2）；gRPC mapper（validation→InvalidArgument, not found→NotFound, policy→PermissionDenied, budget/concurrency→ResourceExhausted, conflict→AlreadyExists, queue/Redis/NATS→Unavailable, DB→Internal）。
2. **grpc-gateway 对齐**：固定 proto field name(snake_case)；create RPC 返回 201；action response shim（heartbeat/cancel/start/ack/nack）；空 pull 返回 204；`limit→page_size` alias；error envelope 一致。parity tests 覆盖 mailbox/dlq/agent/task/api-key。
3. **Mailbox+DLQ proto/gateway 闭环**：新增 `mailbox.proto`/`dlq.proto`+gRPC server+注册 gateway；稳定 route 必须有 proto annotation。
4. **HTTP Task Envelope 完整接入**：复用 proto DTO 或完整 TaskEnvelope DTO；完整解析持久化 `deadline/ttl/budget/policy/context_refs/trace/tool_invocation`。
5. **Go SDK**：attempt-aware；typed `APIError`；API Key；全管理面方法；高层 `Worker` helper。
6. **Python SDK**：`JanusAPIError`(兼容 httpx)；attempt-aware；API Key；TargetType 对齐 core；`JanusWorker`。
7. **TypeScript SDK（新建 `sdk/typescript/`）**：同 contract；`JanusAPIError`/`JanusWorker`/governance fields/tenant+agent list/policy template。
8. **Cross-SDK conformance**：`sdk/conformance/{http_cases,worker_flow}.json`；Go/Python/TS 各自读同一 fixture。
9. **CLI 扩展**：api-key create/list/revoke；全局 `--api-key`；policy template；project config(init/validate/diff/apply/sync)。
10. **契约文档冻结**：完善 api-contract.md；写 `docs/Janus-v0.3-migration.md`（缺失文件）。
11. **mTLS 可选**：server 加 TLS 配置（LoadX509KeyPair、ClientCAs），HTTP/gRPC 同配，TLS 1.2+；`auth.TLS` 配置块；main.go 按 config 启动 TLS。

### 验证门禁
- `make contract-check`(`scripts/check_api_contract.py`)
- `make verify-sdk-cli`(auth-enabled API 上三 SDK real lifecycle+worker+typed error+CLI 全命令)
- `make verify-protocol`(native gRPC+gateway+A2A lifecycle+Audit，M3 加 ACP/MCP/WS)

### 退出标准
三 SDK 跑通同一 worker_flow；无字段分叉；migration note 完整；mTLS 可选；verify 含 Python/TS unit+coverage≥90%。

---

## Milestone 3：Interop + Routing Beta（~3–4 周）

**目标**：接入真实 Agent 生态。

**设计依据**：detail-design §12/§16/§18/§5.5–5.7；api-contract §3；提取的规范 B/D/E（见 phase0-baseline 引用的设计提取）。

### 代码任务
1. **Capability 路由 foundation（替换当前精确匹配）**：候选 online agent+active mailbox；多候选 backlog 最低、mailbox ID 稳定 tie-break；不静默创建无可投递 mailbox 任务。新建 `server/internal/service/routing/`。
2. **硬约束过滤链（顺序）**：online/active→data classification→`task.route` policy deny→capacity→budget→model class。
3. **语义打分（硬约束后）**：本地可审计规则匹配；≥1 正分→`capability_semantic`；全 0→`capability_filtered`。不调 embedding/LLM。
4. **路由审计**：`routing.selected`/`routing.failed` + 指标 `janus_routing_selected_total`/`janus_routing_failures_total`。
5. **group/human 路由**：`routing.{group,human}_mailboxes` tenant-scoped 静态映射；过 active 校验；无映射返 400。
6. **A2A Gateway 完整化**：`/a2a/agent/card`+`/a2a/task/send`+`/a2a/jsonrpc`+`GET /a2a/task/{id}/status`；**无显式 target/mailbox_id 默认 `target.type=intent`**；budget/policy/contextRefs/ttl/trace 透传；错误 envelope。
7. **ACP Gateway（新建 `gateway/acp/`）**：`POST /acp/agent/manifest`+`POST /acp/runs`+`GET /acp/runs/{id}/status`；**无 target 默认 intent**。
8. **MCP Gateway（新建 `gateway/mcp/`）**：`POST /mcp/tools/call`(不得注入 default mailbox,自然语言经 intent)+`GET /mcp/tools/calls/{id}/status`+`POST /mcp/resources`(→ContextRef)。
9. **ContextRef lifecycle**：create/get/list/delete/attach/detach/cross-tenant deny；Task 创建规范化 context_refs；修 detach 路径解析。
10. **Artifact Store**：`core.ArtifactStore`+`LocalArtifactStore`+`ArtifactService`；`POST/GET /v1/tenants/{t}/artifacts`。
11. **Dispatch-time 阻断审计**：fetch 后 policy/DLP/budget 拒绝先写 `policy.denied`/`budget.exceeded` 再 delayed-NACK(5s)。
12. **Interop examples（新建 `examples/interop/`）**：python/{langgraph,autogen,crewai}_worker.py+github-actions/janus-pr-review.yml+README；verify 加 `python-examples-compile`。

### 验证门禁
- `make verify-protocol` 扩展(native gRPC+gateway+A2A+ACP+MCP+WebSocket 真实进程端到端)

### 退出标准
LangGraph/AutoGen/CrewAI/GitHub Actions 示例接入；A2A 链路完整；MCP 不绕过治理；capability 路由硬约束+语义打分+审计闭环。

---

## Milestone 4：Ops + Observability RC（~2–3 周）

**目标**：可部署/观察/升级/回滚。

**设计依据**：detail-design §20/§21/§30.4；规范 C。

### 代码任务
1. **OpenTelemetry tracing（全缺）**：go.mod 加 otel/otlp/otelgrpc/otelhttp；`tracing.enabled` 时初始化 W3C propagator+tracer provider+OTLP gRPC exporter；HTTP middleware request span；gRPC unary interceptor call span；退出 shutdown hook flush。env `JANUS_TRACING_*`。
2. **结构化 JSON 日志（全缺）**：新建 `server/internal/logutil/`(基于 `log/slog`,JSON handler)；observability middleware HTTP/gRPC request log；后台组件错误用 logutil。
3. **完整 Prometheus metrics**：补 HTTP/gRPC request 指标(低基数 label)；补 publish/pull latency、retry/dlq/lease timeout/outbox+mailbox+queue backlog/policy deny/budget throttle/routing 指标埋点；metrics collector 后台刷新 gauge。
4. **healthz/readyz 分离**：readyz=PG/NATS/Redis readiness map+降级恢复语义。
5. **配置模型完整化**：config.go 补 Log/Metrics/Tracing/Outbox/Artifacts/Observability/Routing 字段；outbox 参数读 config；env 覆盖。
6. **Helm chart（新建 `deployments/helm/janus-core/`）**：Deployment/Service/ConfigMap/Secret/ServiceAccount/PDB/probes/securityContext/resources/Prometheus annotations/optional TLS mount/artifact emptyDir 或 PVC/**migrationJob**；values 覆盖全部配置。Dockerfile 同打包 api+migration-probe+nats-persistence-probe。
7. **Grafana dashboard（新建 `deployments/grafana/`）**：`dashboards/janus-core.json`+provisioning。
8. **Probe 工具（新建 `server/cmd/`）**：`janus-migration-probe`+`janus-nats-persistence-probe`+`janus-event-replay-probe`。
9. **Runbook（新建 `docs/Janus-ops-runbook.md`）**：Helm install/upgrade/rollback、controlled migration、artifact PVC、Prometheus scrape、backup/restore、rolling upgrade、OTLP tracing。

### 验证门禁
- `make smoke-prod`(`scripts/smoke_api_dependencies.sh`+`deployments/smoke-deps.compose.yaml` 含 PG/NATS/Redis/Prometheus/Grafana/Tempo/OTel Collector)

### 退出标准
单集群 K8s 可复现部署；滚动重启不丢；指标能定位故障；Dashboard 能解释任务为何未执行。

---

## Milestone 5：Production Beta（~3–5 周）

**目标**：真实受控生产 dogfood + 收敛剩余 P0。

**设计依据**：capability matrix 全部 P0；规范 D/E；roadmap §10。

### 代码（剩余 P0 能力）
1. **Intent Resolver（GOV-13）**：core 加 `TargetType=intent`；`service/intent/`：输入 target.value/payload/content tokens/ContextRef metadata/policy hints；依据 online agent capability 名称/alias/description/schema hints 本地确定性匹配；输出 resolved_capability/confidence/reason/候选摘要；无匹配/歧义/低置信写 routing.failed；成功改写 capability 复用路由链。A2A/ACP 无 target 默认 intent。
2. **Policy Template（GOV-17）**：`PolicyRuleTemplateRequest` builder(12 模板)→编译标准 `policy_rules`；CLI/SDK/API `POST /policy-rules/templates`。
3. **协议中立 tool invocation 审计（GOV-16）**：Envelope `tool_invocation` 字段；HTTP/gRPC/MCP mapping；TaskService/DispatchService 产 `tool.invocation_*`。
4. **Security audit events（SEC-09）**：`security.api_key_created/revoked/auth_failed/tenant_guard_denied`；可插拔审计接口。
5. **多实例 API+多 outbox worker** 真实多进程 exactly-once 验证。

### 验证门禁（全套,新建于 `scripts/`）
- `make smoke-7-agents`：7 agent/artifact ContextRef/capability lookup/fan-out-fan-in/7 task lifecycle/重复 idempotency 不重复投递/non-retriable NACK→DLQ/intent routing/audit trace/metrics。
- `make verify-security`(smoke_core_security.sh+smoke_core_mtls.sh)：API key/rotation/revoked/HTTP+native gRPC+A2A+ACP+MCP+WebSocket tenant guard/cross-tenant deny/log redaction/security audit events/TLS+mTLS。
- `make verify-protocol`/`verify-sdk-cli`（补 ACP/MCP/WebSocket/policy_template）。
- `make verify-governance`：Policy Rule/Budget HTTP 管理/policy deny/approval/capacity+budget+model-class rejection/route explanation audit/ContextRef lifecycle/artifact persistence/Redis TPM throttle/metrics。
- `make verify-reliability`：mailbox lifecycle/重复 ACK 幂等/budget ledger dedupe/retriable NACK retry/retry exhausted DLQ/DLQ replay-discard 幂等/lease timeout recovery/inflight duplicate/old attempt rejection/outbox post-mark failure retry/event replay projection rebuild。
- `make verify-ops-chaos`：Redis+NATS+PostgreSQL restart/readiness 降级恢复/NATS outage outbox retry/API restart bootstrap/双 API+双 outbox worker exactly-once/rolling restart/outbox worker JSON error log。
- `make verify-release-ops`：Helm lint+template/controlled migration rollback drill/PostgreSQL dump-restore/NATS persistence/release load baseline(1000 agents/10000 mailboxes/1000 tasks,publish p95<100ms)/soak preflight。

### 退出标准
dogfood 链路替代点对点；故障恢复有审计证据；无已知 P0；P1 有 owner+下版本目标；load baseline 达标。

---

## Milestone 6：Core v1.0 GA（~1–2 周）

**目标**：触发 GA 门禁通过 + 发布归档。

**设计依据**：capability matrix §1/§9；roadmap §11。

### 任务
1. **`make ga-readiness`**(`scripts/check_ga_readiness.py`)：解析 matrix,任一 P0 非 Covered 即 fail-fast。
2. **`make verify-production`**：总入口,串联 verify+smoke-prod+smoke-7-agents+verify-security+verify-protocol+verify-governance+verify-sdk-cli+verify-reliability+verify-ops-chaos+verify-release-ops+ga-readiness。
3. **Capability Matrix 校准**：全 P0 标 Covered；P1(REL-15/16,GOV-08/14,SDK-06/07,OPS-05)写风险接受+owner+v1.1 目标。
4. **发布归档**：K8s 环境 Helm install/upgrade/rollback 记录；`make beta-soak` 跑满 7 天归档；每 RC 跑 verify-production 存 JSON。
5. **真实 CI/DevOps demo** 通过 Janus 跑通。
6. **tag v1.0.0**+release note(含 P1 风险接受)。

### 退出标准
ga-readiness 通过(全 P0 Covered)；verify-production 全绿；7 天 soak+K8s install/rollback 归档。

---

## 跨 Milestone：Migration 演进

- M1：`budget_usage_ledger`、outbox lease 列(`locked_by/locked_at/lease_expires_at`)、tasks 索引补齐、DLQ replay 事务支持列。
- M3：artifact 表(若 PG 元数据)。
- M5：`mailbox_backlog_index`(`tasks(tenant_id,mailbox_id,status)` 部分索引)。
- 最终 version ≥ 14(对齐 roadmap)。

---

## 工作量估算（~13–22 周）

| 阶段 | 估算 | 关键风险 |
| --- | --- | --- |
| Phase 0 | 3–5 天 ✅（实际 1 天） | grpc-gateway 已修复（报告过时） |
| M1 | 2–3 周 | 并发/事务正确性,coverage 44→90%,NACK 顺序改造 |
| M2 | 2–3 周 | TypeScript SDK 从零,三 SDK conformance |
| M3 | 3–4 周 | ACP/MCP 规范细节,路由硬约束+语义打分 |
| M4 | 2–3 周 | OTel 版本兼容,Helm 生产 values |
| M5 | 3–5 周 | smoke/verify 脚本数量大,chaos/load/soak 真实环境 |
| M6 | 1–2 周 | 7 天 soak 是 wall-clock 瓶颈 |

---

## 执行顺序优化建议

计划严格"按 roadmap 6 Milestone"（已批准选择）。M5 把 Intent/Policy Template/Tool Audit 等 P0 收敛进去，是因 roadmap 把它们放 v0.6.x。

**可选加速**：若想提前获得可演示能力，可把 M5 的 Intent Resolver 前移到 M3（Interop+Routing）与路由一并做——Intent 与 capability 路由天然耦合。执行时可酌情调整，但需同步更新 phase0-baseline 的遗留事项与本计划的归属。

---

## 缺失文档（需在对应 Milestone 创建）

| 文档 | Milestone | 说明 |
| --- | --- | --- |
| `docs/Janus-v0.3-migration.md` | M2 | v0.2→v0.3 SDK 迁移 note（被引用但不存在） |
| `docs/Janus-ops-runbook.md` | M4 | 运维 runbook（被引用但不存在） |
| `docs/Janus-http-gateway-migration-inventory.md` | M2 | gateway 迁移清单（被引用但不存在） |
