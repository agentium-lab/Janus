# Janus 项目交接文档（handoff）

- 日期：2026-10-20
- 当前 HEAD：`d22b7d2`（main，已推送）
- 状态：33 个 Go 包全绿（本地 PG 环境全开真跑）、staticcheck 干净、Python SDK 15 通过、TS SDK 23 通过、helm lint 通过、验收矩阵 18/18
- 最近发版：v1.9.0（后续修复未发版，建议下个版本 v1.10.0）

---

## 一、目标

**项目**：Janus — A2A-native Durable Agent Broker（持久任务 + 治理 + 审计一体化的代理编排服务）。

**短期目标**：
1. 关闭 2026-10-12 深度审计（基线 `82f4cea`）的 T01–T13 全部发现，并经三轮 Codex 复评（10-15 `3f0b156`、10-18 `07bfb74`）逐条核销
2. 达到并维持「生产就绪 9/10」，连续 nightly 绿

**长期目标**：设计伙伴获取——用户采用优先于继续堆功能（用户明确方向）。

---

## 二、环境与硬约束（接手必读）

| 项 | 值 |
|---|---|
| 仓库 | `/home/silv/ai/janus/Janus`，remote `git@github-agentium:agentium-lab/Janus.git`（main 分支） |
| Go 代理 | `GOPROXY=https://goproxy.cn,direct` |
| 本地 PG | `/usr/bin/pg_ctl -D /tmp/opencode/pgcov -l /tmp/opencode/pg.log -o "-p 5432 -k /tmp/opencode/pgsock" start`（用户 janus / 密码 janus；崩溃后可能需删 postmaster.pid） |
| 本地 NATS | `/home/silv/.local/bin/nats-server -js -p 4222 -m 8222` |
| 本地 Redis | `/home/silv/.local/bin/redis-server --daemonize yes --port 6379` |
| 测试环境变量 | `JANUS_PG_HOST=localhost JANUS_PG_PORT=5432 JANUS_PG_USER=janus JANUS_PG_PASSWORD=janus`（不开则 PG 套件静默 skip） |
| **提交铁律** | 单元测试全绿才能提交（用户原话：「在提交代码之前先把单元测试跑一遍，都通过了才可以提交」） |
| **署名铁律** | Co-author 只用 `agentium-lab <agentium-lab@users.noreply.github.com>`，绝不出现 sisyphus-dev-ai |
| GitHub 访问 | 本机无 `gh` CLI、无 token（SSH only）——nightly 只能定时自动跑（03:17 UTC），手动触发需用户在网页操作 |
| Helm | 已装 v3.16.3（`~/.local/bin/helm`）；chart 迁移改动后必须跑 `scripts/sync_helm_migrations.sh`（CI 有 diff 校验） |

---

## 三、已完成的工作

### 3.1 三轮审计的发现闭环（15 组：A1–A4、B1–B7、C1–C4）

**核心正确性（B 批次）**
- B1 终态复活：Pull 的 CAS false 整体回滚（含确定性交错回归测试）；Cancel/Complete/Fail/ExpireTasks 全部同事务收尾活跃 attempt；心跳打到已结束 attempt 显式报错（`UpdateHeartbeat` 返回是否命中活跃行）
- B2 重放身份：结算账本在 `ResetForReplayTx` 同事务清除旧行（每轮重新结算，回归测试证明两轮 usage 都计费）
- B3 池自锁：claim 事务内预算查询走 tx 执行器（`BudgetRepo.GetTx` + `CheckConcurrencyTx`）；`decodeBudgetRow` 共用解码（GetTx 曾漏填回→上限读 0，已修 + cap=1 双并发 Pull 反例）；单连接测试覆盖 Pull + 真实 PG budget repo
- B4 非法状态覆盖：非重试 NACK 不再改写 task.status；**真 PG + 真 NATS 全链路验收**（publish→pull→NACK→dead_lettered→dlq_publish outbox→DLQ 流消息→Replay→重投→ACK→completed）
- B5 审批孤儿：gating approval 与 task 同事务提交（`ApprovalRepo.CreateTx`）；幂等命中补偿缺失 approval（区分 ErrNoRows）；`gatingApproval` 共享构造含 24h ExpiresAt
- B6 跨副本 ACK：**portable ACK**——DeliveryRef 携带 `$JS.ACK.<stream>.<consumer>...` 全局可路由应答主题，cache-miss 直接向 broker 发布结算（A 拉取/B 确认 → ack-pending 50ms 归零）；双向滚动升级安全（旧格式 ref 走 AckWait 收敛回退）
- B7 到期审计：到期事件 ID 长度前缀编码（`exp-{len(tenant)}-{tenant}-{task}`），双租户同名任务 + `a-b`/`c` vs `a`/`b-c` 歧义名两反例均过；到期同事务收尾 attempt

**授权与治理（A 批次）**
- A1 归属授权：owner 校验以**认证 principal 优先**（`actingAgent`：bound key 权威，省略/伪造 agent_id 绕不过；无认证回退自报值）；HTTP/gRPC 共用 As 入口；错误不回显真实 lease；三语言 SDK 执行调用带 agent_id；反例：A 的 key 不能完成 B 的 attempt（省略+伪造两路，零副作用）
- A2 审批旁路：Replay 复核审批裁决（rejected/pending → 拒绝，`GetLatestByTask`）+ 原 `task.publish` 策略（`task.replay` action 无规则匹配默认 Allow 的洞已堵）；dispatch 对策略错误 **fail-closed** 且拦 approval_required
- A3 策略路由：模板 API 可执行；actor enrichment 从可信记录加载 team_id，查询故障 fail-closed（ErrNoRows=良性缺失）；路由候选携带真实 team/data_class/running/backlog（trusted records）；无 mailbox 的 agent 不再炸整个 capability 列表
- A4 命名碰撞：sanitize 改**单射编码**（`_`→`__`、`.`→`_D`、其他→`_Z`、`-` 原样保留以维持 v1.9.0 名字一致）；全域单射测试

**部署与门禁（C 批次）**
- C1 Helm：长期 Secret/SA 无 hook，迁移 Job 专属 `-migration` hook 资源（修 hook-succeeded 删库式回归）；`existingSecret` 时 Job 直接引用外部 Secret；迁移 DSN sslmode 统一（`postgres.sslMode`）
- C2 接线：`JANUS_LLM_API_KEY` 注入 Pod；`OutboxPending` 用 `CountPending` 真实总数（原批次长度≤50 使告警永不触发）；Ingress 模板兑现 values 承诺；NATS URL 支持 `existingSecret`；Redis TLS/PG 密码转义等
- C3 SDK：Go/Python 心跳失败计数 + 失租协作取消（hbCtx/on_lease_lost 回调）；TS 超时覆盖 body 读取、AbortSignal 贯穿、handler 收 lease-lost 信号、`voidCall` 统一清理定时器；错误退避（三语言）
- C4 门禁：`record` 状态枚举校验（带注解的失败不再漏计 exit 0）；go_check 无匹配/全 SKIP 判 FAIL（-v 解析）；TS 跑真测试；7-agents 阈值 =7 且 ACK 失败计 FAIL；matrix + 混合版本门接入 nightly

### 3.2 门禁与测试基础设施
- `scripts/acceptance_matrix.sh`：T01–T13 一键验收（18 检查项），nightly 内置
- `scripts/smoke_mixed_version_locks.sh`：v1.9.0（无锁）+ HEAD（advisory lock）同库混布——**抓到过两个真实滚动升级破坏**（000020 generation PK 使旧副本 ACK 42P10；发现 reconcile 5min 自愈窗口过长）；端口与 nightly 副本隔离（8082/8083/9092/9093/8099/9098）
- `mailbox.reconcile_interval` 配置（默认 60s，原硬编码 5min）
- PG 测试 helper 全部支持 `JANUS_PG_PASSWORD`；**大量测试首次真跑**并修正过时断言（幂等 upsert、注册即首跳、事件幂等等）
- nightly：双副本收敛 + chaos + load + release-ops + matrix + mixed；SDK 依赖安装步骤齐备

### 3.3 发版历史
v1.8.0 / v1.8.1 / v1.8.2 / v1.9.0 全部绿流水线；Codex 评分 5.5 → ~9（生产就绪评估仍差授权/并发隔离/配置契约收尾，见下）。

---

## 四、关键决策（及理由）

1. **B2 用「replay 时删账本行」而非 generation 列**：六列 PK 让 v1.9.0 二进制的五列 `ON CONFLICT` 失配（42P10），升级窗口旧副本全 ACK 失败——混合门抓到后回滚方案；账本职责是幂等结算非历史，聚合（budget_usage）已含旧轮
2. **A1 principal 优先而非强制 agent_id**：向后兼容（无认证 dev 模式、未绑定租户 key 回退自报值），同时堵死「省略字段跳过校验」
3. **A4 单射编码保留 `-` 原样**：`-` 转义会改变 `two-replica` 等现有名字 → 与 v1.9.0 混布 stream 名分歧；`_` 作转义符把碰撞域限制到含 `_` 的罕见 ID
4. **B6 ref 携带 ack 主题**：subject:seq 无法推导 consumer 投递序号，ack 主题是唯一可移植结算句柄；格式变化对下游无耦合（全字符串等值比较）
5. **迁移 000020 是 no-op 占位**：曾被 generation 方案占用又回滚；未随 release 发布故可安全改写；**勿删**（链序号连续性）
6. **matrix 全 SKIP 判 FAIL**：PG 测试长期静默 skip 造成「绿灯=没跑」的假绿，代价是本地跑 matrix 必须开 PG env
7. **e2e 与 nats driver 包并行冲突**（`-p 0` 实际绑 4222）：CI 分 job 无此问题；本地全量请保持 NATS 停止、分串行跑（`go list | grep -v tests/e2e` + 单独 e2e）

---

## 五、未完成事项

| # | 事项 | 说明 | 优先级 |
|---|---|---|---|
| 1 | **A3 模板动作 `tool.invoke`/`task.route` 执行检查** | 唯一代码残留：策略模板可生成这些动作的规则，但运行时无对应 `Evaluate` 调用点（需先定义工具执行信封/入口），属特性级设计 | P2 |
| 2 | **matrix 补 A1/A2 行 + Helm 项实测** | 审计 C4 指出矩阵缺 A1/A2 检查项；T08 仍只做 `helm template` 渲染，未真实 install→upgrade | P2 |
| 3 | nightly 验证 `dc243b5` + `d22b7d2` | 今晚 03:17 UTC 自动跑（含全部新门禁）；无 token 无法手动触发 | P1（等待） |
| 4 | 混合版本测试的 NATS 独占约束 | 脚本会删所连 NATS 的全部 `JANUS_*` 流——只能对独占 broker 跑（nightly 单 job 容器满足；本地跑会清空共享 broker 状态） | 文档已注明 |
| 5 | 旧告警/契约长尾 | C2 的 Ingress/NATS 已补；审计提及的「7 天 broker 保留/流丢失后的 queued 恢复」等运维场景未验收 | P3 |
| 6 | 发版 v1.10.0 | B/C 批全部修复 + 新门禁未随版本发布；chart 版本号、release notes、SDK 三语言版本同步 | P2 |

---

## 六、下一步（建议顺序）

1. **等 nightly 绿**（明早确认 `dc243b5`、`d22b7d2`）；失败则按日志逐项修（参考历史：多数为环境依赖如 pip/nats-py/端口冲突，非产品代码）
2. **matrix 补强**：加 T-A1（owner 校验反例，用现成 `TestAckAs_BoundPrincipalCannotFinishForeignAttempt`）、T-A2（`TestReplay_ApprovalVerdictBlocksRequeue`）；T08 升级为 `helm install --wait` + upgrade（需 k3d/kind，或先文档化限制）
3. **A3 模板动作语义设计**：定义 `tool.invoke`/`task.route` 的执行信封（谁在哪个环节调 Evaluate），先写会失败的验收再实现
4. **发版 v1.10.0**：`migrations/` 已同步；记得 `Chart.yaml` version、`sdk/*/package.toml|json` 版本、release notes、打 tag 触发 release 流水线
5. **转向设计伙伴获取**（用户长期方向）：准备 demo 环境（Helm 一键装 + 7-agent 场景脚本即现成的 demo）、提炼 30 秒价值主张（持久任务 + 治理 + 审计的 A2A broker）

---

## 附：常用命令速查

```bash
# 全量测试（PG 全开；先确保 PG 在跑）
JANUS_PG_HOST=localhost JANUS_PG_PORT=5432 JANUS_PG_USER=janus JANUS_PG_PASSWORD=janus \
  go test $(go list ./core/... ./server/... ./sdk/go/... | grep -v tests/e2e) -count=1 -timeout 900s
go test ./server/tests/e2e/ -count=1        # NATS 停止时自动 skip-ok
staticcheck -checks 'all,-U1000,-ST1000,-ST1020,-ST1021' ./server/... ./core/... ./sdk/go/...

# 验收矩阵（需 helm 在 PATH）
JANUS_PG_HOST=localhost JANUS_PG_PORT=5432 JANUS_PG_USER=janus JANUS_PG_PASSWORD=janus bash scripts/acceptance_matrix.sh

# 混合版本门（独占 broker！会清空 JANUS_* 流）
bash scripts/smoke_mixed_version_locks.sh

# Helm 迁移同步（chart 迁移改动后必跑）
bash scripts/sync_helm_migrations.sh
```
