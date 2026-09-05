# Codex 黑盒评测 → 整改行动计划

> 来源:2026-08 Codex 对 Janus 的黑盒评测(仅看网站,无法访问私有仓库)。
> 本文档把每条评价映射到具体修复(文件/行号/工作量/目标版本),并与现有版本路线对齐。

## 一、评测裁决总览

| # | Codex 论点 | 裁决 | 处置 |
|---|---|---|---|
| 1 | exactly-once 表述偏强 | ✅ 属实 | **P0 已修**(文档) |
| 2 | 双写一致性运维复杂度 | 🟡 部分属实 | P0 文案 + **P1 代码**(Nats-Msg-Id) |
| 3 | 预算计费不准 | ✅ 属实,且更严重 | P0 文案 + **P1 代码**(cost 路径) |
| 4 | 能力路由需验证 | ✅ 属实(死代码) | P0 文案 + **v1.1.0**(catalog-first) |
| 5 | ACP 命名错误 | ✅ 属实 | **P0 已修**(文档) |
| 6 | 无法核验源码(404) | ✅ 属实(私有仓库) | **P2 战略决策** |

---

## 二、P0 文档可信度(✅ 已完成)

| 改动 | 文件 | 状态 |
|---|---|---|
| exactly-once → 至少一次+幂等 | concepts.html / zh | ✅ |
| ACP → Agent Communication Protocol (IBM/BeeAI) + A2A 并入 | concepts.html / zh | ✅ |
| Intent Resolver 标记 Roadmap,删假示例 | concepts/sdk/index.html en+zh | ✅ |
| daily cost → 估算(agent 上报) | concepts/index/quickstart en+zh + README | ✅ |

---

## 三、P1 代码修复(按版本排期)

### v1.0.1 — 可靠性补丁(随 P1 smoke 一起)

**P1-A:补 NATS `Nats-Msg-Id` 去重头**(Codex #2)
- 文件:`server/internal/driver/nats/driver.go` `PublishTask`(约 75-93 行)
- 改动:发布前 `nmsg.Header.Set("Nats-Msg-Id", taskID+":"+attempt)`
- 效果:关掉 outbox 重放时的重复投递窗口(流已配 `Duplicates: 2m`,但从未设 Msg-Id → 死配置)
- 工作量:1 行 + 1 个测试
- 顺手:删 `server/internal/outbox/publisher.go:71` 误导注释 "Nats-Msg-Id dedupe (when enabled)"

**P1-B:撤下 DailyCostUSD 公开配置面**(Codex #3,产品决策后调整)
- 决策依据:成本估算公式(`tokens * 0.00003`)既不分模型、又不分输入/输出,且 token 靠 agent 自报——做"护栏"价值有限,做"账单"会诱导误信。接上等于把"缺失功能"变成"误导功能",声誉风险 > 收益(详见 product-brainstorming 评估)
- 处置:从公开配置/文档面撤下(quickstart 示例删 `daily_usd`、配置表标 *Planned*、concepts/index/README 改"规划中")
- 代码:BudgetSpec 字段保留(无害,cost 维持 0),不接估算公式
- 后续:真正的成本控制需"可信 token 计量"(broker 侧计数/LLM proxy),归 Enterprise 里程碑;v1.1.0 可先做模型定价表,但计量信任仍是前置瓶颈

### v1.0.2 — 治理清理(随 MED/LOW)

**P1-C:`MonthlyCostUSD` 二选一**(Codex #3)
- 现状:`core/budget.go` 定义、入库,但 `budget_service.go` 从未校验
- 选项:① 在 `Reserve` 加月度检查;② 从 BudgetSpec 删除该字段
- 推荐:①(用户预期月度预算),工作量小

**P1-D:成本定价模型化**(Codex #3,可选/较大)
- 现状:`budget_service.go:145` 写死 `tokens * 0.00003`(平价,不分模型)
- 改动:引入模型→单价表(config 或 DB),`Settle` 按模型查价
- 工作量:中等;可推迟到 v1.1.0/Enterprise
- 注:token 仍靠 agent 自报;真正的"可信计费"需 Janus 接管模型调用(Enterprise 范畴)

**P1-E:清理误导注释**(Codex #2)
- `server/internal/outbox/publisher.go:71` "Nats-Msg-Id dedupe (when enabled)" → 改为如实描述(随 P1-A)

---

## 四、P2 战略决策(需你拍板)

**P2-仓库可见性**(Codex #6,影响"可验证成熟度 4/10")
- 现状:仓库私有 → 任何外部评测都只能看网站,无法核查测试/修复/审计
- Codex 的 4/10 几乎完全吃亏在这里
- 选项:
  - ① **公开**:SC+HIGH 修复、20 包测试、7-agent 验证都能被核查,分数会明显回升
  - ② **保持私有**:接受外部评测的低分,靠商业渠道证明成熟度
- 建议:①。技术方向已得 8/10,公开代码是把"可验证成熟度"拉起来的最高 ROI 动作

---

## 五、Codex 点 #4(意图识别)对齐

- Codex 质疑能力路由的准确率/价值;真相是**意图识别为死代码**
- 文档(P0)已将其标记为 Roadmap
- 实现:已在 `Janus-production-roadmap.md §16 v1.1.0` 规划(catalog-first 三阶段)
- 不需要新增计划,沿用 v1.1.0 路线

---

## 六、Codex 的"三件要证明的事"→ 验证任务

Codex 总结:Janus 最需要证明的不是"功能多",而是——
1. **故障时是否真的不丢任务** → 已有 e2e/reliability 测试 + 7-agent 验证;**v1.0.1 smoke** 补 chaos 场景(进程崩溃中途、NATS/PG 单点故障)
2. **重复执行能否控制** → P1-A(Nats-Msg-Id)关掉重复投递 + PullTask 幂等(已有);补重复投递的 e2e 断言
3. **权限和成本限制是否可靠** → SC-1/SC-2(已修)+ P1-B(cost 路径);补"超预算被拒"的 e2e

这三条作为 v1.0.1 smoke 的验收口径,直接回应 Codex 的核心质疑。

---

## 七、版本映射小结

| 版本 | 内容 | 来源 |
|---|---|---|
| **v1.0.0 GA**(已发) | SC+HIGH 修复 | 既定 |
| **v1.0.1** | P1 smoke + P1-A(Nats-Msg-Id)+ ~~P1-B(已改为撤下,见上)~~ + 三件证明的 chaos 验证 | Codex #2/#3 + 既定 smoke |
| **v1.0.2** | MED/LOW + P1-C(MonthlyCostUSD)+ P1-E(注释清理) | Codex #3 + 既定 |
| **v1.1.0** | 意图识别 catalog-first + P1-D(模型定价,可选) | Codex #4/#3 + 既定 |
| **战略** | 仓库公开决策(P2) | Codex #6 |
