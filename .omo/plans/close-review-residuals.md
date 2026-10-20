# 关闭第二次复评剩余问题 — 依赖分析与分阶段计划

日期: 2026-10-20 · 基线: main 7cd0301 (CI/CodeQL/Nightly三件套绿, 用户确认)

## 范围
复评(1619272)的全部残余: B1-Fail语义, C3(服务端lease-lost事件+Python默认), C1(特殊字符密码+TLS实测), C4(真实helm install→upgrade), B3/B4/B6/B7验收; 外加仓库本地化(docs/ + handoff.md退出GitHub)。

## 依赖图
```
P0 untrack ──(先行, 独占git索引)
b1-fail ──────────────┐
c3-server ─┬─→ c3-python (需task.lease_lost契约)
c1-helm ───┴─→ c4-kind (升级演练带c1修复)
b-acceptance(B3/B4/B6/B7) ←─ 排phase2: B7与c3-server同处lease/expiry区域
```
- phase1 = b1-fail + c3-server + c1-helm (写域互斥: service+retry / core+lease+handler(+sdk只读) / helm+config_test) + verify节点
- phase2 = c3-python(sdk/python) + c4-kind(workflows) + b-acceptance(server/tests+driver) + verify节点
- 泳道不写docs/不commit; 契约片段入wave报告, lead合并进本地docs并提交。

## 关键决策
1. Fail=显式终态(与Nack/_expiry的可重试失败分工), 不引入自动重试——固化+测试+文档, 不re-litigate。
2. C4走CI kind job(本机无docker/kind), workflow含install→upgrade→migration幂等→ready断言; 本地以helm template+语法检查验证。
3. P0迁移docs/Janus-core-capability-matrix.md→scripts/ga-readiness-matrix.md保住ga-readiness CI门。
4. 事件名task.lease_lost; lease scanner重排队分支经outbox event_publish发出(镜像dead_lettered先例)。

## 验收
见 /tmp/ulw-close-residuals-iFAl7J/notepad.md 的A-F准则与QA场景。
