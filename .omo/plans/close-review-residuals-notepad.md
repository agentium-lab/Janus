# Ultrawork Notepad — 关闭第二次复评剩余问题 + docs/handoff本地化
Started: 2026-10-20T12:40+08:00
Run root: /tmp/ulw-close-residuals-iFAl7J

## Plan (exhaustively detailed)
依赖分析（用户要求的核心交付）：
```
P0 untrack(docs/+handoff)      —— 无依赖，必须先行（后续泳道只改本地docs，且避免与泳道并发动git索引）
b1-fail 终态语义固化           —— 无依赖（service层）
c3-server lease-lost事件       —— 无依赖（core+lease+handler）
  └→ c3-python worker默认处理  —— 需要 c3-server 定下的事件契约 task.lease_lost
c1-helm 密码转义+渲染断言      —— 无依赖（helm+config test）
  └→ c4-kind 真实install→升级  —— 升级演练必须带上c1修复后的chart
b-acceptance(B3/B4/B6/B7)      —— 排泳道二：B7与c3-server同属lease/expiry区域，串行避免同文件冲突
```
执行序：
1. P0: git mv 能力矩阵→scripts/ga-readiness-matrix.md + 更新check_ga_readiness.py引用 + git rm -r --cached docs handoff.md + .gitignore(/docs/, /handoff.md) + make test + commit。
2. 泳道一(mass-ulw run: close-residuals-phase1): b1-fail(unspecified-low), c3-server(unspecified-high), c1-helm(unspecified-low) + verify(quick) 节点；泳道不写docs/、不commit，契约片段写入wave报告由lead合并。
3. lead审计泳道一+跑make verify→提交增量。
4. 泳道二(run: close-residuals-phase2): c3-python(quick/unspecified-low, sdk/python), c4-kind-ci(unspecified-low, workflows+本地验证), b-acceptance(unspecified-high, server/tests+driver tests) + verify。
5. lead审计+make verify(覆盖率≥90%)+提交。
6. 清理RUN_TMP，最终报告（含依赖图与CI三件套状态）。

## Success criteria + QA scenarios
A. P0本地化: `git ls-files | grep -c '^docs/'`→0; `git ls-files handoff.md`→空; `ls docs/Janus-api-contract.md handoff.md`存在; make test绿; `grep -rn docs .github/workflows`无引用残留(能力矩阵已迁移)。证据: 命令输出贴notepad。
B. B1 Fail语义: 新增fail_semantics_pg_test.go断言①Fail后attempt状态failed且无live lease ②retry Scheduler不拾取failed任务 ③对failed任务再次Fail/Replay拒绝 ④错误载荷事件已发布。VERIFY: JANUS_PG_* go test ./server/internal/service/ -run TestFail -v 全PASS。
C. C3服务端: core/event.go新增EventLeaseLost="task.lease_lost"; lease scanner重排队分支写outbox event_publish; e2e测试真实broadcaster+WS收到事件。VERIFY: go test ./server/internal/lease/ ./server/internal/handler/ 全PASS; go build ./server/... ./sdk/go/...
D. C1: 渲染检查对新模板PASS、对旧模板(临时还原)FAIL(双输出贴报告); config_test特殊字符密码DSN断言PASS。VERIFY: helm template --set postgres.password='p@ss#w?rd/x' 成功渲染。
E. C4: workflows新增kind job含 install→upgrade(值变更)→migration job重跑幂等→pod ready 断言; 本地`helm template`+`node --check`/actionlint(若有)验证。证据: workflow文件+本地验证输出。
F. B3/B4/B6/B7: 四组新测试真实PG/NATS全PASS; B6语义写入本地docs/Janus-api-contract.md。

## Now
P0 本地化执行（能力矩阵迁移 + untrack + make test + commit）

## Todo
- [进行中] P0全套 → make test → commit
- 泳道一分派(b1-fail/c3-server/c1-helm+verify)
- 泳道一审计+提交
- 泳道二分派(c3-python/c4-kind/b-acceptance+verify)
- 泳道二审计+make verify(≥90%)+提交
- 清理+最终报告

## Findings
- config.DSN() (server/internal/config/config.go:66)已url.QueryEscape密码——C1的Go侧已修，残余只在migration-job.yaml:32运行时`$(JANUS_PG_PASSWORD)`裸拼。
- scripts/check_ga_readiness.py:4 解析 docs/Janus-core-capability-matrix.md —— P0必须迁移此文件保住CI门。
- docs/共33个tracked文件; .github/workflows对docs/零引用; README只链janusa2a.com外站 → 移除不影响CI。
- handoff.md被tracked(aae160a "docs: project handoff")。
- TaskService.Fail(task_service.go:450)=transitionAndFinish(failed,EventTaskFailed,attemptInc1,"failed")+错误载荷事件 → 已是终态；残余=固化+测试+文档。
- retry调度器: server/internal/retry/scheduler.go processReadyRetries。
- python worker(sdk/python/janus_broker/worker.py:60-141): on_lease_lost可选回调, 心跳失败N次后log+回调; 默认(无回调)行为待固化。
- core/event.go:13+ 事件类型常量表; lease/scanner.go:177-193 已有outbox event_publish先例(task.dead_lettered)。
- 服务端/core无任何LeaseLost概念(仅SDK有)。
- 本机: docker/kind/kubectl缺失, helm在~/.local/bin → C4只能CI kind job。
- 本机PG: janus:janus@localhost:5432/janus_test 可连; nats-server在~/.local/bin。
- dispatch_pg_test.go已有MaxConns=1测试(944/1095/1705行)→B3补饱和场景。
- .omo/plans计划文件: .omo/plans/close-review-residuals.md
- DAG SDK: /home/silv/.omo/binary-runtime/5.1.38/plugin/runtime/dag/sdk.js

## Learnings
- (待记)

## [2026-10-20 12:55] P0完成 (准则A PASS)
- git mv docs/Janus-core-capability-matrix.md -> scripts/ga-readiness-matrix.md; check_ga_readiness.py第13行MATRIX路径改为 parent/"ga-readiness-matrix.md"; scripts/AGENTS.md行12同步。
- git rm -r --cached docs/ (33文件) + git rm --cached handoff.md; .gitignore追加 /docs/ 与 /handoff.md (标注 owner decision 2026-10-20)。
- ga-readiness探针: "OK: All 68 P0 capabilities are Covered. GA readiness passed."
- make test 全绿(所有包ok, 无FAIL) → 提交 44410ee "chore(repo): keep docs/ and handoff.md local-only; capability matrix moves to scripts" (co-author仅agentium-lab)。
- 准则A自检: docs tracked=0, handoff tracked=0, 本地文件保留(docs/Janus-api-contract.md, handoff.md, scripts/ga-readiness-matrix.md), CI workflows对docs/引用=0, 工作树干净。

## [2026-10-20 12:58] 泳道一DAG已启动
- run_id: dag_868acb2f-3592-463c-9c90-e0525e8f21d1, key close-residuals-phase1-20261020, 无start warnings。
- 节点: b1-fail(unspecified-low) / c3-server(unspecified-high) / c1-helm(unspecified-low) → verify-phase1(quick, dependsOn全部)。
- settle时会唤醒会话；唤醒后: 读三份waves报告+verify报告 → 亲自复跑关键测试 → 合并契约片段到本地docs → make test → 提交增量 → 分派泳道二(c3-python / c4-kind-ci / b-acceptance)。

## Now
等待泳道一settle唤醒 → 审计三lane+verify → 合并契约 → 提交增量 → 分派泳道二

## Todo
- [x] P0全套(make test绿, commit 44410ee)
- [x] 分派泳道一(run dag_868acb2f)
- [进行中] 审计泳道一并提交增量
- [ ] 分派泳道二三节点加verify
- [ ] 审计泳道二并make verify提交
- [ ] 清理RUN_TMP写最终报告

## [2026-10-21] 泳道一收口 (准则B/C/D 证据)
- DAG dag_868acb2f: b1-fail/c3-server/c1-helm 3节点完成, verify-phase1死于provider限流429(23:14重置) — lead亲跑其四命令补位: make build 0, 五包PG测试全ok, Fail 4/4 PASS, lease-lost e2e PASS, RENDER_OK, vet净, check_helm_render全PASS。
- lead两修复(均在泳道报告发现的范围内缺陷):
  1) dispatch_service.go对账switch加 failed/blocked case(ACK-stale): 修复"未ACK重投可复活failed任务"(b1 lane发现)。突变反证: 禁用case→TestFail_PG_RedeliveryDoesNotResurrect FAIL; 还原→PASS。
  2) config.DSN()改url.UserPassword构造: 修复空格密码'+'不还原(c1 lane发现)。TestPostgresConfig_DSN_SpacePassword PASS。
- 三SDK事件名亲验一致: sdk/go/client.go:392 / sdk/typescript/src/client.ts:215 / sdk/python/janus_broker/client.py:307 均映射"task.lease_lost"。
- 本地docs/Janus-api-contract.md追加契约补遗(Fail终态+task.lease_lost+migration Job转义)。
- make test全绿后三笔提交: c0310a1(B1), 19a8867(C3), f606056(C1, amend修bash吞$(...)问题)。树干净。

## [2026-10-21] 泳道二DAG已启动
- run_id: dag_b2cd5495-0f9e-42d0-82e9-8b872c609942, key close-residuals-phase2-20261021。
- 节点: c3-python(quick) / c4-kind-ci(unspecified-low) / b34-acceptance(unspecified-high) / b67-acceptance(unspecified-low) → verify-phase2(quick)。
- settle后: 审计四lane → make verify(覆盖率≥90%) → 提交 → 清理RUN_TMP → 最终报告+memory。

## Now
等待泳道二settle唤醒 → 审计 → make verify → 提交 → 终验

## Todo
- [x] P0 (44410ee)
- [x] 泳道一审计+提交 (c0310a1/19a8867/f606056)
- [x] 分派泳道二 (dag_b2cd5495)
- [进行中] 审计泳道二并make verify提交
- [ ] 清理RUN_TMP写最终报告

## [2026-10-21] 泳道二审计 (TREAT AS FALSE → 亲验)
- 四lane+verify报告齐; lead亲跑: pytest 24/24 PASS, nightly.yml YAML_OK。
- c3-python报告发现真跨面缺陷(lead复核确认): 服务端Pull响应lease JSON只有lease_id/expires_at(dispatch_handler.go:69-72), PullResult结构无Attempt字段; 而Go SDK(client.go:224)与TS(types.ts:112)的Lease类型都声明了attempt, Python worker读result.lease.attempt对真实服务器必AttributeError —— wire契约半边已定, 服务端从未发送。
- lead lockstep修复: service.PullResult+Attempt字段并在PullTask返回处填充(attempt.Attempt); handler.ServicePullResult+Attempt; HTTP lease JSON加"attempt"; adapters.go/scenario/e2e/simulation透传; Python LeaseInfo加attempt:int=0; handler测试断言attempt=3。gRPC pb.Lease未动(proto改动需重新生成, REST是SDK主面——记为已知边界)。
- b67 lane另发现: nats-server 2.14.x把-p 0当默认口4222而非随机口 → 现有startNATSServer harness并发必撞。lead修复: 显式保留OS分配的loopback空闲端口(-a 127.0.0.1 -p <port>)。
- b34 lane: B3饱和(60s上限内全部完成, SQL读回断言) + B4全链(Create→outbox→Publisher→真NATS→pgqueue→Pull→Start→Ack→completed)全PASS, 无生产缺陷。
- b67 lane: B6短AckWait重投+结算栅栏, B7混版ID共存/无错误抑制/流丢失→ReconcileTenant重建→工作仍可投递, 全PASS; B6契约已写成driver.go AckTask doc comment。
- c4 lane: nightly.yml新增chart-install-upgrade job(294行): kind v0.26.0+digest钉版, 本commit构建镜像side-load, 特殊字符密码安装, install→upgrade(replica 1→2)→migration Job完成+schema_migrations不变(幂等)→Ready→/healthz /readyz 200。本地静态验证全过; 真集群证明待用户push后Nightly执行。
- 教训: edit工具多编辑调用是原子的——一处oldText不匹配整批失败, 需检查哪些块真的应用了(build即时抓出)。
