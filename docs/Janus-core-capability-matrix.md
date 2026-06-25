# Janus Core Capability Matrix

状态日期：2026-06-14

本文档是 Janus Core 生产级发布门禁。它覆盖 Core 的协议、可靠性、治理、安全、SDK/CLI 和运维能力，并明确每项能力当前状态、已有验证和 GA 前必须补齐的验证。

本文档只覆盖 Janus Core。SSO/SAML/SCIM、完整 RBAC/ABAC、OPA/Cedar 策略包、高级 DLP / PII 检测引擎、KMS/WORM/SIEM、成本中心、完整物理多租户隔离、Operator、air-gapped 交付和商业连接器包属于 Janus Enterprise，不进入 Core GA 门槛。

---

## 1. 状态与门禁规则

状态定义：

| 状态 | 含义 |
| --- | --- |
| `Covered` | 已实现，并且已有自动化测试或真实依赖验证覆盖该能力的主要生产语义。 |
| `Partial` | 已实现主要路径，但生产级验证不完整，或只覆盖单测/contract，尚缺真实依赖、故障、跨协议或安全验证。 |
| `Missing test` | 能力实现存在或基本存在，但缺少能作为发布证据的测试。 |
| `Not implemented` | Core 设计要求存在，但当前尚未实现。 |
| `Enterprise` | 明确不属于 Core，不能作为 Core GA 阻塞项。 |

优先级定义：

| 优先级 | 发布含义 |
| --- | --- |
| `P0` | Core GA 阻塞项。v1.0 之前必须达到 `Covered`。 |
| `P1` | Core RC 阻塞项。若 GA 时仍为 `Partial`，必须有明确风险接受和后续版本目标。 |
| `P2` | Core 可用性或生态增强项，不阻塞 GA，但必须保留边界说明。 |

GA 判定规则：

1. 所有 `P0` Core 能力必须是 `Covered`。
2. 任一 `P0` 为 `Partial`、`Missing test` 或 `Not implemented` 时，Janus Core 不能标记为生产级发布。
3. `P1` 可在明确风险接受后进入 GA，但必须写入 release note 和下一版本计划。
4. 所有 Core 能力必须映射到至少一个验证入口：单测、contract、integration、scenario、fault、security、ops、load 或 soak。
5. 新增 Core 能力必须先更新本矩阵，再实现代码和测试。

---

## 2. 现有验证入口

| 验证入口 | 覆盖范围 | 当前用途 |
| --- | --- | --- |
| `make verify` | Go 全量测试、Python syntax/unit、TypeScript unit、contract drift、Core 覆盖率 90.0% | 本地合并基线。 |
| `make contract-check` | proto HTTP annotation、SDK conformance fixture、HTTP-only allowlist | API/SDK 漂移门禁。 |
| `make beta-fast` | 内存驱动并发、agent crash、lease expiry、slow consumer、p95 预算 | Production Beta 前置语义验证。 |
| `make smoke-prod` | API/PostgreSQL/NATS/Redis、metrics、OTLP 端口、Prometheus、Grafana、Tempo readiness、task lifecycle metrics、Audit trace query、Tempo trace-id query、结构化异常日志 | 真实依赖 happy path 和基础观测验证。 |
| `make smoke-7-agents` | 7-agent 复杂场景、artifact ContextRef、capability lookup、fan-out/fan-in、异常 ACK/NACK/DLQ、audit、metrics | 生产近似业务流程验证。 |
| `make smoke-security` | auth-enabled API key、rotation、HTTP/native gRPC/A2A/ACP/MCP/WebSocket tenant guard、revoked key、ContextRef/artifact cross-tenant deny、raw API key 和 Task Envelope 敏感内容 log redaction、API key rotation/auth failure/tenant guard audit events | Core 安全生产近似验证。 |
| `make smoke-mtls` | HTTP/gRPC TLS+mTLS、grpc-gateway auth header propagation、native gRPC mTLS + API key | Core TLS/mTLS 生产近似验证。 |
| `make verify-security` | `smoke-security` + `smoke-mtls` | 当前 Core 安全 gate；仍需补 NATS/Redis boundary 和 deep data-isolation chaos。 |
| `make verify-protocol` | native gRPC、grpc-gateway、A2A、ACP、MCP 和 WebSocket lifecycle，包括 create、message/send/run/tool-call/resource、pull、start、ack、non-retriable nack、Audit task/trace query、DLQ query、ACP/MCP 错误 envelope、WebSocket event stream | 当前 Core 协议 gate。 |
| `make verify-governance` | Policy Rule / Budget HTTP 管理 API、policy publish deny、policy route + data classification deny、approval approve/reject/expire、capacity/offline/budget/model-class route rejection、route explanation audit、ContextRef attach/detach/delete/cross-tenant deny、artifact upload/download/restart persistence、Redis-backed TPM budget throttle、policy/budget/routing metrics | 当前 Core 治理 gate。 |
| `make verify-sdk-cli` | auth-enabled API 上的 Go/Python/TypeScript SDK real API lifecycle、高层 SDK worker helper、typed errors、Policy Rule / Policy Template / Budget SDK 方法、CLI agent/task/mailbox/DLQ/api-key/policy、global `--api-key`、API key revoke 和 tenant guard | 当前 Core SDK/CLI gate。 |
| `make verify-reliability` | PostgreSQL/NATS/Redis 真实依赖上的 mailbox pause/resume/backlog、pull/start/heartbeat/ACK、重复 ACK 幂等、budget ledger dedupe、retriable NACK retry、retry exhausted DLQ、DLQ replay/discard、lease timeout recovery、inflight duplicate、old attempt rejection、outbox post-mark failure retry、event replay projection rebuild、ACK/PULL/NACK/retry/DLQ/lease/outbox/mailbox metrics | 当前 Core 可靠性语义 gate；dependency restart 和 bootstrap recovery 由 `make verify-ops-chaos` 覆盖。 |
| `make verify-ops-chaos` | Redis/NATS/PostgreSQL stop/start、readiness 降级、Redis heartbeat restore、NATS outage accepted publish、outbox retry recovery、API restart tenant-stream bootstrap、lazy consumer verification、多 API 进程、多 outbox worker、exactly-once delivery、本地 rolling restart、outbox worker JSON error log | 当前 Core 真实依赖 chaos gate；后续继续扩展 load 和 soak。 |
| `make ga-readiness` | 解析本矩阵，任一 P0 非 `Covered` 即失败 | 防止把部分 gate 通过误判为 GA。 |

GA 前需要新增或扩展的验证入口：

| 目标入口 | 必须覆盖 |
| --- | --- |
| `make verify-protocol` 扩展 | 已接入 ACP、MCP、WebSocket 在真实 API 进程上的端到端验证；native gRPC、grpc-gateway 和 A2A lifecycle 继续作为协议基线。 |
| `make verify-governance` 扩展 | 已接入 approval reject/expire、capacity/offline/budget/model-class rejection、route explanation audit、ContextRef detach/delete/cross-tenant deny、artifact restart persistence。 |
| `make verify-security` | 已覆盖 API key、HTTP/native gRPC/A2A/ACP/MCP/WebSocket tenant guard、API key rotation、TLS/mTLS、secret/log redaction 和安全审计；仍需扩展底层 NATS/Redis/PostgreSQL 资源隔离验证。 |
| `make verify-ops-chaos` | 已接入 Redis/NATS/PostgreSQL restart、readiness 降级、API restart tenant-stream bootstrap、lazy mailbox consumer bootstrap、outbox retry recovery、多实例 outbox 和本地 rolling restart。 |
| `make verify-production` | `verify`、`smoke-prod`、`smoke-7-agents`、`verify-security`、`verify-protocol`、`verify-governance`、`verify-sdk-cli`、`verify-reliability`、`verify-ops-chaos` 和 `ga-readiness` 的总入口；后续继续接入 load/soak gates。 |

---

## 3. 协议与互通能力

| ID | 能力 | 优先级 | 当前状态 | 已有验证 | GA 缺口 |
| --- | --- | --- | --- | --- | --- |
| PROTO-01 | Task Envelope 稳定字段、tenant scope、target、budget、policy、trace、ContextRef | P0 | Covered | `make verify`、`docs/Janus-api-contract.md`、SDK conformance | 继续要求所有字段变更先改 contract fixture。 |
| PROTO-02 | HTTP `/v1` 稳定 API：tenant、agent、mailbox、task、dispatch、DLQ、API key、audit、policy-rules、policy-rules/templates、budgets | P0 | Covered | `make verify`、`make contract-check`、`make smoke-prod`、`make smoke-7-agents`、`make verify-security`、`make verify-governance`、`make verify-protocol`、治理管理 SDK/conformance fixture | 新 HTTP stable route 必须进入 contract fixture；非 `/v1` 入口继续由 protocol/security gate 兜底。 |
| PROTO-03 | native gRPC：Agent、Task、Dispatch、Audit、Mailbox、DLQ、Auth | P0 | Covered | gRPC handler 单测、error mapper、gateway parity、`make verify-protocol` native lifecycle + Audit smoke、`make smoke-security` native gRPC auth probe、`make smoke-mtls` native gRPC TLS probe | DLQ replay/discard 深度语义归 REL-10。 |
| PROTO-04 | grpc-gateway `/grpc/v1` 与 HTTP `/v1` contract parity | P0 | Covered | gateway parity tests、contract drift check、`make verify-protocol` grpc-gateway lifecycle smoke、`make smoke-mtls` grpc-gateway API key list over mTLS | 新增 gateway 路由必须同步 parity test 和 protocol smoke。 |
| PROTO-05 | 结构化错误 envelope 和 gRPC status mapping | P0 | Covered | HTTP/SDK/gRPC 单测、typed `PermissionDeniedError` / `BackpressureError` mapping tests、conformance fixture、`make verify-security` WebSocket/auth rejection、`make verify-governance` governance rejection、`make verify-protocol` ACP/MCP error envelope、`make verify-sdk-cli` SDK typed error smoke | 新入口必须返回标准 `code/message/status` 或对应 gRPC status。 |
| PROTO-06 | W3C `traceparent` 入站提取、响应传播和跨协议 headers | P0 | Covered | observability tests、`make smoke-prod`、`make smoke-7-agents`、`make verify-protocol` gRPC/gateway/A2A/ACP/MCP/WebSocket trace/audit checks | 新协议入口必须继续保留 trace propagation 和响应头/metadata。 |
| PROTO-07 | A2A gateway：Agent Card、message/send、task status、error、trace/context | P0 | Covered | A2A adapter/gateway tests、`make verify-protocol` A2A lifecycle + governance envelope + Audit smoke | 新增 A2A 方法必须继续验证不会丢 policy/budget/context/trace。 |
| PROTO-08 | ACP gateway：manifest、runs、status、error、trace/context | P1 | Covered | ACP adapter/gateway tests、`make verify-protocol` ACP manifest/run/status/error/trace smoke | 新 ACP 字段必须同步 adapter 和 protocol smoke。 |
| PROTO-09 | MCP gateway：tool call、resource ContextRef、status、error、trace/context | P1 | Covered | MCP adapter/gateway tests、`make verify-protocol` MCP resource/tool/status/error/trace smoke | 新 MCP 字段必须同步 adapter 和 protocol smoke。 |
| PROTO-10 | WebSocket dashboard event stream，按 tenant/task/agent 过滤事件 | P1 | Covered | WebSocket handler tests、CLI dashboard tests、`make verify-protocol` NATS event stream -> `/ws` completed event smoke、`make verify-security` auth-enabled tenant guard | CLI dashboard UX 仍归 SDK-06。 |
| PROTO-11 | Audit/event REST API：tenant events、task events、trace events | P0 | Covered | audit repo/handler/gRPC tests、SDK conformance、7-agent trace events、`make smoke-prod` task events + trace events 与 OTLP trace id 关联查询 | 新 event projection 字段必须同步 Audit REST/API conformance 和 trace correlation smoke。 |

---

## 4. Core 数据面与可靠性

| ID | 能力 | 优先级 | 当前状态 | 已有验证 | GA 缺口 |
| --- | --- | --- | --- | --- | --- |
| REL-01 | Tenant lifecycle 和 tenant-scoped resource boundary | P0 | Covered | tenant repo tests、HTTP/SDK contract、`make verify-security` HTTP/native gRPC/A2A/ACP/MCP/WebSocket/ContextRef/artifact cross-tenant deny | 底层 NATS/Redis/PostgreSQL isolation chaos 归 SEC-08。 |
| REL-02 | Agent registry、capabilities、heartbeat、Redis TTL、offline sweeper | P0 | Covered | agent/redis/heartbeat/sweeper tests、7-agent registration、`make verify-ops-chaos` Redis restart + heartbeat restore | 新 heartbeat 状态语义必须同步 Redis restart 和 sweeper regression。 |
| REL-03 | Mailbox create/get/update/pause/resume、active/inactive 校验、backlog | P0 | Covered | handler/repo/SDK/gateway/conformance tests、7-agent mailboxes、`make verify-reliability` pause/resume/inactive/backlog smoke | 新增 mailbox 行为必须继续进入 SDK/contract 和 reliability smoke。 |
| REL-04 | Publish 接受语义：API accepted 不等于 queued，created -> outbox -> queued | P0 | Covered | reliability tests、contract docs、smoke task lifecycle、`make verify-ops-chaos` NATS outage publish accepted + outbox retry recovery | 新 publish side effect 必须保持 outbox-backed accepted semantics。 |
| REL-05 | NATS JetStream tenant stream bootstrap 与 mailbox consumer lazy bootstrap | P0 | Covered | bootstrap unit tests、smoke NATS lifecycle、`make verify-ops-chaos` API restart tenant-stream bootstrap + first-pull lazy consumer monitor verification | 启动阶段不得全量创建 10k mailbox consumers；新 mailbox/tenant provisioning 变更必须进入 bootstrap smoke。 |
| REL-06 | Pull -> Start -> Heartbeat -> ACK lifecycle，attempt/lease 校验，`agent_id` 必填和 mailbox owner check | P0 | Covered | dispatch tests、HTTP/gRPC handler tests、SDK worker fixture、smoke-prod、smoke-7-agents、`make verify-protocol` gRPC/gateway lifecycle、`make verify-sdk-cli` SDK lifecycle、`make verify-reliability` HTTP lifecycle | 新增 lifecycle 字段必须进入 protocol、SDK 和 reliability gates。 |
| REL-07 | ACK 幂等：result_ref、token usage、ledger、预算释放、completed event 不重复 | P0 | Covered | service/outbox reliability tests、7-agent result_ref/token usage、`make verify-reliability` duplicate ACK + budget ledger dedupe + completed event count | 新增 ACK side effect 必须证明重复 ACK 无重复副作用。 |
| REL-08 | NACK retriable：failed attempt、retry_scheduled、delayed outbox、attempt+1 | P0 | Covered | reliability tests、beta-fast、`make verify-reliability` retriable NACK -> retry -> completed | 继续要求 retry delay/outbox 变更进入真实依赖 smoke。 |
| REL-09 | NACK non-retriable 和 retry exhausted -> DLQ | P0 | Covered | reliability tests、7-agent non-retriable NACK DLQ、`make verify-reliability` retry exhausted -> DLQ | 新增 DLQ 规则必须覆盖 exhausted 和 non-retriable 两类路径。 |
| REL-10 | DLQ query/replay/discard 幂等 | P0 | Covered | DLQ unit/reliability/gateway tests、`make verify-reliability` query/replay/discard/idempotency smoke | 继续要求 DLQ route 变更进入 SDK/conformance 和 reliability smoke。 |
| REL-11 | Lease timeout scanner：claimed/running -> retry 或 DLQ，旧 lease ACK/NACK 拒绝 | P0 | Covered | lease/reliability tests、beta-fast、`make verify-reliability` forced lease expiry -> scanner -> redelivery -> completed | API restart/agent crash 仍可在 OPS chaos 中扩展，但核心 lease timeout 语义已覆盖。 |
| REL-12 | Redelivery reconciliation：created、terminal、retry_scheduled、old attempt、inflight duplicate；dispatch-time policy/DLP/budget/claim 前失败记录阻断审计事件并使用 delayed NACK | P0 | Covered | dispatch/reliability tests、dispatch-time policy/DLP/budget blocking audit tests、NATS delayed NACK driver test、`make verify-reliability` created/retry_scheduled outbox race, old attempt rejection, inflight duplicate no-redelivery | 新 redelivery 状态必须同步 NATS-backed reliability smoke。 |
| REL-13 | Transactional outbox：task_publish、event_publish、dlq_publish、dedupe、worker lease | P0 | Covered | outbox repo/publisher tests、outbox ID regression、`make verify-reliability` task/event/DLQ outbox and dedupe smoke、`make verify-ops-chaos` multi API/outbox worker + backlog drain | 新 outbox kind 必须覆盖 dedupe、lease 和真实依赖 drain。 |
| REL-14 | Outbox 后置 transaction failure recovery | P0 | Covered | publisher failure tests、`make verify-reliability` forced task_publish row retry after publish + NATS dedupe/exactly-once delivery | 新 outbox mark failure path 必须证明 retry 不造成重复投递。 |
| REL-15 | Task TTL / expiry scanner | P1 | Partial | expiry scanner tests | 缺真实依赖 TTL expiry smoke 和 event/audit 验证。 |
| REL-16 | Task cancel/replay/block/unblock 管理事件 outbox 化 | P1 | Partial | service/handler tests、contract fixture for cancel/replay | 缺真实依赖 cancel/replay/block/unblock smoke。 |
| REL-17 | Audit event projection 和 replay 查询 | P0 | Covered | event repo/projector tests、NATS durable event consumer test、task events conformance、7-agent trace events、`make verify-reliability` `janus-event-replay-probe` projection rebuild from NATS event stream | Projection 必须由 tenant-scoped durable consumer 驱动；WebSocket raw subscription 只用于实时广播。新 event projection 字段必须可从 event stream 重放重建。 |
| REL-18 | PostgreSQL migrations 幂等执行 | P0 | Covered | startup migration path、`janus-migration-probe`、`make verify-release-ops` migration dry-run/no-change/rollback/roll-forward drill | 新 migration 必须通过 probe 的 `up -> no-change -> down one -> up one -> no-change`。 |
| REL-19 | Redis 用于 heartbeat 与 rate-limit/cache，不承载 durable task/event/audit | P0 | Covered | design boundary、redis driver tests、readyz smoke、`make verify-ops-chaos` Redis restart 后 PostgreSQL agent 状态保持 + heartbeat key restore | 新 Redis 用途不得承载 durable task/event/audit 状态。 |
| REL-20 | 多实例 API 和多 outbox worker leaderless 并发 | P0 | Covered | outbox worker lease tests、`make verify-ops-chaos` dual API processes + dual outbox workers + exactly-once task delivery | 后续 Kubernetes replica rollout 归 OPS-07/OPS-10。 |

---

## 5. 治理与路由能力

| ID | 能力 | 优先级 | 当前状态 | 已有验证 | GA 缺口 |
| --- | --- | --- | --- | --- | --- |
| GOV-01 | 基础 Policy engine：allow、deny、approval_required | P0 | Covered | policy service tests、routing integration tests、Policy Rule HTTP handler/router tests、`make verify-governance` policy publish deny + approval_required e2e | 新 policy action/condition 字段必须同步 governance smoke。 |
| GOV-02 | Approval：request、approve、reject、expire、approval_pending task 恢复或取消 | P0 | Covered | approval service/handler tests、ApprovalRepo nullable-field regression、dispatch approval_pending outbox race regression、`make verify-governance` approval_required approve/reject/expire e2e | 新 approval 状态或公开 API 必须同步 governance smoke。 |
| GOV-03 | Budget：max_tokens、max_cost_usd、model classes、Redis RPM/TPM、`budget_usage.task_count` 并发占用、ACK ledger settlement/release | P0 | Covered | budget service tests、ACK ledger tests、7-agent token usage、Budget HTTP handler/router tests、`make verify-governance` Redis-backed TPM throttle + max_cost/model_class route rejection、`make verify-reliability` token usage settle/release + ledger dedupe | 新预算维度必须同时覆盖 throttle、route rejection、usage accounting 和 ledger dedupe。 |
| GOV-04 | Concurrency / capacity governance：agent/mailbox max_concurrency、backlog filtering | P0 | Covered | routing tests、beta-fast slow consumer、`make verify-governance` capacity exceeded route rejection + audit payload | 新 capacity 维度必须同步 routing audit 断言。 |
| GOV-05 | Routing target `mailbox`：active mailbox 校验，不静默创建不可投递任务 | P0 | Covered | routing tests、smoke task lifecycle、`make verify-governance` mailbox-target policy deny/approval/budget lifecycle | 新 mailbox 管理语义必须同步 smoke。 |
| GOV-06 | Routing target `agent`：online agent + active mailbox 选择 | P0 | Covered | routing tests、`make verify-governance` offline agent target rejection + audit payload | 新 agent 状态语义必须同步真实依赖 smoke。 |
| GOV-07 | Routing target `capability`：online capability agents、backlog、policy、budget、classification、semantic score | P0 | Covered | routing tests、7-agent capability lookup、`make verify-governance` multi-candidate capacity/budget/model_class/classification rejection + semantic selection audit | 新 candidate filter 必须进入 route explanation audit。 |
| GOV-08 | Routing target `group` / `human`：tenant-scoped static mailbox mapping | P1 | Partial | routing tests | 缺真实依赖 mapped target smoke。 |
| GOV-09 | Routing audit and metrics：`routing.failed`、`routing.selected`、candidate explanation | P0 | Covered | service tests、dashboard metrics definitions、`make verify-governance` audit API query for `routing.failed` / `routing.selected` candidate explanation | Prometheus 异常指标增量归 OPS-02。 |
| GOV-10 | ContextRef lifecycle：create/get/list/delete、attach/detach task、cross-tenant deny | P0 | Covered | handler/service/repo tests、7-agent artifact ContextRef lookup and task binding query、`make verify-governance` attach/get/bind/list/detach/delete/cross-tenant deny | 新 ContextRef 生命周期 API 必须同步真实依赖 smoke。 |
| GOV-11 | Artifact store interface、本地实现、upload/download、sha256、ContextRef registration | P0 | Covered | artifact handler/service tests、7-agent upload/download/SHA-256 check、`make verify-governance` artifact upload/download/ContextRef registration + API restart persistence | PVC/Kubernetes persistence 归 OPS-07/OPS-15；Core 本地 artifact persistence 已覆盖。 |
| GOV-12 | Data classification：ContextRef/capability filtering，不把 confidential 投递给不合格 Agent | P0 | Covered | routing classification tests、7-agent confidential ContextRef creation、`make verify-governance` policy classification deny + ContextRef-derived classification route selection audit | 新 classification rank 或 schema 字段必须同步 smoke。 |
| GOV-13 | Natural language intent resolver：将自然语言请求解析为 capability target，例如“我想审查这段代码” -> `target_type=capability,target_value=code_review` | P0 | Covered | Core target validation、TaskService intent resolver tests 覆盖匹配/无匹配/歧义拒绝、A2A/ACP 默认自然语言 intent gateway tests、routing audit payload、`make smoke-7-agents` 真实依赖场景 `intent_routing_checked=true` + ambiguous/unmatched intent rejection | 新 alias/schema hints、置信度阈值或 resolver 特征必须同步 unit test 与真实依赖 smoke；A2A/ACP 不得无目标默认投递到 `default` mailbox。 |
| GOV-14 | MCP resource/tool call 不绕过 Janus policy/budget/audit | P1 | Partial | MCP gateway tests | 缺真实依赖 MCP governance smoke。 |
| GOV-16 | 协议中立 tool invocation audit：Agent、SDK、A2A/ACP、MCP 的 Janus 可见工具型任务必须产生 `tool.invocation_requested/allowed/denied/started/completed/failed` | P0 | Covered | `core.EventToolInvocation*`、Task Envelope `tool_invocation` 字段、HTTP/gRPC/MCP metadata mapping、TaskService/DispatchService unit coverage、`make verify-governance` 真实依赖 Audit API 覆盖 requested/allowed/denied/started/completed/failed、policy deny、NACK/DLQ | Janus 不进入 Agent/MCP tool 内部，也不核算其私有模型成本；MCP 复杂治理 smoke 继续归 GOV-14 P1。 |
| GOV-17 | Policy template 简化配置入口：CLI/SDK/API 生成标准 `policy_rules`，覆盖 agent/team capability、approval、tool、data classification 常见规则 | P0 | Covered | `core.PolicyRuleTemplateRequest` builder tests、Policy Rule HTTP handler/router tests、TaskService source team/tool/target team policy tests、Cross-SDK conformance fixture、CLI unit tests、`make verify-sdk-cli` real API smoke with `policy_template_checked=true` | 新模板必须生成可审计可查询的标准 rule，并继续由 `PolicyService.Evaluate` 执行。 |
| GOV-15 | S3-compatible adapter、KMS、WORM、retention、per-tenant bucket isolation | P2 | Enterprise | Core 只保留 `ArtifactStore` 接口和本地实现 | 不进入 Core GA；文档需持续说明边界。 |

---

## 6. 安全能力

| ID | 能力 | 优先级 | 当前状态 | 已有验证 | GA 缺口 |
| --- | --- | --- | --- | --- | --- |
| SEC-01 | API key bootstrap、create/list/revoke、hash 存储、raw secret 只返回一次 | P0 | Covered | auth manager/handler/SDK/CLI/gateway tests、contract fixture、`make smoke-security` | 新增 API key contract 仍需同步 SDK fixture。 |
| SEC-02 | API key middleware：missing/invalid/revoked key 标准错误 | P0 | Covered | auth tests、SDK error tests、`make smoke-security` | 新增入口必须继续通过同一 auth middleware。 |
| SEC-03 | Tenant guard：API key tenant 不能访问其他 tenant path | P0 | Covered | `TenantGuard` unit tests、`make smoke-security` | HTTP path tenant guard 已覆盖；其他入口统一归入 SEC-06。 |
| SEC-04 | API key rotation：create new、switch client、revoke old、old key denied | P0 | Covered | `make smoke-security` | 需要在 release checklist 中保留运行证据。 |
| SEC-05 | Server TLS 和 mTLS：HTTP/gRPC 同配置，client CA、TLS 1.2+ | P0 | Covered | TLS config tests、Helm config docs、`make smoke-mtls` | 持续保留 release 运行证据。 |
| SEC-06 | Auth 覆盖 HTTP、grpc-gateway、A2A/ACP/MCP、WebSocket 的边界 | P0 | Covered | HTTP auth middleware、native gRPC security interceptor、grpc-gateway auth header matcher、A2A/ACP/MCP/WebSocket tenant guard tests、`make smoke-security`、`make smoke-mtls` | 新入口必须继续接入同一 auth boundary。 |
| SEC-07 | Secret/log redaction：API key、bootstrap key、headers、Task Envelope 中的敏感内容不进入日志 | P0 | Covered | API key list/revoke 不返回 secret、`make smoke-security` 校验 raw API key 和 Task Envelope 敏感内容不进 API log | 持续要求新增日志字段先通过 redaction smoke。 |
| SEC-08 | Tenant data isolation：PostgreSQL query、NATS subject、Redis key、artifact path 均带 tenant boundary | P0 | Covered | repo/handler/service tests、design boundary、NATS subject naming、`make smoke-security` artifact/ContextRef cross-tenant deny、`make verify-ops-chaos` Redis tenant key boundary + NATS tenant stream / lazy consumer verification | 新底层资源命名必须继续以 tenant 为首要 partition。 |
| SEC-09 | Basic audit：关键安全和治理操作可审计 | P0 | Covered | event/audit tests、7-agent trace events、`make verify-security` API key create/revoke、auth failure、tenant guard deny audit events、`make smoke-mtls` mTLS/auth boundary evidence | 新安全入口必须写入 security.* audit event 或在安全 gate 中明确豁免。 |
| SEC-10 | OIDC/SSO/SAML/SCIM、完整 RBAC/ABAC、高级 DLP / PII 检测引擎 | P2 | Enterprise | Core 已提供 DLP hook 接口和 publish/dispatch/context/tool/audit projection 调用点；具体检测/脱敏引擎属于 Enterprise | 不进入 Core GA。 |

---

## 7. SDK、CLI 与开发者体验

| ID | 能力 | 优先级 | 当前状态 | 已有验证 | GA 缺口 |
| --- | --- | --- | --- | --- | --- |
| SDK-01 | Go SDK：tenant-scoped client、API key、tenant/agent list、agent/mailbox/task/dispatch/DLQ/API key/policy template、高层 Worker helper、typed errors | P0 | Covered | Go unit tests、conformance fixtures、worker flow fixture、Worker helper unit tests、`make verify-sdk-cli` real API smoke with `sdk_worker_helper_checked=true`、`policy_template_checked=true` | 继续要求新增方法进入 shared fixture 和 real API smoke。 |
| SDK-02 | Python SDK：与 Go/TS 同 contract，高层 `JanusWorker`、worker lifecycle，typed errors，typed TaskEnvelope governance fields、tenant/agent list、policy template | P0 | Covered | Python unit tests、conformance fixtures、worker flow fixture、JanusWorker unit tests、`make verify-sdk-cli` real API smoke with `sdk_worker_helper_checked=true`、`policy_template_checked=true` | 继续要求新增方法和 envelope 字段进入 shared fixture 和 real API smoke。 |
| SDK-03 | TypeScript SDK：与 Go/Python 同 contract，高层 `JanusWorker`、worker lifecycle，typed errors，typed TaskEnvelope governance fields、tenant/agent list、policy template | P0 | Covered | TypeScript unit tests、conformance fixtures、worker flow fixture、JanusWorker unit tests、`make verify-sdk-cli` real API smoke with `sdk_worker_helper_checked=true`、`policy_template_checked=true` | 继续要求新增方法和 envelope 字段进入 shared fixture 和 real API smoke。 |
| SDK-04 | Cross-SDK conformance：共享 HTTP cases 和 worker flow golden source | P0 | Covered | `sdk/conformance/http_cases.json`、`worker_flow.json`、`make verify` | 每次 API 变更必须先更新 fixture。 |
| SDK-05 | CLI：agent、task、mailbox、DLQ、api-key、policy template、project config、tenant/agent dynamic add、global `--api-key` | P0 | Covered | CLI unit tests、SDK client tests、`make verify-sdk-cli` auth-enabled real API smoke with `policy_template_checked=true` | CLI dashboard 仍归 SDK-06；project config 真实依赖 smoke 后续并入 SDK/CLI gate。 |
| SDK-06 | CLI dashboard：本地 dashboard + WebSocket proxy | P1 | Partial | CLI dashboard tests、static UI | 缺真实 API `/ws` 和 auth-enabled dashboard smoke。 |
| SDK-07 | Interop examples：LangGraph、AutoGen、CrewAI、GitHub Actions | P1 | Covered | example syntax check、docs | 缺至少一个 example 对真实 API 的 smoke 或 recorded run。 |
| SDK-08 | API/SDK migration notes 和 contract audit | P0 | Covered | `docs/Janus-v0.3-migration.md`、`docs/Janus-api-surface-audit.md`、`contract-check` | 继续作为 contract release gate。 |

---

## 8. 运维、观测与发布能力

| ID | 能力 | 优先级 | 当前状态 | 已有验证 | GA 缺口 |
| --- | --- | --- | --- | --- | --- |
| OPS-01 | `/healthz`、`/readyz`：HTTP process 和 PostgreSQL/NATS/Redis readiness 分离 | P0 | Covered | health tests、`make smoke-prod`、`make verify-ops-chaos` Redis/NATS/PostgreSQL stop/start readiness downgrade/recovery | 新依赖必须进入 readiness map 和 chaos gate。 |
| OPS-02 | Prometheus metrics：HTTP/gRPC、publish/pull、ACK/NACK、retry/DLQ、outbox、mailbox、lease、policy、budget、routing | P0 | Covered | metrics tests、Grafana dashboard、`make smoke-prod` HTTP + task lifecycle metric query、`make verify-reliability` ACK/PULL/NACK/retry/DLQ/lease/outbox/mailbox metrics、`make verify-governance` policy/budget/routing metrics、gRPC observability tests | 新指标必须进入 unit test 和至少一个真实依赖 smoke。 |
| OPS-03 | OpenTelemetry OTLP tracing：HTTP/gRPC spans、exporter、shutdown hook、trace headers | P0 | Covered | tracing tests、gRPC observability tests、`make smoke-prod` trace headers + OTLP exporter + Tempo `/api/traces/{traceID}` query + Audit trace correlation | 新入口必须继续产生可由 backend 查询的 trace，并能与 Janus audit/event 关联。 |
| OPS-04 | Structured JSON logs：tenant/task/attempt/trace、后台 worker/scanner/outbox 结构化错误 | P0 | Covered | logutil/observability tests、`make smoke-prod` 404 abnormal HTTP JSON log with tenant/trace、`make verify-ops-chaos` outbox worker JSON error log、`make verify-security` log redaction smoke | 新后台 worker error 必须用 `logutil` 输出结构化字段。 |
| OPS-05 | Grafana dashboard：backlog、online agents、DLQ、latency、errors、policy/budget/routing blocks | P1 | Partial | dashboard JSON `jq`、Grafana provisioning smoke | 缺实际面板查询数据完整性校验。 |
| OPS-06 | Podman/Compose production smoke stack：PostgreSQL、NATS、Redis、Prometheus、Grafana、Tempo、OTel Collector | P0 | Covered | `deployments/smoke-deps.compose.yaml`、`make smoke-prod` | 继续作为本机生产近似验证入口。 |
| OPS-07 | Helm chart：Deployment、Service、ConfigMap、Secret、PDB、probes、resources、TLS、artifact PVC | P0 | Covered | Helm chart templates、migration Job template、`make verify-release-ops` lint/template with migrationJob/TLS/artifact PVC | 真实 Kubernetes cluster install/upgrade/rollback 仍需作为发布环境归档；Core 自动化门禁覆盖 chart 渲染和关键生产 values。 |
| OPS-08 | Migration runbook：auto/controlled migration、schema upgrade | P0 | Covered | runbook、startup migration path、Helm migration Job、`make verify-release-ops` controlled migration rollback drill | 新 schema 变更必须具备 down migration 或明确不可逆说明。 |
| OPS-09 | Backup/restore runbook：PostgreSQL dump/restore、NATS volume snapshot consistency | P0 | Covered | runbook、`make verify-release-ops` PostgreSQL pg_dump/restore sentinel + NATS file-storage restart persistence probe | 生产发布需补充云盘/CSI snapshot 操作记录；Core gate 覆盖逻辑恢复和 NATS 持久化读回。 |
| OPS-10 | Rolling upgrade：多副本 rollout、readiness、outbox/DLQ backlog watch | P0 | Covered | runbook、`make verify-ops-chaos` local two-process rolling restart + outbox backlog drain | Kubernetes rollout/rollback 演练归 OPS-07。 |
| OPS-11 | API/NATS/PostgreSQL/Redis restart chaos | P0 | Covered | `make verify-ops-chaos` API restart、Redis/NATS/PostgreSQL stop/start、NATS outage outbox recovery | Multi-instance rolling restart 归 OPS-10/OPS-12。 |
| OPS-12 | Multi-instance API/outbox worker 并发与 leaderless recovery | P0 | Covered | outbox worker lease unit tests、`make verify-ops-chaos` multi-instance API/outbox worker real dependency smoke | 新后台 worker 必须证明无 leader 也能并发安全。 |
| OPS-13 | Load baseline：1k agents、10k mailboxes、100 task/s、500 event/s、p95 publish/pull < 100ms | P0 | Covered | `make verify-release-ops` real dependency load baseline; GA target run: 1000 agents、10000 mailboxes、1000 tasks、publish p95 23ms、pull p95 75ms、publish 2048/s、estimated events 1872/s | 基线区分 active mailbox prewarm 和 steady-state latency；新性能回归必须保存 JSON 输出。 |
| OPS-14 | 7 天 soak | P0 | Covered | `make beta-soak` explicit long-run entry、`make verify-release-ops` soak profile preflight | 自动化门禁验证 soak profile 可显式运行；正式 release tag 前仍需在发布环境跑满 7 天并归档日志/metrics。 |
| OPS-15 | Artifact persistence：local PVC / configured artifact dir survives restart | P1 | Covered | artifact service tests、Helm PVC config、`make verify-governance` API restart 后 artifact download 验证 | Kubernetes PVC install/rollback 仍归 OPS-07。 |

---

## 9. 当前 GA 阻塞项汇总

截至 2026-06-16，Janus Core 既有 P0 capability 已完成可靠性、协议、安全、SDK/CLI、运维、已知 capability 路由、自然语言 intent -> capability 解析和协议中立 `tool.invocation_*` 审计的自动化验证闭环。

P0 阻塞项：

| ID | 阻塞原因 | 解除条件 |
| --- | --- | --- |
| 无 | 当前无 P0 Core GA 阻塞项。 | 继续保持 `make ga-readiness`、`make verify` 和生产 smoke gate 通过。 |

P1 风险接受与下一版本目标：

| ID | 风险接受 | Owner | 下一版本目标 |
| --- | --- | --- | --- |
| REL-15 | Core GA 接受当前 TTL / expiry scanner 已有单测覆盖，但未把真实依赖 TTL expiry 作为 P0 发布门禁。 | Core Reliability | v1.1 增加 PostgreSQL/NATS-backed TTL expiry smoke，验证 `task.expired` event、audit projection 和 stale delivery cleanup。 |
| REL-16 | Core GA 接受 cancel/replay/block/unblock 已有 service / handler / contract 覆盖，但真实依赖编排回归延后。 | Core Reliability | v1.1 增加 cancel/replay/block/unblock smoke，覆盖 outbox、audit、SDK/CLI 可见状态。 |
| GOV-08 | Core GA 接受 group/human routing 的静态映射已有 routing tests，真实依赖 mapped target 场景不阻塞 P0。 | Core Governance | v1.1 在 `make verify-governance` 增加 group/human mapped mailbox smoke。 |
| GOV-14 | Core GA 接受 MCP tool/resource 已通过 adapter/gateway 单测和 protocol smoke，完整治理 smoke 延后。 | Core Protocol/Governance | v1.1 增加 MCP policy deny、budget throttle、ContextRef classification 和 audit query 的真实依赖 smoke。 |
| SDK-06 | Core GA 接受 CLI dashboard 作为辅助开发体验，不作为生产控制面 P0。 | SDK/CLI | v1.1 增加 auth-enabled `/ws` dashboard smoke 和 WebSocket proxy 回归。 |
| OPS-05 | Core GA 接受 Grafana dashboard 已完成 JSON/provisioning 验证，面板数据完整性截图/查询归发布归档。 | Core Ops | v1.1 增加 dashboard panel query smoke；每个 RC 归档 Grafana 截图与 Prometheus 查询结果。 |

发布归档建议：

1. 在发布 Kubernetes 环境保存 Helm install/upgrade/rollback 记录。
2. 运行 `make beta-soak` 满 7 天并归档日志、Prometheus/Grafana 截图和异常处理记录。
3. 每次 release candidate 运行 `make verify-production` 并保存 JSON 输出。

---

## 10. Enterprise 边界

以下能力明确不作为 Core GA 阻塞项：

| 能力 | Core 边界 | Enterprise 边界 |
| --- | --- | --- |
| 身份 | API key、mTLS、tenant_id 逻辑隔离 | OIDC/SSO/SAML/SCIM、完整 RBAC/ABAC。 |
| 策略 | 基础 policy、approval、budget、classification、routing constraint | OPA/Cedar、策略包版本、dry-run、策略审批流、企业策略 UI。 |
| 数据安全 | ContextRef、ArtifactStore interface、本地 artifact、基础 access scope、DLP hook 接口和关键调用点 | 高级 DLP / PII 检测引擎、KMS、WORM、retention、per-tenant bucket。 |
| 审计 | 基础 audit events、trace、task lifecycle projection | 签名审计、SIEM export、合规报表、incident review。 |
| 运维 | Compose/Helm/runbook、metrics/traces/logs、基础 dashboard | Operator、SLO dashboard、告警包、air-gapped/私有化交付。 |
| 生态 | A2A/ACP/MCP 基础 adapter、SDK/CLI/examples | 商业连接器包、企业 catalog、连接器审批、企业模板。 |