# v1.6.2 — Invariant Enforcement (seventh-review remediation)

Every fix in this release is enforced as an invariant with a table-driven
test that was red-verified (deliberately broken, confirmed failing, fixed).

## Security: identity invariant enforced at the chokepoints

Agent identity binding is no longer distributed across handlers that must
remember to check — it is enforced at the two unavoidable chokepoints, with
a completeness self-check that makes new entrypoints fail CI until they
declare their identity semantics:

- HTTP: `AgentIdentityMiddleware` inside the authenticated chain, ahead of
  every handler of every protocol (REST, A2A v1 + legacy, ACP, MCP, WS)
- gRPC: identity check inside the auth interceptor with an explicit
  method→field map; a test enumerates all 24 methods of all services and
  fails on any undeclared method
- Closes all five seventh-review gaps as a side effect: MCP both paths,
  gRPC PullTask/UpdateAgent, legacy A2A agent/card, and the top-level vs
  envelope identity split (now path-aware, both carriers checked)

## Behavior: validation before side effects (A2A)

`message:send` and `message:stream` now validate a referenced taskId
(existence, terminal state, contextId consistency) BEFORE any task is
created or enqueued — v1.6.1 could create-and-enqueue a new task and then
reject the request. **Behavior change**: `message:send` with a taskId that
cannot be continued now returns 400 instead of silently creating an
unrelated task. Proven by seven zero-side-effect scenarios (creation
counter must stay at zero).

## Reliability: fan-out time no longer scales with slow subscribers

Terminal events are delivered non-blocking; a subscriber whose queue is
full is evicted immediately instead of being waited on for 5 seconds.
One terminal broadcast to N full subscribers is bounded regardless of N
(pressure regression covers 10/100/1000). Evicted clients recover via
GetTask.

## Contract fixes

- TS SDK `publishTask` unwraps the real `CreateTaskResponse` `{id,status,task}`
  envelope; all SDK test fixtures now use the real server response shape
- A2A v1 responses uniformly use `application/a2a+json`; Agent Card skill
  carries the spec-required `tags`; ListTasks returns `totalSize`
- GA capability matrix now describes what `make verify` actually runs and
  names CI as the source of truth for SDK/interop/invariant gates

## Verification

33 Go packages (race), Python 15, TypeScript 23, official-client interop
suite, and the invariants suite all green; `make verify` enforces coverage
86% ≥ 85%.
