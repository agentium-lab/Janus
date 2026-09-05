# v1.5.1 — Review Remediation: Reliability, Conformance, Identity, Release Gates

This release remediates every finding from the third-party review of v1.5.0
(composite 6.0/10, production NO-GO). Each fix ships with a regression test
that was **red-verified**: we deliberately re-injected each defect and
confirmed the new test fails before trusting it.

## Fixes

### [P0] PostgreSQL-only mode no longer panics on agent registration
The v1.5.0 release notes claimed this was fixed; it was not — the patch never
landed, and `Register()` called into a typed-nil heartbeat driver. v1.5.1
fixes it three ways: explicit nil-interface injection in `main.go`,
constructor-time typed-nil normalization (`nilguard`), and a defensive guard
in `AgentService.Register`. A process-level integration test boots the real
binary against PostgreSQL with a dead Redis port and registers an agent.

### [P1] Progress events reach subscribers exactly once
v1.5.0 delivered every progress event twice (fast lane + outbox loopback,
no shared identity). `ReportProgress` now stamps one stable EventID carried by
both lanes; the broadcaster dedupes by EventID.

### [P1] SSE terminal-race closed
Streams subscribe **before** reading task state, so a task completing between
the status read and the subscribe can no longer strand the client. Terminal
events block up to 5s for slow subscribers instead of being silently dropped.

### [P1] A2A conformance aligned with the official spec
- `:subscribe` accepts both GET and POST (the v1.0 spec tables and canonical
  proto disagree; the official a2a-go SDK accepts both)
- Subscribing to a terminal task returns `UnsupportedOperationError` (HTTP
  400) per spec §3.1.6/§9.4.6/§10.4.6 — previously it streamed a snapshot
- Streaming messages referencing a terminal task are rejected likewise

### [P1] Agent identity is now bound to credentials
API keys can carry a `bound_agent_id` (migration 016). Bound keys may only
act as their bound agent at every identity-claiming entrypoint (progress,
task create, pull, A2A send/stream) — impersonation returns 403. Unbound
keys behave exactly as before (backward compatible).

### [P1] Release gates are now real
CI now runs: Python + TypeScript SDK tests, the smart-customer-service
scenario suite, the no-Redis PG-only process test, a real PG-only boot with
agent registration (dead Redis port), and an 85% coverage threshold.
The release and publish workflows **require** their test jobs to pass before
any artifact is published.

### [P2] GA self-check and honesty
`docs/` is tracked again — a fresh clone passes `scripts/check_ga_readiness.py`.
The website drops unverified "Production-Grade" claims and no longer
advertises PG-only beyond what CI verifies.

## Verification per fix

| Fix | Regression test | Red-verified |
|---|---|---|
| typed-nil | `service/typed_nil_regression_test.go` + `tests/pgonly` process test | panic reproduced on revert |
| dedup | `handler/broadcaster_dedupe_test.go` | duplicate delivered on revert |
| SSE race | `a2a/gateway_v1_race_test.go` (terminal emitted mid-read) | stream hung on revert |
| A2A 400 | `gateway_v1_race_test.go` | old 200 assertions updated |
| identity | `handler/identity_binding_test.go` | 403 became 202 on revert |
| CI gates | workflows carry test jobs; release `needs: verify` | — |
