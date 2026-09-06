# v1.6.0 — Security Boundary + A2A Conformance (sixth-review remediation)

Minor bump: contains a P0 security fix and externally visible A2A behavior
changes. Every fix ships with a regression test that was red-verified
(deliberately re-injecting the defect and confirming the test fails).

## Security

### [P0] Agent identity is now bound to credentials at EVERY entrypoint
An API key can carry a `bound_agent_id`; a bound key may only act as its own
agent everywhere an agent identity is claimed — legacy A2A `task/send` and
`jsonrpc`, ACP register/run, HTTP agent register + heartbeat, gRPC
RegisterAgent/Heartbeat/CreateTask, task create, pull, progress, and the A2A
v1 routes. Impersonation returns 403. Unbound keys behave exactly as before.

## A2A v1.0 conformance (still "v1 subset" — multi-turn continuation pending)

- **ListTasks** (`GET /a2a/tasks`): cursor-paginated, newest-first
- **A2A-Version negotiation**: empty/`1.0` accepted; anything else returns
  `VersionNotSupportedError` (400)
- **CancelTask** now returns the updated Task object (canonical proto), not a
  statusUpdate
- **Terminal-task subscribe** returns `UnsupportedOperationError` (400)
- **taskId references** are validated: unknown task → 400, terminal task →
  400, contextId mismatch → 400, live-task continuation honestly rejected as
  not-yet-supported (previously it silently created an unrelated new task)
- Responses use `application/a2a+json`

## Reliability

- **Progress dedup**: one stable EventID across the fast lane and the outbox
  loopback; the broadcaster dedupes within a bounded window (delivery
  semantics: at-least-once with near-duplicate suppression)
- **SSE/WS race closed**: streams subscribe BEFORE reading status, and
  `message:stream` rechecks authoritatively after subscribing
- **Slow subscribers evicted**: a client that cannot accept a terminal event
  within 5s is closed and evicted instead of accumulating head-of-line delay
- **PG-only heartbeat fixed**: the durable PG heartbeat record is written
  even when Redis is absent (previously skipped entirely)

## Engineering evidence

- TS SDK now has a real test suite (23 `node:test` cases over a real HTTP
  server) — previously `npm test` compiled nothing
- `make coverage` threshold is genuinely enforced at 85% (the shell fallback
  previously swallowed the default)
- PG-only CI guard runs as a validated, locally-replayable script
  (`scripts/pgonly_guard.sh`): boots the real binary with a dead Redis port,
  registers an agent, publishes a task — with real error output
