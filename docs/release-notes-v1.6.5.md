# v1.6.5 — Honest Delivery Semantics + Typed Errors (tenth-review remediation)

## Honesty first: delivery guarantees accurately stated

The previous release notes and README overstated delivery semantics. The
actual implementation uses bounded-wait hand-offs (5s) with drop-on-timeout,
not zero-loss guarantees. Documentation now states the real contract:

> Task state is always durable in PostgreSQL. Live notifications are
> best-effort — clients should treat the stream as an optimization and poll
> `GET /tasks/{id}` for the authoritative terminal state.

## Engineering hardening

- **EventProjector retry**: audit projection writes now retry 3× with backoff;
  persistent failures log at error level (visible for alerting)
- **pgqueue terminal timeout returns error**: the outbox publisher retries
  (the outbox row stays pending until successful hand-off) instead of
  silently returning nil while the event was lost
- **NATS terminal timeout visible**: error-level log with event ID and type —
  the loss is no longer silent

## Typed errors

`isNotCancelableErr` replaced fragile `strings.Contains` matching with a
sentinel error chain (`core.ErrTaskInTerminalState` /
`core.ErrInvalidTransition` wrapped via `%w`, checked via `errors.Is`).
Cancel grading can no longer silently break when an error message changes.

## Negative-path test

New test verifies the blocked-downstream scenario: when a consumer stops
draining, terminal events are dropped after the timeout (visibly, not
silently) — proving the honest contract under real backpressure.

## Verification

33 Go packages (race), Python 15, TypeScript 23, official-client interop
suite, invariants suite — all green. `make verify` 85.4% ≥ 85%.
