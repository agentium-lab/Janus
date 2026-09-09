# v1.6.4 — Hardening & Protocol Conformance (ninth-review remediation)

This release completes the ninth-review remediation cycle, hardening the
identity boundary, event delivery guarantees, and A2A v1.0 conformance.

## Security (P0): identity parsing hardened against case-variant attacks

The identity guard's JSON extractor now matches identity keys case-insensitively
(mirroring `encoding/json` semantics) and rejects any case-variant spelling of
identity-related keys (`source_agent`, `agent_id`, `envelope`, `metadata`,
`id` on registration paths) as hostile ambiguity → 400. The service layer
(`TaskService.Create`) now authoritatively overwrites both top-level and
envelope `source_agent` from the authenticated principal, so even a future
extractor gap cannot split identities.

## Reliability: terminal events never silently dropped

`Publish` now bypasses the internal pipeline when the queue is full,
delivering terminal events directly to subscribers; full subscribers are
evicted immediately instead of being waited on for 5 seconds. A pressure test
asserts the registry empties and every channel observes close.

## A2A v1.0 conformance

- Version negotiation per spec §3.6.2: empty header = 0.3 → rejected; only exact `1.0` passes; applies only to v1 routes (legacy v0.x unaffected)
- Error responses carry `google.rpc.ErrorInfo` details so the official client maps typed errors; error responses served as `application/json` (official v2.0.0 parser only decodes that prefix), successes use `application/a2a+json`
- Oversized request bodies now return `413 Payload Too Large` — no silent truncation
- CancelTask returns the bare Task object (canonical proto); not-cancelable tasks return `TASK_NOT_CANCELABLE` (400) with `ErrorInfo`; real internal errors stay 500
- `A2A-Version` accepted via URL query parameter per spec
- `message:send` success responses use `application/a2a+json`; errors use `application/json` (official v2.0.0 parser only accepts that)

## Verification

- 33 Go packages (race), Python 15, TypeScript 23, official-client interop suite, invariants suite — all green
- `make verify` passes with coverage 85.2% ≥ 85%
- GA readiness check passes (matrix aligns with reality)

## Upgrade notes

- `message:send` with a `taskId` that cannot be continued now returns `400 UNSUPPORTED_OPERATION` instead of silently creating an unrelated task.
- `CancelTask` on a terminal task returns `400 TASK_NOT_CANCELABLE` with `ErrorInfo` (previously 500).
- `A2A-Version` header: empty → 0.3 (rejected); only exact `1.0` accepted. Legacy v0.x routes unaffected.
