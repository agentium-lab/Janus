# v1.6.3 — Identity Semantics Alignment (eighth-review remediation)

## Security (P0): identity parsing aligned with the decoder

The identity guard's JSON extractor used case-SENSITIVE map keys while Go's
`encoding/json` matches struct fields case-INSENSITIVELY — a body could
declare a victim identity via `Envelope`/`SOURCE_AGENT` variant keys that
the guard never saw. Three-layer fix:

1. The extractor resolves identity paths case-insensitively AND rejects
   any case-variant spelling of identity-related keys (`source_agent`,
   `agent_id`, `envelope`, `metadata`, `id` on registration paths) as
   hostile ambiguity → 400.
2. **Service-layer authority overwrite**: `TaskService.Create` forces both
   the task's top-level and envelope `source_agent` from the principal's
   bound identity whenever present. Persisted identities can never be
   caller-declared, so no future extractor gap can split them.
3. The invariants suite carries the original exploit as a permanent,
   red-verified case.

## Reliability: terminal events never silently dropped

`Publish` now bypasses the internal pipeline via direct fan-out when the
pipeline is full — a terminal event always reaches every subscriber or
evicts the full ones. The pressure regression asserts strongly: with
10/100/1000 full subscribers, the registry must empty and every channel
must observe close.

## A2A boundary

- Version negotiation per spec §3.6.2: empty header interpreted as 0.3 →
  `VersionNotSupportedError`; only exact `1.0` passes; applies to v1 routes
  only (legacy v0.x predate negotiation)
- Error responses carry `google.rpc.ErrorInfo` details so the official
  client surfaces typed errors; served as `application/json` because the
  official v2.0.0 parser only decodes that prefix (verified in source)
- Oversized request bodies get `413 Payload Too Large` — never a silent
  truncation

## Verification

33 Go packages (race), official-client interop suite, invariants suite,
Python + TypeScript SDK tests all green; coverage 85.2% ≥ 85%.
