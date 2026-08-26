# v1.2.1 — PostgreSQL-only, identity scopes, protocol conformance

**Highlights**

- 🆕 **PostgreSQL-only mode** — run the full broker with just Postgres. A PG-backed
  `QueueDriver` (FOR UPDATE SKIP LOCKED claims, lease-column reservations) sits behind
  the existing driver interface; NATS becomes an optional accelerator.
  `JANUS_QUEUE_DRIVER=pg docker compose up`
- 🔐 **API-key scopes & revocation** — keys carry `admin / task:write / task:read /
  audit:read` scopes with route enforcement; revoked keys fail on next request;
  approval decisions now commit atomically with task transitions.
- 🛡 **Identity hardening** — server-side source_agent ownership checks, admin-gated
  governance endpoints, non-authoritative acting-user attribution on audit events.

**Protocol conformance**

- A2A: well-known agent card discovery (`/.well-known/agent.json`), standard
  `message/send`, `tasks/get`, `tasks/cancel`.
- MCP: JSON-RPC surface on `/mcp` — initialize handshake, tools/list,
  tools/call (content-block results), ping, resources/list.
- gRPC now supports TLS; `/readyz` reports postgres/nats/redis individually.

**First-class SDK releases**

| Registry | Package |
|---|---|
| PyPI | [`janus-broker`](https://pypi.org/project/janus-broker/) |
| npm | [`@agentium-lab/janus-sdk`](https://www.npmjs.com/package/@agentium-lab/janus-sdk) |
| Go | `go get github.com/agentium-lab/Janus/sdk/go` |

Both published via OIDC trusted publishing with provenance attestations.

**Platform**

- CI restored and hardened: retried dependency pulls (Docker Hub → ECR fallback),
  TCP health probes with failure log dumps, weekly CodeQL.
- Container images published to `ghcr.io/agentium-lab/janus-core` (amd64+arm64) —
  the Helm chart's missing dependency finally ships.
- ACP gateway deprecated in favor of A2A (RFC 8594 headers, Sunset 2026-12-31).

**Full changelog**: https://github.com/agentium-lab/Janus/compare/v1.2.0...v1.2.1

> Positioning note: we're calling this a **beta** and actively looking for early
> adopters — especially teams that want broker-grade delivery guarantees without
> standing up a Kafka-class stack.
