# Roadmap

Status reflects what ships, not aspirations. Security-relevant work runs ahead of features.

## Now
- **Trust**: beta positioning, live CI badges, public ADRs (this repo), ACP gateway deprecation
- **Identity model**: API-key scopes (`admin`/`task:write`/`task:read`/`audit:read`), immediate revocation, approval decisions committed atomically with task transitions

## Next
- **PostgreSQL-only mode**: PG-backed QueueDriver behind the existing QueueEventDriver interface — run Janus with a single strong dependency (see [ADR-0001](docs/adr/0001-dual-write.md))
- **Enterprise authd**: OIDC workload identity, RBAC, scoped admin forwarding (Bundle A)
- ~~In-task streaming (SSE)~~ ✅ shipped
- **Envelope signing** (ADR pending): tamper-evident audit chain

## Later
- In-task streaming (SSE semantics over the broker)
- MCP tools/list exported dynamically from the capability catalog
- LangGraph deep adapter (after the generic MCP integration matures)

## Enterprise (separate distribution)
audit-exporter (SIEM/S3) · web console

See also: [SECURITY.md](SECURITY.md) · janusa2a.com/docs
