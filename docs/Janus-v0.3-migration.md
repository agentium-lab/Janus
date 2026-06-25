# Janus v0.3 Migration Guide

This document describes the breaking changes and migration steps for consumers
upgrading from Janus v0.1/v0.2 to v0.3 (API / SDK Contract Beta).

## Summary of changes

| Area | Change | Impact |
| --- | --- | --- |
| **Error envelope** | HTTP errors now return `{error, code, message, status}` (was `{error}` only) | SDKs must parse the new fields; old `error` field preserved |
| **Dispatch lifecycle** | `start`/`heartbeat`/`ack`/`nack` require `attempt` parameter | SDK calls must pass `attempt` from `PullResult.lease.attempt` |
| **Error codes** | Canonical `ErrorCode` enum shared across HTTP/gRPC/SDK | SDKs expose typed `APIError`/`JanusAPIError` |
| **gRPC services** | Added `MailboxService` and `DLQService` gRPC + gateway | All stable routes now have proto annotations |
| **Pull response** | Empty queue returns `204 No Content` with empty body | SDK pull returns `nil`/`None`/`null` |
| **Idempotent create** | Duplicate task POST returns `200 OK` (not `201 Created`) | SDKs handle dedup transparently |
| **mTLS** | Optional TLS + client cert verification | Set `tls.enabled` + cert paths in config |
| **AckRequest** | ACK now carries `token_usage` for budget settlement | Pass usage from task execution |

## SDK changes

### Go SDK

**Attempt-aware lifecycle (breaking):**

```go
// OLD (v0.2):
client.StartTask(ctx, taskID, leaseID)
client.Heartbeat(ctx, taskID, leaseID)
client.AckTask(ctx, taskID, janus.AckRequest{LeaseID: leaseID})

// NEW (v0.3):
result, _ := client.PullTask(ctx, mailboxID, agentID)
attempt := result.Lease.Attempt
client.StartTask(ctx, taskID, attempt, leaseID)
client.Heartbeat(ctx, taskID, attempt, leaseID)
client.AckTask(ctx, taskID, janus.AckRequest{
    LeaseID: result.Lease.LeaseID,
    Attempt: attempt,
    ResultRef: "result://...",
})
```

**New: typed APIError:**

```go
err := client.GetTask(ctx, "nonexistent")
if apiErr, ok := err.(*janus.APIError); ok {
    fmt.Println(apiErr.Code)     // "NOT_FOUND"
    fmt.Println(apiErr.StatusCode) // 404
}
```

**New: JanusWorker helper:**

```go
worker := janus.NewJanusWorker(client, janus.WorkerConfig{
    AgentID: "my-agent", MailboxID: "my-mailbox",
})
worker.Run(ctx, func(ctx context.Context, task *core.Task, agentID string) (string, *core.TokenUsage, error) {
    // Process task...
    return "result://ok", &core.TokenUsage{TotalTokens: 100}, nil
})
```

**New: API Key authentication:**

```go
client := janus.NewClient(janus.Config{
    BaseURL: "https://janus.example.com",
    TenantID: "acme",
    APIKey: "janus_xxx",  // NEW: sent as X-API-Key header
})
```

### Python SDK

**New: API Key + typed error:**

```python
client = JanusClient(base_url="...", tenant_id="acme", api_key="janus_xxx")

try:
    client.get_task("nonexistent")
except JanusAPIError as e:
    print(e.code)     # "NOT_FOUND"
    print(e.status)   # 404
```

**New: JanusWorker:**

```python
worker = JanusWorker(client, agent_id="my-agent", mailbox_id="my-mailbox")
worker.run(handler=lambda task: ("result://ok", {"total_tokens": 100}))
```

### TypeScript SDK (new in v0.3)

```typescript
import { Client, JanusAPIError, JanusWorker } from "@janus/sdk";

const client = new Client({
  baseURL: "https://janus.example.com",
  tenantID: "acme",
  apiKey: "janus_xxx",
});

try {
  await client.getTask("nonexistent");
} catch (e) {
  if (e instanceof JanusAPIError) {
    console.log(e.code, e.statusCode); // "NOT_FOUND" 404
  }
}
```

## Server configuration changes

### TLS (optional)

```yaml
tls:
  enabled: true
  cert_file: /etc/janus/server.crt
  key_file: /etc/janus/server.key
  client_ca_file: /etc/janus/ca.crt  # set for mTLS
```

Environment variables: `JANUS_TLS_ENABLED`, `JANUS_TLS_CERT_FILE`,
`JANUS_TLS_KEY_FILE`, `JANUS_TLS_CLIENT_CA_FILE`.

## gRPC service additions

New gRPC services registered:

- `MailboxService`: CreateMailbox, GetMailbox, UpdateMailbox, PauseMailbox, ResumeMailbox
- `DLQService`: QueryDLQ, ReplayDLQ, DiscardDLQ

All are accessible via grpc-gateway at `/grpc/` mount point with the same REST
paths as the native HTTP handlers.

## Migration checklist

- [ ] Update SDK to pass `attempt` in dispatch lifecycle calls
- [ ] Handle `204 No Content` on empty pull (return null/nil)
- [ ] Handle `200 OK` on duplicate task create (idempotency)
- [ ] Parse new error envelope fields (`code`, `status`) for typed errors
- [ ] Set `X-API-Key` header if API key auth is enabled
- [ ] Configure TLS if needed
