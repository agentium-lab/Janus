package nats

import (
	"context"
	"testing"

	"github.com/agentium-lab/Janus/core"
)

func dlqTestMailboxSpec(tenantID, mailboxID string) core.MailboxSpec {
	return core.MailboxSpec{TenantID: tenantID, MailboxID: mailboxID, AgentID: "a1"}
}

// A DLQ stream deleted on the broker must be recreated by
// ReconcileMailboxDLQ even after EnsureMailbox cached it — the cache alone
// would leave dead-letter publishes failing forever.
func TestReconcileMailboxDLQ_RecreatesDeletedStream(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()

	if err := d.EnsureTenant(ctx, "acme"); err != nil {
		t.Fatalf("ensure tenant: %v", err)
	}
	if err := d.EnsureMailbox(ctx, dlqTestMailboxSpec("acme", "mb-1")); err != nil {
		t.Fatalf("ensure mailbox: %v", err)
	}

	name := streamName("acme", "DLQ_"+sanitize("mb-1"))
	if err := d.js.DeleteStream(ctx, name); err != nil {
		t.Fatalf("delete stream: %v", err)
	}

	if err := d.ReconcileMailboxDLQ(ctx, "acme", "mb-1"); err != nil {
		t.Fatalf("reconcile dlq: %v", err)
	}
	if _, err := d.js.Stream(ctx, name); err != nil {
		t.Fatalf("DLQ stream not restored: %v", err)
	}
	d.mu.Lock()
	_, cached := d.tenant["acme"].dlqStreams["mb-1"]
	d.mu.Unlock()
	if !cached {
		t.Fatal("DLQ cache entry must be refreshed")
	}
}

// An EXISTING healthy DLQ stream is left untouched.
func TestReconcileMailboxDLQ_HealthyStreamUntouched(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()

	if err := d.EnsureTenant(ctx, "acme"); err != nil {
		t.Fatalf("ensure tenant: %v", err)
	}
	if err := d.EnsureMailbox(ctx, dlqTestMailboxSpec("acme", "mb-1")); err != nil {
		t.Fatalf("ensure mailbox: %v", err)
	}
	if err := d.ReconcileMailboxDLQ(ctx, "acme", "mb-1"); err != nil {
		t.Fatalf("reconcile dlq: %v", err)
	}
}
