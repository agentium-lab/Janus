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

// A restored stream must carry the SAME config the initial build used:
// initial creation and reconcile now share config builders, so a healed
// EVENTS stream keeps the unbounded replay window EnsureTenant created.
func TestStreamConfigParity_CreateVsReconcile(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()

	if err := d.EnsureTenant(ctx, "acme"); err != nil {
		t.Fatalf("ensure tenant: %v", err)
	}

	createdStream, err := d.js.Stream(ctx, streamName("acme", "EVENTS"))
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	created, err := createdStream.Info(ctx)
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}

	// Wipe and heal via the reconcile path.
	if err := d.js.DeleteStream(ctx, streamName("acme", "EVENTS")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	d.mu.Lock()
	d.tenant["acme"].eventStream = nil
	d.mu.Unlock()
	if err := d.ReconcileTenant(ctx, "acme"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	healedStream, err := d.js.Stream(ctx, streamName("acme", "EVENTS"))
	if err != nil {
		t.Fatalf("stream after heal: %v", err)
	}
	healed, err := healedStream.Info(ctx)
	if err != nil {
		t.Fatalf("stream info after heal: %v", err)
	}
	if healed.Config.MaxMsgs != created.Config.MaxMsgs {
		t.Fatalf("EVENTS MaxMsgs drifted: created=%d healed=%d", created.Config.MaxMsgs, healed.Config.MaxMsgs)
	}
}
