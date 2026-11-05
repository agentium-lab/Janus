package recovery

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
	natsdrv "github.com/agentium-lab/Janus/server/internal/driver/nats"
)

// GA-10 stock-recovery leg: a queued task whose NATS message was lost (the
// PG row is the surviving truth — the state after a TASKS-stream wipe) must
// be RE-PUBLISHED by the TaskScanner and consumed through the live rebuilt
// consumer, and the recovered delivery must ACK. Composes with B8 (stream
// deletion -> reconcile rebuilds the consumer) and the service-level
// pull->start->ack chains: this test pins the scanner's republish leg
// against the REAL queue, not the mock.
func TestTaskScanner_RecoveryAfterMessageLoss_RealNATS_PG(t *testing.T) {
	pool := openScannerTestDB(t)
	ctx := context.Background()

	natsURL := os.Getenv("JANUS_NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}
	// Unique tenant per run: the scanner republishes with a stable dedupe
	// key (recovery:<tenant>:<task>) that JetStream silently drops on
	// re-publish within the stream's dedupe window — re-running against a
	// reused tenant would publish into a dedupe black hole.
	tenant := fmt.Sprintf("scanrec_%d", time.Now().UnixNano())
	const mb = "mb-scanloss"
	d, err := natsdrv.NewDriver(natsdrv.Config{URL: natsURL})
	if err != nil {
		t.Skipf("nats not reachable at %s (set JANUS_NATS_URL to enable): %v", natsURL, err)
	}
	defer d.Close()
	nctx := natsdrv.ContextWithTenant(ctx, tenant)

	_, err = pool.Exec(ctx, `INSERT INTO tenants (id, name) VALUES ($1, 'Scanner NATS Recovery Test')`, tenant)
	require.NoError(t, err)

	// The surviving PG truth: a queued task old enough for the scanner,
	// with mailbox routing and a valid envelope — and NO message anywhere.
	tid := "scanloss-task-1"
	env, err := json.Marshal(core.TaskEnvelope{
		TaskID: tid, TenantID: tenant, SourceAgent: "src-agent",
		Target: core.Target{Type: "mailbox", Value: mb},
	})
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`INSERT INTO tasks (tenant_id, id, source_agent, target_type, target_value, mailbox_id, status, priority, envelope, created_at)
		 VALUES ($1, $2, 'src-agent', 'mailbox', $3, $3, 'queued', 'normal', $4, now() - interval '1 hour')`,
		tenant, tid, mb, string(env))
	require.NoError(t, err)

	// The live consumer the recovered message must flow through.
	require.NoError(t, d.EnsureTenant(nctx, tenant))
	require.NoError(t, d.EnsureMailbox(nctx, core.MailboxSpec{
		TenantID: tenant, MailboxID: mb, AgentID: "scan-agent",
		MaxConcurrency: 1, ACKWaitSeconds: 30, MaxDeliver: 3, RetentionSeconds: 3600,
	}))
	require.NoError(t, d.EnsureConsumer(nctx, core.ConsumerSpec{
		TenantID: tenant, MailboxID: mb, ACKWaitSeconds: 30, MaxDeliver: 3, MaxACKPending: 10,
	}))

	// Nothing delivers before the scan: the loss state is real.
	empty, err := d.FetchTasks(nctx, tenant, mb, core.FetchOptions{MaxMessages: 1, WaitTime: 300 * time.Millisecond})
	require.NoError(t, err)
	assert.Empty(t, empty, "pre-scan pull must find nothing (message was lost)")

	scanner := &TaskScanner{
		pool: pool, queue: d, interval: time.Second,
		minAge: 5 * time.Minute, stopCh: make(chan struct{}),
	}
	scanner.scan(nctx)

	// Given the scan, the recovered message delivers through the live
	// consumer and the delivery ACKs.
	var delivery *core.TaskDelivery
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		got, ferr := d.FetchTasks(nctx, tenant, mb, core.FetchOptions{MaxMessages: 1, WaitTime: time.Second})
		if ferr == nil && len(got) > 0 {
			delivery = &got[0]
			break
		}
	}
	require.NotNil(t, delivery, "scanner republish must deliver the lost task through the live consumer")
	assert.Equal(t, tid, delivery.TaskID, "delivered task must be the recovered PG row")
	require.NoError(t, d.AckTask(nctx, tenant, delivery.DeliveryRef), "recovered delivery must ACK")
}
