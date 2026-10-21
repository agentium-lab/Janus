package nats

// B6 pin: AckWait redelivery semantics on a REAL nats-server with a SHORT
// AckWait. Replica A fetches a delivery and DIES (connection closed, no
// ack) -> the durable pull consumer keeps the delivery ack-pending and, once
// AckWait elapses, hands it to ANY waiting fetch -> replica B (a separate
// driver handle with its own empty pending map) fetches the SAME task as a
// redelivery and settles it -> the broker never redelivers again, so the
// task cannot be processed twice from the queue. The contract text lives on
// AckTask in driver.go.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

// fetchUntilDelivery polls FetchTasks in short bounded windows until a
// delivery arrives or the timeout elapses. Each FetchTasks call blocks
// server-side for wait per attempt (the observable), so the loop contains
// no fixed sleeps.
func fetchUntilDelivery(t *testing.T, d *Driver, ctx context.Context, tn, mb string, timeout time.Duration) []core.TaskDelivery {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		deliveries, err := d.FetchTasks(ctx, tn, mb, core.FetchOptions{MaxMessages: 1, WaitTime: 500 * time.Millisecond})
		require.NoError(t, err)
		if len(deliveries) > 0 {
			return deliveries
		}
	}
	return nil
}

func TestB6_AckWaitRedelivery_ShortAckWaitDeathAndSettle(t *testing.T) {
	dA := openDriver(t) // starts (or reuses) the real nats-server; sets natsURL
	ctx := ContextWithTenant(context.Background(), genTenant(t))
	tn := tenantFromCtx(ctx)
	mb := "b6_short_ackwait"

	ensure := func(d *Driver) {
		require.NoError(t, d.EnsureTenant(ctx, tn))
		require.NoError(t, d.EnsureMailbox(ctx, core.MailboxSpec{
			TenantID: tn, MailboxID: mb, AgentID: "b6-agent",
			MaxConcurrency: 1, ACKWaitSeconds: 1, MaxDeliver: 3, RetentionSeconds: 3600,
		}))
		require.NoError(t, d.EnsureConsumer(ctx, core.ConsumerSpec{
			TenantID: tn, MailboxID: mb, ACKWaitSeconds: 1, MaxDeliver: 3, MaxACKPending: 10,
		}))
	}
	ensure(dA)

	// Cheap max_deliver interplay pin: the mailbox's business MaxDeliver (3)
	// must NOT become the broker's redelivery ceiling — the runaway
	// backstop floors it at 100, leaving PG RetryPolicy the authority.
	cons, err := dA.js.Consumer(ctx, streamName(tn, "TASKS"), consumerName(tn, mb))
	require.NoError(t, err)
	info, err := cons.Info(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1*time.Second, info.Config.AckWait, "short AckWait must reach the broker consumer")
	assert.Equal(t, 100, info.Config.MaxDeliver, "MaxDeliver is the runaway backstop (floor 100), not the business retry budget")

	require.NoError(t, dA.PublishTask(ctx, core.TaskMessage{
		TenantID: tn, MailboxID: mb, TaskID: "task-b6-redelivery",
		Priority: core.PriorityNormal, Payload: []byte(`{"b6":true}`),
	}))

	// Replica A fetches the delivery...
	first := fetchUntilDelivery(t, dA, ctx, tn, mb, 5*time.Second)
	require.Len(t, first, 1, "replica A must fetch the delivery")
	assert.Equal(t, "task-b6-redelivery", first[0].TaskID)
	assert.Equal(t, 0, first[0].RedeliveryCount, "first delivery is not a redelivery")
	require.True(t, strings.HasPrefix(string(first[0].DeliveryRef), "$JS.ACK."),
		"refs must carry the portable ack subject, got %s", first[0].DeliveryRef)

	// ...then DIES without acking: closing the connection settles nothing.
	require.NoError(t, dA.Close())

	// Replica B — a separate driver handle (empty pending map) against the
	// same broker — ensures the same tenant and pulls. The un-acked delivery
	// only becomes fetchable once AckWait (1s) elapses; the expecting-fetch
	// loop absorbs that wait server-side.
	dB, err := NewDriver(Config{URL: natsURL})
	require.NoError(t, err)
	t.Cleanup(func() { dB.Close() })
	ensure(dB)

	redelivered := fetchUntilDelivery(t, dB, ctx, tn, mb, 8*time.Second)
	require.Len(t, redelivered, 1, "after AckWait the dead replica's delivery must be redelivered to replica B")
	assert.Equal(t, "task-b6-redelivery", redelivered[0].TaskID, "the SAME task is redelivered")
	assert.Equal(t, 1, redelivered[0].RedeliveryCount, "second delivery must report exactly one redelivery")
	require.True(t, strings.HasPrefix(string(redelivered[0].DeliveryRef), "$JS.ACK."))

	// Replica B settles the delivery.
	require.NoError(t, dB.AckTask(ctx, tn, redelivered[0].DeliveryRef))

	// Settlement is the duplicate-processing fence: ack-pending drains to
	// zero and stays there...
	require.NoError(t, withDeadlineNATS(func() error {
		return assertAckPendingZero(t, dB, tn, mb)
	}))
	c, err := dB.js.Consumer(ctx, streamName(tn, "TASKS"), consumerName(tn, mb))
	require.NoError(t, err)
	info, err = c.Info(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 0, info.NumPending, "no message may remain pending for the consumer after settle")

	// ...and a fetch window LONGER than AckWait sees no further redelivery
	// (an unsettled delivery would come back within AckWait).
	quiet, err := dB.FetchTasks(ctx, tn, mb, core.FetchOptions{MaxMessages: 1, WaitTime: 2 * time.Second})
	require.NoError(t, err)
	assert.Len(t, quiet, 0, "a settled delivery must never be redelivered — no duplicate processing")
}
