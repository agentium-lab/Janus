package nats

// B7 broker-stream-loss pin: the tenant's EVENTS JetStream stream is
// deleted (storage wiped / stream lost). EnsureTenant CANNOT heal it (the
// in-process cache is a hit, so it no-ops) — ReconcileTenant recreates the
// stream, after which event publishing and event replay work again. The
// TASKS stream and its durable consumer are untouched by an EVENTS loss:
// a held (fetched-but-unacked) delivery still settles, and queued tasks
// stay deliverable across the outage. Pre-loss event history inside the
// deleted stream is gone — the EVENTS stream is a projection window, PG
// audit remains the source of truth; recovery heals the PATH, not the
// lost history.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/agentium-lab/Janus/core"
)

func replayEventIDs(t *testing.T, d *Driver, ctx context.Context, tn string) []string {
	t.Helper()
	iter, err := d.ReplayEvents(ctx, core.EventReplayFilter{TenantID: tn})
	require.NoError(t, err)
	defer iter.Close()
	var ids []string
	for {
		ev, nerr := iter.Next(ctx)
		require.NoError(t, nerr)
		if ev == nil {
			break
		}
		ids = append(ids, ev.EventID)
	}
	return ids
}

func TestB7_EventsStreamLoss_EnsureNoOp_ReconcileHeals_WorkStaysDeliverable(t *testing.T) {
	d := openDriver(t)
	ctx := ContextWithTenant(context.Background(), genTenant(t))
	tn := tenantFromCtx(ctx)
	mb := "b7_loss_mb"

	require.NoError(t, d.EnsureTenant(ctx, tn))
	require.NoError(t, d.EnsureMailbox(ctx, core.MailboxSpec{
		TenantID: tn, MailboxID: mb, AgentID: "b7-agent",
		MaxConcurrency: 1, ACKWaitSeconds: 30, MaxDeliver: 3, RetentionSeconds: 3600,
	}))
	require.NoError(t, d.EnsureConsumer(ctx, core.ConsumerSpec{
		TenantID: tn, MailboxID: mb, ACKWaitSeconds: 30, MaxDeliver: 3, MaxACKPending: 10,
	}))

	// Pre-loss event history: both writer generations' event-ID formats for
	// the SAME task (old: no generation suffix; new: -g<replay_count>).
	oldID := fmt.Sprintf("obx-exp-%d-%s-task-b7", len(tn), tn)
	newID := fmt.Sprintf("obx-exp-%d-%s-task-b7-g1", len(tn), tn)
	for _, id := range []string{oldID, newID} {
		require.NoError(t, d.PublishEvent(ctx, core.JanusEvent{
			EventID: id, EventType: core.EventTaskExpired, TenantID: tn, TaskID: "task-b7",
			Payload: []byte(`{}`), Timestamp: time.Now().UTC(),
		}))
	}

	// Queued work + a held (un-acked) delivery that must survive the loss.
	require.NoError(t, d.PublishTask(ctx, core.TaskMessage{
		TenantID: tn, MailboxID: mb, TaskID: "task-b7-q1",
		Priority: core.PriorityNormal, Payload: []byte("queued-before-loss"),
	}))
	held := fetchUntilDelivery(t, d, ctx, tn, mb, 5*time.Second)
	require.Len(t, held, 1, "queued task must be fetchable before the loss")

	// The events stream is LOST.
	require.NoError(t, d.js.DeleteStream(ctx, streamName(tn, "EVENTS")))

	// Symptom: event publishing fails while the stream is gone.
	err := d.PublishEvent(ctx, core.JanusEvent{
		EventID: "wont-land", EventType: core.EventTaskExpired, TenantID: tn, TaskID: "task-b7",
	})
	assert.Error(t, err, "publishing into a deleted stream must fail, not silently drop")

	// EnsureTenant is a cache-hit no-op — it must NOT report healing.
	require.NoError(t, d.EnsureTenant(ctx, tn))
	_, serr := d.js.Stream(ctx, streamName(tn, "EVENTS"))
	assert.Error(t, serr, "EnsureTenant must not recreate a lost stream (cache hit); only ReconcileTenant heals")

	// The reconcile/ensure-recovery path recreates it.
	require.NoError(t, d.ReconcileTenant(ctx, tn))
	healed, herr := d.js.Stream(ctx, streamName(tn, "EVENTS"))
	require.NoError(t, herr, "ReconcileTenant must recreate the lost EVENTS stream")
	info, ierr := healed.Info(ctx)
	require.NoError(t, ierr)
	assert.Equal(t, jetstream.LimitsPolicy, info.Config.Retention, "recreated stream must match the shared stream config")
	assert.Equal(t, 30*24*time.Hour, info.Config.MaxAge)

	// Publishing works again — and BOTH event-ID generations coexist on the
	// rebuilt stream with no dedupe collision (distinct Nats-Msg-Ids).
	postOld := fmt.Sprintf("obx-exp-%d-%s-task-b7-post", len(tn), tn)
	postNew := fmt.Sprintf("obx-exp-%d-%s-task-b7-post-g0", len(tn), tn)
	for _, id := range []string{postOld, postNew} {
		require.NoError(t, d.PublishEvent(ctx, core.JanusEvent{
			EventID: id, EventType: core.EventTaskExpired, TenantID: tn, TaskID: "task-b7",
			Payload: []byte(`{}`), Timestamp: time.Now().UTC(),
		}))
	}

	// Replay over the mixed set: exactly the post-recovery events (the
	// deleted stream's history is gone — expected loss boundary), and both
	// cohorts for the same task come back (no wrongful suppression).
	got := replayEventIDs(t, d, ctx, tn)
	assert.ElementsMatch(t, []string{postOld, postNew}, got,
		"replay must return both post-recovery cohorts and nothing from the deleted history")

	// Queued/pending work stayed deliverable THROUGH the events loss: the
	// held delivery still settles...
	require.NoError(t, d.AckTask(ctx, tn, held[0].DeliveryRef))
	// ...and newly queued work is fetchable after recovery.
	require.NoError(t, d.PublishTask(ctx, core.TaskMessage{
		TenantID: tn, MailboxID: mb, TaskID: "task-b7-q2",
		Priority: core.PriorityNormal, Payload: []byte("queued-after-recovery"),
	}))
	next := fetchUntilDelivery(t, d, ctx, tn, mb, 5*time.Second)
	require.Len(t, next, 1, "work queued after recovery must be deliverable")
	assert.Equal(t, "task-b7-q2", next[0].TaskID)
	require.NoError(t, d.AckTask(ctx, tn, next[0].DeliveryRef))
}
