package nats

// B8 TASKS-stream-loss pin: the tenant's TASKS JetStream stream (the task
// delivery plane in NATS mode) is deleted. In-flight broker messages are
// gone — PG task rows remain the durable authority — and consumption must
// REBUILD: ReconcileTenant recreates the stream, EnsureConsumer reattaches
// the durable consumer, and a task published after the loss delivers
// through FetchTasks again. Mirrors the B7 EVENTS-loss pin for the TASKS
// plane (GA-10 evidence: TASKS data loss rebuild).
import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

func TestB8_TasksStreamLoss_RebuildRestoresConsumption(t *testing.T) {
	d := openDriver(t)
	ctx := ContextWithTenant(context.Background(), genTenant(t))
	tn := tenantFromCtx(ctx)
	mb := "b8_loss_mb"

	require.NoError(t, d.EnsureTenant(ctx, tn))
	require.NoError(t, d.EnsureMailbox(ctx, core.MailboxSpec{
		TenantID: tn, MailboxID: mb, AgentID: "b8-agent",
		MaxConcurrency: 1, ACKWaitSeconds: 30, MaxDeliver: 3, RetentionSeconds: 3600,
	}))
	require.NoError(t, d.EnsureConsumer(ctx, core.ConsumerSpec{
		TenantID: tn, MailboxID: mb, ACKWaitSeconds: 30, MaxDeliver: 3, MaxACKPending: 10,
	}))

	// Pre-loss delivery works.
	preID := fmt.Sprintf("b8-pre-%d", time.Now().UnixNano())
	require.NoError(t, d.PublishTask(ctx, core.TaskMessage{
		TenantID: tn, MailboxID: mb, TaskID: preID,
		Priority: core.PriorityNormal, Payload: []byte("pre-loss"),
	}))
	held := fetchUntilDelivery(t, d, ctx, tn, mb, 5*time.Second)
	require.Len(t, held, 1, "pre-loss task must deliver")
	require.NoError(t, d.AckTask(ctx, tn, held[0].DeliveryRef))

	// The TASKS stream is LOST (storage wiped).
	require.NoError(t, d.js.DeleteStream(ctx, streamName(tn, "TASKS")))

	// Symptom: fetching through the destroyed consumer delivers nothing
	// (the cached handle targets a dead consumer; it may error or silently
	// return empty — either way the loss is visible as no delivery).
	dead, derr := d.FetchTasks(ctx, tn, mb, core.FetchOptions{MaxMessages: 1, WaitTime: 200 * time.Millisecond})
	if derr == nil {
		t.Log("fetch on the dead consumer reported no error (silent empty within the wait window)")
	}
	assert.Empty(t, dead, "no delivery may come through the deleted stream")

	// Rebuild path: reconcile recreates the stream, ensure reattaches the
	// durable consumer.
	require.NoError(t, d.ReconcileTenant(ctx, tn))
	require.NoError(t, d.EnsureMailbox(ctx, core.MailboxSpec{
		TenantID: tn, MailboxID: mb, AgentID: "b8-agent",
		MaxConcurrency: 1, ACKWaitSeconds: 30, MaxDeliver: 3, RetentionSeconds: 3600,
	}))
	require.NoError(t, d.EnsureConsumer(ctx, core.ConsumerSpec{
		TenantID: tn, MailboxID: mb, ACKWaitSeconds: 30, MaxDeliver: 3, MaxACKPending: 10,
	}))
	_, serr := d.js.Stream(ctx, streamName(tn, "TASKS"))
	assert.NoError(t, serr, "reconcile must recreate the TASKS stream")

	// Given a task published after the rebuild, consumption delivers it.
	postID := fmt.Sprintf("b8-post-%d", time.Now().UnixNano())
	require.NoError(t, d.PublishTask(ctx, core.TaskMessage{
		TenantID: tn, MailboxID: mb, TaskID: postID,
		Priority: core.PriorityNormal, Payload: []byte("post-rebuild"),
	}))
	got := fetchUntilDelivery(t, d, ctx, tn, mb, 5*time.Second)
	require.Len(t, got, 1, "post-rebuild task must deliver through the rebuilt consumer")
	assert.Equal(t, postID, got[0].TaskID)
	require.NoError(t, d.AckTask(ctx, tn, got[0].DeliveryRef))
}
