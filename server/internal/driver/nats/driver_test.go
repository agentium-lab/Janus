package nats

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

var natsURL string

var listenAddrRe = regexp.MustCompile(`Listening for client connections on (.+)`)

func startNATSServer(t *testing.T) {
	t.Helper()
	natsURL = os.Getenv("JANUS_NATS_URL")
	if natsURL != "" {
		return
	}

	cmd := exec.Command(os.ExpandEnv("$HOME/go/bin/nats-server"),
		"-p", "0", "-js", "--store_dir", t.TempDir())
	stderr, err := cmd.StderrPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		cmd.Process.Signal(os.Interrupt)
		cmd.Wait()
	})

	addrCh := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			line := scanner.Text()
			if m := listenAddrRe.FindStringSubmatch(line); len(m) > 1 {
				addrCh <- m[1]
				return
			}
		}
	}()

	select {
	case addr := <-addrCh:
		natsURL = "nats://" + addr
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for nats-server to start")
	}
}

func openDriver(t *testing.T) *Driver {
	startNATSServer(t)
	d, err := NewDriver(Config{URL: natsURL})
	require.NoError(t, err)
	t.Cleanup(func() { d.Close() })
	return d
}

func setupTenantAndConsumer(t *testing.T, d *Driver, tenantID, mailboxID string) context.Context {
	ctx := ContextWithTenant(context.Background(), tenantID)
	require.NoError(t, d.EnsureTenant(ctx, tenantID))
	require.NoError(t, d.EnsureMailbox(ctx, core.MailboxSpec{
		TenantID:         tenantID,
		MailboxID:        mailboxID,
		AgentID:          "test-agent",
		MaxConcurrency:   1,
		ACKWaitSeconds:   30,
		MaxDeliver:       3,
		RetentionSeconds: 3600,
	}))
	require.NoError(t, d.EnsureConsumer(ctx, core.ConsumerSpec{
		TenantID:       tenantID,
		MailboxID:      mailboxID,
		ACKWaitSeconds: 30,
		MaxDeliver:     3,
		MaxACKPending:  10,
	}))
	return ctx
}

func TestDriver_EnsureTenant(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()
	require.NoError(t, d.EnsureTenant(ctx, "acme"))
	require.NoError(t, d.EnsureTenant(ctx, "acme"))
}

func TestDriver_EnsureTenantMultiple(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()
	require.NoError(t, d.EnsureTenant(ctx, "tenant-a"))
	require.NoError(t, d.EnsureTenant(ctx, "tenant-b"))
}

func TestDriver_EnsureMailbox(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()
	require.NoError(t, d.EnsureTenant(ctx, "acme"))

	require.NoError(t, d.EnsureMailbox(ctx, core.MailboxSpec{
		TenantID: "acme", MailboxID: "reviewer_default",
	}))
	require.NoError(t, d.EnsureMailbox(ctx, core.MailboxSpec{
		TenantID: "acme", MailboxID: "reviewer_default",
	}))
}

func TestDriver_EnsureMailboxWithoutTenant(t *testing.T) {
	d := openDriver(t)
	err := d.EnsureMailbox(context.Background(), core.MailboxSpec{
		TenantID: "nonexistent", MailboxID: "mb",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not initialized")
}

func TestDriver_EnsureConsumerWithoutTenant(t *testing.T) {
	d := openDriver(t)
	err := d.EnsureConsumer(context.Background(), core.ConsumerSpec{
		TenantID: "nonexistent", MailboxID: "mb",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not initialized")
}

func TestDriver_PublishAndFetch(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()
	tctx := setupTenantAndConsumer(t, d, "acme", "reviewer_default")

	require.NoError(t, d.PublishTask(ctx, core.TaskMessage{
		TenantID:  "acme",
		MailboxID: "reviewer_default",
		TaskID:    "task_001",
		Priority:  core.PriorityNormal,
		Payload:   []byte(`{"hello":"world"}`),
		Headers:   map[string]string{"X-Test": "yes"},
	}))

	time.Sleep(200 * time.Millisecond)

	deliveries, err := d.FetchTasks(tctx, "reviewer_default", core.FetchOptions{
		MaxMessages: 1, WaitTime: 2 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, deliveries, 1)
	assert.Equal(t, "task_001", deliveries[0].TaskID)
	assert.Equal(t, `{"hello":"world"}`, string(deliveries[0].Payload))
	assert.Equal(t, 0, deliveries[0].RedeliveryCount)
}

func TestDriver_AckTask(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()
	tctx := setupTenantAndConsumer(t, d, "acme", "reviewer_default")

	require.NoError(t, d.PublishTask(ctx, core.TaskMessage{
		TenantID: "acme", MailboxID: "reviewer_default",
		TaskID: "task_ack", Priority: core.PriorityNormal, Payload: []byte("data"),
	}))

	time.Sleep(200 * time.Millisecond)

	deliveries, err := d.FetchTasks(tctx, "reviewer_default", core.FetchOptions{
		MaxMessages: 1, WaitTime: 2 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, deliveries, 1)
	require.NoError(t, d.AckTask(ctx, deliveries[0].DeliveryRef))
}

func TestDriver_NackRetriable(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()
	tctx := setupTenantAndConsumer(t, d, "acme", "reviewer_default")

	require.NoError(t, d.PublishTask(ctx, core.TaskMessage{
		TenantID: "acme", MailboxID: "reviewer_default",
		TaskID: "task_nack_retry", Priority: core.PriorityNormal, Payload: []byte("data"),
	}))

	time.Sleep(200 * time.Millisecond)

	deliveries, err := d.FetchTasks(tctx, "reviewer_default", core.FetchOptions{
		MaxMessages: 1, WaitTime: 2 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, deliveries, 1)
	require.NoError(t, d.NackTask(ctx, deliveries[0].DeliveryRef, core.NackRetriable))
}

func TestDriver_NackNonRetriable(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()
	tctx := setupTenantAndConsumer(t, d, "acme", "reviewer_default")

	require.NoError(t, d.PublishTask(ctx, core.TaskMessage{
		TenantID: "acme", MailboxID: "reviewer_default",
		TaskID: "task_nack_term", Priority: core.PriorityNormal, Payload: []byte("data"),
	}))

	time.Sleep(200 * time.Millisecond)

	deliveries, err := d.FetchTasks(tctx, "reviewer_default", core.FetchOptions{
		MaxMessages: 1, WaitTime: 2 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, deliveries, 1)
	require.NoError(t, d.NackTask(ctx, deliveries[0].DeliveryRef, core.NackNonRetriable))
}

func TestDriver_AckNotFound(t *testing.T) {
	d := openDriver(t)
	err := d.AckTask(context.Background(), "nonexistent:0")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestDriver_NackNotFound(t *testing.T) {
	d := openDriver(t)
	err := d.NackTask(context.Background(), "nonexistent:0", core.NackRetriable)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestDriver_PublishEvent(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()
	require.NoError(t, d.EnsureTenant(ctx, "acme"))

	require.NoError(t, d.PublishEvent(ctx, core.JanusEvent{
		EventID:   "evt_001",
		EventType: core.EventTaskCreated,
		TenantID:  "acme",
		TaskID:    "task_001",
		Payload:   []byte(`{"status":"created"}`),
	}))
}

func TestDriver_ReplayEvents(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()
	require.NoError(t, d.EnsureTenant(ctx, "acme"))

	now := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, d.PublishEvent(ctx, core.JanusEvent{
		EventID: "evt_r1", EventType: core.EventTaskCreated,
		TenantID: "acme", TaskID: "task_r1",
		Payload: []byte(`{"s":"created"}`), Timestamp: now,
	}))
	require.NoError(t, d.PublishEvent(ctx, core.JanusEvent{
		EventID: "evt_r2", EventType: core.EventTaskCompleted,
		TenantID: "acme", TaskID: "task_r2",
		Payload: []byte(`{"s":"completed"}`), Timestamp: now,
	}))

	time.Sleep(300 * time.Millisecond)

	iter, err := d.ReplayEvents(ctx, core.EventReplayFilter{
		TenantID:   "acme",
		EventTypes: []core.EventType{core.EventTaskCreated},
	})
	require.NoError(t, err)
	defer iter.Close()

	var events []*core.JanusEvent
	for {
		ev, err := iter.Next(ctx)
		require.NoError(t, err)
		if ev == nil {
			break
		}
		events = append(events, ev)
	}
	assert.Len(t, events, 1)
	assert.Equal(t, "evt_r1", events[0].EventID)
}

func TestDriver_ReplayEventsWithoutTenant(t *testing.T) {
	d := openDriver(t)
	_, err := d.ReplayEvents(context.Background(), core.EventReplayFilter{TenantID: "nonexistent"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not initialized")
}

func TestDriver_FetchTasksWithoutConsumer(t *testing.T) {
	d := openDriver(t)
	ctx := ContextWithTenant(context.Background(), "acme")
	require.NoError(t, d.EnsureTenant(ctx, "acme"))

	_, err := d.FetchTasks(ctx, "nonexistent", core.FetchOptions{
		MaxMessages: 1, WaitTime: 500 * time.Millisecond,
	})
	assert.Error(t, err)
}

func TestDriver_FetchEmpty(t *testing.T) {
	d := openDriver(t)
	tctx := setupTenantAndConsumer(t, d, "acme", "empty_mb")

	deliveries, err := d.FetchTasks(tctx, "empty_mb", core.FetchOptions{
		MaxMessages: 1, WaitTime: 500 * time.Millisecond,
	})
	require.NoError(t, err)
	assert.Len(t, deliveries, 0)
}

func TestDriver_EnsureConsumerDefaults(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()
	require.NoError(t, d.EnsureTenant(ctx, "acme"))

	require.NoError(t, d.EnsureConsumer(ctx, core.ConsumerSpec{
		TenantID: "acme", MailboxID: "default_mb",
	}))
	require.NoError(t, d.EnsureConsumer(ctx, core.ConsumerSpec{
		TenantID: "acme", MailboxID: "default_mb",
	}))
}

func TestDriver_PublishMultipleFetchInOrder(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()
	tctx := setupTenantAndConsumer(t, d, "acme", "ordered_mb")

	for i := 0; i < 3; i++ {
		require.NoError(t, d.PublishTask(ctx, core.TaskMessage{
			TenantID: "acme", MailboxID: "ordered_mb",
			TaskID:   fmt.Sprintf("task_%03d", i),
			Priority: core.PriorityNormal,
			Payload:  []byte(fmt.Sprintf("payload_%d", i)),
		}))
	}

	time.Sleep(300 * time.Millisecond)

	deliveries, err := d.FetchTasks(tctx, "ordered_mb", core.FetchOptions{
		MaxMessages: 3, WaitTime: 2 * time.Second,
	})
	require.NoError(t, err)
	assert.Len(t, deliveries, 3)

	for _, del := range deliveries {
		require.NoError(t, d.AckTask(ctx, del.DeliveryRef))
	}
}

func TestSanitize(t *testing.T) {
	assert.Equal(t, "hello_world", sanitize("hello world"))
	assert.Equal(t, "abc-123_def", sanitize("abc-123.def"))
	assert.Equal(t, "ALL_CAPS", sanitize("ALL CAPS"))
}

func TestStreamName(t *testing.T) {
	assert.Equal(t, "JANUS_acme_TASKS", streamName("acme", "TASKS"))
}

func TestTaskSubject(t *testing.T) {
	assert.Equal(t, "janus.acme.tasks.reviewer_default", taskSubject("acme", "reviewer_default"))
}

func TestEventSubject(t *testing.T) {
	assert.Equal(t, "janus.acme.events.task.created", eventSubject("acme", "task.created"))
}

func TestDLQSubject(t *testing.T) {
	assert.Equal(t, "janus.acme.tasks_dlq.reviewer_default", dlqSubject("acme", "reviewer_default"))
}

func TestConsumerName(t *testing.T) {
	assert.Equal(t, "consumer_acme_reviewer_default", consumerName("acme", "reviewer_default"))
}

func TestContextWithTenant(t *testing.T) {
	ctx := context.Background()
	assert.Equal(t, "default", tenantFromCtx(ctx))

	ctx2 := ContextWithTenant(ctx, "acme")
	assert.Equal(t, "acme", tenantFromCtx(ctx2))
}

func TestDriver_NewDriverBadURL(t *testing.T) {
	_, err := NewDriver(Config{URL: "nats://127.0.0.1:1"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "connect to NATS")
}

func TestDriver_Conn(t *testing.T) {
	d := openDriver(t)
	assert.NotNil(t, d.Conn())
}

func TestDriver_PublishEventNoTenant(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()
	err := d.PublishEvent(ctx, core.JanusEvent{
		EventID: "evt_no_tenant", EventType: core.EventTaskCreated,
		TenantID: "nonexistent", Payload: []byte("x"),
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "publish event")
}

func TestDriver_PublishTaskNoTenant(t *testing.T) {
	d := openDriver(t)
	err := d.PublishTask(context.Background(), core.TaskMessage{
		TenantID: "nonexistent", MailboxID: "mb",
		TaskID: "t1", Payload: []byte("x"),
	})
	assert.Error(t, err)
}

func TestDriver_ReplayEventsCancelled(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()
	require.NoError(t, d.EnsureTenant(ctx, "acme"))

	require.NoError(t, d.PublishEvent(ctx, core.JanusEvent{
		EventID: "evt_cancel", EventType: core.EventTaskCreated,
		TenantID: "acme", Payload: []byte("x"),
	}))
	time.Sleep(200 * time.Millisecond)

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()

	iter, err := d.ReplayEvents(cancelCtx, core.EventReplayFilter{TenantID: "acme"})
	if err != nil {
		assert.Contains(t, err.Error(), "replay")
		return
	}
	defer iter.Close()

	_, _ = iter.Next(cancelCtx)
}

func TestDriver_EnsureTenantError(t *testing.T) {
	d := openDriver(t)
	badCtx, cancel := context.WithCancel(context.Background())
	cancel()

	err := d.EnsureTenant(badCtx, "bad_tenant")
	assert.Error(t, err)
}

func TestDriver_EnsureMailboxDuplicate(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()
	require.NoError(t, d.EnsureTenant(ctx, "acme"))

	require.NoError(t, d.EnsureMailbox(ctx, core.MailboxSpec{
		TenantID: "acme", MailboxID: "mb1",
	}))
	require.NoError(t, d.EnsureMailbox(ctx, core.MailboxSpec{
		TenantID: "acme", MailboxID: "mb1",
	}))
}
