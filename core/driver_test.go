package core

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockDriver struct {
	publishedTasks []TaskMessage
	publishedEvents []JanusEvent
	mailboxes      map[string]MailboxSpec
	consumers      map[string]ConsumerSpec
	tenants        map[string]bool
	deliveries     map[string][]TaskDelivery
	acked          []DeliveryRef
	nacked         []DeliveryRef
	closed         bool
}

func newMockDriver() *mockDriver {
	return &mockDriver{
		mailboxes:  make(map[string]MailboxSpec),
		consumers:  make(map[string]ConsumerSpec),
		tenants:    make(map[string]bool),
		deliveries: make(map[string][]TaskDelivery),
	}
}

func (m *mockDriver) PublishTask(ctx context.Context, msg TaskMessage) error {
	m.publishedTasks = append(m.publishedTasks, msg)
	if m.deliveries[msg.MailboxID] == nil {
		m.deliveries[msg.MailboxID] = []TaskDelivery{}
	}
	m.deliveries[msg.MailboxID] = append(m.deliveries[msg.MailboxID], TaskDelivery{
		TaskID:      msg.TaskID,
		Payload:     msg.Payload,
		DeliveryRef: DeliveryRef("ref-" + msg.TaskID),
	})
	return nil
}

func (m *mockDriver) FetchTasks(ctx context.Context, _, mailbox string, opts FetchOptions) ([]TaskDelivery, error) {
	return m.deliveries[mailbox], nil
}

func (m *mockDriver) AckTask(ctx context.Context, _ string, ref DeliveryRef) error {
	m.acked = append(m.acked, ref)
	return nil
}

func (m *mockDriver) NackTask(ctx context.Context, _ string, ref DeliveryRef, reason NackReason) error {
	m.nacked = append(m.nacked, ref)
	return nil
}

func (m *mockDriver) PublishDLQ(ctx context.Context, msg TaskMessage, errPayload []byte) error {
	return nil
}

func (m *mockDriver) PublishEvent(ctx context.Context, event JanusEvent) error {
	m.publishedEvents = append(m.publishedEvents, event)
	return nil
}

func (m *mockDriver) ReplayEvents(ctx context.Context, filter EventReplayFilter) (EventIterator, error) {
	return &mockEventIterator{events: m.publishedEvents}, nil
}

func (m *mockDriver) EnsureTenant(ctx context.Context, tenantID string) error {
	m.tenants[tenantID] = true
	return nil
}

func (m *mockDriver) EnsureMailbox(ctx context.Context, spec MailboxSpec) error {
	m.mailboxes[spec.MailboxID] = spec
	return nil
}

func (m *mockDriver) EnsureConsumer(ctx context.Context, spec ConsumerSpec) error {
	m.consumers[spec.DurableName] = spec
	return nil
}

func (m *mockDriver) Close() error {
	m.closed = true
	return nil
}

type mockEventIterator struct {
	events []JanusEvent
	idx    int
}

func (i *mockEventIterator) Next(ctx context.Context) (*JanusEvent, error) {
	if i.idx >= len(i.events) {
		return nil, nil
	}
	e := i.events[i.idx]
	i.idx++
	return &e, nil
}

func (i *mockEventIterator) Close() error { return nil }

func TestQueueEventDriver_Interface(t *testing.T) {
	var _ QueueEventDriver = newMockDriver()
}

func TestMockDriver_PublishAndFetch(t *testing.T) {
	d := newMockDriver()
	ctx := context.Background()

	msg := TaskMessage{
		TenantID:  "acme",
		MailboxID: "reviewer.default",
		TaskID:    "task_001",
		Priority:  PriorityNormal,
		Payload:   []byte(`{"type":"review"}`),
	}

	err := d.PublishTask(ctx, msg)
	require.NoError(t, err)

	deliveries, err := d.FetchTasks(ctx, "", "reviewer.default", FetchOptions{MaxMessages: 10})
	require.NoError(t, err)
	require.Len(t, deliveries, 1)
	assert.Equal(t, "task_001", deliveries[0].TaskID)
	assert.Equal(t, DeliveryRef("ref-task_001"), deliveries[0].DeliveryRef)
}

func TestMockDriver_AckAndNack(t *testing.T) {
	d := newMockDriver()
	ctx := context.Background()

	_ = d.PublishTask(ctx, TaskMessage{TaskID: "t1", MailboxID: "mb1"})
	_ = d.PublishTask(ctx, TaskMessage{TaskID: "t2", MailboxID: "mb1"})

	err := d.AckTask(ctx, "", DeliveryRef("ref-t1"))
	require.NoError(t, err)

	err = d.NackTask(ctx, "", DeliveryRef("ref-t2"), NackRetriable)
	require.NoError(t, err)

	assert.Len(t, d.acked, 1)
	assert.Equal(t, DeliveryRef("ref-t1"), d.acked[0])
	assert.Len(t, d.nacked, 1)
	assert.Equal(t, DeliveryRef("ref-t2"), d.nacked[0])
}

func TestMockDriver_EnsureTenant(t *testing.T) {
	d := newMockDriver()
	ctx := context.Background()

	err := d.EnsureTenant(ctx, "acme")
	require.NoError(t, err)
	assert.True(t, d.tenants["acme"])
	assert.False(t, d.tenants["other"])
}

func TestMockDriver_EnsureMailbox(t *testing.T) {
	d := newMockDriver()
	ctx := context.Background()

	spec := MailboxSpec{
		TenantID:       "acme",
		MailboxID:      "reviewer.default",
		AgentID:        "code-reviewer.team-a",
		MaxConcurrency: 4,
		ACKWaitSeconds: 300,
	}

	err := d.EnsureMailbox(ctx, spec)
	require.NoError(t, err)
	assert.Equal(t, spec, d.mailboxes["reviewer.default"])
}

func TestMockDriver_EnsureConsumer(t *testing.T) {
	d := newMockDriver()
	ctx := context.Background()

	spec := ConsumerSpec{
		TenantID:      "acme",
		MailboxID:     "reviewer.default",
		DurableName:   "consumer.acme.reviewer.default",
		ACKWaitSeconds: 300,
		MaxDeliver:    5,
	}

	err := d.EnsureConsumer(ctx, spec)
	require.NoError(t, err)
	assert.Equal(t, spec, d.consumers["consumer.acme.reviewer.default"])
}

func TestMockDriver_PublishEvent(t *testing.T) {
	d := newMockDriver()
	ctx := context.Background()

	event := JanusEvent{
		EventID:   "evt_001",
		EventType: EventTaskCreated,
		TenantID:  "acme",
		TaskID:    "task_001",
		Timestamp: time.Now(),
		Payload:   []byte(`{}`),
	}

	err := d.PublishEvent(ctx, event)
	require.NoError(t, err)
	require.Len(t, d.publishedEvents, 1)
	assert.Equal(t, EventTaskCreated, d.publishedEvents[0].EventType)
}

func TestMockDriver_ReplayEvents(t *testing.T) {
	d := newMockDriver()
	ctx := context.Background()

	_ = d.PublishEvent(ctx, JanusEvent{EventID: "e1", EventType: EventTaskCreated, Timestamp: time.Now()})
	_ = d.PublishEvent(ctx, JanusEvent{EventID: "e2", EventType: EventTaskQueued, Timestamp: time.Now()})

	iter, err := d.ReplayEvents(ctx, EventReplayFilter{TenantID: "acme"})
	require.NoError(t, err)
	defer iter.Close()

	e1, err := iter.Next(ctx)
	require.NoError(t, err)
	assert.Equal(t, "e1", e1.EventID)

	e2, err := iter.Next(ctx)
	require.NoError(t, err)
	assert.Equal(t, "e2", e2.EventID)

	e3, err := iter.Next(ctx)
	require.NoError(t, err)
	assert.Nil(t, e3)
}

func TestMockDriver_Close(t *testing.T) {
	d := newMockDriver()
	err := d.Close()
	require.NoError(t, err)
	assert.True(t, d.closed)
}
