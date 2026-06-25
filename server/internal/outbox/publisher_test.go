package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/driver/postgres"
)

// fakeDriver records the calls made by publishOne so tests can assert on them.
type fakeDriver struct {
	publishedTasks []core.TaskMessage
	publishedEvent []core.JanusEvent
	publishedDLQ   []struct {
		Msg core.TaskMessage
		Err []byte
	}
	publishErr error
}

func (d *fakeDriver) PublishTask(_ context.Context, msg core.TaskMessage) error {
	if d.publishErr != nil {
		return d.publishErr
	}
	d.publishedTasks = append(d.publishedTasks, msg)
	return nil
}

func (d *fakeDriver) FetchTasks(_ context.Context, _ string, _ core.FetchOptions) ([]core.TaskDelivery, error) {
	return nil, nil
}

func (d *fakeDriver) AckTask(_ context.Context, _ core.DeliveryRef) error { return nil }

func (d *fakeDriver) NackTask(_ context.Context, _ core.DeliveryRef, _ core.NackReason) error {
	return nil
}

func (d *fakeDriver) PublishDLQ(_ context.Context, msg core.TaskMessage, errPayload []byte) error {
	if d.publishErr != nil {
		return d.publishErr
	}
	d.publishedDLQ = append(d.publishedDLQ, struct {
		Msg core.TaskMessage
		Err []byte
	}{msg, errPayload})
	return nil
}

func (d *fakeDriver) PublishEvent(_ context.Context, event core.JanusEvent) error {
	if d.publishErr != nil {
		return d.publishErr
	}
	d.publishedEvent = append(d.publishedEvent, event)
	return nil
}

func (d *fakeDriver) ReplayEvents(_ context.Context, _ core.EventReplayFilter) (core.EventIterator, error) {
	return nil, nil
}

func (d *fakeDriver) EnsureTenant(_ context.Context, _ string) error          { return nil }
func (d *fakeDriver) EnsureMailbox(_ context.Context, _ core.MailboxSpec) error { return nil }
func (d *fakeDriver) EnsureConsumer(_ context.Context, _ core.ConsumerSpec) error { return nil }
func (d *fakeDriver) Close() error                                              { return nil }

func TestPublishOne_TaskPublish(t *testing.T) {
	drv := &fakeDriver{}
	p := &Publisher{driver: drv}

	msg := core.TaskMessage{TenantID: "t1", MailboxID: "mb1", TaskID: "task-1"}
	payload, _ := json.Marshal(msg)

	err := p.publishOne(context.Background(), postgres.OutboxEntry{Kind: "task_publish", Payload: payload})
	require.NoError(t, err)
	require.Len(t, drv.publishedTasks, 1)
	assert.Equal(t, "task-1", drv.publishedTasks[0].TaskID)
}

func TestPublishOne_EventPublish(t *testing.T) {
	drv := &fakeDriver{}
	p := &Publisher{driver: drv}

	event := core.JanusEvent{EventType: core.EventTaskCompleted, TenantID: "t1", TaskID: "task-1"}
	payload, _ := json.Marshal(event)

	err := p.publishOne(context.Background(), postgres.OutboxEntry{Kind: "event_publish", Payload: payload})
	require.NoError(t, err)
	require.Len(t, drv.publishedEvent, 1)
	assert.Equal(t, core.EventTaskCompleted, drv.publishedEvent[0].EventType)
}

func TestPublishOne_DLQPublish(t *testing.T) {
	drv := &fakeDriver{}
	p := &Publisher{driver: drv}

	msg := core.TaskMessage{
		TenantID: "t1", MailboxID: "mb1", TaskID: "task-1",
		Headers: map[string]string{"error": `{"code":"TIMEOUT","message":"timed out"}`},
	}
	payload, _ := json.Marshal(msg)

	err := p.publishOne(context.Background(), postgres.OutboxEntry{Kind: "dlq_publish", Payload: payload})
	require.NoError(t, err)
	require.Len(t, drv.publishedDLQ, 1)
	assert.Equal(t, "task-1", drv.publishedDLQ[0].Msg.TaskID)
	assert.Contains(t, string(drv.publishedDLQ[0].Err), "TIMEOUT")
}

func TestPublishOne_UnknownKind_NoOp(t *testing.T) {
	drv := &fakeDriver{}
	p := &Publisher{driver: drv}

	err := p.publishOne(context.Background(), postgres.OutboxEntry{Kind: "unknown_kind", Payload: json.RawMessage(`{}`)})
	require.NoError(t, err)
	assert.Empty(t, drv.publishedTasks)
	assert.Empty(t, drv.publishedEvent)
	assert.Empty(t, drv.publishedDLQ)
}

func TestPublishOne_InvalidPayload_ReturnsError(t *testing.T) {
	drv := &fakeDriver{}
	p := &Publisher{driver: drv}

	err := p.publishOne(context.Background(), postgres.OutboxEntry{Kind: "task_publish", Payload: json.RawMessage(`{invalid json`)})
	require.Error(t, err)
}

func TestPublishOne_PublishError_Propagated(t *testing.T) {
	expectedErr := errors.New("NATS unavailable")
	drv := &fakeDriver{publishErr: expectedErr}
	p := &Publisher{driver: drv}

	msg := core.TaskMessage{TaskID: "task-1"}
	payload, _ := json.Marshal(msg)
	err := p.publishOne(context.Background(), postgres.OutboxEntry{Kind: "task_publish", Payload: payload})
	assert.Equal(t, expectedErr, err)
}

func TestNewPublisher(t *testing.T) {
	drv := &fakeDriver{}
	p := NewPublisher(nil, drv)
	assert.NotNil(t, p)
	assert.NotNil(t, p.done)
}

func TestPublisher_StartStop_ContextCancel(t *testing.T) {
	p := NewPublisher(nil, &fakeDriver{})
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		p.Start(ctx, 1*time.Hour)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not exit after context cancel")
	}
}

func TestPublisher_StartStop_StopMethod(t *testing.T) {
	p := NewPublisher(nil, &fakeDriver{})
	ctx := context.Background()

	done := make(chan struct{})
	go func() {
		p.Start(ctx, 1*time.Hour)
		close(done)
	}()

	p.Stop()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not exit after Stop")
	}
}
