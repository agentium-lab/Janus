package recovery

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/agentium-lab/Janus/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockQueueDriver struct {
	published []core.TaskMessage
}

func (m *mockQueueDriver) PublishTask(ctx context.Context, msg core.TaskMessage) error {
	m.published = append(m.published, msg)
	return nil
}

func (m *mockQueueDriver) FetchTasks(ctx context.Context, tenantID, mailbox string, opts core.FetchOptions) ([]core.TaskDelivery, error) {
	return nil, nil
}

func (m *mockQueueDriver) AckTask(ctx context.Context, tenantID string, ref core.DeliveryRef) error {
	return nil
}

func (m *mockQueueDriver) NackTask(ctx context.Context, tenantID string, ref core.DeliveryRef, reason core.NackReason) error {
	return nil
}

func (m *mockQueueDriver) PublishDLQ(ctx context.Context, msg core.TaskMessage, errPayload []byte) error {
	return nil
}

func (m *mockQueueDriver) PublishEvent(ctx context.Context, event core.JanusEvent) error {
	return nil
}

func (m *mockQueueDriver) ReplayEvents(ctx context.Context, filter core.EventReplayFilter) (core.EventIterator, error) {
	return nil, nil
}

func (m *mockQueueDriver) EnsureTenant(ctx context.Context, tenantID string) error {
	return nil
}

func (m *mockQueueDriver) EnsureMailbox(ctx context.Context, spec core.MailboxSpec) error {
	return nil
}

func (m *mockQueueDriver) EnsureConsumer(ctx context.Context, spec core.ConsumerSpec) error {
	return nil
}

func (m *mockQueueDriver) Close() error {
	return nil
}

func TestTaskScanner_RecoverOldQueuedTasks(t *testing.T) {
	queue := &mockQueueDriver{}
	scanner := &TaskScanner{
		queue:    queue,
		interval: time.Second,
		minAge:   5 * time.Minute,
		stopCh:   make(chan struct{}),
	}

	envelope := core.TaskEnvelope{
		JanusVersion: "0.1",
		TaskID:       "task-1",
		TenantID:     "acme",
		SourceAgent:  "agent-1",
		Target:       core.Target{Type: "agent", Value: "agent-1"},
		Payload:      core.Payload{Type: "json", Content: "{}"},
	}
	envJSON, _ := json.Marshal(envelope)

	task := struct {
		TenantID  string
		ID        string
		MailboxID string
		Priority  core.Priority
		Envelope  []byte
		CreatedAt time.Time
	}{
		TenantID:  "acme",
		ID:        "task-1",
		MailboxID: "mb-1",
		Priority:  core.PriorityNormal,
		Envelope:  envJSON,
		CreatedAt: time.Now().Add(-10 * time.Minute),
	}

	ctx := context.Background()
	recovered := scanner.recoverTask(ctx, task)
	assert.True(t, recovered, "task should be recovered")
	require.Len(t, queue.published, 1, "one message should be published")
	assert.Equal(t, "task-1", queue.published[0].TaskID)
	assert.Equal(t, "acme", queue.published[0].TenantID)
	assert.Equal(t, "mb-1", queue.published[0].MailboxID)
	assert.Contains(t, queue.published[0].DedupeKey, "recovery:acme:task-1")
}

func TestTaskScanner_SkipTaskWithoutMailbox(t *testing.T) {
	queue := &mockQueueDriver{}
	scanner := &TaskScanner{
		queue:    queue,
		interval: time.Second,
		minAge:   5 * time.Minute,
		stopCh:   make(chan struct{}),
	}

	task := struct {
		TenantID  string
		ID        string
		MailboxID string
		Priority  core.Priority
		Envelope  []byte
		CreatedAt time.Time
	}{
		TenantID:  "acme",
		ID:        "task-1",
		MailboxID: "",
		Priority:  core.PriorityNormal,
		Envelope:  []byte("{}"),
		CreatedAt: time.Now().Add(-10 * time.Minute),
	}

	ctx := context.Background()
	recovered := scanner.recoverTask(ctx, task)
	assert.False(t, recovered, "task without mailbox should not be recovered")
	assert.Empty(t, queue.published, "no messages should be published")
}

func TestTaskScanner_SkipTaskWithInvalidEnvelope(t *testing.T) {
	queue := &mockQueueDriver{}
	scanner := &TaskScanner{
		queue:    queue,
		interval: time.Second,
		minAge:   5 * time.Minute,
		stopCh:   make(chan struct{}),
	}

	task := struct {
		TenantID  string
		ID        string
		MailboxID string
		Priority  core.Priority
		Envelope  []byte
		CreatedAt time.Time
	}{
		TenantID:  "acme",
		ID:        "task-1",
		MailboxID: "mb-1",
		Priority:  core.PriorityNormal,
		Envelope:  []byte("invalid json"),
		CreatedAt: time.Now().Add(-10 * time.Minute),
	}

	ctx := context.Background()
	recovered := scanner.recoverTask(ctx, task)
	assert.False(t, recovered, "task with invalid envelope should not be recovered")
	assert.Empty(t, queue.published, "no messages should be published")
}
