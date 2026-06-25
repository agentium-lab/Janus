package service

import (
	"context"
	"sync"
	"testing"

	"github.com/agentium-lab/Janus/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockEventRepo struct {
	mu     sync.Mutex
	events []core.JanusEvent
	err    error
}

func (m *mockEventRepo) Insert(_ context.Context, evt core.JanusEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.events = append(m.events, evt)
	return nil
}

func (m *mockEventRepo) ListByTask(_ context.Context, tenantID, taskID string, limit int) ([]*core.JanusEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []*core.JanusEvent
	for i := range m.events {
		e := &m.events[i]
		if e.TenantID == tenantID && e.TaskID == taskID {
			result = append(result, e)
		}
		if len(result) >= limit {
			break
		}
	}
	return result, nil
}

func (m *mockEventRepo) ListByTrace(_ context.Context, tenantID, traceID string, limit int) ([]*core.JanusEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []*core.JanusEvent
	for i := range m.events {
		e := &m.events[i]
		if e.TenantID == tenantID && e.TraceID == traceID {
			result = append(result, e)
		}
		if len(result) >= limit {
			break
		}
	}
	return result, nil
}

func (m *mockEventRepo) ListByTenant(_ context.Context, tenantID string, limit int) ([]*core.JanusEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []*core.JanusEvent
	for i := range m.events {
		e := &m.events[i]
		if e.TenantID == tenantID {
			result = append(result, e)
		}
		if len(result) >= limit {
			break
		}
	}
	return result, nil
}

func TestEventService_Record(t *testing.T) {
	repo := &mockEventRepo{}
	svc := NewEventService(repo)

	err := svc.Record(context.Background(), core.JanusEvent{
		EventType: core.EventTaskCreated,
		TenantID:  "acme",
		Payload:   []byte(`{"priority":"high"}`),
	})
	require.NoError(t, err)

	repo.mu.Lock()
	assert.Len(t, repo.events, 1)
	assert.NotEmpty(t, repo.events[0].EventID)
	assert.False(t, repo.events[0].Timestamp.IsZero())
	repo.mu.Unlock()
}

func TestEventService_Record_WithExistingID(t *testing.T) {
	repo := &mockEventRepo{}
	svc := NewEventService(repo)

	err := svc.Record(context.Background(), core.JanusEvent{
		EventID:   "custom-id",
		EventType: core.EventTaskCompleted,
		TenantID:  "acme",
		Payload:   []byte(`{}`),
	})
	require.NoError(t, err)

	repo.mu.Lock()
	assert.Equal(t, "custom-id", repo.events[0].EventID)
	repo.mu.Unlock()
}

func TestEventService_Record_NilPayload(t *testing.T) {
	repo := &mockEventRepo{}
	svc := NewEventService(repo)

	err := svc.Record(context.Background(), core.JanusEvent{
		EventType: core.EventTaskFailed,
		TenantID:  "acme",
	})
	require.NoError(t, err)

	repo.mu.Lock()
	assert.Equal(t, []byte(`{}`), repo.events[0].Payload)
	repo.mu.Unlock()
}

func TestEventService_Record_RepoError(t *testing.T) {
	repo := &mockEventRepo{err: assert.AnError}
	svc := NewEventService(repo)

	err := svc.Record(context.Background(), core.JanusEvent{
		EventType: core.EventTaskCreated,
		TenantID:  "acme",
	})
	assert.Error(t, err)
}

func TestEventService_QueryByTask(t *testing.T) {
	repo := &mockEventRepo{events: []core.JanusEvent{
		{EventID: "e1", TenantID: "acme", TaskID: "task-1", EventType: core.EventTaskCreated},
		{EventID: "e2", TenantID: "acme", TaskID: "task-1", EventType: core.EventTaskCompleted},
		{EventID: "e3", TenantID: "acme", TaskID: "task-2", EventType: core.EventTaskCreated},
	}}
	svc := NewEventService(repo)

	events, err := svc.QueryByTask(context.Background(), "acme", "task-1", 10)
	require.NoError(t, err)
	assert.Len(t, events, 2)
}

func TestEventService_QueryByTask_DefaultLimit(t *testing.T) {
	repo := &mockEventRepo{}
	svc := NewEventService(repo)

	events, err := svc.QueryByTask(context.Background(), "acme", "task-1", 0)
	require.NoError(t, err)
	assert.Len(t, events, 0)
}

func TestEventService_QueryByTrace(t *testing.T) {
	repo := &mockEventRepo{events: []core.JanusEvent{
		{EventID: "e1", TenantID: "acme", TraceID: "trace-1", EventType: core.EventTaskCreated},
		{EventID: "e2", TenantID: "acme", TraceID: "trace-1", EventType: core.EventTaskCompleted},
		{EventID: "e3", TenantID: "acme", TraceID: "trace-2", EventType: core.EventTaskCreated},
	}}
	svc := NewEventService(repo)

	events, err := svc.QueryByTrace(context.Background(), "acme", "trace-1", 10)
	require.NoError(t, err)
	assert.Len(t, events, 2)
}

func TestEventService_QueryByTrace_DefaultLimit(t *testing.T) {
	repo := &mockEventRepo{}
	svc := NewEventService(repo)

	events, err := svc.QueryByTrace(context.Background(), "acme", "trace-1", -1)
	require.NoError(t, err)
	assert.Len(t, events, 0)
}

func TestEventService_PublishEvent(t *testing.T) {
	repo := &mockEventRepo{}
	svc := NewEventService(repo)

	err := svc.PublishEvent(context.Background(), "acme", core.EventTaskClaimed, "task-1", "trace-1", "agent-1", map[string]string{"lease": "abc"})
	require.NoError(t, err)

	repo.mu.Lock()
	assert.Len(t, repo.events, 1)
	assert.Equal(t, core.EventTaskClaimed, repo.events[0].EventType)
	assert.Equal(t, "task-1", repo.events[0].TaskID)
	assert.Equal(t, "trace-1", repo.events[0].TraceID)
	assert.Equal(t, "agent-1", repo.events[0].SourceAgent)
	assert.Contains(t, string(repo.events[0].Payload), "lease")
	repo.mu.Unlock()
}

func TestEventService_PublishEvent_NilPayload(t *testing.T) {
	repo := &mockEventRepo{}
	svc := NewEventService(repo)

	err := svc.PublishEvent(context.Background(), "acme", core.EventTaskCreated, "task-1", "", "", nil)
	require.NoError(t, err)

	repo.mu.Lock()
	assert.Equal(t, []byte(`{}`), repo.events[0].Payload)
	repo.mu.Unlock()
}

func TestEventService_QueryByTenant(t *testing.T) {
	repo := &mockEventRepo{events: []core.JanusEvent{
		{EventID: "e1", TenantID: "acme", EventType: core.EventTaskCreated},
	}}
	svc := NewEventService(repo)

	events, err := svc.QueryByTenant(context.Background(), "acme", 10)
	require.NoError(t, err)
	assert.Len(t, events, 1)
}

func TestEventService_QueryByTenant_DefaultLimit(t *testing.T) {
	repo := &mockEventRepo{}
	svc := NewEventService(repo)

	events, err := svc.QueryByTenant(context.Background(), "acme", 0)
	require.NoError(t, err)
	assert.Nil(t, events)
}
