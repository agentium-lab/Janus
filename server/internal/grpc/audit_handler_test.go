package grpc

import (
	"context"
	"testing"

	"github.com/agentium-lab/Janus/core"
	pb "github.com/agentium-lab/Janus/proto/gen/janus/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockAuditService struct {
	taskEvents   []*core.JanusEvent
	traceEvents  []*core.JanusEvent
	tenantEvents []*core.JanusEvent
	err          error
}

func (m *mockAuditService) QueryByTask(_ context.Context, tenantID, taskID string, limit int) ([]*core.JanusEvent, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.taskEvents, nil
}

func (m *mockAuditService) QueryByTrace(_ context.Context, tenantID, traceID string, limit int) ([]*core.JanusEvent, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.traceEvents, nil
}

func (m *mockAuditService) QueryByTenant(_ context.Context, tenantID string, limit int) ([]*core.JanusEvent, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.tenantEvents, nil
}

func makeTestEvent(id, eventType, tenantID, taskID, traceID string) *core.JanusEvent {
	return &core.JanusEvent{
		EventID:     id,
		EventType:   core.EventType(eventType),
		TenantID:    tenantID,
		TaskID:      taskID,
		TraceID:     traceID,
		SourceAgent: "agent-1",
		Payload:     []byte(`{"status":"completed"}`),
	}
}

func TestAuditServiceServer_ListTaskEvents(t *testing.T) {
	mock := &mockAuditService{
		taskEvents: []*core.JanusEvent{
			makeTestEvent("evt-1", "task.completed", "acme", "task-1", "trace-1"),
			makeTestEvent("evt-2", "task.started", "acme", "task-1", "trace-1"),
		},
	}
	s := NewAuditServiceServer(mock)

	resp, err := s.ListTaskEvents(context.Background(), &pb.ListTaskEventsRequest{
		TenantId: "acme",
		TaskId:   "task-1",
		PageSize: 10,
	})
	require.NoError(t, err)
	assert.Len(t, resp.Events, 2)
	assert.Equal(t, "evt-1", resp.Events[0].EventId)
}

func TestAuditServiceServer_ListTaskEvents_Empty(t *testing.T) {
	mock := &mockAuditService{}
	s := NewAuditServiceServer(mock)

	resp, err := s.ListTaskEvents(context.Background(), &pb.ListTaskEventsRequest{
		TenantId: "acme",
		TaskId:   "task-1",
	})
	require.NoError(t, err)
	assert.Len(t, resp.Events, 0)
}

func TestAuditServiceServer_GetTrace(t *testing.T) {
	mock := &mockAuditService{
		traceEvents: []*core.JanusEvent{
			makeTestEvent("evt-1", "task.created", "acme", "task-1", "trace-1"),
			makeTestEvent("evt-2", "task.completed", "acme", "task-1", "trace-1"),
		},
	}
	s := NewAuditServiceServer(mock)

	resp, err := s.GetTrace(context.Background(), &pb.GetTraceRequest{
		TenantId: "acme",
		TraceId:  "trace-1",
	})
	require.NoError(t, err)
	assert.Len(t, resp.Events, 2)
}

func TestAuditServiceServer_ListEvents_ByTask(t *testing.T) {
	mock := &mockAuditService{
		taskEvents: []*core.JanusEvent{
			makeTestEvent("evt-1", "task.completed", "acme", "task-1", "trace-1"),
		},
	}
	s := NewAuditServiceServer(mock)

	resp, err := s.ListEvents(context.Background(), &pb.ListEventsRequest{
		TenantId: "acme",
		TaskId:   "task-1",
		PageSize: 10,
	})
	require.NoError(t, err)
	assert.Len(t, resp.Events, 1)
}

func TestAuditServiceServer_ListEvents_ByTrace(t *testing.T) {
	mock := &mockAuditService{
		traceEvents: []*core.JanusEvent{
			makeTestEvent("evt-1", "task.created", "acme", "task-1", "trace-1"),
		},
	}
	s := NewAuditServiceServer(mock)

	resp, err := s.ListEvents(context.Background(), &pb.ListEventsRequest{
		TenantId: "acme",
		TraceId:  "trace-1",
	})
	require.NoError(t, err)
	assert.Len(t, resp.Events, 1)
}

func TestAuditServiceServer_ListEvents_NoFilter(t *testing.T) {
	mock := &mockAuditService{}
	s := NewAuditServiceServer(mock)

	resp, err := s.ListEvents(context.Background(), &pb.ListEventsRequest{
		TenantId: "acme",
	})
	require.NoError(t, err)
	assert.Len(t, resp.Events, 0)
}

func TestAuditServiceServer_ListEvents_ByTenant_WithEvents(t *testing.T) {
	mock := &mockAuditService{
		tenantEvents: []*core.JanusEvent{
			makeTestEvent("evt-1", "task.completed", "acme", "task-1", "trace-1"),
			makeTestEvent("evt-2", "task.started", "acme", "task-2", "trace-2"),
		},
	}
	s := NewAuditServiceServer(mock)

	resp, err := s.ListEvents(context.Background(), &pb.ListEventsRequest{
		TenantId: "acme",
		PageSize: 10,
	})
	require.NoError(t, err)
	assert.Len(t, resp.Events, 2)
	assert.Equal(t, "evt-1", resp.Events[0].EventId)
}

func TestAuditServiceServer_GetTrace_Error(t *testing.T) {
	mock := &mockAuditService{err: context.DeadlineExceeded}
	s := NewAuditServiceServer(mock)

	_, err := s.GetTrace(context.Background(), &pb.GetTraceRequest{
		TenantId: "acme",
		TraceId:  "trace-1",
	})
	assert.Error(t, err)
}

func TestAuditServiceServer_ListTaskEvents_Error(t *testing.T) {
	mock := &mockAuditService{err: context.DeadlineExceeded}
	s := NewAuditServiceServer(mock)

	_, err := s.ListTaskEvents(context.Background(), &pb.ListTaskEventsRequest{
		TenantId: "acme",
		TaskId:   "task-1",
	})
	assert.Error(t, err)
}
