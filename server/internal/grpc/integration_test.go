package grpc

import (
	"context"
	"testing"
	"time"

	pb "github.com/agentium-lab/Janus/proto/gen/janus/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestGRPCCreateTask_WithTrace tests that gRPC CreateTask correctly maps trace fields
// and passes validation (which requires TraceID to be non-empty).
func TestGRPCCreateTask_WithTrace(t *testing.T) {
	mock := &mockTaskService{}
	s := &TaskServiceServer{svc: mock}

	expiresAt := time.Now().Add(24 * time.Hour).UTC()
	req := &pb.CreateTaskRequest{
		TenantId: "tenant-1",
		Envelope: &pb.TaskEnvelope{
			JanusVersion:   "0.4.0",
			TaskId:         "task-1",
			IdempotencyKey: "idempotency-key",
			SourceAgent:    "agent-1",
			Target: &pb.Target{
				Type:  "agent",
				Value: "agent-2",
			},
			Priority:   "normal",
			TtlSeconds: 3600,
			Policy: &pb.PolicyContext{
				DataClassification:    "confidential",
				RequiresHumanApproval: true,
				AllowedTools:          []string{"tool1", "tool2"},
			},
			ContextRefs: []*pb.ContextRef{
				{
					Type:           "document",
					Uri:            "https://example.com/doc1",
					Hash:           "abc123",
					Classification: "internal",
					ExpiresAt:      timestamppb.New(expiresAt),
				},
			},
			Trace: &pb.TraceContext{
				TraceId:      "trace-123",
				ParentTaskId: "parent-task-456",
				SpanId:       "span-789",
			},
			Payload: &pb.TaskPayload{
				Type:    "text",
				Content: "test content",
			},
		},
	}

	// 调用 CreateTask
	resp, err := s.CreateTask(context.Background(), req)
	require.NoError(t, err, "CreateTask should succeed with trace fields")
	require.NotNil(t, resp)

	// 验证返回的任务
	assert.Equal(t, "tenant-1", resp.TenantId)
	assert.Equal(t, "task-1", resp.Id)

	// 验证存储的任务包含完整的 envelope
	storedTask := mock.tasks["tenant-1:task-1"]
	require.NotNil(t, storedTask)

	// 验证 Policy 映射
	require.NotNil(t, storedTask.Envelope.Policy)
	assert.Equal(t, "confidential", storedTask.Envelope.Policy.DataClassification)
	assert.Equal(t, true, storedTask.Envelope.Policy.RequiresHumanApproval)
	assert.Equal(t, []string{"tool1", "tool2"}, storedTask.Envelope.Policy.AllowedTools)

	// 验证 ContextRefs 映射
	require.Len(t, storedTask.Envelope.ContextRefs, 1)
	assert.Equal(t, "document", storedTask.Envelope.ContextRefs[0].Type)
	assert.Equal(t, "https://example.com/doc1", storedTask.Envelope.ContextRefs[0].URI)
	assert.Equal(t, "abc123", storedTask.Envelope.ContextRefs[0].Hash)
	assert.Equal(t, "internal", storedTask.Envelope.ContextRefs[0].Classification)
	assert.Equal(t, expiresAt.Format(time.RFC3339), storedTask.Envelope.ContextRefs[0].ExpiresAt)

	// 验证 Trace 映射
	assert.Equal(t, "trace-123", storedTask.Envelope.Trace.TraceID)
	assert.Equal(t, "parent-task-456", storedTask.Envelope.Trace.ParentTaskID)
	assert.Equal(t, "span-789", storedTask.Envelope.Trace.SpanID)
}

// TestGRPCCreateTask_WithoutTrace tests that gRPC CreateTask fails validation
// when trace.trace_id is missing (as required by core/task.go:302).
func TestGRPCCreateTask_WithoutTrace(t *testing.T) {
	mock := &mockTaskService{}
	s := &TaskServiceServer{svc: mock}

	req := &pb.CreateTaskRequest{
		TenantId: "tenant-1",
		Envelope: &pb.TaskEnvelope{
			JanusVersion: "0.4.0",
			TaskId:       "task-1",
			SourceAgent:  "agent-1",
			Target: &pb.Target{
				Type:  "agent",
				Value: "agent-2",
			},
			Priority: "normal",
			Payload: &pb.TaskPayload{
				Type:    "text",
				Content: "test",
			},
			// Trace 字段缺失
		},
	}

	// 调用 CreateTask 应该失败（因为 Validate 要求 TraceID 非空）
	_, err := s.CreateTask(context.Background(), req)
	require.Error(t, err, "CreateTask should fail without trace.trace_id")
	assert.Contains(t, err.Error(), "trace.trace_id", "Error should mention trace.trace_id validation")
}

// TestGRPCCreateTask_EmptyTraceID tests that gRPC CreateTask fails validation
// when trace is provided but trace_id is empty.
func TestGRPCCreateTask_EmptyTraceID(t *testing.T) {
	mock := &mockTaskService{}
	s := &TaskServiceServer{svc: mock}

	req := &pb.CreateTaskRequest{
		TenantId: "tenant-1",
		Envelope: &pb.TaskEnvelope{
			JanusVersion: "0.4.0",
			TaskId:       "task-1",
			SourceAgent:  "agent-1",
			Target: &pb.Target{
				Type:  "agent",
				Value: "agent-2",
			},
			Priority: "normal",
			Trace: &pb.TraceContext{
				TraceId: "", // 空 TraceID
				SpanId:  "span-1",
			},
			Payload: &pb.TaskPayload{
				Type:    "text",
				Content: "test",
			},
		},
	}

	// 调用 CreateTask 应该失败
	_, err := s.CreateTask(context.Background(), req)
	require.Error(t, err, "CreateTask should fail with empty trace_id")
	assert.Contains(t, err.Error(), "trace.trace_id", "Error should mention trace.trace_id validation")
}
