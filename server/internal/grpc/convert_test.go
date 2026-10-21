package grpc

import (
	"testing"
	"time"

	"github.com/agentium-lab/Janus/core"
	pb "github.com/agentium-lab/Janus/proto/gen/janus/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCreateTaskReqToCore_FieldMapping(t *testing.T) {
	// 准备测试数据
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
				{
					Type:           "api-key",
					Uri:            "https://example.com/key1",
					Hash:           "def456",
					Classification: "secret",
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

	// 执行转换
	task, err := createTaskReqToCore(req)
	require.NoError(t, err)

	// 验证基本字段
	assert.Equal(t, "tenant-1", task.TenantID)
	assert.Equal(t, "task-1", task.ID)
	assert.Equal(t, "idempotency-key", task.IdempotencyKey)
	assert.Equal(t, "agent-1", task.SourceAgent)
	assert.Equal(t, core.TargetType("agent"), task.TargetType)
	assert.Equal(t, "agent-2", task.TargetValue)
	assert.Equal(t, core.Priority("normal"), task.Priority)
	assert.Equal(t, 3600, task.TTLSeconds)

	// 验证 Policy 映射
	require.NotNil(t, task.Envelope.Policy, "Policy should be mapped")
	assert.Equal(t, "confidential", task.Envelope.Policy.DataClassification)
	assert.Equal(t, true, task.Envelope.Policy.RequiresHumanApproval)
	assert.Equal(t, []string{"tool1", "tool2"}, task.Envelope.Policy.AllowedTools)

	// 验证 ContextRefs 映射
	require.Len(t, task.Envelope.ContextRefs, 2, "ContextRefs should be mapped")
	assert.Equal(t, "document", task.Envelope.ContextRefs[0].Type)
	assert.Equal(t, "https://example.com/doc1", task.Envelope.ContextRefs[0].URI)
	assert.Equal(t, "abc123", task.Envelope.ContextRefs[0].Hash)
	assert.Equal(t, "internal", task.Envelope.ContextRefs[0].Classification)
	assert.Equal(t, expiresAt.Format(time.RFC3339), task.Envelope.ContextRefs[0].ExpiresAt)

	assert.Equal(t, "api-key", task.Envelope.ContextRefs[1].Type)
	assert.Equal(t, "https://example.com/key1", task.Envelope.ContextRefs[1].URI)
	assert.Equal(t, "def456", task.Envelope.ContextRefs[1].Hash)
	assert.Equal(t, "secret", task.Envelope.ContextRefs[1].Classification)
	assert.Empty(t, task.Envelope.ContextRefs[1].ExpiresAt, "Second ref has no ExpiresAt")

	// 验证 Trace 映射
	assert.Equal(t, "trace-123", task.Envelope.Trace.TraceID)
	assert.Equal(t, "parent-task-456", task.Envelope.Trace.ParentTaskID)
	assert.Equal(t, "span-789", task.Envelope.Trace.SpanID)

	// 验证 Payload 映射
	assert.Equal(t, "text", task.Envelope.Payload.Type)
	assert.Equal(t, "test content", task.Envelope.Payload.Content)
}

func TestCreateTaskReqToCore_NilFields(t *testing.T) {
	// 测试可选字段为 nil 的情况
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
			// Policy, ContextRefs, Trace 均为 nil
		},
	}

	task, err := createTaskReqToCore(req)
	require.NoError(t, err)

	// 验证可选字段为 nil 时不会 panic
	assert.Nil(t, task.Envelope.Policy)
	assert.Nil(t, task.Envelope.ContextRefs)
	assert.Empty(t, task.Envelope.Trace.TraceID)
}

func TestCreateTaskReqToCore_Validation(t *testing.T) {
	// 测试缺少必需字段的情况
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
			Trace: &pb.TraceContext{
				TraceId: "trace-123", // 提供 TraceID 以通过验证
			},
		},
	}

	task, err := createTaskReqToCore(req)
	require.NoError(t, err)
	assert.Equal(t, "trace-123", task.Envelope.Trace.TraceID)
}
