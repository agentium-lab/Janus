package grpc

import (
	"testing"
	"time"

	pb "github.com/agentium-lab/Janus/proto/gen/janus/v1"
	"github.com/agentium-lab/Janus/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestAgentToProto_Nil(t *testing.T) {
	assert.Nil(t, agentToProto(nil))
}

func TestAgentToProto_WithHeartbeat(t *testing.T) {
	hb := time.Now()
	agent := &core.Agent{
		TenantID:       "acme",
		ID:             "a1",
		DisplayName:    "test",
		Protocol:       core.ProtocolA2A,
		Endpoint:       "http://localhost",
		Status:         core.AgentStatusOnline,
		Capabilities:   []core.AgentCapability{{Capability: "chat"}},
		MaxConcurrency: 5,
		RPM:            100,
		TPM:            10000,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
		LastHeartbeatAt: &hb,
	}
	pbAgent := agentToProto(agent)
	assert.Equal(t, "a1", pbAgent.Id)
	assert.NotNil(t, pbAgent.LastHeartbeatAt)
	assert.Len(t, pbAgent.Capabilities, 1)
}

func TestTaskToProto_WithDeadline(t *testing.T) {
	dl := time.Now()
	task := &core.Task{
		TenantID:   "acme",
		ID:         "t1",
		Status:     core.TaskStatusRunning,
		Deadline:   &dl,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
		CompletedAt: &dl,
		Error:      &core.TaskError{Code: "ERR", Message: "fail"},
		Envelope: core.TaskEnvelope{
			JanusVersion: "0.4.0",
			Target:       core.Target{Type: "agent", Value: "a1"},
			Payload:      core.Payload{Type: "text", Content: "hi"},
			Trace:        core.TraceContext{TraceID: "t1"},
		},
	}
	pbTask := taskToProto(task)
	assert.Equal(t, "t1", pbTask.Id)
	assert.Equal(t, "running", pbTask.Status)
	assert.NotNil(t, pbTask.Deadline)
	assert.NotNil(t, pbTask.CompletedAt)
	assert.Equal(t, "ERR", pbTask.Error.Code)
	assert.NotNil(t, pbTask.Envelope)
}

func TestTaskToProto_Nil(t *testing.T) {
	assert.Nil(t, taskToProto(nil))
}

func TestCreateTaskReqToCore_NoEnvelope(t *testing.T) {
	_, err := createTaskReqToCore(&pb.CreateTaskRequest{TenantId: "acme"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "envelope")
}

func TestCreateTaskReqToCore_WithBudget(t *testing.T) {
	dl := timestamppb.Now()
	task, err := createTaskReqToCore(&pb.CreateTaskRequest{
		TenantId: "acme",
		Envelope: &pb.TaskEnvelope{
			TaskId:   "t1",
			Priority: "high",
			Target:   &pb.Target{Type: "agent", Value: "a1"},
			Deadline: dl,
			Budget:   &pb.Budget{MaxTokens: 1000, MaxCostUsd: 5.0, ModelClasses: []string{"gpt-4"}},
			Payload:  &pb.TaskPayload{Type: "text", Content: "hi"},
			Trace:    &pb.TraceContext{TraceId: "t1"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "acme", task.TenantID)
	assert.Equal(t, "t1", task.ID)
	assert.Equal(t, core.PriorityHigh, task.Priority)
	assert.Equal(t, core.TargetTypeAgent, task.TargetType)
	assert.NotNil(t, task.Deadline)
	assert.NotNil(t, task.Envelope.Budget)
	assert.Equal(t, 1000, task.Envelope.Budget.MaxTokens)
	assert.Equal(t, 5.0, task.Envelope.Budget.MaxCostUSD)
	assert.Equal(t, []string{"gpt-4"}, task.Envelope.Budget.ModelClasses)
}

func TestCreateTaskReqToCore_Minimal(t *testing.T) {
	task, err := createTaskReqToCore(&pb.CreateTaskRequest{
		TenantId: "acme",
		Envelope: &pb.TaskEnvelope{
			TaskId: "t1",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "t1", task.ID)
	assert.Nil(t, task.Deadline)
}

func TestEnvelopeToProto_NilDeadline(t *testing.T) {
	env := core.TaskEnvelope{
		JanusVersion:   "0.4.0",
		TaskID:         "t1",
		TenantID:       "acme",
		Target:         core.Target{Type: "agent", Value: "a1"},
		Priority:       core.PriorityHigh,
		TTLSeconds:     300,
		Budget:         nil, // explicitly nil
		Trace:          core.TraceContext{TraceID: "trace-1", ParentTaskID: "parent-1", SpanID: "span-1"},
		Payload:        core.Payload{Type: "text", Content: "hello"},
	}
	pbEnv := envelopeToProto(env)
	assert.Equal(t, "t1", pbEnv.TaskId)
	assert.Equal(t, "acme", pbEnv.TenantId)
	assert.Equal(t, "high", pbEnv.Priority)
	assert.Nil(t, pbEnv.Deadline) // nil deadline path
	assert.Nil(t, pbEnv.Budget)    // nil budget path
	assert.Equal(t, "trace-1", pbEnv.Trace.TraceId)
	assert.Equal(t, "parent-1", pbEnv.Trace.ParentTaskId)
}

func TestEnvelopeToProto_NilBudget(t *testing.T) {
	dl := time.Now()
	env := core.TaskEnvelope{
		JanusVersion: "0.4.0",
		TaskID:       "t1",
		TenantID:     "acme",
		Target:       core.Target{Type: "agent", Value: "a1"},
		Priority:     core.PriorityNormal,
		TTLSeconds:    60,
		Deadline:      &dl,
		Budget:        nil, // explicitly nil
		Trace:         core.TraceContext{},
		Payload:       core.Payload{Type: "text", Content: "hi"},
	}
	pbEnv := envelopeToProto(env)
	assert.NotNil(t, pbEnv.Deadline)  // deadline is set
	assert.Nil(t, pbEnv.Budget)        // budget is nil
}
