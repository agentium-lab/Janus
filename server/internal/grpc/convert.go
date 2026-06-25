package grpc

import (
	"fmt"

	pb "github.com/agentium-lab/Janus/proto/gen/janus/v1"
	"github.com/agentium-lab/Janus/core"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// --- Agent conversions ---

func agentToProto(a *core.Agent) *pb.Agent {
	if a == nil {
		return nil
	}
	caps := make([]*pb.AgentCapability, len(a.Capabilities))
	for i, c := range a.Capabilities {
		caps[i] = &pb.AgentCapability{
			Capability:  c.Capability,
			Description: c.Description,
		}
	}
	pbAgent := &pb.Agent{
		Id:             a.ID,
		TenantId:       a.TenantID,
		TeamId:         a.TeamID,
		DisplayName:    a.DisplayName,
		Protocol:       string(a.Protocol),
		Endpoint:       a.Endpoint,
		Status:         string(a.Status),
		Description:    a.Description,
		Capabilities:   caps,
		MaxConcurrency: int32(a.MaxConcurrency),
		Rpm:            int32(a.RPM),
		Tpm:            int32(a.TPM),
		CreatedAt:      timestamppb.New(a.CreatedAt),
		UpdatedAt:      timestamppb.New(a.UpdatedAt),
	}
	if a.LastHeartbeatAt != nil {
		pbAgent.LastHeartbeatAt = timestamppb.New(*a.LastHeartbeatAt)
	}
	return pbAgent
}

func registerReqToAgent(req *pb.RegisterAgentRequest) core.Agent {
	caps := make([]core.AgentCapability, len(req.Capabilities))
	for i, c := range req.Capabilities {
		caps[i] = core.AgentCapability{
			Capability:  c.Capability,
			Description: c.Description,
		}
	}
	return core.Agent{
		TenantID:       req.TenantId,
		ID:             req.Id,
		TeamID:         req.TeamId,
		DisplayName:    req.DisplayName,
		Protocol:       core.AgentProtocol(req.Protocol),
		Endpoint:       req.Endpoint,
		Description:    req.Description,
		Capabilities:   caps,
		MaxConcurrency: int(req.MaxConcurrency),
		RPM:            int(req.Rpm),
		TPM:            int(req.Tpm),
	}
}

// --- Task / Envelope conversions ---

func taskToProto(t *core.Task) *pb.Task {
	if t == nil {
		return nil
	}
	pbTask := &pb.Task{
		TenantId:       t.TenantID,
		Id:             t.ID,
		IdempotencyKey: t.IdempotencyKey,
		SourceAgent:    t.SourceAgent,
		TargetType:     string(t.TargetType),
		TargetValue:    t.TargetValue,
		MailboxId:      t.MailboxID,
		Status:         string(t.Status),
		Priority:       string(t.Priority),
		TtlSeconds:     int32(t.TTLSeconds),
		AttemptCount:   int32(t.AttemptCount),
		CreatedAt:      timestamppb.New(t.CreatedAt),
		UpdatedAt:      timestamppb.New(t.UpdatedAt),
		ResultRef:      t.ResultRef,
	}
	if t.Deadline != nil {
		pbTask.Deadline = timestamppb.New(*t.Deadline)
	}
	if t.CompletedAt != nil {
		pbTask.CompletedAt = timestamppb.New(*t.CompletedAt)
	}
	if t.Error != nil {
		pbTask.Error = &pb.TaskError{
			Code:    t.Error.Code,
			Message: t.Error.Message,
		}
	}
	pbTask.Envelope = envelopeToProto(t.Envelope)
	return pbTask
}

func envelopeToProto(e core.TaskEnvelope) *pb.TaskEnvelope {
	pbEnv := &pb.TaskEnvelope{
		JanusVersion:   e.JanusVersion,
		TaskId:         e.TaskID,
		IdempotencyKey: e.IdempotencyKey,
		TenantId:       e.TenantID,
		SourceAgent:    e.SourceAgent,
		Priority:       string(e.Priority),
		TtlSeconds:     int32(e.TTLSeconds),
	}
	pbEnv.Target = &pb.Target{
		Type:  string(e.Target.Type),
		Value: e.Target.Value,
	}
	if e.Deadline != nil {
		pbEnv.Deadline = timestamppb.New(*e.Deadline)
	}
	if e.Budget != nil {
		pbEnv.Budget = &pb.Budget{
			MaxTokens:    int64(e.Budget.MaxTokens),
			MaxCostUsd:   e.Budget.MaxCostUSD,
			ModelClasses: e.Budget.ModelClasses,
		}
	}
	pbEnv.Trace = &pb.TraceContext{
		TraceId:      e.Trace.TraceID,
		ParentTaskId: e.Trace.ParentTaskID,
		SpanId:       e.Trace.SpanID,
	}
	pbEnv.Payload = &pb.TaskPayload{
		Type:    e.Payload.Type,
		Content: e.Payload.Content,
	}
	return pbEnv
}

func createTaskReqToCore(req *pb.CreateTaskRequest) (core.Task, error) {
	if req.Envelope == nil {
		return core.Task{}, fmt.Errorf("envelope is required")
	}
	e := req.Envelope
	env := core.TaskEnvelope{
		JanusVersion:   e.JanusVersion,
		TaskID:         e.TaskId,
		IdempotencyKey: e.IdempotencyKey,
		TenantID:       req.TenantId,
		SourceAgent:    e.SourceAgent,
		Priority:       core.Priority(e.Priority),
		TTLSeconds:     int(e.TtlSeconds),
	}
	if e.Target != nil {
		env.Target = core.Target{
			Type:  core.TargetType(e.Target.Type),
			Value: e.Target.Value,
		}
	}
	if e.Deadline != nil {
		dl := e.Deadline.AsTime()
		env.Deadline = &dl
	}
	if e.Budget != nil {
		env.Budget = &core.Budget{
			MaxTokens:    int(e.Budget.MaxTokens),
			MaxCostUSD:   e.Budget.MaxCostUsd,
			ModelClasses: e.Budget.ModelClasses,
		}
	}
	if e.Payload != nil {
		env.Payload = core.Payload{
			Type:    e.Payload.Type,
			Content: e.Payload.Content,
		}
	}
	task := core.Task{
		TenantID:       req.TenantId,
		ID:             e.TaskId,
		IdempotencyKey: e.IdempotencyKey,
		SourceAgent:    e.SourceAgent,
		Priority:       core.Priority(e.Priority),
		TTLSeconds:     int(e.TtlSeconds),
		Envelope:       env,
	}
	if e.Target != nil {
		task.TargetType = core.TargetType(e.Target.Type)
		task.TargetValue = e.Target.Value
		if task.TargetType == core.TargetTypeMailbox {
			task.MailboxID = task.TargetValue
		}
	}
	if e.Deadline != nil {
		dl := e.Deadline.AsTime()
		task.Deadline = &dl
	}
	return task, nil
}
