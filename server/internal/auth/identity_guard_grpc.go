package auth

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/agentium-lab/Janus/proto/gen/janus/v1"
)

// GRPC agent-identity carriers for the identity invariant. Every unary
// method of every service must appear in exactly one of these maps; the
// invariants suite enumerates the generated ServiceDescs and fails the
// build when a new method is undeclared.
var grpcAgentIdentityExtractors = map[string]func(req interface{}) string{
	"/janus.v1.AgentService/RegisterAgent": func(req interface{}) string {
		if r, ok := req.(*pb.RegisterAgentRequest); ok {
			return r.GetId()
		}
		return ""
	},
	"/janus.v1.AgentService/UpdateAgent": func(req interface{}) string {
		if r, ok := req.(*pb.UpdateAgentRequest); ok {
			return r.GetAgentId()
		}
		return ""
	},
	"/janus.v1.AgentService/Heartbeat": func(req interface{}) string {
		if r, ok := req.(*pb.HeartbeatRequest); ok {
			return r.GetAgentId()
		}
		return ""
	},
	"/janus.v1.DispatchService/PullTask": func(req interface{}) string {
		if r, ok := req.(*pb.PullTaskRequest); ok {
			return r.GetAgentId()
		}
		return ""
	},
	"/janus.v1.TaskService/CreateTask": func(req interface{}) string {
		if r, ok := req.(*pb.CreateTaskRequest); ok {
			return r.GetEnvelope().GetSourceAgent()
		}
		return ""
	},
}

// grpcMethodsWithoutAgentIdentity lists methods whose requests carry no
// caller-declared agent identity (read-only or agent-agnostic operations).
var grpcMethodsWithoutAgentIdentity = map[string]bool{
	"/janus.v1.AgentService/ListAgents":       true,
	"/janus.v1.AgentService/GetAgent":         true,
	"/janus.v1.AuditService/ListEvents":       true,
	"/janus.v1.AuditService/GetTrace":         true,
	"/janus.v1.AuditService/ListTaskEvents":   true,
	"/janus.v1.DispatchService/StartTask":     true,
	"/janus.v1.DispatchService/TaskHeartbeat": true,
	"/janus.v1.DispatchService/AckTask":       true,
	"/janus.v1.DispatchService/NackTask":      true,
	"/janus.v1.DLQService/QueryDLQ":           true,
	"/janus.v1.DLQService/ReplayDLQ":          true,
	"/janus.v1.DLQService/DiscardDLQ":         true,
	"/janus.v1.MailboxService/CreateMailbox":  true,
	"/janus.v1.MailboxService/GetMailbox":     true,
	"/janus.v1.MailboxService/UpdateMailbox":  true,
	"/janus.v1.MailboxService/PauseMailbox":   true,
	"/janus.v1.MailboxService/ResumeMailbox":  true,
	"/janus.v1.TaskService/GetTask":           true,
	"/janus.v1.TaskService/CancelTask":        true,
	"/janus.v1.TaskService/ReplayTask":        true,
}

// GRPCAgentIdentityClaim extracts the declared agent identity for a method,
// reporting whether the method carries one at all.
func GRPCAgentIdentityClaim(fullMethod string, req interface{}) (string, bool) {
	if extract, ok := grpcAgentIdentityExtractors[fullMethod]; ok {
		return extract(req), true
	}
	return "", false
}

// GRPCMethodHasNoAgentIdentity reports the exemption-list membership used
// by the completeness self-check in the invariants suite.
func GRPCMethodHasNoAgentIdentity(fullMethod string) bool {
	return grpcMethodsWithoutAgentIdentity[fullMethod]
}

// CheckGRPCAgentIdentity enforces the identity invariant inside the auth
// interceptor, sharing decision logic with the HTTP middleware.
func CheckGRPCAgentIdentity(ctx context.Context, fullMethod string, req interface{}) error {
	claim, carries := GRPCAgentIdentityClaim(fullMethod, req)
	if !carries || claim == "" {
		return nil
	}
	p, ok := PrincipalFromContext(ctx)
	if !ok {
		return nil
	}
	if err := p.CheckAgentIdentity(claim); err != nil {
		return status.Error(codes.PermissionDenied, err.Error())
	}
	return nil
}
