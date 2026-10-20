package auth

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/agentium-lab/Janus/proto/gen/janus/v1"
)

func TestGRPCAgentIdentityClaim_Extractors(t *testing.T) {
	cases := []struct {
		method string
		req    interface{}
		want   string
	}{
		{"/janus.v1.AgentService/RegisterAgent", &pb.RegisterAgentRequest{Id: "a1"}, "a1"},
		{"/janus.v1.AgentService/UpdateAgent", &pb.UpdateAgentRequest{AgentId: "a2"}, "a2"},
		{"/janus.v1.AgentService/Heartbeat", &pb.HeartbeatRequest{AgentId: "a3"}, "a3"},
		{"/janus.v1.DispatchService/PullTask", &pb.PullTaskRequest{AgentId: "a4"}, "a4"},
		{"/janus.v1.TaskService/CreateTask", &pb.CreateTaskRequest{Envelope: &pb.TaskEnvelope{SourceAgent: "a5"}}, "a5"},
	}
	for _, tc := range cases {
		claim, carries := GRPCAgentIdentityClaim(tc.method, tc.req)
		require.True(t, carries, tc.method)
		assert.Equal(t, tc.want, claim, tc.method)
	}

	claim, carries := GRPCAgentIdentityClaim("/janus.v1.AgentService/RegisterAgent", &pb.HeartbeatRequest{})
	require.True(t, carries, "carrier method with wrong request type still reports carries")
	assert.Empty(t, claim)

	claim, carries = GRPCAgentIdentityClaim("/janus.v1.TaskService/GetTask", &pb.GetTaskRequest{})
	assert.False(t, carries)
	assert.Empty(t, claim)
}

func TestGRPCMethodHasNoAgentIdentity(t *testing.T) {
	assert.True(t, GRPCMethodHasNoAgentIdentity("/janus.v1.TaskService/GetTask"))
	assert.True(t, GRPCMethodHasNoAgentIdentity("/janus.v1.DLQService/ReplayDLQ"))
	assert.False(t, GRPCMethodHasNoAgentIdentity("/janus.v1.AgentService/RegisterAgent"))
	assert.False(t, GRPCMethodHasNoAgentIdentity("/janus.v1.Unknown/Method"))
}

func TestCheckGRPCAgentIdentity(t *testing.T) {
	method := "/janus.v1.AgentService/RegisterAgent"
	req := &pb.RegisterAgentRequest{Id: "a1"}

	assert.NoError(t, CheckGRPCAgentIdentity(context.Background(), method, req), "no principal: no enforcement")

	unbound := context.WithValue(context.Background(), PrincipalCtxKey, Principal{})
	assert.NoError(t, CheckGRPCAgentIdentity(unbound, method, req), "unbound principal: no enforcement")

	bound := context.WithValue(context.Background(), PrincipalCtxKey, Principal{BoundAgentID: "a1"})
	assert.NoError(t, CheckGRPCAgentIdentity(bound, method, req), "matching identity")

	mismatch := context.WithValue(context.Background(), PrincipalCtxKey, Principal{BoundAgentID: "someone-else"})
	err := CheckGRPCAgentIdentity(mismatch, method, req)
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))

	assert.NoError(t, CheckGRPCAgentIdentity(mismatch, method, &pb.RegisterAgentRequest{}), "empty claim never enforces")
	assert.NoError(t, CheckGRPCAgentIdentity(mismatch, "/janus.v1.TaskService/GetTask", &pb.GetTaskRequest{}), "exempt method carries no claim")
}

func TestCheckAgentIdentityRequest_Variants(t *testing.T) {
	assert.NoError(t, CheckAgentIdentityRequest("", []string{"anyone"}))
	assert.NoError(t, CheckAgentIdentityRequest("a1", []string{"a1", "a1"}))
	require.Error(t, CheckAgentIdentityRequest("a1", []string{"a1", "a2"}))
}
