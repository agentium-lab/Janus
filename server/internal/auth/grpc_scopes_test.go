package auth

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	pb "github.com/agentium-lab/Janus/proto/gen/janus/v1"
)

// paritySurfaces pairs each named surface with its HTTP rule and gRPC
// method; the parity tests assert both protocols require the same scope.
var paritySurfaces = []struct {
	name       string
	httpMethod string
	httpPath   string
	grpcMethod string
	scope      string
}{
	{
		name:       "DLQ replay",
		httpMethod: http.MethodPost,
		httpPath:   "/v1/tenants/acme/dlq/e1/replay",
		grpcMethod: "/janus.v1.DLQService/ReplayDLQ",
		scope:      ScopeAdmin,
	},
	{
		name:       "DLQ discard",
		httpMethod: http.MethodPost,
		httpPath:   "/v1/tenants/acme/dlq/e1/discard",
		grpcMethod: "/janus.v1.DLQService/DiscardDLQ",
		scope:      ScopeAdmin,
	},
	{
		name:       "GetTrace",
		httpMethod: http.MethodGet,
		httpPath:   "/v1/tenants/acme/traces/tr-1",
		grpcMethod: "/janus.v1.AuditService/GetTrace",
		scope:      ScopeAuditRead,
	},
	{
		name:       "WS subscribe",
		httpMethod: http.MethodGet,
		httpPath:   "/ws",
		grpcMethod: "", // no gRPC twin: the /ws rule is the contract both ScopeGuard and the WS handler enforce
		scope:      ScopeTaskRead,
	},
}

func TestRequiredScope_ParityWithGRPC(t *testing.T) {
	for _, s := range paritySurfaces {
		httpScope, ok := RequiredScope(s.httpMethod, s.httpPath)
		require.True(t, ok, "%s: HTTP path must be scope-gated", s.name)
		assert.Equal(t, s.scope, httpScope, "%s: unexpected HTTP scope", s.name)

		if s.grpcMethod == "" {
			continue
		}
		assert.Equal(t, httpScope, RequiredScopeGRPC(s.grpcMethod),
			"%s: gRPC method must require the same scope as its HTTP counterpart", s.name)
	}
}

func TestGRPCMethodScopes_CoversEveryGeneratedMethod(t *testing.T) {
	descs := []grpc.ServiceDesc{
		pb.AgentService_ServiceDesc,
		pb.TaskService_ServiceDesc,
		pb.DispatchService_ServiceDesc,
		pb.AuditService_ServiceDesc,
		pb.MailboxService_ServiceDesc,
		pb.DLQService_ServiceDesc,
	}
	mapped := 0
	for _, d := range descs {
		for _, m := range d.Methods {
			full := "/" + d.ServiceName + "/" + m.MethodName
			_, ok := grpcMethodScopes[full]
			assert.True(t, ok, "%s is missing from grpcMethodScopes; map it to the scope of its HTTP counterpart", full)
			if ok {
				mapped++
			}
		}
	}
	assert.Equal(t, len(grpcMethodScopes), mapped,
		"grpcMethodScopes must not carry stale entries beyond the generated methods")
}

func TestRequiredScopeGRPC_UnknownMethodFailsClosed(t *testing.T) {
	assert.Equal(t, ScopeTaskWrite, RequiredScopeGRPC("/janus.v1.TaskService/UndocumentedFutureMutation"),
		"unmapped methods must fail closed to task:write, never task:read")
}
