package invariants

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/agentium-lab/Janus/core"
	janusv1 "github.com/agentium-lab/Janus/proto/gen/janus/v1"
	"github.com/agentium-lab/Janus/server/internal/auth"
)

// INVARIANT UNDER TEST (see server/internal/auth/identity_guard.go):
//
//	A caller with a bound agent identity may only declare that identity,
//	on every entrypoint of every protocol, no matter which carrier the
//	declaration travels in (query, body top level, envelope, metadata).
//
// Layer 4 of the framework: the gRPC half of this file enumerates the
// generated ServiceDescs and fails if any method is undeclared — new
// gRPC methods cannot appear without an identity-semantics decision.

const victim = "victim-agent"
const real = "real-agent"

func boundCtx(r *http.Request) *http.Request {
	ctx := context.WithValue(r.Context(), auth.TenantCtxKey, "acme")
	ctx = context.WithValue(ctx, auth.PrincipalCtxKey, auth.Principal{TenantID: "acme", BoundAgentID: real})
	return r.WithContext(ctx)
}

// httpSpoofCase describes one entrypoint attempt to act as the victim.
type httpSpoofCase struct {
	name   string
	method string
	path   string // may contain query parameters
	body   string
}

// The table IS the coverage claim. Every entrypoint that accepts a caller
// declared agent identity appears here; the middleware makes coverage
// structural, this table proves it behaviorally per protocol surface.
var httpSpoofCases = []httpSpoofCase{
	// HTTP core
	{name: "HTTP agent register", method: "POST", path: "/v1/tenants/acme/agents", body: `{"id":"` + victim + `","display_name":"V"}`},
	{name: "HTTP task create top-level", method: "POST", path: "/v1/tenants/acme/tasks", body: `{"id":"t1","source_agent":"` + victim + `","target_type":"mailbox","target_value":"m"}`},
	{name: "HTTP task create envelope split", method: "POST", path: "/v1/tenants/acme/tasks", body: `{"id":"t1","source_agent":"` + real + `","target_type":"mailbox","target_value":"m","envelope":{"source_agent":"` + victim + `"}}`},
	{name: "HTTP pull query", method: "POST", path: "/v1/tenants/acme/mailboxes/mb/pull?agent_id=" + victim, body: `{}`},
	{name: "HTTP pull body", method: "POST", path: "/v1/tenants/acme/mailboxes/mb/pull", body: `{"agent_id":"` + victim + `"}`},
	{name: "HTTP progress body", method: "POST", path: "/v1/tenants/acme/tasks/t1/progress", body: `{"message":"x","agent_id":"` + victim + `"}`},

	// A2A v1
	{name: "A2A v1 send metadata", method: "POST", path: "/a2a/message:send", body: `{"message":{"role":"ROLE_USER","parts":[]},"metadata":{"source_agent":"` + victim + `"}}`},
	{name: "A2A v1 stream query", method: "POST", path: "/a2a/message:stream?source_agent=" + victim, body: `{"message":{"role":"ROLE_USER","parts":[]}}`},

	// A2A legacy
	{name: "A2A legacy send query", method: "POST", path: "/a2a/task/send?source_agent=" + victim, body: `{"message":{"role":"user","parts":[]}}`},
	{name: "A2A legacy jsonrpc query", method: "POST", path: "/a2a/jsonrpc?source_agent=" + victim, body: `{"jsonrpc":"2.0","method":"message/send","id":1,"params":{}}`},
	{name: "A2A legacy agent card", method: "POST", path: "/a2a/agent/card", body: `{"id":"` + victim + `","name":"V","url":"http://x"}`},

	// ACP
	{name: "ACP run query", method: "POST", path: "/acp/run?source_agent=" + victim, body: `{"target":"x","input":"y"}`},

	// MCP
	{name: "MCP tools/call query", method: "POST", path: "/mcp/tools/call?source_agent=" + victim, body: `{"name":"t","arguments":{}}`},
}

func TestIdentityInvariant_HTTP_AllEntrypoints(t *testing.T) {
	// The middleware runs in front of a sink that would otherwise accept
	// anything; the invariant must reject BEFORE any handler logic, so the
	// handler behind it is irrelevant to the test.
	sink := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = boundCtx(r)
		auth.AgentIdentityMiddleware(sink).ServeHTTP(w, r)
	}))
	defer ts.Close()

	for _, tc := range httpSpoofCases {
		t.Run(tc.name, func(t *testing.T) {
			var body *strings.Reader
			if tc.body != "" {
				body = strings.NewReader(tc.body)
			} else {
				body = strings.NewReader("")
			}
			req, err := http.NewRequest(tc.method, ts.URL+tc.path, body)
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			resp.Body.Close()
			assert.Equal(t, http.StatusForbidden, resp.StatusCode,
				"identity invariant violated: bound %q may not act as %q", real, victim)
		})
	}
}

func TestIdentityInvariant_HTTP_LegitimateAndUnbound(t *testing.T) {
	reached := false
	sink := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	})

	t.Run("own identity passes", func(t *testing.T) {
		reached = false
		r := httptest.NewRequest("POST", "/v1/tenants/acme/tasks", bytes.NewReader([]byte(`{"source_agent":"`+real+`"}`)))
		r = boundCtx(r)
		w := httptest.NewRecorder()
		auth.AgentIdentityMiddleware(sink).ServeHTTP(w, r)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.True(t, reached, "legitimate request must reach the handler")
	})

	t.Run("unbound principal passes (backward compat)", func(t *testing.T) {
		reached = false
		r := httptest.NewRequest("POST", "/v1/tenants/acme/tasks", bytes.NewReader([]byte(`{"source_agent":"`+victim+`"}`)))
		ctx := context.WithValue(r.Context(), auth.PrincipalCtxKey, auth.Principal{TenantID: "acme"})
		r = r.WithContext(ctx)
		w := httptest.NewRecorder()
		auth.AgentIdentityMiddleware(sink).ServeHTTP(w, r)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.True(t, reached)
	})

	t.Run("body readable downstream after extraction", func(t *testing.T) {
		r := httptest.NewRequest("POST", "/x", bytes.NewReader([]byte(`{"source_agent":"`+real+`"}`)))
		r = boundCtx(r)
		w := httptest.NewRecorder()
		auth.AgentIdentityMiddleware(sink).ServeHTTP(w, r)
		buf := make([]byte, 64)
		n, _ := r.Body.Read(buf)
		assert.Contains(t, string(buf[:n]), real, "middleware must restore the body for downstream handlers")
	})
}

// Layer 4: completeness self-check. Enumerate every unary method of every
// generated gRPC ServiceDesc and require an identity-semantics decision:
// extractor (carries identity) or explicit exemption. A new method that
// forgets to declare itself fails here, not in production.
func TestIdentityInvariant_GRPC_EveryMethodDeclared(t *testing.T) {
	descs := []grpc.ServiceDesc{
		janusv1.AgentService_ServiceDesc,
		janusv1.AuditService_ServiceDesc,
		janusv1.DispatchService_ServiceDesc,
		janusv1.DLQService_ServiceDesc,
		janusv1.MailboxService_ServiceDesc,
		janusv1.TaskService_ServiceDesc,
	}
	require.NotEmpty(t, descs)
	for i := range descs {
		sd := &descs[i]
		for _, m := range sd.Methods {
			full := "/" + sd.ServiceName + "/" + m.MethodName
			_, carries := auth.GRPCAgentIdentityClaim(full, nil)
			exempt := auth.GRPCMethodHasNoAgentIdentity(full)
			require.True(t, carries || exempt,
				"gRPC method %s is undeclared: add an extractor to identity_guard_grpc.go or an explicit exemption", full)
		}
	}
}

func TestIdentityInvariant_GRPC_SpoofRejected(t *testing.T) {
	claimCases := []struct {
		method string
		req    interface{}
	}{
		{"/janus.v1.AgentService/RegisterAgent", &janusv1.RegisterAgentRequest{Id: victim}},
		{"/janus.v1.AgentService/UpdateAgent", &janusv1.UpdateAgentRequest{AgentId: victim}},
		{"/janus.v1.AgentService/Heartbeat", &janusv1.HeartbeatRequest{AgentId: victim}},
		{"/janus.v1.DispatchService/PullTask", &janusv1.PullTaskRequest{AgentId: victim}},
		{"/janus.v1.TaskService/CreateTask", &janusv1.CreateTaskRequest{Envelope: &janusv1.TaskEnvelope{SourceAgent: victim}}},
	}
	for _, tc := range claimCases {
		t.Run(tc.method, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), auth.PrincipalCtxKey, auth.Principal{BoundAgentID: real})
			err := auth.CheckGRPCAgentIdentity(ctx, tc.method, tc.req)
			require.Error(t, err, "bound %q must not act as %q over gRPC", real, victim)
		})
	}
}

var _ = core.Task{}
