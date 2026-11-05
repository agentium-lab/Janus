package auth

import (
	"context"
	"strings"
	"testing"

	pb "github.com/agentium-lab/Janus/proto/gen/janus/v1"
)

// B1 cross-protocol parity: gRPC CreateMailbox must enforce the SAME bound
// agent identity as the HTTP surface — a key bound to a1 creating a
// mailbox owned by a2 is rejected, and self-owned creation passes.
func TestCheckGRPCAgentIdentity_CreateMailboxOwner(t *testing.T) {
	bound := WithPrincipal(context.Background(), Principal{
		TenantID: "acme", Scopes: []string{"task:write"}, BoundAgentID: "a1",
	})

	foreign := &pb.CreateMailboxRequest{AgentId: "a2"}
	err := CheckGRPCAgentIdentity(bound, "/janus.v1.MailboxService/CreateMailbox", foreign)
	if err == nil || !strings.Contains(err.Error(), "bound to agent") {
		t.Fatalf("foreign owner must be rejected, got: %v", err)
	}

	self := &pb.CreateMailboxRequest{AgentId: "a1"}
	if err := CheckGRPCAgentIdentity(bound, "/janus.v1.MailboxService/CreateMailbox", self); err != nil {
		t.Fatalf("self-owned mailbox must pass, got: %v", err)
	}

	// unbound keys and agentless requests keep working
	unbound := WithPrincipal(context.Background(), Principal{TenantID: "acme"})
	if err := CheckGRPCAgentIdentity(unbound, "/janus.v1.MailboxService/CreateMailbox", foreign); err != nil {
		t.Fatalf("unbound key must not be constrained, got: %v", err)
	}
	if err := CheckGRPCAgentIdentity(bound, "/janus.v1.MailboxService/CreateMailbox", &pb.CreateMailboxRequest{}); err != nil {
		t.Fatalf("agentless request must pass, got: %v", err)
	}
}
