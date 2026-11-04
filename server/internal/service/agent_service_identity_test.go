package service

import (
	"context"
	"strings"
	"testing"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/auth"
)

type spyAgentRepo struct {
	registered []core.Agent
}

func (s *spyAgentRepo) Register(_ context.Context, agent core.Agent) error {
	s.registered = append(s.registered, agent)
	return nil
}
func (s *spyAgentRepo) Get(context.Context, string, string) (*core.Agent, error) { return nil, nil }
func (s *spyAgentRepo) UpsertCapabilities(context.Context, core.Agent) error     { return nil }
func (s *spyAgentRepo) List(context.Context, string) ([]*core.Agent, error)     { return nil, nil }
func (s *spyAgentRepo) ListByStatus(context.Context, string, core.AgentStatus) ([]*core.Agent, error) {
	return nil, nil
}
func (s *spyAgentRepo) ListAllByStatus(context.Context, core.AgentStatus) ([]*core.Agent, error) {
	return nil, nil
}
func (s *spyAgentRepo) UpdateHeartbeat(context.Context, string, string) error { return nil }
func (s *spyAgentRepo) UpdateStatus(context.Context, string, string, core.AgentStatus) error {
	return nil
}
func (s *spyAgentRepo) FindByCapability(context.Context, string, string) ([]*core.Agent, error) {
	return nil, nil
}

// GA-01 service-layer identity authority: a key bound to agent a1 must not
// register any other agent id — the service is the last line of defense and
// must reject BEFORE the repository is touched, no matter which gateway
// decoded the request.
func TestAgentService_Register_BoundKeyCannotRegisterForeignAgent(t *testing.T) {
	repo := &spyAgentRepo{}
	svc := NewAgentService(repo, nil, nil, nil)

	ctx := auth.WithPrincipal(context.Background(), auth.Principal{
		TenantID: "acme", Scopes: []string{"task:write"}, BoundAgentID: "a1",
	})

	// Given a bound key registering its OWN id, the repository is reached.
	if err := svc.Register(ctx, core.Agent{TenantID: "acme", ID: "a1", DisplayName: "self"}); err != nil {
		t.Fatalf("self registration: %v", err)
	}
	if len(repo.registered) != 1 || repo.registered[0].ID != "a1" {
		t.Fatalf("self registration must reach the repository once: %+v", repo.registered)
	}

	// Given the same bound key registering a FOREIGN id, the service
	// rejects and the repository is never called.
	err := svc.Register(ctx, core.Agent{TenantID: "acme", ID: "victim", DisplayName: "x"})
	if err == nil || !strings.Contains(err.Error(), "bound to agent") {
		t.Fatalf("foreign registration must be rejected with a binding error, got: %v", err)
	}
	if len(repo.registered) != 1 {
		t.Fatalf("foreign registration reached the repository: %+v", repo.registered)
	}

	// Given an UNBOUND key, registration of any agent still works.
	unbound := auth.WithPrincipal(context.Background(), auth.Principal{
		TenantID: "acme", Scopes: []string{"task:write"},
	})
	if err := svc.Register(unbound, core.Agent{TenantID: "acme", ID: "anyone", DisplayName: "y"}); err != nil {
		t.Fatalf("unbound registration: %v", err)
	}
	if len(repo.registered) != 2 {
		t.Fatalf("unbound registration must reach the repository: %+v", repo.registered)
	}

	// Given NO principal in context (auth disabled / internal call),
	// registration passes — the guard only constrains bound principals.
	if err := svc.Register(context.Background(), core.Agent{TenantID: "acme", ID: "internal", DisplayName: "z"}); err != nil {
		t.Fatalf("principal-free registration: %v", err)
	}
}
