package a2a

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/auth"
)

type spyRegistrar struct {
	registered []core.Agent
	err        error
}

func (s *spyRegistrar) Register(_ context.Context, agent core.Agent) error {
	s.registered = append(s.registered, agent)
	return s.err
}

type nilTaskSvc struct{}

func (nilTaskSvc) Create(_ context.Context, task core.Task) (*core.Task, error) { return &task, nil }
func (nilTaskSvc) Cancel(context.Context, string, string) error             { return nil }

// GA-01 P1: a body with a trailing JSON object bypasses the identity
// middleware's Unmarshal (empty claims = pass-through) while the A2A
// handler's Decoder.Decode reads only the first value. Both layers must
// now reject: readJSONLimit rejects trailing data (400), and even if the
// body somehow reaches the service, AgentService.Register's
// GuardAgentIdentity rejects a bound key registering a foreign agent.
func TestA2A_TrailingJSONCannotBypassIdentityBinding(t *testing.T) {
	reg := &spyRegistrar{}
	gw := NewGateway(reg, nilTaskSvc{})

	mkReq := func(principal auth.Principal, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/a2a/agent/card", strings.NewReader(body))
		ctx := auth.WithPrincipal(req.Context(), principal)
		ctx = context.WithValue(ctx, auth.TenantCtxKey, principal.TenantID)
		req = req.WithContext(ctx)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		gw.ServeHTTP(rec, req)
		return rec
	}

	boundKey := auth.Principal{TenantID: "acme", Scopes: []string{"task:write"}, BoundAgentID: "a1"}

	// Legitimate self-registration with clean JSON succeeds.
	rec := mkReq(boundKey, `{"id":"a1","name":"self","url":"https://a1.test"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("legitimate self-registration: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if len(reg.registered) != 1 || reg.registered[0].ID != "a1" {
		t.Fatalf("legitimate registration must reach the repository: %+v", reg.registered)
	}

	// Trailing JSON object: readJSONLimit must reject (400), repository untouched.
	reg.registered = nil
	rec = mkReq(boundKey, `{"id":"victim","name":"x","url":"https://example.test"} {}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("trailing-JSON bypass: want 400, got %d body=%s", rec.Code, rec.Body.String())
	}
	if len(reg.registered) != 0 {
		t.Fatalf("trailing-JSON bypass reached the repository: %+v", reg.registered)
	}

	// Trailing garbage (non-JSON after valid value).
	rec = mkReq(boundKey, `{"id":"victim","name":"x"} trailing-garbage`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("trailing garbage: want 400, got %d", rec.Code)
	}

	// Foreign agent ID with clean JSON: service-layer GuardAgentIdentity rejects.
	// (This path exercises the service layer; the gateway test uses the spy
	// registrar so we verify the service check separately in the service package.)
	reg.registered = nil
	rec = mkReq(boundKey, `{"id":"victim","name":"x","url":"https://example.test"}`)
	if rec.Code == http.StatusOK && len(reg.registered) > 0 && reg.registered[0].ID == "victim" {
		// The spy registrar doesn't implement the service check; the real
		// AgentService.Register does. This assertion documents that the
		// gateway alone does NOT reject foreign IDs — the service layer is
		// the authoritative check (tested in agent_service_test.go).
		t.Log("gateway passes foreign ID to service (service layer is the authority)")
	}

	// Unbound key can register any agent (backward compatibility).
	unbound := auth.Principal{TenantID: "acme", Scopes: []string{"task:write"}}
	reg.registered = nil
	rec = mkReq(unbound, `{"id":"anyone","name":"free","url":"https://any.test"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("unbound key registration: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
}
