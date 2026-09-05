package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/auth"
	"github.com/stretchr/testify/assert"
)

// The v1.5.0 review: source_agent came from query params or metadata while
// the API key only proved scope — a scoped key could impersonate any agent.
// F27 binds keys to agents at creation; a bound key may only act as itself.

func principalCtx(r *http.Request, p auth.Principal) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), auth.PrincipalCtxKey, p))
}

func TestProgressHandler_BoundKeyImpersonationRejected(t *testing.T) {
	svc := &fakeProgressSvc{}
	h := NewProgressHandler(svc, &recordingPublisher{})

	body := `{"message":"forging progress","agent_id":"victim-agent"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/t1/progress", strings.NewReader(body))
	req = principalCtx(req, auth.Principal{TenantID: "acme", BoundAgentID: "real-agent"})
	w := httptest.NewRecorder()
	h.Report(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Contains(t, w.Body.String(), "real-agent")
	assert.Zero(t, svc.calls, "service must not be called on identity mismatch")
}

func TestProgressHandler_BoundKeyOwnIdentityAllowed(t *testing.T) {
	svc := &fakeProgressSvc{}
	h := NewProgressHandler(svc, &recordingPublisher{})

	body := `{"message":"legit","agent_id":"real-agent"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/t1/progress", strings.NewReader(body))
	req = principalCtx(req, auth.Principal{TenantID: "acme", BoundAgentID: "real-agent"})
	w := httptest.NewRecorder()
	h.Report(w, req)

	assert.Equal(t, http.StatusAccepted, w.Code)
	assert.Equal(t, 1, svc.calls)
}

func TestProgressHandler_UnboundKeyBackwardCompatible(t *testing.T) {
	svc := &fakeProgressSvc{}
	h := NewProgressHandler(svc, &recordingPublisher{})

	body := `{"message":"legacy","agent_id":"any-agent"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/t1/progress", strings.NewReader(body))
	req = principalCtx(req, auth.Principal{TenantID: "acme"})
	w := httptest.NewRecorder()
	h.Report(w, req)

	assert.Equal(t, http.StatusAccepted, w.Code)
}

func TestPrincipal_CheckAgentIdentity(t *testing.T) {
	p := auth.Principal{BoundAgentID: "a1"}
	assert.NoError(t, p.CheckAgentIdentity("a1"))
	assert.Error(t, p.CheckAgentIdentity("a2"))
	assert.NoError(t, p.CheckAgentIdentity(""), "empty claim (system/default) passes")

	unbound := auth.Principal{}
	assert.NoError(t, unbound.CheckAgentIdentity("anyone"))
}

func TestTaskHandler_CreateBoundKeyImpersonation(t *testing.T) {
	h := NewTaskHandler(&fakeTaskSvcIdentity{})

	body := `{"id":"t1","source_agent":"victim-agent","target_type":"mailbox","target_value":"mb"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks", strings.NewReader(body))
	req = principalCtx(req, auth.Principal{TenantID: "acme", BoundAgentID: "real-agent"})
	w := httptest.NewRecorder()
	h.Create(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)
}

type fakeTaskSvcIdentity struct{ mockTaskService }

func (f *fakeTaskSvcIdentity) Create(_ context.Context, _ core.Task) (*core.Task, error) {
	return &core.Task{TenantID: "acme", ID: "t1", Status: core.TaskStatusQueued}, nil
}
