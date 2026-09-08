package invariants

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/auth"
	"github.com/agentium-lab/Janus/server/internal/service"
)

// CASE-VARIANT INVARIANT (eighth review P0):
//
//	Go's encoding/json matches struct fields case-insensitively, so a body
//	can declare an agent identity via case-variant keys ("Envelope",
//	"SOURCE_AGENT") that a case-sensitive extractor cannot see. The
//	extractor MUST resolve identities case-insensitively AND reject
//
// bodies that use case aliases of identity paths (ambiguity is hostile).
// Contract: identity-related keys (source_agent, agent_id, envelope,
// metadata, id-on-registration-paths) must be spelled exactly as the
// canonical JSON tags. The extractor resolves them case-insensitively for
// safety, but ANY case variant is rejected 400 — legitimate clients never
// case-variant these names, and ambiguity is hostile.
var caseVariantBodies = []struct {
	name     string
	body     string
	mustFail bool
}{
	{
		name:     "uppercase envelope key (original P0)",
		body:     `{"source_agent":"real-agent","Envelope":{"SOURCE_AGENT":"victim-agent"}}`,
		mustFail: true,
	},
	{
		name:     "uppercase top-level agent_id alone",
		body:     `{"Agent_ID":"victim-agent"}`,
		mustFail: true,
	},
	{
		name:     "mixed-case nested metadata",
		body:     `{"message":{"parts":[]},"Metadata":{"Source_Agent":"victim-agent"}}`,
		mustFail: true,
	},
	{
		name:     "same path both cases same value",
		body:     `{"source_agent":"x","Source_Agent":"x"}`,
		mustFail: true,
	},
	{
		name:     "canonical lowercase passes",
		body:     `{"source_agent":"real-agent","envelope":{"source_agent":"real-agent"}}`,
		mustFail: false,
	},
}

func TestIdentityInvariant_CaseVariantExtraction(t *testing.T) {
	for _, tc := range caseVariantBodies {
		t.Run(tc.name, func(t *testing.T) {
			claims, ambiguous, _ := auth.InspectAgentIdentity(
				httptest.NewRequest("POST", "/v1/tenants/acme/tasks", strings.NewReader(tc.body)))
			if tc.mustFail {
				require.True(t, ambiguous, "case-variant identity keys must be rejected; got claims %v", claims)
				return
			}
			require.False(t, ambiguous, "canonical lowercase must pass cleanly")
			require.Contains(t, claims, "real-agent")
		})
	}
}

func TestIdentityInvariant_CaseVariantSpoofRejected(t *testing.T) {
	// The original P0: bound real-agent, victim hidden behind "Envelope"/"SOURCE_AGENT".
	body := `{"source_agent":"real-agent","Envelope":{"SOURCE_AGENT":"victim-agent"}}`
	reached := false
	sink := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached = true })
	r := httptest.NewRequest("POST", "/x", strings.NewReader(body))
	ctx := context.WithValue(r.Context(), auth.PrincipalCtxKey, auth.Principal{TenantID: "acme", BoundAgentID: "real-agent"})
	r = r.WithContext(ctx)
	w := httptest.NewRecorder()
	auth.AgentIdentityMiddleware(sink).ServeHTTP(w, r)
	assert.NotEqual(t, http.StatusOK, w.Code, "case-variant spoof must not pass")
	assert.False(t, reached)
}

// SERVICE-LAYER AUTHORITY (eighth review fix principle): when the key is
// bound, persisted identities come from the principal — the service
// overwrites both top-level and envelope source_agent. Even a future
// extractor gap cannot split identities.
func TestIdentityInvariant_ServiceLayerAuthorityOverwrite(t *testing.T) {
	repo := &overwriteRepo{}
	queue := &overwriteQueue{}
	svc := service.NewTaskService(repo, queue, nil, nil)

	ctx := context.WithValue(context.Background(), auth.PrincipalCtxKey,
		auth.Principal{TenantID: "acme", BoundAgentID: "real-agent"})
	_, err := svc.Create(ctx, core.Task{
		TenantID: "acme", ID: "t1",
		SourceAgent: "victim-agent",
		TargetType:  core.TargetTypeMailbox, TargetValue: "mb",
		Envelope: core.TaskEnvelope{JanusVersion: "1.0", TaskID: "t1", TenantID: "acme", SourceAgent: "victim-agent", Target: core.Target{Type: core.TargetTypeMailbox, Value: "mb"}, Trace: core.TraceContext{TraceID: "tr-inv"}, Payload: core.Payload{Type: "text", Content: "x"}},
	})
	require.NoError(t, err)
	require.NotNil(t, repo.last)
	assert.Equal(t, "real-agent", repo.last.SourceAgent, "top-level identity must be overwritten by the principal")
	assert.Equal(t, "real-agent", repo.last.Envelope.SourceAgent, "envelope identity must be overwritten by the principal")
}

type overwriteRepo struct{ last *core.Task }

func (r *overwriteRepo) Create(_ context.Context, task core.Task) error {
	cp := task
	r.last = &cp
	return nil
}
func (r *overwriteRepo) Get(_ context.Context, _, _ string) (*core.Task, error) {
	return nil, fmt.Errorf("nf")
}
func (r *overwriteRepo) GetByIdempotencyKey(_ context.Context, _, _ string) (*core.Task, error) {
	return nil, fmt.Errorf("nf")
}
func (r *overwriteRepo) UpdateStatus(_ context.Context, _, _ string, _ core.TaskStatus, _ int) error {
	return nil
}
func (r *overwriteRepo) UpdateStatusWithCheck(_ context.Context, _, _ string, _, _ core.TaskStatus, _ int) (bool, error) {
	return true, nil
}
func (r *overwriteRepo) UpdateRetryAt(_ context.Context, _, _ string, _ time.Time) error { return nil }
func (r *overwriteRepo) ListByStatus(_ context.Context, _ string, _ core.TaskStatus, _ int) ([]*core.Task, error) {
	return nil, nil
}
func (r *overwriteRepo) SetResultRef(_ context.Context, _, _, _ string) error { return nil }
func (r *overwriteRepo) CountByStatus(_ context.Context, _ string, _ core.TaskStatus) (int, error) {
	return 0, nil
}
func (r *overwriteRepo) CountRunningByAgent(_ context.Context, _, _ string) (int, error) {
	return 0, nil
}
func (r *overwriteRepo) ResetForReplay(_ context.Context, _, _ string) error { return nil }

type overwriteQueue struct{}

func (q *overwriteQueue) PublishTask(_ context.Context, _ core.TaskMessage) error { return nil }
func (q *overwriteQueue) FetchTasks(_ context.Context, _, _ string, _ core.FetchOptions) ([]core.TaskDelivery, error) {
	return nil, nil
}
func (q *overwriteQueue) AckTask(_ context.Context, _ string, _ core.DeliveryRef) error { return nil }
func (q *overwriteQueue) NackTask(_ context.Context, _ string, _ core.DeliveryRef, _ core.NackReason) error {
	return nil
}
func (q *overwriteQueue) PublishDLQ(_ context.Context, _ core.TaskMessage, _ []byte) error {
	return nil
}
func (q *overwriteQueue) PublishEvent(_ context.Context, _ core.JanusEvent) error { return nil }
func (q *overwriteQueue) ReplayEvents(_ context.Context, _ core.EventReplayFilter) (core.EventIterator, error) {
	return nil, fmt.Errorf("nf")
}
func (q *overwriteQueue) EnsureTenant(_ context.Context, _ string) error              { return nil }
func (q *overwriteQueue) EnsureMailbox(_ context.Context, _ core.MailboxSpec) error   { return nil }
func (q *overwriteQueue) EnsureConsumer(_ context.Context, _ core.ConsumerSpec) error { return nil }
func (q *overwriteQueue) Close() error                                                { return nil }
