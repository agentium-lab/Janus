package a2a

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/agentium-lab/Janus/core"
	"github.com/stretchr/testify/assert"
)

// VALIDATION-BEFORE-SIDE-EFFECTS INVARIANT:
//
//	When a message references a taskId that cannot be continued (unknown,
//	terminal, context mismatch, or continuation-unsupported), the request
//	must be rejected with ZERO side effects: no task created, nothing
//	enqueued, nothing published.
//
// Regression origin: v1.6.1 created-and-enqueued the new task BEFORE the
// taskId check ran (stream), and never checked at all (send).

type sideEffectCounters struct {
	creates int
	publish int
}

type countingCreator struct {
	mockTaskCreator
	counters *sideEffectCounters
}

func (m *countingCreator) Create(_ context.Context, task core.Task) (*core.Task, error) {
	m.counters.creates++
	return &task, nil
}

type countingSubscriber struct {
	mockSubscriber
	counters *sideEffectCounters
}

func (s *countingSubscriber) Subscribe(tenantID string) <-chan core.JanusEvent {
	s.counters.publish++ // subscription counts as stream-side effect setup
	return s.mockSubscriber.Subscribe(tenantID)
}

func zeroSideEffectCases() map[string]func(g *Gateway) (*http.Request, *sideEffectCounters) {
	terminal := &core.Task{TenantID: "acme", ID: "t-done", Status: core.TaskStatusCompleted, CreatedAt: time.Now()}
	live := &core.Task{TenantID: "acme", ID: "t-live", Status: core.TaskStatusRunning, CreatedAt: time.Now(),
		Envelope: core.TaskEnvelope{Trace: core.TraceContext{TraceID: "ctx-live"}}}
	return map[string]func(g *Gateway) (*http.Request, *sideEffectCounters){
		"send_unknown_taskId": func(g *Gateway) (*http.Request, *sideEffectCounters) {
			return httptest.NewRequest(http.MethodPost, "/a2a/message:send", strings.NewReader(`{"message":{"role":"ROLE_USER","parts":[{"text":"x"}],"taskId":"ghost"}}`)), nil
		},
		"stream_unknown_taskId": func(g *Gateway) (*http.Request, *sideEffectCounters) {
			return httptest.NewRequest(http.MethodPost, "/a2a/message:stream", strings.NewReader(`{"message":{"role":"ROLE_USER","parts":[{"text":"x"}],"taskId":"ghost"}}`)), nil
		},
		"send_terminal_taskId": func(g *Gateway) (*http.Request, *sideEffectCounters) {
			g.statusSvc = &mockStatusGetter{task: terminal}
			return httptest.NewRequest(http.MethodPost, "/a2a/message:send", strings.NewReader(`{"message":{"role":"ROLE_USER","parts":[{"text":"x"}],"taskId":"t-done"}}`)), nil
		},
		"stream_terminal_taskId": func(g *Gateway) (*http.Request, *sideEffectCounters) {
			g.statusSvc = &mockStatusGetter{task: terminal}
			return httptest.NewRequest(http.MethodPost, "/a2a/message:stream", strings.NewReader(`{"message":{"role":"ROLE_USER","parts":[{"text":"x"}],"taskId":"t-done"}}`)), nil
		},
		"send_live_taskId_continuation_unsupported": func(g *Gateway) (*http.Request, *sideEffectCounters) {
			g.statusSvc = &mockStatusGetter{task: live}
			return httptest.NewRequest(http.MethodPost, "/a2a/message:send", strings.NewReader(`{"message":{"role":"ROLE_USER","parts":[{"text":"x"}],"taskId":"t-live"}}`)), nil
		},
		"stream_live_taskId_continuation_unsupported": func(g *Gateway) (*http.Request, *sideEffectCounters) {
			g.statusSvc = &mockStatusGetter{task: live}
			return httptest.NewRequest(http.MethodPost, "/a2a/message:stream", strings.NewReader(`{"message":{"role":"ROLE_USER","parts":[{"text":"x"}],"taskId":"t-live"}}`)), nil
		},
		"send_contextId_mismatch": func(g *Gateway) (*http.Request, *sideEffectCounters) {
			g.statusSvc = &mockStatusGetter{task: live}
			return httptest.NewRequest(http.MethodPost, "/a2a/message:send", strings.NewReader(`{"message":{"role":"ROLE_USER","parts":[{"text":"x"}],"taskId":"t-live","contextId":"ctx-other"}}`)), nil
		},
	}
}

func TestValidationBeforeSideEffects_TaskIdRejection(t *testing.T) {
	for name, setup := range zeroSideEffectCases() {
		t.Run(name, func(t *testing.T) {
			counters := &sideEffectCounters{}
			creator := &countingCreator{counters: counters}
			g := NewGatewayWithStatus(&mockAgentRegistrar{}, creator, &mockStatusGetter{err: fmt.Errorf("no rows")})
			g.subscriber = &countingSubscriber{counters: counters}

			req, _ := setup(g)
			req = withAuthCtx(req)
			w := httptest.NewRecorder()
			g.ServeHTTP(w, req)

			assert.Equal(t, http.StatusBadRequest, w.Code, "body=%s", w.Body.String())
			assert.Zero(t, counters.creates, "invariant violated: task was created before/without validation")
		})
	}
}
