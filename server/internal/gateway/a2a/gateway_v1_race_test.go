package a2a

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/agentium-lab/Janus/core"
	"github.com/stretchr/testify/assert"
)

func TestGatewayV1_Subscribe_TerminalTask_ReturnsUnsupportedOperation(t *testing.T) {
	sub := newMockSubscriber()
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, &mockStatusGetter{task: completedTask("t-done")}).WithEventSubscriber(sub)

	req := withAuthCtx(httptest.NewRequest(http.MethodGet, "/a2a/tasks/t-done:subscribe", nil))
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "UNSUPPORTED_OPERATION")
	assert.Contains(t, w.Body.String(), "terminal state")
}

func TestGatewayV1_Subscribe_PostMethod(t *testing.T) {
	now := time.Now().UTC()
	running := &core.Task{TenantID: "acme", ID: "t-live", Status: core.TaskStatusRunning, CreatedAt: now, UpdatedAt: now}
	sub := newMockSubscriber()
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, &mockStatusGetter{task: running}).WithEventSubscriber(sub)

	done := make(chan struct{})
	w := httptest.NewRecorder()
	go func() {
		defer close(done)
		req := withAuthCtx(httptest.NewRequest(http.MethodPost, "/a2a/tasks/t-live:subscribe", nil))
		gw.ServeHTTP(w, req)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for sub.count("acme") == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	sub.emit("acme", core.JanusEvent{EventType: core.EventTaskCompleted, TaskID: "t-live", Timestamp: time.Now()})
	<-done

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), V1StateCompleted)
}

func TestGatewayV1_Subscribe_RaceNoLostTerminal(t *testing.T) {
	// The race from the review: the task reaches terminal AFTER the status
	// read but BEFORE subscribe. We inject the terminal event from inside
	// the status getter itself — with subscribe-last ordering the event is
	// published to zero subscribers and the stream hangs; with
	// subscribe-first it is queued on the channel and delivered.
	running := &core.Task{TenantID: "acme", ID: "t-race", Status: core.TaskStatusRunning,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	sub := newMockSubscriber()
	getter := &racingStatusGetter{task: running, sub: sub, tenantID: "acme"}
	gw := NewGatewayWithStatus(&mockAgentRegistrar{}, &mockTaskCreator{}, getter).WithEventSubscriber(sub)

	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		req := withAuthCtx(httptest.NewRequest(http.MethodGet, "/a2a/tasks/t-race:subscribe", nil))
		gw.ServeHTTP(w, req)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("stream hung: terminal event was lost in the read-subscribe race window")
	}

	out := w.Body.String()
	assert.Contains(t, out, `"task":`)
	assert.Contains(t, out, V1StateCompleted, "terminal event must survive the race window")
}

type racingStatusGetter struct {
	task     *core.Task
	sub      *mockSubscriber
	tenantID string
}

func (g *racingStatusGetter) Get(ctx context.Context, _, _ string) (*core.Task, error) {
	// Emit the terminal event DURING the status read — exactly the moment
	// the old subscribe-last implementation was not yet subscribed.
	g.sub.emit(g.tenantID, core.JanusEvent{
		EventType: core.EventTaskCompleted, TaskID: g.task.ID, Timestamp: time.Now(),
	})
	return g.task, nil
}
