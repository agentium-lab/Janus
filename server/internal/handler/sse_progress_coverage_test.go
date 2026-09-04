package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/driver/postgres"
)

// --- SSE handler edge cases ---

type nonFlusherRW struct{ http.ResponseWriter }

type fakeTaskStatusChecker struct {
	task      *core.Task
	err       error
	gotTenant string
	gotTask   string
}

func (f *fakeTaskStatusChecker) Get(_ context.Context, tenantID, taskID string) (*core.Task, error) {
	f.gotTenant, f.gotTask = tenantID, taskID
	return f.task, f.err
}

type subscribeCountingBroadcaster struct {
	ch         chan core.JanusEvent
	subscribed atomic.Int32
}

func (b *subscribeCountingBroadcaster) Subscribe(string) <-chan core.JanusEvent {
	b.subscribed.Add(1)
	return b.ch
}

func (b *subscribeCountingBroadcaster) Unsubscribe(string, <-chan core.JanusEvent) {}

func TestSSEHandler_NonFlusherWriter(t *testing.T) {
	h := NewSSEHandler(&subscribeCountingBroadcaster{ch: make(chan core.JanusEvent)})

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/tasks/task-1/stream", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(nonFlusherRW{rec}, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), "streaming unsupported")
}

func TestSSEHandler_MissingTaskID(t *testing.T) {
	h := NewSSEHandler(&subscribeCountingBroadcaster{ch: make(chan core.JanusEvent)})

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/tasks/stream", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "missing task id")
}

func TestSSEHandler_TerminalPreCheckClosesImmediately(t *testing.T) {
	bc := &subscribeCountingBroadcaster{ch: make(chan core.JanusEvent)}
	checker := &fakeTaskStatusChecker{task: &core.Task{ID: "task-9", TenantID: "acme", Status: core.TaskStatusCompleted}}
	h := NewSSEHandler(bc).WithStatusChecker(checker)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/tasks/task-9/stream", nil)
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { h.ServeHTTP(w, req); close(done) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("terminal pre-check did not close the stream")
	}

	assert.Equal(t, "acme", checker.gotTenant)
	assert.Equal(t, "task-9", checker.gotTask)
	assert.Contains(t, w.Body.String(), "event: task.completed")
	assert.Contains(t, w.Body.String(), `"status":"completed"`)
	assert.EqualValues(t, 0, bc.subscribed.Load(), "should not subscribe when task is already terminal")
}

func TestSSEHandler_StatusCheckerError_FallsThroughToStream(t *testing.T) {
	bc := &subscribeCountingBroadcaster{ch: make(chan core.JanusEvent, 16)}
	checker := &fakeTaskStatusChecker{err: context.DeadlineExceeded}
	h := NewSSEHandler(bc).WithStatusChecker(checker)

	go func() {
		time.Sleep(50 * time.Millisecond)
		bc.ch <- core.JanusEvent{EventType: core.EventTaskFailed, TenantID: "acme", TaskID: "task-9", EventID: "e1"}
	}()

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/tasks/task-9/stream", nil)
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { h.ServeHTTP(w, req); close(done) }()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("stream did not close after terminal event")
	}
	assert.EqualValues(t, 1, bc.subscribed.Load())
	assert.Contains(t, w.Body.String(), "event: task.failed")
}

func TestSSEHandler_StatusCheckerNonTerminal_StreamsUntilTerminal(t *testing.T) {
	bc := &subscribeCountingBroadcaster{ch: make(chan core.JanusEvent, 16)}
	checker := &fakeTaskStatusChecker{task: &core.Task{ID: "task-9", TenantID: "acme", Status: core.TaskStatusRunning}}
	h := NewSSEHandler(bc).WithStatusChecker(checker)

	go func() {
		time.Sleep(50 * time.Millisecond)
		bc.ch <- core.JanusEvent{EventType: core.EventTaskProgress, TenantID: "acme", TaskID: "task-9", EventID: "e1"}
		bc.ch <- core.JanusEvent{EventType: core.EventTaskCancelled, TenantID: "acme", TaskID: "task-9", EventID: "e2"}
	}()

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/tasks/task-9/stream", nil)
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { h.ServeHTTP(w, req); close(done) }()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("stream did not close after terminal event")
	}
	assert.EqualValues(t, 1, bc.subscribed.Load())
	assert.Contains(t, w.Body.String(), "event: task.progress")
	assert.Contains(t, w.Body.String(), "event: task.cancelled")
}

type closedChanBroadcaster struct{}

func (closedChanBroadcaster) Subscribe(string) <-chan core.JanusEvent {
	ch := make(chan core.JanusEvent)
	close(ch)
	return ch
}

func (closedChanBroadcaster) Unsubscribe(string, <-chan core.JanusEvent) {}

func TestSSEHandler_ClosedSubscriberChannelReturns(t *testing.T) {
	h := NewSSEHandler(closedChanBroadcaster{})

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/tasks/task-1/stream", nil)
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { h.ServeHTTP(w, req); close(done) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not return on closed subscriber channel")
	}
}

// --- Progress handler ---

type fakeProgressSvc struct {
	err      error
	calls    int
	tenant   string
	task     string
	agent    string
	prog     core.TaskProgress
}

func (f *fakeProgressSvc) ReportProgress(_ context.Context, tenantID, taskID, agentID string, prog core.TaskProgress) error {
	f.calls++
	f.tenant, f.task, f.agent = tenantID, taskID, agentID
	f.prog = prog
	return f.err
}

type recordingPublisher struct {
	mu     sync.Mutex
	events []core.JanusEvent
}

func (p *recordingPublisher) Publish(evt core.JanusEvent) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, evt)
}

func (p *recordingPublisher) snapshot() []core.JanusEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]core.JanusEvent(nil), p.events...)
}

func newProgressTestServer(svc *fakeProgressSvc, pub *recordingPublisher) http.HandlerFunc {
	return NewProgressHandler(svc, pub).Report
}

func TestProgressHandler_Report_Success(t *testing.T) {
	svc := &fakeProgressSvc{}
	pub := &recordingPublisher{}
	handlerFunc := newProgressTestServer(svc, pub)

	body := `{"message":"working","percent":42,"agent_id":"agent-1","data":{"step":2}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/progress", strings.NewReader(body))
	w := httptest.NewRecorder()
	handlerFunc(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	assert.Equal(t, 1, svc.calls)
	assert.Equal(t, "acme", svc.tenant)
	assert.Equal(t, "task-1", svc.task)
	assert.Equal(t, "agent-1", svc.agent)
	require.NotNil(t, svc.prog.Percent)
	assert.Equal(t, 42, *svc.prog.Percent)

	events := pub.snapshot()
	require.Len(t, events, 1)
	assert.Equal(t, core.EventTaskProgress, events[0].EventType)
	assert.Equal(t, "acme", events[0].TenantID)
	assert.Equal(t, "task-1", events[0].TaskID)
	assert.Equal(t, "agent-1", events[0].SourceAgent)
}

func TestProgressHandler_Report_MissingTaskID(t *testing.T) {
	handlerFunc := newProgressTestServer(&fakeProgressSvc{}, &recordingPublisher{})

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/progress", strings.NewReader(`{"message":"m","agent_id":"a"}`))
	w := httptest.NewRecorder()
	handlerFunc(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "missing task id")
}

func TestProgressHandler_Report_BadJSON(t *testing.T) {
	handlerFunc := newProgressTestServer(&fakeProgressSvc{}, &recordingPublisher{})

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/progress", strings.NewReader("not-json"))
	w := httptest.NewRecorder()
	handlerFunc(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "invalid json")
}

func TestProgressHandler_Report_MissingMessage(t *testing.T) {
	handlerFunc := newProgressTestServer(&fakeProgressSvc{}, &recordingPublisher{})

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/progress", strings.NewReader(`{"agent_id":"a"}`))
	w := httptest.NewRecorder()
	handlerFunc(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "message is required")
}

func TestProgressHandler_Report_PercentOutOfRange(t *testing.T) {
	handlerFunc := newProgressTestServer(&fakeProgressSvc{}, &recordingPublisher{})

	for _, pct := range []int{-1, 101} {
		body := strings.NewReader(`{"message":"m","agent_id":"a","percent":` + strconv.Itoa(pct) + `}`)
		req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/progress", body)
		w := httptest.NewRecorder()
		handlerFunc(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code, "percent=%d", pct)
		assert.Contains(t, w.Body.String(), "percent must be 0-100")
	}
}


func TestProgressHandler_Report_MissingAgentID(t *testing.T) {
	handlerFunc := newProgressTestServer(&fakeProgressSvc{}, &recordingPublisher{})

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/progress", strings.NewReader(`{"message":"m"}`))
	w := httptest.NewRecorder()
	handlerFunc(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "agent_id is required")
}

func TestProgressHandler_Report_RateLimited(t *testing.T) {
	handlerFunc := newProgressTestServer(&fakeProgressSvc{}, &recordingPublisher{})

	body := `{"message":"m","agent_id":"a"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/progress", strings.NewReader(body))
	w := httptest.NewRecorder()
	handlerFunc(w, req)
	require.Equal(t, http.StatusAccepted, w.Code)

	req2 := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/progress", strings.NewReader(body))
	w2 := httptest.NewRecorder()
	handlerFunc(w2, req2)

	require.Equal(t, http.StatusTooManyRequests, w2.Code)
	assert.Contains(t, w2.Body.String(), "rate limit")
}

func TestProgressHandler_Report_DistinctTasksNotRateLimited(t *testing.T) {
	handlerFunc := newProgressTestServer(&fakeProgressSvc{}, &recordingPublisher{})

	for _, taskID := range []string{"task-1", "task-2"} {
		req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/"+taskID+"/progress",
			strings.NewReader(`{"message":"m","agent_id":"a"}`))
		w := httptest.NewRecorder()
		handlerFunc(w, req)
		assert.Equal(t, http.StatusAccepted, w.Code, taskID)
	}
}

func TestProgressHandler_Report_ServiceError(t *testing.T) {
	handlerFunc := newProgressTestServer(&fakeProgressSvc{err: context.DeadlineExceeded}, &recordingPublisher{})

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/tasks/task-1/progress", strings.NewReader(`{"message":"m","agent_id":"a"}`))
	w := httptest.NewRecorder()
	handlerFunc(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestProgressRateLimiter(t *testing.T) {
	rl := newProgressRateLimiter(10)
	assert.True(t, rl.allow("k1"))
	assert.False(t, rl.allow("k1"), "second call within window should be denied")
	assert.True(t, rl.allow("k2"), "different key must not be affected")
}

func TestProgressRateLimiter_CleansUpStaleEntries(t *testing.T) {
	rl := newProgressRateLimiter(10)
	stale := time.Now().Add(-2 * time.Minute)
	for i := 0; i < 10001; i++ {
		rl.lastSeen["stale-"+strconv.Itoa(i)] = stale
	}

	assert.True(t, rl.allow("fresh"))

	rl.mu.Lock()
	n := len(rl.lastSeen)
	rl.mu.Unlock()
	assert.LessOrEqual(t, n, 5, "stale entries should have been purged, got %d", n)
}

// --- FanoutBroadcaster.Publish ---

func TestFanoutBroadcaster_Publish_EndToEnd(t *testing.T) {
	inbound := make(chan core.JanusEvent, 4)
	b := NewFanoutBroadcaster(inbound)

	ch := b.Subscribe("t1")
	b.Publish(core.JanusEvent{TenantID: "t1", TaskID: "p1"})

	select {
	case got := <-ch:
		assert.Equal(t, "p1", got.TaskID)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for published event")
	}

	b.Unsubscribe("t1", ch)
}

func TestFanoutBroadcaster_Publish_DropsWhenInboundFull(t *testing.T) {
	b := &FanoutBroadcaster{
		fans:    map[string][]chan core.JanusEvent{},
		inbound: make(chan core.JanusEvent, 1),
	}
	b.inbound <- core.JanusEvent{TaskID: "first"}

	done := make(chan struct{})
	go func() { b.Publish(core.JanusEvent{TaskID: "dropped"}); close(done) }()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Publish blocked on full inbound channel")
	}

	assert.Equal(t, "first", (<-b.inbound).TaskID)
	b.Publish(core.JanusEvent{TaskID: "second"})
	assert.Equal(t, "second", (<-b.inbound).TaskID)
}

// --- DLQ outbox wiring / ULID ---

func TestDlqULID_Uniqueness(t *testing.T) {
	a, b := dlqULID(), dlqULID()
	assert.Len(t, a, 32)
	assert.NotEqual(t, a, b)
}

func TestDLQServiceAdapter_WithOutbox_RequiresPostgresRepo(t *testing.T) {
	pool, err := pgxpool.New(context.Background(),
		"host=127.0.0.1 port=1 user=test password=test dbname=test sslmode=disable")
	require.NoError(t, err)
	defer pool.Close()

	outbox := postgres.NewOutboxRepo(pool)
	repo := &mockDLQTaskRepo{tasks: map[string]*core.Task{
		"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusDeadLettered, MailboxID: "mb-1"},
	}}
	adapter := NewDLQServiceAdapter(repo, &mockDLQQueueDriver{}).WithOutbox(outbox, pool)
	require.NotNil(t, adapter.outboxRepo)
	require.NotNil(t, adapter.pool)

	_, err = adapter.ReplayDLQ(context.Background(), "acme", "t1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "postgres task repo required")
}
