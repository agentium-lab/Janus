package janus

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agentium-lab/Janus/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func silenceLog(t *testing.T) {
	t.Helper()
	orig := log.Default().Writer()
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(orig) })
}

func TestFromSimple_AdaptsLegacyHandler(t *testing.T) {
	inner := func(ctx context.Context, task *core.Task, agentID string) (string, *core.TokenUsage, error) {
		assert.Equal(t, "t-1", task.ID)
		assert.Equal(t, "agent-9", agentID)
		assert.NotNil(t, ctx)
		return "ref://done", &core.TokenUsage{TotalTokens: 7}, nil
	}

	wrapped := FromSimple(inner)
	ref, usage, err := wrapped(context.Background(), &core.Task{ID: "t-1"}, "agent-9", func(string, *int, map[string]interface{}) {
		t.Fatal("simple handler must not receive the progress fn")
	})
	require.NoError(t, err)
	assert.Equal(t, "ref://done", ref)
	assert.Equal(t, 7, usage.TotalTokens)
}

func TestNewJanusWorker_Defaults(t *testing.T) {
	w := NewJanusWorker(nil, WorkerConfig{AgentID: "a", MailboxID: "m"})
	assert.Equal(t, 2*time.Second, w.config.PollInterval)
	assert.Equal(t, 30*time.Second, w.config.Heartbeat)

	w2 := NewJanusWorker(nil, WorkerConfig{AgentID: "a", MailboxID: "m", PollInterval: 250 * time.Millisecond, Heartbeat: 5 * time.Second})
	assert.Equal(t, 250*time.Millisecond, w2.config.PollInterval)
	assert.Equal(t, 5*time.Second, w2.config.Heartbeat)
}

type pullScript struct {
	mu        sync.Mutex
	pulls     int32
	starts    int32
	acks      int32
	nacks     int32
	progress  int32
	heartbeats int32
	pullErr   bool
	startErr  bool
	ackErr    bool
	nackErr   bool
	noTask    bool
	ackBody   map[string]interface{}
	nackBody  map[string]interface{}
}

func (s *pullScript) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/tenants/test-tenant/mailboxes/mb-1/pull":
			atomic.AddInt32(&s.pulls, 1)
			if s.pullErr {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			if s.noTask {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			json.NewEncoder(w).Encode(PullResult{
				Task: &core.Task{ID: "task-1", TenantID: "test-tenant", Status: core.TaskStatusClaimed},
				Lease: struct {
					LeaseID   string      `json:"lease_id"`
					Attempt   int         `json:"attempt"`
					ExpiresAt interface{} `json:"expires_at"`
				}{LeaseID: "lease-1", Attempt: 2},
			})
		case r.URL.Path == "/v1/tenants/test-tenant/tasks/task-1/start":
			atomic.AddInt32(&s.starts, 1)
			if s.startErr {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/v1/tenants/test-tenant/tasks/task-1/ack":
			atomic.AddInt32(&s.acks, 1)
			s.mu.Lock()
			json.NewDecoder(r.Body).Decode(&s.ackBody)
			s.mu.Unlock()
			if s.ackErr {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/v1/tenants/test-tenant/tasks/task-1/nack":
			atomic.AddInt32(&s.nacks, 1)
			s.mu.Lock()
			json.NewDecoder(r.Body).Decode(&s.nackBody)
			s.mu.Unlock()
			if s.nackErr {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/v1/tenants/test-tenant/tasks/task-1/progress":
			atomic.AddInt32(&s.progress, 1)
			w.WriteHeader(http.StatusForbidden)
		case r.URL.Path == "/v1/tenants/test-tenant/tasks/task-1/heartbeat":
			atomic.AddInt32(&s.heartbeats, 1)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	})
}

func newScriptWorker(t *testing.T, s *pullScript) *JanusWorker {
	t.Helper()
	srv := httptest.NewServer(s.handler())
	t.Cleanup(srv.Close)
	return NewJanusWorker(NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"}), WorkerConfig{
		AgentID: "agent-1", MailboxID: "mb-1",
		PollInterval: 5 * time.Millisecond, Heartbeat: 10 * time.Second,
	})
}

func TestWorker_ProcessOne_AckCarriesLeaseAndUsage(t *testing.T) {
	silenceLog(t)
	s := &pullScript{}
	w := newScriptWorker(t, s)

	err := w.processOne(context.Background(), func(ctx context.Context, task *core.Task, agentID string, progress ProgressFn) (string, *core.TokenUsage, error) {
		assert.Equal(t, "task-1", task.ID)
		assert.Equal(t, "agent-1", agentID)
		pct := 10
		progress("step", &pct, nil)
		return "s3://ref", &core.TokenUsage{TotalTokens: 99}, nil
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), atomic.LoadInt32(&s.acks))
	assert.Equal(t, int32(0), atomic.LoadInt32(&s.nacks))
	assert.GreaterOrEqual(t, atomic.LoadInt32(&s.progress), int32(1), "progress endpoint hit despite 403 (non-fatal)")

	s.mu.Lock()
	defer s.mu.Unlock()
	require.NotNil(t, s.ackBody)
	assert.Equal(t, "lease-1", s.ackBody["lease_id"])
	assert.Equal(t, float64(2), s.ackBody["attempt"])
	assert.Equal(t, "s3://ref", s.ackBody["result_ref"])
	tu, ok := s.ackBody["token_usage"].(map[string]interface{})
	require.True(t, ok, "token usage must ride the ack")
	assert.Equal(t, float64(99), tu["total_tokens"])
}

func TestWorker_ProcessOne_NackOnHandlerError(t *testing.T) {
	silenceLog(t)
	s := &pullScript{}
	w := newScriptWorker(t, s)

	err := w.processOne(context.Background(), func(context.Context, *core.Task, string, ProgressFn) (string, *core.TokenUsage, error) {
		return "", nil, context.DeadlineExceeded
	})
	require.NoError(t, err, "successful NACK is not a worker error")
	assert.Equal(t, int32(1), atomic.LoadInt32(&s.nacks))

	s.mu.Lock()
	defer s.mu.Unlock()
	require.NotNil(t, s.nackBody)
	assert.Equal(t, true, s.nackBody["retriable"])
	nerr, ok := s.nackBody["error"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "HANDLER_ERROR", nerr["code"])
}

func TestWorker_ProcessOne_PullError(t *testing.T) {
	silenceLog(t)
	s := &pullScript{pullErr: true}
	w := newScriptWorker(t, s)

	err := w.processOne(context.Background(), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pull:")
	assert.Equal(t, int32(0), atomic.LoadInt32(&s.starts))
}

func TestWorker_ProcessOne_StartError(t *testing.T) {
	silenceLog(t)
	s := &pullScript{startErr: true}
	w := newScriptWorker(t, s)

	err := w.processOne(context.Background(), func(context.Context, *core.Task, string, ProgressFn) (string, *core.TokenUsage, error) {
		t.Fatal("handler must not run when StartTask fails")
		return "", nil, nil
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "start task task-1")
}

func TestWorker_ProcessOne_AckError(t *testing.T) {
	silenceLog(t)
	s := &pullScript{ackErr: true}
	w := newScriptWorker(t, s)

	err := w.processOne(context.Background(), func(context.Context, *core.Task, string, ProgressFn) (string, *core.TokenUsage, error) {
		return "ref", nil, nil
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ack task task-1")
}

func TestWorker_ProcessOne_NackError(t *testing.T) {
	silenceLog(t)
	s := &pullScript{nackErr: true}
	w := newScriptWorker(t, s)

	err := w.processOne(context.Background(), func(context.Context, *core.Task, string, ProgressFn) (string, *core.TokenUsage, error) {
		return "", nil, context.Canceled
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nack task task-1")
	assert.Contains(t, err.Error(), "handler error: context canceled")
}

func TestWorker_ProcessOne_EmptyQueueBackoff(t *testing.T) {
	silenceLog(t)
	s := &pullScript{noTask: true}
	w := newScriptWorker(t, s)

	start := time.Now()
	err := w.processOne(context.Background(), nil)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, time.Since(start), 5*time.Millisecond, "empty queue must back off for PollInterval")
}

func TestWorker_Run_StopsOnContextCancel(t *testing.T) {
	silenceLog(t)
	s := &pullScript{noTask: true}
	w := newScriptWorker(t, s)

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	err := w.Run(ctx, func(context.Context, *core.Task, string, ProgressFn) (string, *core.TokenUsage, error) {
		return "", nil, nil
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.GreaterOrEqual(t, atomic.LoadInt32(&s.pulls), int32(2), "worker must keep polling across empty results")
}

func TestWorker_Run_SurvivesProcessOneErrors(t *testing.T) {
	silenceLog(t)
	s := &pullScript{pullErr: true}
	w := newScriptWorker(t, s)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	err := w.Run(ctx, func(context.Context, *core.Task, string, ProgressFn) (string, *core.TokenUsage, error) {
		return "", nil, nil
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.GreaterOrEqual(t, atomic.LoadInt32(&s.pulls), int32(2), "Run must log and continue after pull errors")
}

func TestWorker_HeartbeatLoop_KeepsBeatingUntilCancel(t *testing.T) {
	s := &pullScript{}
	srv := httptest.NewServer(s.handler())
	defer srv.Close()
	w := NewJanusWorker(NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"}), WorkerConfig{
		AgentID: "agent-1", MailboxID: "mb-1", Heartbeat: 15 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); w.heartbeatLoop(ctx, "task-1", 1, "lease-1") }()

	time.Sleep(70 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeatLoop must exit on ctx cancel")
	}
	assert.GreaterOrEqual(t, atomic.LoadInt32(&s.heartbeats), int32(2))
}

func TestWorker_HeartbeatLoop_StopsOnHeartbeatError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	w := NewJanusWorker(NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"}), WorkerConfig{
		AgentID: "agent-1", MailboxID: "mb-1", Heartbeat: 15 * time.Millisecond,
	})

	done := make(chan struct{})
	go func() { defer close(done); w.heartbeatLoop(context.Background(), "task-1", 1, "lease-1") }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeatLoop must exit after a failed heartbeat")
	}
}
