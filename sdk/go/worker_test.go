package janus

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agentium-lab/Janus/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJanusWorker_ACKOnSuccess(t *testing.T) {
	var pullCount, startCount, ackCount int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/tenants/test-tenant/mailboxes/mb-1/pull" && r.Method == "POST":
			atomic.AddInt32(&pullCount, 1)
			if atomic.LoadInt32(&pullCount) > 1 {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			json.NewEncoder(w).Encode(PullResult{
				Task: &core.Task{ID: "task-1", TenantID: "test-tenant", Status: core.TaskStatusClaimed},
				Lease: struct {
					LeaseID   string      `json:"lease_id"`
					Attempt   int         `json:"attempt"`
					ExpiresAt interface{} `json:"expires_at"`
				}{LeaseID: "lease-1", Attempt: 1},
			})
		case r.URL.Path == "/v1/tenants/test-tenant/tasks/task-1/start" && r.Method == "POST":
			atomic.AddInt32(&startCount, 1)
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/v1/tenants/test-tenant/tasks/task-1/ack" && r.Method == "POST":
			atomic.AddInt32(&ackCount, 1)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	client := NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"})
	worker := NewJanusWorker(client, WorkerConfig{
		AgentID: "agent-1", MailboxID: "mb-1",
		PollInterval: 50 * time.Millisecond, Heartbeat: 10 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	var processed int32
	err := worker.Run(ctx, func(ctx context.Context, task *core.Task, agentID string) (string, *core.TokenUsage, error) {
		atomic.AddInt32(&processed, 1)
		return "result://ok", nil, nil
	})
	assert.Equal(t, context.DeadlineExceeded, err)

	require.Equal(t, int32(1), atomic.LoadInt32(&processed), "handler should be called once")
	assert.Equal(t, int32(1), atomic.LoadInt32(&startCount), "StartTask should be called once")
	assert.Equal(t, int32(1), atomic.LoadInt32(&ackCount), "AckTask should be called once")
}

func TestJanusWorker_NACKOnError(t *testing.T) {
	var nackCount int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/tenants/test-tenant/mailboxes/mb-1/pull" && r.Method == "POST":
			json.NewEncoder(w).Encode(PullResult{
				Task: &core.Task{ID: "task-err", TenantID: "test-tenant", Status: core.TaskStatusClaimed},
				Lease: struct {
					LeaseID   string      `json:"lease_id"`
					Attempt   int         `json:"attempt"`
					ExpiresAt interface{} `json:"expires_at"`
				}{LeaseID: "lease-2", Attempt: 1},
			})
		case r.URL.Path == "/v1/tenants/test-tenant/tasks/task-err/nack" && r.Method == "POST":
			atomic.AddInt32(&nackCount, 1)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	client := NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"})
	worker := NewJanusWorker(client, WorkerConfig{
		AgentID: "agent-1", MailboxID: "mb-1",
		PollInterval: 50 * time.Millisecond, Heartbeat: 10 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	_ = worker.Run(ctx, func(ctx context.Context, task *core.Task, agentID string) (string, *core.TokenUsage, error) {
		return "", nil, fmt.Errorf("handler failed")
	})

	assert.GreaterOrEqual(t, atomic.LoadInt32(&nackCount), int32(1), "NackTask should be called on error")
}
