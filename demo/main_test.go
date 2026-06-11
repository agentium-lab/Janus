package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agentium-lab/Janus/core"
	janus "github.com/agentium-lab/Janus/sdk/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCodingAgentHandler(t *testing.T) {
	task := &core.Task{ID: "t1"}
	result := codingAgentHandler(task)
	var parsed map[string]string
	require.NoError(t, json.Unmarshal([]byte(result), &parsed))
	assert.Equal(t, "coded", parsed["status"])
	assert.Equal(t, "t1", parsed["task_id"])
}

func TestReviewAgentHandler(t *testing.T) {
	task := &core.Task{ID: "t2"}
	result := reviewAgentHandler(task)
	var parsed map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(result), &parsed))
	assert.Equal(t, "reviewed", parsed["status"])
	assert.Equal(t, "t2", parsed["task_id"])
	assert.Contains(t, parsed, "approved")
}

func TestTestAgentHandler(t *testing.T) {
	task := &core.Task{ID: "t3"}
	result := testAgentHandler(task)
	var parsed map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(result), &parsed))
	assert.Equal(t, "tested", parsed["status"])
	assert.Equal(t, "t3", parsed["task_id"])
	assert.Contains(t, parsed, "coverage")
}

func TestPublishDemoTasks(t *testing.T) {
	var published int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/tenants/demo/tasks" && r.Method == "POST" {
			atomic.AddInt32(&published, 1)
			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(map[string]string{"id": "ok"})
			return
		}
		if r.Method == "POST" {
			w.WriteHeader(http.StatusCreated)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := janus.NewClient(janus.Config{BaseURL: srv.URL, TenantID: "demo"})
	publishDemoTasks(context.Background(), client, "demo")
	assert.Equal(t, int32(3), atomic.LoadInt32(&published))
}

func TestEnvOr(t *testing.T) {
	assert.Equal(t, "fallback", envOr("NONEXISTENT_KEY_XYZ", "fallback"))
	t.Setenv("TEST_ENV_OR_KEY", "value")
	assert.Equal(t, "value", envOr("TEST_ENV_OR_KEY", "fallback"))
}

func TestDemoAgent_Run(t *testing.T) {
	var pullCount int32
	var acked int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/tenants/demo/mailboxes/coding-inbox/pull":
			if atomic.AddInt32(&pullCount, 1) > 1 {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"task": map[string]string{
					"id": "run-task-1", "status": "pending",
				},
				"lease": map[string]string{
					"lease_id": "lease-1",
				},
			})
		case r.URL.Path == "/v1/tenants/demo/tasks/run-task-1/ack":
			atomic.AddInt32(&acked, 1)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	client := janus.NewClient(janus.Config{BaseURL: srv.URL, TenantID: "demo"})
	agent := &DemoAgent{
		id:      "coding-agent",
		name:    "Coding Agent",
		tenant:  "demo",
		client:  client,
		mailbox: "coding-inbox",
		onTask:  codingAgentHandler,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	agent.run(ctx)

	assert.Equal(t, int32(1), atomic.LoadInt32(&acked))
}

func TestDemoAgent_Run_Empty(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	client := janus.NewClient(janus.Config{BaseURL: srv.URL, TenantID: "demo"})
	agent := &DemoAgent{
		id:      "coding-agent",
		name:    "Coding Agent",
		tenant:  "demo",
		client:  client,
		mailbox: "coding-inbox",
		onTask:  codingAgentHandler,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	agent.run(ctx)
}
