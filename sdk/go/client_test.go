package janus

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/agentium-lab/Janus/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestClient(t *testing.T, handler http.Handler) *Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"})
}

func testMux() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("/v1/tenants", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"id": "test-tenant"})
	})

	mux.HandleFunc("/v1/tenants/test-tenant/mailboxes", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"id": "mb-1"})
	})

	mux.HandleFunc("/v1/tenants/test-tenant/agents", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"id": "agent-1"})
	})

	mux.HandleFunc("/v1/tenants/test-tenant/tasks", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"id": "task-1", "status": "created"})
	})

	mux.HandleFunc("/v1/tenants/test-tenant/tasks/task-1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.NotFound(w, r)
			return
		}
		json.NewEncoder(w).Encode(core.Task{ID: "task-1", Status: core.TaskStatusQueued})
	})

	mux.HandleFunc("/v1/tenants/test-tenant/tasks/task-1/start", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"status": "running"})
	})

	mux.HandleFunc("/v1/tenants/test-tenant/tasks/task-1/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	mux.HandleFunc("/v1/tenants/test-tenant/tasks/task-1/ack", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"status": "completed"})
	})

	mux.HandleFunc("/v1/tenants/test-tenant/tasks/task-1/nack", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"status": "nacked"})
	})

	mux.HandleFunc("/v1/tenants/test-tenant/tasks/task-1/cancel", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"status": "cancelled"})
	})

	mux.HandleFunc("/v1/tenants/test-tenant/tasks/task-1/events", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"events": []map[string]string{
				{"event_id": "e1", "event_type": "task.created"},
			},
		})
	})

	mux.HandleFunc("/v1/tenants/test-tenant/mailboxes/mb-1/pull", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			AgentID string `json:"agent_id"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		if req.AgentID == "empty" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"task": core.Task{ID: "task-1", Status: core.TaskStatusClaimed},
			"lease": map[string]string{
				"lease_id":   "lease-abc",
				"expires_at": "2026-06-12T00:00:00Z",
			},
		})
	})

	return mux
}

func TestClient_CreateTenant(t *testing.T) {
	c := newTestClient(t, testMux())
	err := c.CreateTenant(context.Background(), "test-tenant", "Test")
	require.NoError(t, err)
}

func TestClient_CreateMailbox(t *testing.T) {
	c := newTestClient(t, testMux())
	err := c.CreateMailbox(context.Background(), "mb-1", "agent-1")
	require.NoError(t, err)
}

func TestClient_RegisterAgent(t *testing.T) {
	c := newTestClient(t, testMux())
	err := c.RegisterAgent(context.Background(), RegisterAgentRequest{
		ID: "agent-1", DisplayName: "Test Agent", Protocol: "a2a",
	})
	require.NoError(t, err)
}

func TestClient_PublishTask(t *testing.T) {
	c := newTestClient(t, testMux())
	resp, err := c.PublishTask(context.Background(), PublishTaskRequest{
		ID: "task-1", SourceAgent: "agent-1", TargetType: "agent", TargetValue: "agent-2",
		MailboxID: "mb-1", Priority: "normal",
		Envelope: core.TaskEnvelope{
			JanusVersion: "0.1", TaskID: "task-1", TenantID: "test-tenant",
			SourceAgent: "agent-1", Payload: core.Payload{Type: "json", Content: "{}"},
			Trace: core.TraceContext{TraceID: "trace-1"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "task-1", resp.ID)
	assert.Equal(t, "created", resp.Status)
}

func TestClient_GetTask(t *testing.T) {
	c := newTestClient(t, testMux())
	task, err := c.GetTask(context.Background(), "task-1")
	require.NoError(t, err)
	assert.Equal(t, "task-1", task.ID)
	assert.Equal(t, core.TaskStatusQueued, task.Status)
}

func TestClient_PullTask(t *testing.T) {
	c := newTestClient(t, testMux())
	result, err := c.PullTask(context.Background(), "mb-1", "agent-1")
	require.NoError(t, err)
	assert.Equal(t, "task-1", result.Task.ID)
	assert.Equal(t, "lease-abc", result.Lease.LeaseID)
}

func TestClient_PullTask_NoMessages(t *testing.T) {
	c := newTestClient(t, testMux())
	result, err := c.PullTask(context.Background(), "mb-1", "empty")
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestClient_StartTask(t *testing.T) {
	c := newTestClient(t, testMux())
	err := c.StartTask(context.Background(), "task-1", "lease-abc")
	require.NoError(t, err)
}

func TestClient_Heartbeat(t *testing.T) {
	c := newTestClient(t, testMux())
	err := c.Heartbeat(context.Background(), "task-1", "lease-abc")
	require.NoError(t, err)
}

func TestClient_AckTask(t *testing.T) {
	c := newTestClient(t, testMux())
	err := c.AckTask(context.Background(), "task-1", AckRequest{
		LeaseID:   "lease-abc",
		ResultRef: "s3://results/1",
		TokenUsage: &core.TokenUsage{PromptTokens: 10000, CompletionTokens: 5000, TotalTokens: 15000},
	})
	require.NoError(t, err)
}

func TestClient_NackTask(t *testing.T) {
	c := newTestClient(t, testMux())
	err := c.NackTask(context.Background(), "task-1", NackRequest{
		LeaseID:   "lease-abc",
		Retriable: true,
		Error:     &core.TaskError{Code: "TIMEOUT", Message: "agent timed out"},
	})
	require.NoError(t, err)
}

func TestClient_CancelTask(t *testing.T) {
	c := newTestClient(t, testMux())
	err := c.CancelTask(context.Background(), "task-1")
	require.NoError(t, err)
}

func TestClient_GetTaskEvents(t *testing.T) {
	c := newTestClient(t, testMux())
	events, err := c.GetTaskEvents(context.Background(), "task-1")
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, "e1", events[0].EventID)
}

func TestClient_APIError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/tenants/test-tenant/tasks/bad", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "task not found"})
	})
	c := newTestClient(t, mux)
	_, err := c.GetTask(context.Background(), "bad")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "task not found")
}

func TestClient_ServerError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	c := newTestClient(t, mux)
	err := c.StartTask(context.Background(), "task-1", "lease-abc")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "500")
}

func TestClient_ConnectionError(t *testing.T) {
	c := NewClient(Config{BaseURL: "http://127.0.0.1:0", TenantID: "test"})
	err := c.RegisterAgent(context.Background(), RegisterAgentRequest{ID: "agent-1"})
	assert.Error(t, err)
}

func TestClient_PullTask_ServerError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/tenants/test-tenant/mailboxes/mb-1/pull", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"error": "queue unavailable"})
	})
	c := newTestClient(t, mux)
	_, err := c.PullTask(context.Background(), "mb-1", "agent-1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "queue unavailable")
}

func TestClient_PullTask_ServerErrorNoBody(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/tenants/test-tenant/mailboxes/mb-1/pull", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	c := newTestClient(t, mux)
	_, err := c.PullTask(context.Background(), "mb-1", "agent-1")
	assert.Error(t, err)
}

func TestClient_PublishTask_NilEnvelope(t *testing.T) {
	c := newTestClient(t, testMux())
	_, err := c.PublishTask(context.Background(), PublishTaskRequest{
		ID: "task-1", SourceAgent: "agent-1", TargetType: "agent", TargetValue: "agent-2",
	})
	require.NoError(t, err)
}

func TestClient_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := NewClient(Config{BaseURL: "http://127.0.0.1:0", TenantID: "test"})
	err := c.RegisterAgent(ctx, RegisterAgentRequest{ID: "agent-1"})
	assert.Error(t, err)
}

func TestClient_GetTask_NotFoundNoBody(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	c := newTestClient(t, mux)
	_, err := c.GetTask(context.Background(), "nonexistent")
	assert.Error(t, err)
}

func TestClient_PublishTask_DecodeError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/tenants/test-tenant/tasks", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("invalid json"))
	})
	c := newTestClient(t, mux)
	_, err := c.PublishTask(context.Background(), PublishTaskRequest{
		ID: "task-1", SourceAgent: "agent-1", TargetType: "agent", TargetValue: "agent-2",
	})
	assert.Error(t, err)
}

func TestClient_GetTaskEvents_DecodeError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("not json"))
	})
	c := newTestClient(t, mux)
	_, err := c.GetTaskEvents(context.Background(), "task-1")
	assert.Error(t, err)
}

func TestClient_GetTask_DecodeError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("not json"))
	})
	c := newTestClient(t, mux)
	_, err := c.GetTask(context.Background(), "task-1")
	assert.Error(t, err)
}

func TestClient_CancelTask_NilBody(t *testing.T) {
	c := newTestClient(t, testMux())
	err := c.CancelTask(context.Background(), "task-1")
	require.NoError(t, err)
}

func TestClient_AckTask_NoTokenUsage(t *testing.T) {
	c := newTestClient(t, testMux())
	err := c.AckTask(context.Background(), "task-1", AckRequest{LeaseID: "lease-abc"})
	require.NoError(t, err)
}

func TestClient_NackTask_NoError(t *testing.T) {
	c := newTestClient(t, testMux())
	err := c.NackTask(context.Background(), "task-1", NackRequest{LeaseID: "lease-abc", Retriable: false})
	require.NoError(t, err)
}
