package janus

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient_ReportProgress_FullPayload(t *testing.T) {
	pct := 42
	var gotBody map[string]interface{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/tenants/test-tenant/tasks/t-1/progress", r.URL.Path)
		require.Equal(t, http.MethodPost, r.Method)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant", APIKey: "sekret"})

	err := c.ReportProgress(context.Background(), "t-1", "halfway", "agent-1", &pct, map[string]interface{}{"step": 2})
	require.NoError(t, err)
	assert.Equal(t, "halfway", gotBody["message"])
	assert.Equal(t, "agent-1", gotBody["agent_id"])
	assert.Equal(t, float64(42), gotBody["percent"])
	assert.Equal(t, map[string]interface{}{"step": float64(2)}, gotBody["data"])
}

func TestClient_ReportProgress_MinimalPayload(t *testing.T) {
	var gotBody map[string]interface{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"})

	err := c.ReportProgress(context.Background(), "t-1", "starting", "agent-1", nil, nil)
	require.NoError(t, err)
	assert.NotContains(t, gotBody, "percent")
	assert.NotContains(t, gotBody, "data")
}

func TestClient_ReportProgress_Error(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(`{"error":"scope denied"}`))
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"})

	err := c.ReportProgress(context.Background(), "t-1", "msg", "agent-1", nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scope denied")
}

func sseServer(t *testing.T, events []string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		taskID := "t-1"
		if strings.HasSuffix(r.URL.Path, "/stream") {
			parts := strings.Split(strings.TrimSuffix(r.URL.Path, "/stream"), "/")
			taskID = parts[len(parts)-1]
		}
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		for _, e := range events {
			for _, line := range strings.Split(strings.TrimSuffix(e, "\n"), "\n") {
				if strings.HasPrefix(line, "data: {TASKID}") {
					line = "data: " + `{"task_id":"` + taskID + `"}`
				}
				_, _ = io.WriteString(w, line+"\n")
			}
			_, _ = io.WriteString(w, "\n")
			flusher.Flush()
		}
	}))
}

func TestClient_StreamTask_Lifecycle(t *testing.T) {
	srv := sseServer(t, []string{
		"event: task.progress\ndata: {\"task_id\":\"t-1\",\"payload\":{\"percent\":10}}\n",
		"id: 1\n: keep-alive comment\n",
		"event: task.progress\ndata: {TASKID}\n",
		"event: task.completed\ndata: {TASKID}\n",
	})
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant", APIKey: "sekret"})

	var types []string
	err := c.StreamTask(context.Background(), "t-1", func(evt StreamEvent) error {
		types = append(types, evt.EventType)
		if evt.EventType == "task.progress" && len(types) == 1 {
			assert.Equal(t, "t-1", evt.TaskID)
			assert.Equal(t, float64(10), evt.Payload["percent"])
		}
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"task.progress", "task.progress", "task.completed"}, types)
}

func TestClient_StreamTask_TerminalEventVariants(t *testing.T) {
	terminals := []string{
		"task.completed", "task.failed", "task.cancelled", "task.dead_lettered", "task.expired",
	}
	for _, term := range terminals {
		term := term
		t.Run(term, func(t *testing.T) {
			srv := sseServer(t, []string{"event: " + term + "\ndata: {TASKID}\n"})
			defer srv.Close()
			c := NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"})

			called := false
			err := c.StreamTask(context.Background(), term, func(evt StreamEvent) error {
				called = true
				assert.Equal(t, term, evt.EventType)
				return nil
			})
			require.NoError(t, err, "terminal event %s must end the stream with nil", term)
			assert.True(t, called)
		})
	}
}

func TestClient_StreamTask_CallbackErrorPropagates(t *testing.T) {
	srv := sseServer(t, []string{"event: task.progress\ndata: {TASKID}\n"})
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"})

	sentinel := errors.New("consumer failed")
	err := c.StreamTask(context.Background(), "t-1", func(StreamEvent) error { return sentinel })
	require.ErrorIs(t, err, sentinel)
}

func TestClient_StreamTask_SkipsBadJSONAndBareData(t *testing.T) {
	srv := sseServer(t, []string{
		"event: task.progress\ndata: {broken json\n",
		"data: no preceding event\n",
		"event: task.completed\ndata: {TASKID}\n",
	})
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"})

	seen := 0
	err := c.StreamTask(context.Background(), "t-1", func(StreamEvent) error { seen++; return nil })
	require.NoError(t, err)
	assert.Equal(t, 1, seen, "malformed data lines must be skipped")
}

func TestClient_StreamTask_EOFWitoutTerminal(t *testing.T) {
	srv := sseServer(t, []string{"event: task.progress\ndata: {TASKID}\n"})
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"})

	err := c.StreamTask(context.Background(), "t-1", func(StreamEvent) error { return nil })
	require.NoError(t, err, "server EOF without terminal event is not an error")
}

func TestClient_StreamTask_Non200Status(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"})

	err := c.StreamTask(context.Background(), "t-1", func(StreamEvent) error { return nil })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stream failed: status 403")
}

func TestClient_StreamTask_Headers(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "sekret", r.Header.Get("X-API-Key"))
		assert.Equal(t, "text/event-stream", r.Header.Get("Accept"))
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant", APIKey: "sekret"})

	err := c.StreamTask(context.Background(), "t-1", func(StreamEvent) error { return nil })
	require.NoError(t, err)
}

func TestClient_StreamTask_RequestError(t *testing.T) {
	c := NewClient(Config{BaseURL: "http://exa mple.invalid", TenantID: "test-tenant"})
	err := c.StreamTask(context.Background(), "t-1", func(StreamEvent) error { return nil })
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "http request", "must fail at request build time")
}

func TestClient_StreamTask_TransportError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close()
	c := NewClient(Config{BaseURL: url, TenantID: "test-tenant"})
	err := c.StreamTask(context.Background(), "t-1", func(StreamEvent) error { return nil })
	require.Error(t, err)
}

func TestClient_PullTask_DecodeError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("{not json"))
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"})

	_, err := c.PullTask(context.Background(), "mb-1", "agent-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode response")
}

func TestClient_PullTask_RequestError(t *testing.T) {
	c := NewClient(Config{BaseURL: "http://exa mple.invalid", TenantID: "test-tenant"})
	_, err := c.PullTask(context.Background(), "mb-1", "agent-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create request")
}

func TestClient_PullTask_SendsBodyAndKey(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "sekret", r.Header.Get("X-API-Key"))
		b, _ := io.ReadAll(r.Body)
		assert.JSONEq(t, `{"agent_id":"agent-1"}`, string(b))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant", APIKey: "sekret"})

	res, err := c.PullTask(context.Background(), "mb-1", "agent-1")
	require.NoError(t, err)
	assert.Nil(t, res)
}

func TestClient_doGet_RequestError(t *testing.T) {
	c := NewClient(Config{BaseURL: "http://exa mple.invalid", TenantID: "t"})
	err := c.doGet(context.Background(), "/x", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create request")
}

func TestClient_doPost_MarshalError(t *testing.T) {
	c := NewClient(Config{BaseURL: "http://127.0.0.1:1", TenantID: "t"})
	err := c.doPost(context.Background(), "/x", map[string]interface{}{"fn": func() {}}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "marshal body")
}

func TestClient_doPost_RequestError(t *testing.T) {
	c := NewClient(Config{BaseURL: "http://exa mple.invalid", TenantID: "t"})
	err := c.doPost(context.Background(), "/x", map[string]string{"a": "b"}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create request")
}

func TestClient_do_SetsAPIKeyHeader(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "sekret", r.Header.Get("X-API-Key"))
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "t", APIKey: "sekret"})

	require.NoError(t, c.doGet(context.Background(), "/x", &struct{}{}))
}

func TestClient_QueryDLQ_FullOptions(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "limit=7&mailbox=mb-9", r.URL.RawQuery)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"tasks":[]}`))
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"})

	tasks, err := c.QueryDLQ(context.Background(), DLQQueryOptions{MailboxID: "mb-9", Limit: 7})
	require.NoError(t, err)
	assert.Empty(t, tasks)
}

func TestDecodeAPIError_BodyStatusOverride(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte(`{"status":429,"code":"CUSTOM","message":"slow down","error":"legacy text"}`))
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "t"})

	err := c.HeartbeatAgent(context.Background(), "a1")
	require.Error(t, err)
	var apiErr *APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, 429, apiErr.StatusCode)
	assert.Equal(t, "CUSTOM", apiErr.Code)
	assert.Equal(t, "slow down", apiErr.Message, "message wins over legacy error field")
}
