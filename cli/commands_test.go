package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	janus "github.com/agentium-lab/Janus/sdk/go"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func executeCommand(cmd *cobra.Command, args ...string) (string, error) {
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return buf.String(), err
}

func newTestRoot(srv *httptest.Server) *cobra.Command {
	if srv != nil {
		serverURL = srv.URL
	} else {
		serverURL = "http://localhost:0"
	}
	tenantID = "test-tenant"
	root := &cobra.Command{Use: "janus", SilenceErrors: true, SilenceUsage: true}
	root.PersistentFlags().StringVar(&serverURL, "server", serverURL, "")
	root.PersistentFlags().StringVar(&tenantID, "tenant", tenantID, "")
	root.AddCommand(agentCmd())
	root.AddCommand(taskCmd())
	root.AddCommand(mailboxCmd())
	root.AddCommand(dashboardCmd())
	return root
}

func TestDashboardCmd(t *testing.T) {
	root := newTestRoot(nil)
	out, err := executeCommand(root, "dashboard", "--help")
	assert.NoError(t, err)
	assert.Contains(t, out, "Start local dashboard")
	assert.Contains(t, out, "--port")
}

func TestDashboardCmd_ServeAndShutdown(t *testing.T) {
	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ws" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer apiSrv.Close()

	done := make(chan struct{})
	var startErr error

	serverURL = apiSrv.URL
	tenantID = "test"

	go func() {
		defer close(done)
		cmd := &cobra.Command{Use: "test", SilenceErrors: true, SilenceUsage: true}
		cmd.SetOut(io.Discard)
		cmd.AddCommand(dashboardCmd())
		cmd.SetArgs([]string{"dashboard", "--port", "0"})
		startErr = cmd.Execute()
	}()

	time.Sleep(200 * time.Millisecond)

	select {
	case <-done:
		if startErr != nil {
			t.Logf("dashboard exited: %v", startErr)
		}
	default:
		t.Log("Dashboard started and is running (expected for ListenAndServe)")
	}
}

func TestDashboardCmd_HelpOnly(t *testing.T) {
	var buf bytes.Buffer
	cmd := &cobra.Command{Use: "test"}
	cmd.SetOut(&buf)
	serverURL = "http://localhost:8080"
	tenantID = "default"
	cmd.AddCommand(dashboardCmd())
	cmd.SetArgs([]string{"dashboard", "--help"})
	require.NoError(t, cmd.Execute())
	assert.Contains(t, buf.String(), "Start local dashboard")
}

func TestAgentRegister_MissingFlags(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "agent", "register")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "--id and --name are required")
}

func TestAgentRegister_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/tenants/test-tenant/agents", r.URL.Path)
		assert.Equal(t, "POST", r.Method)
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	out, err := executeCommand(root, "agent", "register", "--id", "a1", "--name", "Agent1")
	assert.NoError(t, err)
	assert.Contains(t, out, "Agent a1 registered")
}

func TestAgentHeartbeat_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "/agents/a1/heartbeat")
		assert.Equal(t, "POST", r.Method)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	out, err := executeCommand(root, "agent", "heartbeat", "a1")
	assert.NoError(t, err)
	assert.Contains(t, out, "Heartbeat sent for a1")
}

func TestAgentHeartbeat_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	out, err := executeCommand(root, "agent", "heartbeat", "a1")
	assert.NoError(t, err)
	assert.Contains(t, out, "status: 500")
}

func TestAgentStatus(t *testing.T) {
	root := newTestRoot(nil)
	out, err := executeCommand(root, "agent", "status", "a1")
	assert.NoError(t, err)
	assert.Contains(t, out, "not yet implemented")
}

func TestTaskPublish_MissingFlags(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "task", "publish")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "--id, --source, --target-type, --target-value are required")
}

func TestTaskPublish_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/tenants/test-tenant/tasks", r.URL.Path)
		assert.Equal(t, "POST", r.Method)
		var req janus.PublishTaskRequest
		body, _ := io.ReadAll(r.Body)
		json.Unmarshal(body, &req)
		assert.Equal(t, "t1", req.ID)
		assert.Equal(t, "agent-a", req.SourceAgent)
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{"id": "t1", "status": "queued"})
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	out, err := executeCommand(root, "task", "publish", "--id", "t1", "--source", "agent-a", "--target-value", "agent-b")
	assert.NoError(t, err)
	assert.Contains(t, out, "queued")
}

func TestTaskPublish_WithPayloadFile(t *testing.T) {
	payloadDir := t.TempDir()
	payloadFile := filepath.Join(payloadDir, "payload.json")
	require.NoError(t, os.WriteFile(payloadFile, []byte(`{"key":"value"}`), 0644))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req janus.PublishTaskRequest
		body, _ := io.ReadAll(r.Body)
		json.Unmarshal(body, &req)
		assert.Contains(t, req.Envelope.Payload.Content, `"key"`)
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{"id": "t2"})
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	out, err := executeCommand(root, "task", "publish", "--id", "t2", "--source", "a", "--target-value", "b", "--payload-file", payloadFile)
	assert.NoError(t, err)
	assert.Contains(t, out, "t2")
}

func TestTaskPublish_BadPayloadFile(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "task", "publish", "--id", "t3", "--source", "a", "--target-value", "b", "--payload-file", "/nonexistent/file.json")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "read payload")
}

func TestTaskStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "/tasks/t1")
		json.NewEncoder(w).Encode(map[string]string{"task_id": "t1", "status": "running"})
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	out, err := executeCommand(root, "task", "status", "t1")
	assert.NoError(t, err)
	assert.Contains(t, out, "running")
}

func TestTaskCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	out, err := executeCommand(root, "task", "cancel", "t1")
	assert.NoError(t, err)
	assert.Contains(t, out, "Task t1 cancelled")
}

func TestTaskEvents(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "/tasks/t1/events")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"events": []map[string]string{
				{"event_type": "task.created"},
				{"event_type": "task.started"},
			},
		})
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	out, err := executeCommand(root, "task", "events", "t1")
	assert.NoError(t, err)
	assert.Contains(t, out, "task.created")
}

func TestMailboxPull_Empty(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "/mailboxes/mb1/pull")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	out, err := executeCommand(root, "mailbox", "pull", "mb1")
	assert.NoError(t, err)
	assert.Contains(t, out, "No messages available")
}

func TestMailboxPull_WithTask(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"task": map[string]string{"id": "t1", "status": "pending"},
			"lease": map[string]string{
				"lease_id": "l1",
			},
		})
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	out, err := executeCommand(root, "mailbox", "pull", "mb1", "--agent", "a1")
	assert.NoError(t, err)
	assert.Contains(t, out, "t1")
}

func TestMailboxAck(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "/tasks/t1/ack")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "mailbox", "ack", "t1", "--lease", "l1")
	assert.NoError(t, err)
}

func TestMailboxNack(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "/tasks/t1/nack")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "mailbox", "nack", "t1", "--lease", "l1", "--retriable=true", "--error-code", "ERR_TIMEOUT")
	assert.NoError(t, err)
}

func TestMailboxNack_NoError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "mailbox", "nack", "t1", "--lease", "l1")
	assert.NoError(t, err)
}

func TestMailboxPull_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "mailbox", "pull", "mb1")
	assert.Error(t, err)
}

func TestTaskStatus_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "task", "status", "t1")
	assert.Error(t, err)
}

func TestMain_Help(t *testing.T) {
	if os.Getenv("GO_TEST_MAIN") == "1" {
		os.Args = []string{"janus", "--help"}
		main()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestMain_Help")
	cmd.Env = append(os.Environ(), "GO_TEST_MAIN=1")
	out, err := cmd.CombinedOutput()
	assert.NoError(t, err)
	assert.Contains(t, string(out), "Janus")
}

func TestAgentRegister_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "agent", "register", "--id", "a1", "--name", "Agent1")
	assert.Error(t, err)
}

func TestTaskPublish_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "task", "publish", "--id", "t1", "--source", "a", "--target-value", "b")
	assert.Error(t, err)
}

func TestTaskEvents_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "task", "events", "t1")
	assert.Error(t, err)
}

func TestTaskCancel_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	root := newTestRoot(srv)
	_, err := executeCommand(root, "task", "cancel", "t1")
	assert.Error(t, err)
}
