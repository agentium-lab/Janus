package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/agentium-lab/Janus/core"
	janus "github.com/agentium-lab/Janus/sdk/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func serverURL() string {
	if v := os.Getenv("JANUS_URL"); v != "" {
		return v
	}
	return "http://localhost:8080"
}

func uniqueTenant() string {
	return fmt.Sprintf("test-pipeline-%d", time.Now().UnixNano())
}

type agentCompletion struct {
	agentID  string
	taskID   string
	status   string
	duration time.Duration
}

type agentDef struct {
	id          string
	mailbox     string
	nextMailbox string
	stage       string
}

func TestPipeline7Agents(t *testing.T) {
	if os.Getenv("JANUS_INTEGRATION") == "" {
		t.Skip("skipping: set JANUS_INTEGRATION=1 to run (requires running Janus server)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	url := serverURL()
	tenant := uniqueTenant()
	client := janus.NewClient(janus.Config{BaseURL: url, TenantID: tenant})

	t.Logf("tenant=%s url=%s", tenant, url)

	require.NoError(t, client.CreateTenant(ctx, tenant, "Pipeline Test"), "create tenant")

	defs := []agentDef{
		{"product-agent", "product-inbox", "coding-inbox", "code"},
		{"coding-agent", "coding-inbox", "review-inbox", "review"},
		{"review-agent", "review-inbox", "test-inbox", "test"},
		{"test-agent", "test-inbox", "security-inbox", "security"},
		{"security-agent", "security-inbox", "approval-inbox", "approval"},
		{"approver-agent", "approval-inbox", "release-inbox", "release"},
		{"release-agent", "release-inbox", "", "done"},
	}

	for _, d := range defs {
		require.NoError(t, client.CreateMailbox(ctx, d.mailbox, d.id), "create mailbox %s", d.mailbox)
		require.NoError(t, client.RegisterAgent(ctx, janus.RegisterAgentRequest{
			ID: d.id, DisplayName: d.id, Protocol: "a2a",
		}), "register agent %s", d.id)
	}

	completions := make(chan agentCompletion, 7)
	var wg sync.WaitGroup

	for _, d := range defs {
		d := d
		wg.Add(1)
		go func() {
			defer wg.Done()
			runAgent(ctx, t, client, tenant, d, completions)
		}()
	}

	time.Sleep(300 * time.Millisecond)

	reqID := fmt.Sprintf("req-%d", time.Now().UnixNano())
	publishTime := time.Now()
	require.NoError(t, publishTask(ctx, client, tenant, "product-inbox", reqID, "orchestrator", map[string]interface{}{
		"title":  "Add user authentication module",
		"repo":   "github.com/acme/backend",
		"branch": "feature/auth",
	}), "publish initial requirement")

	completed := map[string]agentCompletion{}
	timeout := time.After(20 * time.Second)

	for len(completed) < 7 {
		select {
		case c := <-completions:
			completed[c.agentID] = c
			t.Logf("  ✓ [%s] %s → %s (%v)", c.agentID, c.taskID, c.status, c.duration)
		case <-timeout:
			t.Fatalf("pipeline timeout: only %d/7 agents completed", len(completed))
		case <-ctx.Done():
			t.Fatalf("context cancelled: only %d/7 agents completed", len(completed))
		}
	}

	cancel()
	wg.Wait()

	assert.Equal(t, 7, len(completed), "all 7 agents must complete")

	totalElapsed := time.Since(publishTime)
	t.Logf("")
	t.Logf("=== Pipeline Result ===")
	t.Logf("  Total agents: %d", len(completed))
	t.Logf("  Total time:   %v", totalElapsed)
	t.Logf("  Per-agent:    %v", totalElapsed/7)

	for _, d := range defs {
		c, ok := completed[d.id]
		assert.True(t, ok, "agent %s should have completed", d.id)
		if ok {
			assert.NotEmpty(t, c.taskID, "agent %s should have a task", d.id)
		}
	}

	assert.Less(t, totalElapsed, 10*time.Second, "pipeline should complete within 10s")
}

func runAgent(ctx context.Context, t *testing.T, client *janus.Client, tenant string, def agentDef, completions chan<- agentCompletion) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		result, err := client.PullTask(ctx, def.mailbox, def.id)
		if err != nil || result == nil || result.Task == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		task := result.Task
		start := time.Now()

		var payload map[string]interface{}
		if task.Envelope.Payload.Content != "" {
			_ = json.Unmarshal([]byte(task.Envelope.Payload.Content), &payload)
		}

		time.Sleep(time.Duration(50) * time.Millisecond)

		output := processAgent(def.id, payload)

		if result.Lease.LeaseID != "" {
			resultRef := fmt.Sprintf("result://%s/%s", def.id, task.ID)
			if err := client.AckTask(ctx, task.ID, janus.AckRequest{
				LeaseID:   result.Lease.LeaseID,
				Attempt:   result.Lease.Attempt,
				ResultRef: resultRef,
			}); err != nil {
				t.Logf("[WARN] %s ack failed for %s: %v", def.id, task.ID, err)
				continue
			}
		}

		if def.nextMailbox != "" {
			nextID := fmt.Sprintf("%s-%d", def.stage, time.Now().UnixNano())
			if err := publishTask(ctx, client, tenant, def.nextMailbox, nextID, def.id, output); err != nil {
				t.Logf("[WARN] %s publish to %s failed: %v", def.id, def.nextMailbox, err)
			}
		}

		completions <- agentCompletion{
			agentID:  def.id,
			taskID:   task.ID,
			status:   output["status"].(string),
			duration: time.Since(start),
		}
		return
	}
}

func processAgent(agentID string, payload map[string]interface{}) map[string]interface{} {
	switch agentID {
	case "product-agent":
		return map[string]interface{}{
			"status": "approved", "stage": "code",
			"details": fmt.Sprintf("Requirement approved: %v", payload["title"]),
			"files":   []string{"auth.go", "auth_test.go", "middleware.go"},
		}
	case "coding-agent":
		return map[string]interface{}{
			"status": "coded", "stage": "review",
			"details": "Implemented auth module in 3 files",
			"lines":   247,
		}
	case "review-agent":
		return map[string]interface{}{
			"status": "approved", "stage": "test",
			"details": "Code quality approved",
		}
	case "test-agent":
		return map[string]interface{}{
			"status": "passed", "stage": "security",
			"details": "21 tests passed, coverage 98.7%",
		}
	case "security-agent":
		return map[string]interface{}{
			"status": "clean", "stage": "approval",
			"details": "No vulnerabilities found",
		}
	case "approver-agent":
		return map[string]interface{}{
			"status": "approved", "stage": "release",
			"details": "Approved for production",
		}
	case "release-agent":
		return map[string]interface{}{
			"status": "released", "stage": "done",
			"details": "Deployed to production",
			"version": "v0.1.0",
		}
	default:
		return map[string]interface{}{"status": "unknown"}
	}
}

func publishTask(ctx context.Context, client *janus.Client, tenant, mailbox, taskID, source string, payload map[string]interface{}) error {
	content, _ := json.Marshal(payload)
	_, err := client.PublishTask(ctx, janus.PublishTaskRequest{
		ID:          taskID,
		SourceAgent: source,
		TargetType:  "mailbox",
		TargetValue: mailbox,
		Priority:    "normal",
		Envelope: core.TaskEnvelope{
			JanusVersion: "0.1",
			TaskID:       taskID,
			TenantID:     tenant,
			SourceAgent:  source,
			Target:       core.Target{Type: "mailbox", Value: mailbox},
			Payload:      core.Payload{Type: "json", Content: string(content)},
			Trace:        core.TraceContext{TraceID: "test-" + taskID},
		},
	})
	return err
}
