package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/agentium-lab/Janus/core"
	janus "github.com/agentium-lab/Janus/sdk/go"
)

type PipelineAgent struct {
	id          string
	name        string
	mailbox     string
	nextMailbox string
	processor   func(taskID string, payload map[string]interface{}) map[string]interface{}
}

type PipelineResult struct {
	Agent    string                 `json:"agent"`
	TaskID   string                 `json:"task_id"`
	Status   string                 `json:"status"`
	Output   map[string]interface{} `json:"output"`
}

func main() {
	serverURL := envOr("JANUS_URL", "http://localhost:8080")
	tenant := envOr("JANUS_TENANT", "pipeline")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := janus.NewClient(janus.Config{BaseURL: serverURL, TenantID: tenant})

	agents := []*PipelineAgent{
		{
			id: "product-agent", name: "Product Agent", mailbox: "product-inbox", nextMailbox: "coding-inbox",
			processor: processProduct,
		},
		{
			id: "coding-agent", name: "Coding Agent", mailbox: "coding-inbox", nextMailbox: "review-inbox",
			processor: processCoding,
		},
		{
			id: "review-agent", name: "Review Agent", mailbox: "review-inbox", nextMailbox: "test-inbox",
			processor: processReview,
		},
		{
			id: "test-agent", name: "Test Agent", mailbox: "test-inbox", nextMailbox: "security-inbox",
			processor: processTest,
		},
		{
			id: "security-agent", name: "Security Agent", mailbox: "security-inbox", nextMailbox: "approval-inbox",
			processor: processSecurity,
		},
		{
			id: "approver-agent", name: "Human Approver", mailbox: "approval-inbox", nextMailbox: "release-inbox",
			processor: processApproval,
		},
		{
			id: "release-agent", name: "Release Agent", mailbox: "release-inbox", nextMailbox: "",
			processor: processRelease,
		},
	}

	log.Println("=== Janus Pipeline Demo: 7-Agent Software Delivery ===")
	log.Printf("Server: %s | Tenant: %s\n", serverURL, tenant)
	log.Println()

	log.Println("[Setup] Creating tenant...")
	if err := client.CreateTenant(ctx, tenant, "Pipeline Demo"); err != nil {
		if !isConflict(err) {
			log.Fatalf("create tenant: %v", err)
		}
	}

	log.Println("[Setup] Registering agents and creating mailboxes...")
	for _, a := range agents {
		if err := client.CreateMailbox(ctx, a.mailbox, a.id); err != nil {
			if !isConflict(err) {
				log.Fatalf("create mailbox %s: %v", a.mailbox, err)
			}
		}
		if err := client.RegisterAgent(ctx, janus.RegisterAgentRequest{
			ID: a.id, DisplayName: a.name, Protocol: "a2a",
		}); err != nil {
			if !isConflict(err) {
				log.Fatalf("register %s: %v", a.id, err)
			}
		}
		log.Printf("  ✓ %s (%s)", a.name, a.id)
	}

	log.Println()

	var wg sync.WaitGroup
	results := make(chan PipelineResult, 100)
	done := make(chan struct{})

	for _, a := range agents {
		a := a
		wg.Add(1)
		go func() {
			defer wg.Done()
			a.run(ctx, client, tenant, results)
		}()
	}

	go func() {
		wg.Wait()
		close(done)
	}()

	time.Sleep(500 * time.Millisecond)

	requirementID := fmt.Sprintf("req-%d", time.Now().UnixNano())
	log.Printf("[Kickoff] Product Agent publishing requirement: %s\n", requirementID)
	log.Println()

	publishNextTask(ctx, client, tenant, "product-inbox", requirementID, "orchestrator", map[string]interface{}{
		"title":       "Add user authentication module",
		"description": "Implement JWT-based authentication with refresh token rotation",
		"priority":    "high",
		"repository":  "github.com/acme/backend",
		"branch":      "feature/auth",
	})

	printTicker := time.NewTicker(3 * time.Second)
	defer printTicker.Stop()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	completed := 0
	totalAgents := len(agents)

	for {
		select {
		case r := <-results:
			icon := "✓"
			if r.Status == "rejected" || r.Status == "failed" {
				icon = "✗"
			}
			log.Printf("  %s [%s] %s → %s", icon, r.Agent, r.TaskID, r.Status)
			if details, ok := r.Output["details"]; ok {
				log.Printf("         %v", details)
			}
			completed++
			if completed >= totalAgents {
				log.Println()
				log.Println("=== Pipeline Complete ===")
				goto finish
			}

		case <-printTicker.C:
			log.Printf("  ... (%d/%d agents completed)\n", completed, totalAgents)

		case <-quit:
			log.Println("\nInterrupted.")
			cancel()
			<-done
			return
		}
	}

finish:
	cancel()
	<-done
}

func (a *PipelineAgent) run(ctx context.Context, client *janus.Client, tenantID string, results chan<- PipelineResult) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		result, err := client.PullTask(ctx, a.mailbox, a.id)
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}
		if result == nil || result.Task == nil {
			time.Sleep(300 * time.Millisecond)
			continue
		}

		task := result.Task
		var payload map[string]interface{}
		if task.Envelope.Payload.Content != "" {
			json.Unmarshal([]byte(task.Envelope.Payload.Content), &payload)
		}

		log.Printf("[Processing] %s handling %s...", a.name, task.ID)

		time.Sleep(time.Duration(200+rand.Intn(800)) * time.Millisecond)

		output := a.processor(task.ID, payload)

		if result.Lease.LeaseID != "" {
			resultRef := fmt.Sprintf("result://%s/%s", a.id, task.ID)
			if err := client.AckTask(ctx, task.ID, janus.AckRequest{
				LeaseID:   result.Lease.LeaseID,
				Attempt:   result.Lease.Attempt,
				ResultRef: resultRef,
			}); err != nil {
				log.Printf("[Error] %s ACK failed for %s: %v", a.name, task.ID, err)
				continue
			}
		}

		results <- PipelineResult{
			Agent:  a.name,
			TaskID: task.ID,
			Status: output["status"].(string),
			Output: output,
		}

		if a.nextMailbox != "" {
			nextID := fmt.Sprintf("%s-%s", output["stage"].(string), time.Now().Format("150405"))
			publishNextTask(ctx, client, tenantID, a.nextMailbox, nextID, a.id, output)
		}
	}
}

func publishNextTask(ctx context.Context, client *janus.Client, tenant, mailbox, taskID, source string, payload map[string]interface{}) {
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
			Trace:        core.TraceContext{TraceID: "pipeline-" + taskID},
		},
	})
	if err != nil {
		log.Printf("[Error] publish to %s: %v", mailbox, err)
	}
}

func processProduct(taskID string, payload map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{
		"status":      "approved",
		"stage":       "code",
		"task_id":     taskID,
		"title":       payload["title"],
		"repository":  payload["repository"],
		"branch":      payload["branch"],
		"details":     fmt.Sprintf("Requirement approved: %s", payload["title"]),
		"files_scope": []string{"auth.go", "auth_test.go", "middleware.go", "config.go"},
	}
}

func processCoding(taskID string, payload map[string]interface{}) map[string]interface{} {
	files := []string{"auth.go", "auth_test.go", "middleware.go"}
	return map[string]interface{}{
		"status":     "coded",
		"stage":      "review",
		"task_id":    taskID,
		"files":      files,
		"lines_added": 20 + rand.Intn(200),
		"details":    fmt.Sprintf("Implemented auth module in %d files", len(files)),
		"test_coverage": fmt.Sprintf("%.1f%%", 80+rand.Float64()*20),
	}
}

func processReview(taskID string, payload map[string]interface{}) map[string]interface{} {
	approved := rand.Float32() > 0.15
	result := map[string]interface{}{
		"stage":   "test",
		"task_id": taskID,
	}
	if approved {
		result["status"] = "approved"
		result["details"] = "Code quality approved. Clean architecture, proper error handling."
	} else {
		result["status"] = "approved"
		result["details"] = "Minor suggestions addressed inline. Approved with comments."
	}
	return result
}

func processTest(taskID string, payload map[string]interface{}) map[string]interface{} {
	total := 12 + rand.Intn(20)
	passed := total - rand.Intn(2)
	coverage := 82 + rand.Float64()*18
	return map[string]interface{}{
		"status":   "passed",
		"stage":    "security",
		"task_id":  taskID,
		"tests_total": total,
		"tests_passed": passed,
		"tests_failed": total - passed,
		"coverage": fmt.Sprintf("%.1f%%", coverage),
		"details": fmt.Sprintf("Ran %d tests: %d passed, %d failed. Coverage: %.1f%%",
			total, passed, total-passed, coverage),
	}
}

func processSecurity(taskID string, payload map[string]interface{}) map[string]interface{} {
	vulns := rand.Intn(3)
	result := map[string]interface{}{
		"stage":   "approval",
		"task_id": taskID,
	}
	if vulns == 0 {
		result["status"] = "clean"
		result["details"] = "No vulnerabilities found. Dependency scan clean."
	} else {
		result["status"] = "approved_with_notes"
		result["details"] = fmt.Sprintf("Found %d low-severity advisory(s). Acceptable risk.", vulns)
	}
	return result
}

func processApproval(taskID string, payload map[string]interface{}) map[string]interface{} {
	approved := rand.Float32() > 0.1
	result := map[string]interface{}{
		"stage":   "release",
		"task_id": taskID,
	}
	if approved {
		result["status"] = "approved"
		result["approver"] = "tech-lead"
		result["details"] = "Human review: Approved for production release."
	} else {
		result["status"] = "rejected"
		result["approver"] = "tech-lead"
		result["details"] = "Human review: Needs more integration testing."
	}
	return result
}

func processRelease(taskID string, payload map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{
		"status":   "released",
		"stage":    "done",
		"task_id":  taskID,
		"version":  fmt.Sprintf("v0.%d.%d", rand.Intn(10), rand.Intn(100)),
		"details":  "Deployed to production. Health checks passing.",
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func isConflict(err error) bool {
	return err != nil && (err.Error() == "conflict" || contains(err.Error(), "409") || contains(err.Error(), "already exists"))
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 || findSubstring(s, sub))
}

func findSubstring(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
