package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/agentium-lab/Janus/core"
	janus "github.com/agentium-lab/Janus/sdk/go"
)

type PipelineAgent struct {
	id       string
	name     string
	tenant   string
	client   *janus.Client
	mailbox  string
	wg       sync.WaitGroup
	onTask   func(ctx context.Context, task *core.Task, client *janus.Client, tenant string) string
}

type pipelineStep struct {
	agent   string
	mailbox string
	action  string
}

func main() {
	serverURL := envOr("JANUS_URL", "http://localhost:8080")
	tenant := envOr("JANUS_TENANT", "demo")

	client := janus.NewClient(janus.Config{BaseURL: serverURL, TenantID: tenant})

	agents := []*PipelineAgent{
		{id: "product-agent", name: "Product Manager", tenant: tenant, client: client, mailbox: "product-inbox", onTask: productAgentHandler},
		{id: "review-agent", name: "Code Reviewer", tenant: tenant, client: client, mailbox: "review-inbox", onTask: reviewAgentHandler},
		{id: "code-agent", name: "Coding Agent", tenant: tenant, client: client, mailbox: "code-inbox", onTask: codeAgentHandler},
		{id: "test-agent", name: "Test Agent", tenant: tenant, client: client, mailbox: "test-inbox", onTask: testAgentHandler},
		{id: "security-agent", name: "Security Scanner", tenant: tenant, client: client, mailbox: "security-inbox", onTask: securityAgentHandler},
		{id: "human-approver", name: "Human Approver", tenant: tenant, client: client, mailbox: "approval-inbox", onTask: humanApproverHandler},
		{id: "deploy-agent", name: "Deploy Agent", tenant: tenant, client: client, mailbox: "deploy-inbox", onTask: deployAgentHandler},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Println("=== Janus 7-Agent Coding/DevOps Pipeline Demo ===")
	log.Println()

	if err := client.CreateTenant(ctx, tenant, "Demo Corp"); err != nil {
		log.Printf("tenant (may already exist): %v", err)
	}

	for _, a := range agents {
		if err := client.RegisterAgent(ctx, janus.RegisterAgentRequest{
			ID: a.id, DisplayName: a.name, Protocol: "a2a",
		}); err != nil {
			log.Fatalf("register %s: %v", a.id, err)
		}
		if err := client.CreateMailbox(ctx, a.mailbox, a.id); err != nil {
			log.Printf("mailbox %s (may exist): %v", a.mailbox, err)
		}
		log.Printf("  Registered: %s (%s)", a.name, a.id)
	}
	log.Println()

	for _, a := range agents {
		a := a
		a.wg.Add(1)
		go func() {
			defer a.wg.Done()
			a.run(ctx)
		}()
	}

	traceID := fmt.Sprintf("pipeline-%d", time.Now().UnixMilli())
	taskID := fmt.Sprintf("feature-%d", time.Now().UnixMilli())
	log.Printf("Kicking off pipeline with task %s (trace: %s)", taskID, traceID)
	log.Println()

	_, err := client.PublishTask(ctx, janus.PublishTaskRequest{
		ID:          taskID,
		SourceAgent: "orchestrator",
		TargetType:  "mailbox",
		TargetValue: "product-inbox",
		MailboxID:   "product-inbox",
		Priority:    "normal",
		Envelope: core.TaskEnvelope{
			JanusVersion: "0.1",
			TaskID:       taskID,
			TenantID:     tenant,
			SourceAgent:  "orchestrator",
			Target:       core.Target{Type: "mailbox", Value: "product-inbox"},
			Payload: core.Payload{
				Type:    "feature_request",
				Content: `{"feature": "Add user authentication", "priority": "high"}`,
			},
			Trace: core.TraceContext{TraceID: traceID},
		},
	})
	if err != nil {
		log.Fatalf("publish: %v", err)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println()
	log.Println("Shutting down...")
	cancel()
	for _, a := range agents {
		a.wg.Wait()
	}
}

func (a *PipelineAgent) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		result, err := a.client.PullTask(ctx, a.mailbox, a.id)
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}
		if result == nil || result.Task == nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		task := result.Task
		log.Printf("[%s] Pulled task %s", a.name, task.ID)

		if result.Lease.LeaseID != "" {
			_ = a.client.StartTask(ctx, task.ID, result.Lease.Attempt, result.Lease.LeaseID)
		}

		output := a.onTask(ctx, task, a.client, a.tenant)

		if result.Lease.LeaseID != "" {
			err = a.client.AckTask(ctx, task.ID, janus.AckRequest{
				LeaseID:   result.Lease.LeaseID,
				Attempt:   result.Lease.Attempt,
				ResultRef: fmt.Sprintf("result://%s/%s", a.id, task.ID),
			})
			if err != nil {
				log.Printf("[%s] ACK failed for %s: %v", a.name, task.ID, err)
				continue
			}
		}

		log.Printf("[%s] Completed task %s: %s", a.name, task.ID, output)
		return
	}
}

func sendToNext(ctx context.Context, client *janus.Client, tenant, fromAgent, targetMailbox, taskID, traceID, payloadType, payloadContent string) {
	nextID := fmt.Sprintf("%s-step-%d", taskID, time.Now().UnixMilli())
	log.Printf("  -> Sending to %s: %s", targetMailbox, nextID)
	_, err := client.PublishTask(ctx, janus.PublishTaskRequest{
		ID:          nextID,
		SourceAgent: fromAgent,
		TargetType:  "mailbox",
		TargetValue: targetMailbox,
		MailboxID:   targetMailbox,
		Priority:    "normal",
		Envelope: core.TaskEnvelope{
			JanusVersion: "0.1",
			TaskID:       nextID,
			TenantID:     tenant,
			SourceAgent:  fromAgent,
			Target:       core.Target{Type: "mailbox", Value: targetMailbox},
			Payload:      core.Payload{Type: payloadType, Content: payloadContent},
			Trace:        core.TraceContext{TraceID: traceID, ParentTaskID: taskID},
		},
	})
	if err != nil {
		log.Printf("  -> Failed to send to %s: %v", targetMailbox, err)
	}
}

func productAgentHandler(ctx context.Context, task *core.Task, client *janus.Client, tenant string) string {
	time.Sleep(300 * time.Millisecond)
	result := map[string]string{
		"status":       "requirements_defined",
		"feature":      "Add user authentication",
		"requirements": "JWT tokens, login/signup endpoints, session management",
	}
	data, _ := json.Marshal(result)
	sendToNext(ctx, client, tenant, "product-agent", "review-inbox", task.ID, task.Envelope.Trace.TraceID, "review_request", string(data))
	return "Requirements defined -> sent to review"
}

func reviewAgentHandler(ctx context.Context, task *core.Task, client *janus.Client, tenant string) string {
	time.Sleep(400 * time.Millisecond)
	result := map[string]string{
		"status":   "review_approved",
		"comments": "LGTM, proceed with implementation",
	}
	data, _ := json.Marshal(result)
	sendToNext(ctx, client, tenant, "review-agent", "code-inbox", task.ID, task.Envelope.Trace.TraceID, "code_request", string(data))
	return "Review approved -> sent to coding"
}

func codeAgentHandler(ctx context.Context, task *core.Task, client *janus.Client, tenant string) string {
	time.Sleep(600 * time.Millisecond)
	result := map[string]string{
		"status": "code_complete",
		"files":  "auth.go, handler.go, middleware.go",
	}
	data, _ := json.Marshal(result)
	sendToNext(ctx, client, tenant, "code-agent", "test-inbox", task.ID, task.Envelope.Trace.TraceID, "test_request", string(data))
	return "Code written -> sent to testing"
}

func testAgentHandler(ctx context.Context, task *core.Task, client *janus.Client, tenant string) string {
	time.Sleep(500 * time.Millisecond)
	result := map[string]string{
		"status":   "tests_passed",
		"coverage": "92.5%",
		"tests":    "12/12 passed",
	}
	data, _ := json.Marshal(result)
	sendToNext(ctx, client, tenant, "test-agent", "security-inbox", task.ID, task.Envelope.Trace.TraceID, "security_scan_request", string(data))
	return "Tests passed -> sent to security"
}

func securityAgentHandler(ctx context.Context, task *core.Task, client *janus.Client, tenant string) string {
	time.Sleep(400 * time.Millisecond)
	result := map[string]string{
		"status":     "scan_passed",
		"vulnerabilities": "0 critical, 0 high",
	}
	data, _ := json.Marshal(result)
	sendToNext(ctx, client, tenant, "security-agent", "approval-inbox", task.ID, task.Envelope.Trace.TraceID, "approval_request", string(data))
	return "Security scan passed -> sent to human approval"
}

func humanApproverHandler(ctx context.Context, task *core.Task, client *janus.Client, tenant string) string {
	time.Sleep(200 * time.Millisecond)
	result := map[string]string{
		"status":    "approved",
		"approver":  "human-approver",
		"condition": "Deploy to staging first",
	}
	data, _ := json.Marshal(result)
	sendToNext(ctx, client, tenant, "human-approver", "deploy-inbox", task.ID, task.Envelope.Trace.TraceID, "deploy_request", string(data))
	return "Human approved -> sent to deploy"
}

func deployAgentHandler(ctx context.Context, task *core.Task, client *janus.Client, tenant string) string {
	time.Sleep(500 * time.Millisecond)
	log.Println()
	log.Println("========================================")
	log.Println("  PIPELINE COMPLETE!")
	log.Println("  Feature: Add user authentication")
	log.Println("  Status: Deployed to staging")
	log.Println("  Agents: 7/7 completed successfully")
	log.Println("========================================")
	log.Println()
	return "Deployed to staging successfully"
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
