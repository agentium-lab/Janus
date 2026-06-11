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

type DemoAgent struct {
	id       string
	name     string
	tenant   string
	client   *janus.Client
	mailbox  string
	stop     chan struct{}
	wg       sync.WaitGroup
	onTask   func(task *core.Task) string
}

func main() {
	serverURL := envOr("JANUS_URL", "http://localhost:8080")
	tenant := envOr("JANUS_TENANT", "demo")

	client := janus.NewClient(janus.Config{BaseURL: serverURL, TenantID: tenant})

	agents := []*DemoAgent{
		{
			id: "coding-agent", name: "Coding Agent", tenant: tenant,
			client: client, mailbox: "coding-inbox",
			onTask: codingAgentHandler,
		},
		{
			id: "review-agent", name: "Review Agent", tenant: tenant,
			client: client, mailbox: "review-inbox",
			onTask: reviewAgentHandler,
		},
		{
			id: "test-agent", name: "Test Agent", tenant: tenant,
			client: client, mailbox: "test-inbox",
			onTask: testAgentHandler,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, a := range agents {
		if err := a.client.RegisterAgent(ctx, janus.RegisterAgentRequest{
			ID: a.id, DisplayName: a.name, Protocol: "a2a",
		}); err != nil {
			log.Fatalf("register %s: %v", a.id, err)
		}
		log.Printf("Registered: %s (%s)", a.name, a.id)
	}

	for _, a := range agents {
		a := a
		go func() {
			a.wg.Add(1)
			defer a.wg.Done()
			a.run(ctx)
		}()
	}

	publishDemoTasks(ctx, client, tenant)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down...")
	cancel()
	for _, a := range agents {
		a.wg.Wait()
	}
}

func (a *DemoAgent) run(ctx context.Context) {
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
		log.Printf("[%s] Processing task %s", a.name, task.ID)

		output := a.onTask(task)

		if result.Lease.LeaseID != "" {
			err = a.client.AckTask(ctx, task.ID, janus.AckRequest{
				LeaseID:   result.Lease.LeaseID,
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

func codingAgentHandler(task *core.Task) string {
	time.Sleep(time.Duration(500+rand.Intn(1000)) * time.Millisecond)
	result := map[string]string{
		"status":   "coded",
		"files":    "main.go, handler.go, service.go",
		"task_id":  task.ID,
		"agent":    "coding-agent",
	}
	data, _ := json.Marshal(result)
	return string(data)
}

func reviewAgentHandler(task *core.Task) string {
	time.Sleep(time.Duration(300+rand.Intn(700)) * time.Millisecond)
	approved := rand.Float32() > 0.2
	result := map[string]interface{}{
		"status":   "reviewed",
		"approved": approved,
		"task_id":  task.ID,
		"agent":    "review-agent",
	}
	if !approved {
		result["comments"] = "Consider error handling edge cases"
	}
	data, _ := json.Marshal(result)
	return string(data)
}

func testAgentHandler(task *core.Task) string {
	time.Sleep(time.Duration(200+rand.Intn(500)) * time.Millisecond)
	passed := rand.Float32() > 0.15
	result := map[string]interface{}{
		"status":    "tested",
		"passed":    passed,
		"task_id":   task.ID,
		"agent":     "test-agent",
		"coverage":  fmt.Sprintf("%.1f%%", 85+rand.Float64()*15),
	}
	if !passed {
		result["failed_tests"] = rand.Intn(3) + 1
	}
	data, _ := json.Marshal(result)
	return string(data)
}

func publishDemoTasks(ctx context.Context, client *janus.Client, tenant string) {
	for i := 1; i <= 3; i++ {
		taskID := fmt.Sprintf("demo-task-%d", i)
		var targetMailbox string
		switch i {
		case 1:
			targetMailbox = "coding-inbox"
		case 2:
			targetMailbox = "review-inbox"
		case 3:
			targetMailbox = "test-inbox"
		}

		_, err := client.PublishTask(ctx, janus.PublishTaskRequest{
			ID:          taskID,
			SourceAgent: "demo-orchestrator",
			TargetType:  "mailbox",
			TargetValue: targetMailbox,
			Priority:    "normal",
			Envelope: core.TaskEnvelope{
				JanusVersion: "0.1",
				TaskID:       taskID,
				TenantID:     tenant,
				SourceAgent:  "demo-orchestrator",
				Target:       core.Target{Type: "mailbox", Value: targetMailbox},
				Payload: core.Payload{
					Type:    "json",
					Content: fmt.Sprintf(`{"step": %d, "description": "Demo task %d"}`, i, i),
				},
			},
		})
		if err != nil {
			log.Printf("Failed to publish task %s: %v", taskID, err)
			continue
		}
		log.Printf("Published: %s → %s", taskID, targetMailbox)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
