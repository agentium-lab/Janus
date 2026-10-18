package service

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/driver/postgres"
)

// --- PG test harness for the service package ---

// serviceTestEnv holds a fully wired set of PG-backed repos + services for
// lifecycle integration tests. Each test gets a fresh database (cleaned via
// migrations up/down).
type serviceTestEnv struct {
	pool        *pgxpool.Pool
	taskRepo    *postgres.TaskRepository
	attemptRepo *postgres.TaskAttemptRepository
	mailboxRepo *postgres.MailboxRepository
	outboxRepo  *postgres.OutboxRepo
	eventRepo   *postgres.EventRepo
	budgetUsage *postgres.BudgetUsageRepo
	tenantRepo  *postgres.TenantRepository
	agentRepo   *postgres.AgentRepository
	dispatch    *DispatchService
	taskSvc     *TaskService
	lifecycle   Lifecycle
	driver      *mockDispatchQueueDriver
}

func openServiceTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	host := os.Getenv("JANUS_PG_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("JANUS_PG_PORT")
	if port == "" {
		port = "5432"
	}
	user := os.Getenv("JANUS_PG_USER")
	password := os.Getenv("JANUS_PG_PASSWORD")
	if user == "" {
		user = "janus"
	}
	testDB := fmt.Sprintf("janus_svctest_%d", time.Now().UnixNano())

	ctx := context.Background()
	adminDSN := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=janus_test sslmode=disable", host, port, user, password)
	adminConn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Skipf("postgres not reachable (set JANUS_PG_HOST/PORT/USER to enable): %v", err)
	}
	_, err = adminConn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", testDB))
	require.NoError(t, err, "create test DB")
	adminConn.Close(ctx)

	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, testDB)
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	require.NoError(t, pool.Ping(ctx))

	// Run all migrations.
	runServiceMigrations(t, pool)

	t.Cleanup(func() {
		pool.Close()
		ctx := context.Background()
		c, err := pgx.Connect(ctx, adminDSN)
		if err == nil {
			c.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDB))
			c.Close(ctx)
		}
	})

	return pool
}

func runServiceMigrations(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	// Find migrations dir relative to repo root.
	migrationsDir := findMigrationsDir()
	entries, err := os.ReadDir(migrationsDir)
	require.NoError(t, err, "read migrations dir: %s", migrationsDir)
	var upFiles []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".up.sql") {
			upFiles = append(upFiles, e.Name())
		}
	}
	for _, m := range upFiles {
		up, err := os.ReadFile(migrationsDir + "/" + m)
		require.NoError(t, err, "read migration %s", m)
		_, err = pool.Exec(ctx, string(up))
		require.NoError(t, err, "run migration %s", m)
	}
}

func findMigrationsDir() string {
	// service package is at server/internal/service/, migrations at repo root.
	for _, candidate := range []string{
		"../../../migrations",
		"../../../../migrations",
		os.Getenv("JANUS_MIGRATION_PATH"),
	} {
		if candidate == "" {
			continue
		}
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	return "../../../migrations"
}

func setupServiceTestEnv(t *testing.T) *serviceTestEnv {
	t.Helper()
	pool := openServiceTestDB(t)
	ctx := context.Background()

	taskRepo := postgres.NewTaskRepository(pool)
	attemptRepo := postgres.NewTaskAttemptRepository(pool)
	mailboxRepo := postgres.NewMailboxRepository(pool)
	outboxRepo := postgres.NewOutboxRepo(pool)
	eventRepo := postgres.NewEventRepo(pool)
	budgetUsage := postgres.NewBudgetUsageRepo(pool)
	tenantRepo := postgres.NewTenantRepository(pool)
	agentRepo := postgres.NewAgentRepository(pool)
	budgetRepo := postgres.NewBudgetRepository(pool)
	policyRepo := postgres.NewPolicyRuleRepository(pool)

	budgetSvc := NewBudgetService(budgetRepo)
	policySvc := NewPolicyService(policyRepo)
	drv := &mockDispatchQueueDriver{}

	lifecycle := NewPGLifecycle(pool)
	dispatch := NewDispatchService(taskRepo, attemptRepo, mailboxRepo, drv, policySvc, budgetSvc)
	dispatch = dispatch.WithTxPath(lifecycle, outboxRepo, PGBudgetLedger(budgetUsage))

	taskSvc := NewTaskService(taskRepo, drv, pool, outboxRepo).WithPolicy(policySvc)
	taskSvc = taskSvc.WithLifecycle(lifecycle)

	// Seed a tenant + agent + mailbox.
	tenantRepo.Create(ctx, "acme", "Acme")
	agentRepo.Register(ctx, core.Agent{
		ID: "agent-1", TenantID: "acme", DisplayName: "A1",
		Status: core.AgentStatusOnline, Protocol: core.ProtocolA2A,
	})
	mailboxRepo.Create(ctx, core.Mailbox{
		ID: "mb-1", TenantID: "acme", AgentID: "agent-1", Status: "active",
		RetryPolicy: core.RetryPolicy{MaxAttempts: 5, BackoffType: "exponential", InitialSeconds: 1, MaxSeconds: 10},
		MaxDeliver:  5,
	})

	return &serviceTestEnv{
		pool: pool, taskRepo: taskRepo, attemptRepo: attemptRepo,
		mailboxRepo: mailboxRepo, outboxRepo: outboxRepo, eventRepo: eventRepo, budgetUsage: budgetUsage,
		tenantRepo: tenantRepo, agentRepo: agentRepo,
		dispatch: dispatch, taskSvc: taskSvc, lifecycle: lifecycle, driver: drv,
	}
}

// createTestTask inserts a queued task directly into PG (bypassing NATS).
func createTestTask(t *testing.T, env *serviceTestEnv, taskID string) *core.Task {
	t.Helper()
	task := core.Task{
		ID: taskID, TenantID: "acme", MailboxID: "mb-1", SourceAgent: "agent-1",
		Status: core.TaskStatusQueued, Priority: core.PriorityNormal,
		Envelope: core.TaskEnvelope{
			TaskID: taskID, TenantID: "acme", SourceAgent: "agent-1",
			Target: core.Target{Type: "agent", Value: "agent-1"},
		},
	}
	err := env.taskRepo.Create(context.Background(), task)
	require.NoError(t, err)
	return &task
}

// --- Tests ---

func TestDispatch_AckTask_IdempotentSettlement(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	task := createTestTask(t, env, "task-ack-1")
	_, _ = env.pool.Exec(ctx, "UPDATE tasks SET status = 'running' WHERE tenant_id = 'acme' AND id = 'task-ack-1'")

	// Simulate a claim: insert an attempt in claimed state.
	attempt := core.TaskAttempt{
		TenantID: "acme", TaskID: task.ID, Attempt: 1, AgentID: "agent-1",
		LeaseID: "lease-1", Status: "claimed", StartedAt: time.Now(),
		DeliveryRef: "delivery-1",
	}
	require.NoError(t, env.attemptRepo.Create(ctx, attempt))
	// Move task to claimed.
	_, err := env.taskRepo.UpdateStatusWithCheck(ctx, "acme", task.ID, core.TaskStatusQueued, core.TaskStatusClaimed, 1)
	require.NoError(t, err)

	usage := &core.TokenUsage{PromptTokens: 100, CompletionTokens: 50, TotalTokens: 150}

	// First ACK — should settle.
	err = env.dispatch.AckTask(ctx, "acme", task.ID, "lease-1", "result://x", usage)
	require.NoError(t, err)

	// Verify task is completed.
	got, _ := env.taskRepo.Get(ctx, "acme", task.ID)
	assert.Equal(t, core.TaskStatusCompleted, got.Status)

	// Verify budget_usage was incremented once.
	tokens, _, _, _ := env.budgetUsage.GetDailyUsage(ctx, "acme", "tenant", "acme")
	assert.Equal(t, 150, tokens, "tenant scope tokens should be 150 after first ACK")

	// Second ACK (duplicate) — should be no-op.
	err = env.dispatch.AckTask(ctx, "acme", task.ID, "lease-1", "result://x", usage)
	require.NoError(t, err)

	// Verify budget_usage NOT incremented again (idempotent).
	tokens2, _, _, _ := env.budgetUsage.GetDailyUsage(ctx, "acme", "tenant", "acme")
	assert.Equal(t, 150, tokens2, "tokens should still be 150 after duplicate ACK")

	// Verify outbox has exactly one completed event.
	outbox, _ := env.outboxRepo.FetchPending(ctx, 100)
	completedCount := 0
	for _, e := range outbox {
		if e.Kind == "event_publish" {
			var event core.JanusEvent
			if json.Unmarshal(e.Payload, &event) == nil && event.EventType == core.EventTaskCompleted {
				completedCount++
			}
		}
	}
	assert.Equal(t, 1, completedCount, "exactly one completed event in outbox")
}

func TestDispatch_NackTask_DBBeforeNATS(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	task := createTestTask(t, env, "task-nack-1")

	attempt := core.TaskAttempt{
		TenantID: "acme", TaskID: task.ID, Attempt: 1, AgentID: "agent-1",
		LeaseID: "lease-2", Status: "claimed", StartedAt: time.Now(),
		DeliveryRef: "delivery-2",
	}
	require.NoError(t, env.attemptRepo.Create(ctx, attempt))
	_, err := env.taskRepo.UpdateStatusWithCheck(ctx, "acme", task.ID, core.TaskStatusQueued, core.TaskStatusClaimed, 1)
	require.NoError(t, err)

	// NACK non-retriable.
	err = env.dispatch.NackTask(ctx, "acme", task.ID, "lease-2", false, &core.TaskError{Code: "FATAL", Message: "boom"})
	require.NoError(t, err)

	// DB state should be committed BEFORE NATS was touched.
	got, _ := env.taskRepo.Get(ctx, "acme", task.ID)
	assert.Equal(t, core.TaskStatusDeadLettered, got.Status, "task should be dead_lettered after non-retriable NACK")

	// NATS should have been called with NackTask (non-retriable).
	assert.NotEmpty(t, env.driver.nackCalls, "driver NackTask should have been called")
}

func TestDispatch_NackTask_Retriable_SchedulesRetry(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	task := createTestTask(t, env, "task-nack-2")

	attempt := core.TaskAttempt{
		TenantID: "acme", TaskID: task.ID, Attempt: 1, AgentID: "agent-1",
		LeaseID: "lease-3", Status: "claimed", StartedAt: time.Now(),
		DeliveryRef: "delivery-3",
	}
	require.NoError(t, env.attemptRepo.Create(ctx, attempt))
	_, err := env.taskRepo.UpdateStatusWithCheck(ctx, "acme", task.ID, core.TaskStatusQueued, core.TaskStatusClaimed, 1)
	require.NoError(t, err)

	// NACK retriable — should move to retry_scheduled.
	err = env.dispatch.NackTask(ctx, "acme", task.ID, "lease-3", true, &core.TaskError{Code: "TIMEOUT", Message: "slow"})
	require.NoError(t, err)

	got, _ := env.taskRepo.Get(ctx, "acme", task.ID)
	assert.Equal(t, core.TaskStatusRetryScheduled, got.Status, "task should be retry_scheduled")
}

func TestDispatch_PullTask_LifecycleEventOutbox(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	task := createTestTask(t, env, "task-pull-1")

	// Simulate a NATS delivery.
	env.driver.deliveries = []core.TaskDelivery{{
		TaskID: task.ID, DeliveryRef: "delivery-pull-1",
	}}

	result, err := env.dispatch.PullTask(ctx, "acme", "mb-1", "agent-1")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.NotEmpty(t, result.LeaseID, "should have a lease")

	// Task should be claimed.
	got, _ := env.taskRepo.Get(ctx, "acme", task.ID)
	assert.Equal(t, core.TaskStatusClaimed, got.Status)

	// Outbox should have a claimed event.
	outbox, _ := env.outboxRepo.FetchPending(ctx, 100)
	hasClaimed := false
	for _, e := range outbox {
		if e.Kind == "event_publish" {
			var event core.JanusEvent
			if json.Unmarshal(e.Payload, &event) == nil && event.EventType == core.EventTaskClaimed {
				hasClaimed = true
			}
		}
	}
	assert.True(t, hasClaimed, "outbox should contain task.claimed event")
}

func TestDispatch_StartTask_LifecycleEventOutbox(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	task := createTestTask(t, env, "task-start-1")

	attempt := core.TaskAttempt{
		TenantID: "acme", TaskID: task.ID, Attempt: 1, AgentID: "agent-1",
		LeaseID: "lease-start-1", Status: "claimed", StartedAt: time.Now(),
	}
	require.NoError(t, env.attemptRepo.Create(ctx, attempt))
	_, err := env.taskRepo.UpdateStatusWithCheck(ctx, "acme", task.ID, core.TaskStatusQueued, core.TaskStatusClaimed, 1)
	require.NoError(t, err)

	err = env.dispatch.StartTask(ctx, "acme", task.ID, "lease-start-1")
	require.NoError(t, err)

	got, _ := env.taskRepo.Get(ctx, "acme", task.ID)
	assert.Equal(t, core.TaskStatusRunning, got.Status)

	// Outbox should have a started event.
	outbox, _ := env.outboxRepo.FetchPending(ctx, 100)
	hasStarted := false
	for _, e := range outbox {
		if e.Kind == "event_publish" {
			var event core.JanusEvent
			if json.Unmarshal(e.Payload, &event) == nil && event.EventType == core.EventTaskStarted {
				hasStarted = true
			}
		}
	}
	assert.True(t, hasStarted, "outbox should contain task.started event")
}

func TestDispatch_AckTask_LeaseMismatch_Rejected(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	task := createTestTask(t, env, "task-ack-2")

	attempt := core.TaskAttempt{
		TenantID: "acme", TaskID: task.ID, Attempt: 1, AgentID: "agent-1",
		LeaseID: "correct-lease", Status: "claimed", StartedAt: time.Now(),
	}
	require.NoError(t, env.attemptRepo.Create(ctx, attempt))

	// ACK with wrong lease should fail.
	err := env.dispatch.AckTask(ctx, "acme", task.ID, "wrong-lease", "", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "lease mismatch")
}

// --- TaskService management path tests (lifecycle + outbox) ---

func TestTaskService_Cancel_LifecycleOutbox(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	task := createTestTask(t, env, "task-cancel-1")

	err := env.taskSvc.Cancel(ctx, "acme", task.ID)
	require.NoError(t, err)

	got, _ := env.taskRepo.Get(ctx, "acme", task.ID)
	assert.Equal(t, core.TaskStatusCancelled, got.Status)

	// Verify outbox has a cancelled event.
	outbox, _ := env.outboxRepo.FetchPending(ctx, 100)
	hasCancelled := false
	for _, e := range outbox {
		if e.Kind == "event_publish" {
			var event core.JanusEvent
			if json.Unmarshal(e.Payload, &event) == nil && event.EventType == core.EventTaskCancelled {
				hasCancelled = true
			}
		}
	}
	assert.True(t, hasCancelled, "outbox should contain task.cancelled event")
}

func TestTaskService_Block_LifecycleOutbox(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	task := createTestTask(t, env, "task-block-1")

	err := env.taskSvc.Block(ctx, "acme", task.ID, "manual review")
	require.NoError(t, err)

	got, _ := env.taskRepo.Get(ctx, "acme", task.ID)
	assert.Equal(t, core.TaskStatusBlocked, got.Status)

	// Verify outbox has a blocked event.
	outbox, _ := env.outboxRepo.FetchPending(ctx, 100)
	hasBlocked := false
	for _, e := range outbox {
		if e.Kind == "event_publish" {
			var event core.JanusEvent
			if json.Unmarshal(e.Payload, &event) == nil && event.EventType == core.EventTaskBlocked {
				hasBlocked = true
			}
		}
	}
	assert.True(t, hasBlocked, "outbox should contain task.blocked event")
}

func TestTaskService_Unblock_LifecycleOutbox(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	task := createTestTask(t, env, "task-unblock-1")

	// First block it.
	require.NoError(t, env.taskSvc.Block(ctx, "acme", task.ID, "review"))
	// Consume the blocked event outbox entry.
	_, _ = env.outboxRepo.FetchPending(ctx, 100)

	// Now unblock.
	err := env.taskSvc.Unblock(ctx, "acme", task.ID)
	require.NoError(t, err)

	got, _ := env.taskRepo.Get(ctx, "acme", task.ID)
	assert.Equal(t, core.TaskStatusRunning, got.Status)

	outbox, _ := env.outboxRepo.FetchPending(ctx, 100)
	hasStarted := false
	for _, e := range outbox {
		if e.Kind == "event_publish" {
			var event core.JanusEvent
			if json.Unmarshal(e.Payload, &event) == nil && event.EventType == core.EventTaskStarted {
				hasStarted = true
			}
		}
	}
	assert.True(t, hasStarted, "outbox should contain task.started event after unblock")
}

func TestTaskService_Replay_OutboxPublish(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	task := createTestTask(t, env, "task-replay-1")

	// Cancel the task first (terminal state required for replay).
	require.NoError(t, env.taskSvc.Cancel(ctx, "acme", task.ID))

	// Replay should re-queue the task.
	result, err := env.taskSvc.Replay(ctx, "acme", task.ID)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Task should be back in queued (or created).
	got, _ := env.taskRepo.Get(ctx, "acme", task.ID)
	assert.Contains(t, []string{"created", "queued"}, string(got.Status),
		"replayed task should be created or queued, got %s", got.Status)

	// Outbox should have a task_publish entry.
	outbox, _ := env.outboxRepo.FetchPending(ctx, 100)
	hasTaskPublish := false
	for _, e := range outbox {
		if e.Kind == "task_publish" {
			hasTaskPublish = true
		}
	}
	assert.True(t, hasTaskPublish, "outbox should contain task_publish entry for replay")
}

func TestTaskService_AuditProjection_LifecycleOutbox(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	task := createTestTask(t, env, "task-audit-1")

	require.NoError(t, env.taskSvc.Cancel(ctx, "acme", task.ID))

	pending, _ := env.outboxRepo.FetchPending(ctx, 100)
	for _, e := range pending {
		if e.Kind != "event_publish" {
			continue
		}
		var event core.JanusEvent
		if json.Unmarshal(e.Payload, &event) == nil {
			require.NoError(t, env.eventRepo.Insert(ctx, event))
		}
	}

	events, err := env.eventRepo.ListByTask(ctx, "acme", task.ID, 10)
	require.NoError(t, err)
	require.NotEmpty(t, events, "audit projection should have events for the task")
	found := false
	for _, evt := range events {
		if evt.EventType == core.EventTaskCancelled {
			found = true
		}
	}
	assert.True(t, found, "audit projection should contain task.cancelled")
}

func TestTaskService_Create_WithPolicy(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	task := core.Task{
		ID: "task-create-1", TenantID: "acme", MailboxID: "mb-1", SourceAgent: "agent-1",
		TargetType: core.TargetTypeMailbox, TargetValue: "mb-1",
		Status: core.TaskStatusCreated, Priority: core.PriorityNormal,
		Envelope: core.TaskEnvelope{
			JanusVersion: "1", TaskID: "task-create-1", TenantID: "acme", SourceAgent: "agent-1",
			Target:  core.Target{Type: "mailbox", Value: "mb-1"},
			Payload: core.Payload{Type: "json", Content: "{}"},
			Trace:   core.TraceContext{TraceID: "trace-create-1"},
		},
	}

	result, err := env.taskSvc.Create(ctx, task)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "task-create-1", result.ID)
}

func TestTaskService_Create_IdempotentDedup(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	task := core.Task{
		ID: "task-dedup-1", TenantID: "acme", MailboxID: "mb-1", SourceAgent: "agent-1",
		TargetType: core.TargetTypeMailbox, TargetValue: "mb-1",
		IdempotencyKey: "dedup-key-1",
		Status:         core.TaskStatusCreated, Priority: core.PriorityNormal,
		Envelope: core.TaskEnvelope{
			JanusVersion: "1", TaskID: "task-dedup-1", TenantID: "acme", SourceAgent: "agent-1",
			Target:  core.Target{Type: "mailbox", Value: "mb-1"},
			Payload: core.Payload{Type: "json", Content: "{}"},
			Trace:   core.TraceContext{TraceID: "trace-dedup-1"},
		},
	}

	// First create.
	first, err := env.taskSvc.Create(ctx, task)
	require.NoError(t, err)
	assert.Equal(t, "task-dedup-1", first.ID)

	// Second create with same idempotency key → returns existing.
	second, err := env.taskSvc.Create(ctx, task)
	require.NoError(t, err)
	assert.Equal(t, first.ID, second.ID, "dedup should return same task")
}

func toolTask(id string) core.Task {
	return core.Task{
		ID: id, TenantID: "acme", MailboxID: "mb-1", SourceAgent: "agent-1",
		TargetType: core.TargetTypeMailbox, TargetValue: "mb-1",
		Status: core.TaskStatusCreated, Priority: core.PriorityNormal,
		Envelope: core.TaskEnvelope{
			JanusVersion: "1", TaskID: id, TenantID: "acme", SourceAgent: "agent-1",
			Target:         core.Target{Type: "mailbox", Value: "mb-1"},
			Payload:        core.Payload{Type: "json", Content: "{}"},
			Trace:          core.TraceContext{TraceID: "trace-" + id},
			ToolInvocation: &core.ToolInvocation{ID: id, Name: "search", Namespace: "mcp", SourceProtocol: "mcp"},
		},
	}
}

func TestTaskService_ToolEvent_AllowedDenied(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	allowed, err := env.taskSvc.Create(ctx, toolTask("tool-allow-1"))
	require.NoError(t, err)
	require.NotNil(t, allowed)

	drv := env.driver
	hasAllowed := false
	for _, e := range drv.events {
		if e.EventType == core.EventToolInvocationAllowed && e.TaskID == "tool-allow-1" {
			hasAllowed = true
		}
	}
	assert.True(t, hasAllowed, "tool.invocation_allowed should be published for tool tasks")

	require.NoError(t, postgres.NewPolicyRuleRepository(env.pool).Create(ctx, core.PolicyRule{
		TenantID:  "acme",
		ID:        "rule-tool-deny-test",
		Name:      "test deny mb-1",
		Status:    "active",
		Priority:  10,
		Condition: json.RawMessage(`{"resource.value":"mb-1"}`),
		Action:    json.RawMessage(`{"decision":"deny"}`),
	}))
	_, err = env.taskSvc.Create(ctx, toolTask("tool-deny-1"))
	require.Error(t, err)
	hasDenied := false
	for _, e := range drv.events {
		if e.EventType == core.EventToolInvocationDenied && e.TaskID == "tool-deny-1" {
			hasDenied = true
		}
	}
	assert.True(t, hasDenied, "tool.invocation_denied should be published on policy deny")

	noTool := core.Task{
		ID: "plain-tool-evt-2", TenantID: "acme", MailboxID: "mb-1", SourceAgent: "agent-1",
		TargetType: core.TargetTypeMailbox, TargetValue: "plain-target",
		Status: core.TaskStatusCreated, Priority: core.PriorityNormal,
		Envelope: core.TaskEnvelope{
			JanusVersion: "1", TaskID: "plain-tool-evt-2", TenantID: "acme", SourceAgent: "agent-1",
			Target:  core.Target{Type: "mailbox", Value: "plain-target"},
			Payload: core.Payload{Type: "json", Content: "{}"},
			Trace:   core.TraceContext{TraceID: "trace-plain-tool-evt-2"},
		},
	}
	_, err = env.taskSvc.Create(ctx, noTool)
	require.NoError(t, err)
	for _, e := range drv.events {
		if e.TaskID == "plain-tool-evt-2" &&
			(e.EventType == core.EventToolInvocationAllowed || e.EventType == core.EventToolInvocationDenied) {
			t.Fatal("non-tool tasks must not emit tool.invocation_* events")
		}
	}
}

func TestDispatch_AckTask_ToolInvocationCompleted(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	tool := toolTask("tool-done-1")
	tool.Status = core.TaskStatusQueued
	require.NoError(t, env.taskRepo.Create(ctx, tool))

	env.driver.deliveries = []core.TaskDelivery{{
		TaskID: tool.ID, DeliveryRef: "delivery-tool-done-1",
	}}
	pulled, err := env.dispatch.PullTask(ctx, "acme", "mb-1", "agent-1")
	require.NoError(t, err)
	require.NotNil(t, pulled)

	require.NoError(t, env.dispatch.StartTask(ctx, "acme", tool.ID, pulled.LeaseID))
	require.NoError(t, env.dispatch.AckTask(ctx, "acme", tool.ID, pulled.LeaseID, "ref-1", nil))

	outbox, _ := env.outboxRepo.FetchPending(ctx, 100)
	hasCompleted := false
	for _, e := range outbox {
		if e.Kind != "event_publish" {
			continue
		}
		var event core.JanusEvent
		if json.Unmarshal(e.Payload, &event) == nil &&
			event.EventType == core.EventToolInvocationCompleted && event.TaskID == tool.ID {
			hasCompleted = true
		}
	}
	assert.True(t, hasCompleted, "outbox should contain tool.invocation_completed for tool tasks")
}

func TestTaskService_Replay_RepeatDeliversEachGeneration(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	task := createTestTask(t, env, "task-replay-repeat-1")

	require.NoError(t, env.taskSvc.Cancel(ctx, "acme", task.ID))
	_, err := env.taskSvc.Replay(ctx, "acme", task.ID)
	require.NoError(t, err)
	// Replay again after the first replayed run completes.
	got, err := env.taskRepo.Get(ctx, "acme", task.ID)
	require.NoError(t, err)
	require.Equal(t, core.TaskStatusQueued, got.Status, "first replay should re-queue")
	require.NoError(t, env.taskSvc.Cancel(ctx, "acme", task.ID))
	_, err = env.taskSvc.Replay(ctx, "acme", task.ID)
	require.NoError(t, err)

	taskPublish := 0
	entries, _ := env.outboxRepo.FetchPending(ctx, 500)
	for _, e := range entries {
		if e.Kind == "task_publish" && e.TenantID == "acme" {
			var msg core.TaskMessage
			require.NoError(t, json.Unmarshal(e.Payload, &msg), "task_publish payload must stay a TaskMessage")
			if msg.TaskID == task.ID {
				taskPublish++
			}
		}
	}
	assert.Equal(t, 2, taskPublish,
		"a repeated replay must deliver again (generation-keyed dedupe), not be swallowed")

	got, err = env.taskRepo.Get(ctx, "acme", task.ID)
	require.NoError(t, err)
	assert.Equal(t, core.TaskStatusQueued, got.Status)
	assert.GreaterOrEqual(t, got.ReplayCount, 2, "replay generation must advance in the task row")
}

func TestTaskService_OutboxEventIdentity_EndToEnd(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	task := createTestTask(t, env, "task-identity-1")

	require.NoError(t, env.taskSvc.Cancel(ctx, "acme", task.ID))

	entries, _ := env.outboxRepo.FetchPending(ctx, 500)
	payloadIDs := map[string]bool{}
	for _, e := range entries {
		if e.Kind != "event_publish" || e.TenantID != "acme" {
			continue
		}
		var event core.JanusEvent
		require.NoError(t, json.Unmarshal(e.Payload, &event))
		require.NotEmpty(t, event.EventID, "outbox event payload must carry a stable event_id (write-point enrichment)")
		require.False(t, event.Timestamp.IsZero(), "outbox event payload must carry a timestamp")
		payloadIDs[event.EventID] = true

		// The audit projection must reuse the SAME identity.
		require.NoError(t, env.eventRepo.Insert(ctx, event))
	}

	auditEvents, err := env.eventRepo.ListByTask(ctx, "acme", task.ID, 100)
	require.NoError(t, err)
	require.NotEmpty(t, auditEvents)
	for _, evt := range auditEvents {
		assert.True(t, payloadIDs[evt.EventID],
			"audit row event_id %q must equal the outbox payload event_id (one identity end-to-end)", evt.EventID)
	}

	// Idempotent re-projection: inserting the same events again must not grow the audit trail.
	for _, e := range entries {
		if e.Kind != "event_publish" || e.TenantID != "acme" {
			continue
		}
		var event core.JanusEvent
		require.NoError(t, json.Unmarshal(e.Payload, &event))
		require.NoError(t, env.eventRepo.Insert(ctx, event))
	}
	again, err := env.eventRepo.ListByTask(ctx, "acme", task.ID, 100)
	require.NoError(t, err)
	assert.Len(t, again, len(auditEvents), "re-projection with stable event_ids must be idempotent")
}

func TestTaskService_Replay_ConcurrentSingleDelivery(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	task := createTestTask(t, env, "task-replay-race-1")

	require.NoError(t, env.taskSvc.Cancel(ctx, "acme", task.ID))

	// Two concurrent replays race for the same terminal task. The
	// conditional reset (status IN terminal) must let exactly one through;
	// the loser gets a conflict and MUST NOT write a second task_publish.
	const racers = 2
	results := make(chan error, racers)
	var start sync.WaitGroup
	start.Add(1)
	for i := 0; i < racers; i++ {
		go func() {
			start.Wait()
			_, err := env.taskSvc.Replay(ctx, "acme", task.ID)
			results <- err
		}()
	}
	start.Done()

	var succeeded, conflicted int
	for i := 0; i < racers; i++ {
		err := <-results
		switch {
		case err == nil:
			succeeded++
		case strings.Contains(err.Error(), "not in a replayable terminal state"),
			strings.Contains(err.Error(), "only terminal tasks can be replayed"):
			// The pre-check or the conditional reset caught the loser —
			// both are correct conflict outcomes.
			conflicted++
		default:
			t.Fatalf("unexpected replay error: %v", err)
		}
	}
	assert.Equal(t, 1, succeeded, "exactly one replay must win")
	assert.Equal(t, 1, conflicted, "the losing replay must get a conflict")

	taskPublish := 0
	entries, _ := env.outboxRepo.FetchPending(ctx, 500)
	for _, e := range entries {
		if e.Kind == "task_publish" && e.TenantID == "acme" {
			var msg core.TaskMessage
			require.NoError(t, json.Unmarshal(e.Payload, &msg))
			if msg.TaskID == task.ID {
				taskPublish++
			}
		}
	}
	assert.Equal(t, 1, taskPublish,
		"concurrent replay must produce exactly one delivery, not one per racer")
}

func TestAgentService_Register_SweepLifecycle(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	agentSvc := NewAgentService(env.agentRepo, nil, nil, nil)
	agent := core.Agent{
		ID: "sweep-agent", TenantID: "acme", DisplayName: "Sweep Agent",
		Protocol: core.ProtocolA2A,
	}

	// Registration must stamp the first heartbeat; the sweeper offlines
	// online agents with a NULL heartbeat, which used to kill every
	// freshly registered agent within one sweep tick.
	require.NoError(t, agentSvc.Register(ctx, agent))

	got, err := env.agentRepo.Get(ctx, "acme", "sweep-agent")
	require.NoError(t, err)
	assert.Equal(t, core.AgentStatusOnline, got.Status)
	require.NotNil(t, got.LastHeartbeatAt, "registration must stamp last_heartbeat_at")
	assert.WithinDuration(t, time.Now(), *got.LastHeartbeatAt, 10*time.Second)

	// Within the TTL: a sweep must NOT touch the agent.
	n, err := env.agentRepo.MarkStaleOnline(ctx, 90*time.Second)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n, "fresh registration must survive the sweeper")

	got, err = env.agentRepo.Get(ctx, "acme", "sweep-agent")
	require.NoError(t, err)
	assert.Equal(t, core.AgentStatusOnline, got.Status)

	// Past the TTL (simulate by aging the heartbeat): sweep offlines it.
	_, err = env.pool.Exec(ctx,
		`UPDATE agents SET last_heartbeat_at = now() - interval '10 minutes' WHERE tenant_id = 'acme' AND id = 'sweep-agent'`)
	require.NoError(t, err)
	n, err = env.agentRepo.MarkStaleOnline(ctx, 90*time.Second)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, n, int64(1), "stale agent must be offlined")

	got, err = env.agentRepo.Get(ctx, "acme", "sweep-agent")
	require.NoError(t, err)
	assert.Equal(t, core.AgentStatusOffline, got.Status)
}

// B2: a FULL replay round against real PG — execute (pull→start→ack),
// replay, execute AGAIN (pull→start→ack). The second round's pull must
// not hit the attempt-1 PK collision that ResetForReplayTx previously
// left behind (attempt rows were never deleted, attempt_count reset to 0).
func TestReplay_FullSecondRound_Execution(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	// Fixture: tenant, agent, mailbox.
	_, _ = env.pool.Exec(ctx, `INSERT INTO agents (tenant_id, id, display_name, protocol, status) VALUES ('acme','r-agent','R','a2a','online') ON CONFLICT DO NOTHING`)
	_, _ = env.pool.Exec(ctx, `INSERT INTO mailboxes (tenant_id, id, agent_id, status, retry_policy) VALUES ('acme','r-mb','r-agent','active','{"max_attempts":3,"backoff_type":"exponential","initial_seconds":1,"max_seconds":60,"jitter":false}') ON CONFLICT DO NOTHING`)

	// Round 1: create → pull → start → ack.
	task1 := core.Task{
		ID: "replay-round-1", TenantID: "acme", SourceAgent: "r-agent",
		TargetType: core.TargetTypeMailbox, TargetValue: "r-mb", MailboxID: "r-mb",
		Status: core.TaskStatusQueued, Priority: core.PriorityNormal,
		Envelope: core.TaskEnvelope{JanusVersion: "1", TaskID: "replay-round-1", TenantID: "acme",
			SourceAgent: "r-agent", Target: core.Target{Type: "mailbox", Value: "r-mb"},
			Payload: core.Payload{Type: "text", Content: "round1"}},
	}
	require.NoError(t, env.taskRepo.Create(ctx, task1))

	// Simulate delivery + pull via the dispatch path (bypass NATS: the test
	// harness inserts an attempt directly through PullTask's claim logic).
	// We drive the state machine directly: claimed → running → completed.
	_, _ = env.pool.Exec(ctx, `UPDATE tasks SET status='claimed' WHERE tenant_id='acme' AND id='replay-round-1'`)
	require.NoError(t, env.attemptRepo.Create(ctx, core.TaskAttempt{
		TenantID: "acme", TaskID: "replay-round-1", Attempt: 1, AgentID: "r-agent",
		LeaseID: "lease-r1", Status: "claimed",
	}))
	require.NoError(t, env.dispatch.StartTask(ctx, "acme", "replay-round-1", "lease-r1"))

	err := env.dispatch.AckTask(ctx, "acme", "replay-round-1", "lease-r1", "s3://r1", nil)
	require.NoError(t, err, "round 1: ack must succeed")

	got, _ := env.taskRepo.Get(ctx, "acme", "replay-round-1")
	require.Equal(t, core.TaskStatusCompleted, got.Status, "round 1: task completed")

	// Verify attempt=1 exists in the DB (the collision source).
	var attemptCount int
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_attempts WHERE tenant_id='acme' AND task_id='replay-round-1'`).Scan(&attemptCount))
	require.Greater(t, attemptCount, 0, "round 1 attempt row must exist")

	// Replay.
	_, err = env.taskSvc.Replay(ctx, "acme", "replay-round-1")
	require.NoError(t, err, "replay must succeed")

	got, _ = env.taskRepo.Get(ctx, "acme", "replay-round-1")
	require.Equal(t, core.TaskStatusQueued, got.Status, "replay: task re-queued")
	require.Equal(t, 1, got.ReplayCount, "replay_count bumped")

	// Attempts must be gone (ResetForReplayTx deletes them).
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_attempts WHERE tenant_id='acme' AND task_id='replay-round-1'`).Scan(&attemptCount))
	require.Equal(t, 0, attemptCount, "replay: old attempts must be deleted")

	// Round 2: the pull that previously failed with a unique violation.
	_, _ = env.pool.Exec(ctx, `UPDATE tasks SET status='claimed' WHERE tenant_id='acme' AND id='replay-round-1'`)
	require.NoError(t, env.attemptRepo.Create(ctx, core.TaskAttempt{
		TenantID: "acme", TaskID: "replay-round-1", Attempt: 1, AgentID: "r-agent",
		LeaseID: "lease-r2", Status: "claimed",
	}), "round 2: attempt-1 insert must NOT collide — old rows were deleted")

	require.NoError(t, env.dispatch.StartTask(ctx, "acme", "replay-round-1", "lease-r2"))
	err = env.dispatch.AckTask(ctx, "acme", "replay-round-1", "lease-r2", "s3://r2", nil)
	require.NoError(t, err, "round 2: ack must succeed")

	got, _ = env.taskRepo.Get(ctx, "acme", "replay-round-1")
	require.Equal(t, core.TaskStatusCompleted, got.Status, "round 2: task completed again")
}

// B3: the full task flow (create → start → ack) must complete under a
// SINGLE-CONNECTION pool — the dispatch path previously acquired a second
// pool connection inside the lifecycle transaction (self-starvation).
func TestSingleConnectionPool_FullTaskFlow(t *testing.T) {
	pool := openServiceTestDB(t)
	ctx := context.Background()

	// Single-connection pool: any nested acquisition deadlocks.
	cfg := pool.Config().Copy()
	cfg.MaxConns = 1
	cfg.MinConns = 1
	single, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Skipf("single-conn pool: %v", err)
	}
	defer single.Close()

	taskRepo := postgres.NewTaskRepository(single)
	attemptRepo := postgres.NewTaskAttemptRepository(single)
	mailboxRepo := postgres.NewMailboxRepository(single)
	taskSvc := NewTaskService(taskRepo, nil, single, nil)
	budgetSvc := NewBudgetService(&cbBudgetSpecRepo{})
	dispatch := NewDispatchService(taskRepo, attemptRepo, mailboxRepo, &cbQueueDriver{},
		NewPolicyService(&mockPolicyRuleRepo{}), budgetSvc).
		WithTxPath(NewPGLifecycle(single), nil, nil)

	// Fixture.
	_, _ = single.Exec(ctx, `INSERT INTO agents (tenant_id, id, display_name, protocol, status) VALUES ('acme','sc-agent','S','a2a','online') ON CONFLICT DO NOTHING`)
	_, _ = single.Exec(ctx, `INSERT INTO mailboxes (tenant_id, id, agent_id, status, retry_policy) VALUES ('acme','sc-mb','sc-agent','active','{"max_attempts":3,"backoff_type":"exponential","initial_seconds":1,"max_seconds":60,"jitter":false}') ON CONFLICT DO NOTHING`)

	// Create + claim + start — all inside lifecycle txs on the 1-conn pool.
	_, _ = single.Exec(ctx, `INSERT INTO tasks (tenant_id, id, source_agent, target_type, target_value, mailbox_id, status, priority, envelope)
		VALUES ('acme','sc-task','sc-agent','mailbox','sc-mb','sc-mb','claimed','normal','{"task_id":"sc-task","tenant_id":"acme","source_agent":"sc-agent"}')
		ON CONFLICT DO NOTHING`)
	require.NoError(t, attemptRepo.Create(ctx, core.TaskAttempt{
		TenantID: "acme", TaskID: "sc-task", Attempt: 1, AgentID: "sc-agent",
		LeaseID: "lease-sc", Status: "claimed",
	}))

	// StartTask opens ApplyTx (holds the 1 conn), then reads the task —
	// GetTx runs on the TX executor, so no second connection is needed.
	// With the old pool-based Get, this would deadlock forever.
	done := make(chan error, 1)
	go func() {
		done <- dispatch.StartTask(ctx, "acme", "sc-task", "lease-sc")
	}()
	select {
	case err := <-done:
		require.NoError(t, err, "start under 1-conn pool must complete (tx executor, no nested pool query)")
	case <-time.After(5 * time.Second):
		t.Fatal("StartTask deadlocked under single-connection pool (nested pool query)")
	}

	// AckTask: same pattern — UpdateFinishedWithCheckTx + GetTx on tx.
	_, _ = single.Exec(ctx, `UPDATE tasks SET status='running' WHERE tenant_id='acme' AND id='sc-task'`)
	done2 := make(chan error, 1)
	go func() {
		done2 <- dispatch.AckTask(ctx, "acme", "sc-task", "lease-sc", "s3://sc", nil)
	}()
	select {
	case err := <-done2:
		require.NoError(t, err, "ack under 1-conn pool must complete")
	case <-time.After(5 * time.Second):
		t.Fatal("AckTask deadlocked under single-connection pool")
	}

	got, _ := taskRepo.Get(ctx, "acme", "sc-task")
	require.Equal(t, core.TaskStatusCompleted, got.Status)
	_ = taskSvc
}

// cancelWinningDriver delays the delivery until AFTER a concurrent Cancel
// has committed — the audit's deterministic interleaving: Pull reads
// 'queued', Cancel commits (sees no attempt to finish), Pull inserts the
// attempt and CAS-claims. Before the fix, the discarded CAS result let the
// claim commit onto a cancelled task.
type cancelWinningDriver struct {
	mockDispatchQueueDriver
	onFetch func()
}

func (d *cancelWinningDriver) FetchTasks(ctx context.Context, tenantID, mailbox string, opts core.FetchOptions) ([]core.TaskDelivery, error) {
	if d.onFetch != nil {
		d.onFetch()
		d.onFetch = nil
	}
	return d.mockDispatchQueueDriver.FetchTasks(ctx, tenantID, mailbox, opts)
}

func TestPull_ClaimRace_CancelWins_RollsBackAttempt_PG(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	createTestTask(t, env, "race-task")

	drv := &cancelWinningDriver{}
	drv.deliveries = []core.TaskDelivery{{
		TaskID: "race-task", DeliveryRef: core.DeliveryRef("race-ref"),
		Payload: []byte(`{"task_id":"race-task"}`),
	}}
	drv.onFetch = func() {
		// Cancel commits while the pull is between its status read and the
		// claim transaction: task goes queued -> cancelled with no attempt.
		_, cerr := env.pool.Exec(ctx,
			`UPDATE tasks SET status='cancelled', updated_at=now() WHERE tenant_id='acme' AND id='race-task'`)
		require.NoError(t, cerr)
	}
	env.dispatch = env.dispatch.WithQueueDriver(drv)

	result, err := env.dispatch.PullTask(ctx, "acme", "mb-1", "agent-1")
	require.Error(t, err, "claim must fail when Cancel wins the race")
	require.Nil(t, result)

	task, gerr := env.taskRepo.Get(ctx, "acme", "race-task")
	require.NoError(t, gerr)
	require.Equal(t, core.TaskStatusCancelled, task.Status, "task stays cancelled")

	var attempts int
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_attempts WHERE tenant_id='acme' AND task_id='race-task'`).Scan(&attempts))
	require.Zero(t, attempts, "the racing claim's attempt must roll back with the transaction")
}

// The single-connection proof must cover PullTask too: the claim
// transaction's budget lookups previously took a SECOND pool connection
// (BudgetRepo.Get on the pool) and self-starved. This run uses the REAL PG
// budget repo with a tenant cap set, not a mock.
func TestSingleConnectionPool_PullTask_RealBudgetRepo(t *testing.T) {
	pool := openServiceTestDB(t)
	ctx := context.Background()

	cfg := pool.Config().Copy()
	cfg.MaxConns = 1
	cfg.MinConns = 1
	single, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Skipf("single-conn pool: %v", err)
	}
	defer single.Close()

	taskRepo := postgres.NewTaskRepository(single)
	attemptRepo := postgres.NewTaskAttemptRepository(single)
	mailboxRepo := postgres.NewMailboxRepository(single)
	budgetRepo := postgres.NewBudgetRepository(single)
	budgetSvc := NewBudgetService(budgetRepo)

	drv := &cancelWinningDriver{}
	drv.deliveries = []core.TaskDelivery{{
		TaskID: "sc-pull-task", DeliveryRef: core.DeliveryRef("sc-pull-ref"),
		Payload: []byte(`{"task_id":"sc-pull-task"}`),
	}}
	dispatch := NewDispatchService(taskRepo, attemptRepo, mailboxRepo, drv,
		NewPolicyService(&mockPolicyRuleRepo{}), budgetSvc).
		WithTxPath(NewPGLifecycle(single), nil, nil)

	_, _ = single.Exec(ctx, `INSERT INTO agents (tenant_id, id, display_name, protocol, status) VALUES ('acme','sc2-agent','S','a2a','online') ON CONFLICT DO NOTHING`)
	_, _ = single.Exec(ctx, `INSERT INTO mailboxes (tenant_id, id, agent_id, status, retry_policy) VALUES ('acme','sc2-mb','sc2-agent','active','{"max_attempts":3,"backoff_type":"exponential","initial_seconds":1,"max_seconds":60,"jitter":false}') ON CONFLICT DO NOTHING`)
	_, _ = single.Exec(ctx, `INSERT INTO tasks (tenant_id, id, source_agent, target_type, target_value, mailbox_id, status, priority, envelope)
		VALUES ('acme','sc-pull-task','sc2-agent','mailbox','sc2-mb','sc2-mb','queued','normal','{"task_id":"sc-pull-task","tenant_id":"acme","source_agent":"sc2-agent"}')`)
	// Tenant cap exists — the claim MUST read it (through the tx now).
	_, _ = single.Exec(ctx, `INSERT INTO budgets (tenant_id, scope_type, scope_id, max_concurrency)
		VALUES ('acme','tenant','acme',5)`)

	done := make(chan error, 1)
	var result *PullResult
	go func() {
		r, perr := dispatch.PullTask(ctx, "acme", "sc2-mb", "sc2-agent")
		result = r
		done <- perr
	}()
	select {
	case err := <-done:
		require.NoError(t, err, "pull under 1-conn pool with real budget repo must complete")
		require.NotNil(t, result)
		require.NotEmpty(t, result.LeaseID)
	case <-time.After(5 * time.Second):
		t.Fatal("PullTask deadlocked under single-connection pool (budget Get on pool inside claim tx)")
	}
}

// B2 regression: replay restarts attempt numbering at 1, so the settlement
// ledger must key on (generation, attempt) — without generation, round 2's
// attempt-1 usage hit round 1's ledger row and was swallowed by ON CONFLICT.
func TestReplay_SecondRound_UsageNotSwallowedByLedger(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	_, _ = env.pool.Exec(ctx, `INSERT INTO agents (tenant_id, id, display_name, protocol, status) VALUES ('acme','lg-agent','L','a2a','online') ON CONFLICT DO NOTHING`)
	_, _ = env.pool.Exec(ctx, `INSERT INTO mailboxes (tenant_id, id, agent_id, status, retry_policy) VALUES ('acme','lg-mb','lg-agent','active','{"max_attempts":3,"backoff_type":"exponential","initial_seconds":1,"max_seconds":60,"jitter":false}') ON CONFLICT DO NOTHING`)
	_, _ = env.pool.Exec(ctx, `INSERT INTO tasks (tenant_id, id, source_agent, target_type, target_value, mailbox_id, status, priority, envelope)
		VALUES ('acme','lg-task','lg-agent','mailbox','lg-mb','lg-mb','claimed','normal','{"task_id":"lg-task","tenant_id":"acme","source_agent":"lg-agent"}')`)
	_, _ = env.pool.Exec(ctx, `INSERT INTO task_attempts (tenant_id, task_id, attempt, agent_id, lease_id, status)
		VALUES ('acme','lg-task',1,'lg-agent','lease-lg1','claimed')`)

	u1 := &core.TokenUsage{PromptTokens: 100, CompletionTokens: 100, TotalTokens: 200}
	require.NoError(t, env.dispatch.StartTask(ctx, "acme", "lg-task", "lease-lg1"))
	require.NoError(t, env.dispatch.AckTask(ctx, "acme", "lg-task", "lease-lg1", "s3://lg1", u1))

	_, err := env.taskSvc.Replay(ctx, "acme", "lg-task")
	require.NoError(t, err)

	// Round 2: attempt numbering restarted at 1.
	_, _ = env.pool.Exec(ctx, `UPDATE tasks SET status='claimed' WHERE tenant_id='acme' AND id='lg-task'`)
	_, _ = env.pool.Exec(ctx, `INSERT INTO task_attempts (tenant_id, task_id, attempt, agent_id, lease_id, status)
		VALUES ('acme','lg-task',1,'lg-agent','lease-lg2','claimed')`)
	u2 := &core.TokenUsage{PromptTokens: 50, CompletionTokens: 50, TotalTokens: 100}
	require.NoError(t, env.dispatch.StartTask(ctx, "acme", "lg-task", "lease-lg2"))
	require.NoError(t, env.dispatch.AckTask(ctx, "acme", "lg-task", "lease-lg2", "s3://lg2", u2))

	// Tenant-scope ledger rows: one per generation, both settled.
	var rows, totalTokens int
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT count(*), COALESCE(sum(total_tokens),0) FROM budget_usage_ledger
		 WHERE tenant_id='acme' AND task_id='lg-task' AND scope_type='tenant'`).Scan(&rows, &totalTokens))
	require.Equal(t, 2, rows, "both replay generations must settle their own ledger row")
	require.Equal(t, 300, totalTokens, "round 1 (200) + round 2 (100) tokens must both be billed")
}
