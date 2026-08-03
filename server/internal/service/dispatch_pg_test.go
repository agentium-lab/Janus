package service

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
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
	pool         *pgxpool.Pool
	taskRepo     *postgres.TaskRepository
	attemptRepo  *postgres.TaskAttemptRepository
	mailboxRepo  *postgres.MailboxRepository
	outboxRepo   *postgres.OutboxRepo
	eventRepo    *postgres.EventRepo
	budgetUsage  *postgres.BudgetUsageRepo
	tenantRepo   *postgres.TenantRepository
	agentRepo    *postgres.AgentRepository
	dispatch     *DispatchService
	taskSvc      *TaskService
	lifecycle    *LifecycleService
	driver       *mockDispatchQueueDriver
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
	if user == "" {
		user = "janus"
	}
	testDB := fmt.Sprintf("janus_svctest_%d", time.Now().UnixNano())

	ctx := context.Background()
	adminDSN := fmt.Sprintf("host=%s port=%s user=%s dbname=janus_test sslmode=disable", host, port, user)
	adminConn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Skipf("postgres not reachable (set JANUS_PG_HOST/PORT/USER to enable): %v", err)
	}
	_, err = adminConn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", testDB))
	require.NoError(t, err, "create test DB")
	adminConn.Close(ctx)

	dsn := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable", host, port, user, testDB)
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

	lifecycle := NewLifecycleService(pool)
	dispatch := NewDispatchService(taskRepo, attemptRepo, mailboxRepo, drv, policySvc, budgetSvc)
	dispatch = dispatch.WithLifecycle(lifecycle, outboxRepo, budgetUsage)

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
			Target: core.Target{Type: "mailbox", Value: "mb-1"},
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
		Status: core.TaskStatusCreated, Priority: core.PriorityNormal,
		Envelope: core.TaskEnvelope{
			JanusVersion: "1", TaskID: "task-dedup-1", TenantID: "acme", SourceAgent: "agent-1",
			Target: core.Target{Type: "mailbox", Value: "mb-1"},
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
