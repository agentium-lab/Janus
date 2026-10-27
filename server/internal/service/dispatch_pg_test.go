package service

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	natsgo "github.com/nats-io/nats.go"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/auth"
	natsdriver "github.com/agentium-lab/Janus/server/internal/driver/nats"
	"github.com/agentium-lab/Janus/server/internal/driver/postgres"
	"github.com/agentium-lab/Janus/server/internal/service/routing"
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
	adminDSN := fmt.Sprintf("host=%s port=%s user=%s dbname=janus_test sslmode=disable", host, port, user)
	if password != "" {
		adminDSN = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=janus_test sslmode=disable", host, port, user, password)
	}
	adminConn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Skipf("postgres not reachable (set JANUS_PG_HOST/PORT/USER to enable): %v", err)
	}
	_, err = adminConn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", testDB))
	require.NoError(t, err, "create test DB")
	adminConn.Close(ctx)

	dsn := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable", host, port, user, testDB)
	if password != "" {
		dsn = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, testDB)
	}
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
	taskSvc = taskSvc.WithAttemptRepo(attemptRepo)

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
	assert.Equal(t, core.TaskStatusQueued, got.Status, "Unblock should re-queue the task (not set to running)")

	outbox, _ := env.outboxRepo.FetchPending(ctx, 100)
	hasQueued := false
	for _, e := range outbox {
		if e.Kind == "event_publish" {
			var event core.JanusEvent
			if json.Unmarshal(e.Payload, &event) == nil && event.EventType == core.EventTaskQueued {
				hasQueued = true
			}
		}
	}
	assert.True(t, hasQueued, "outbox should contain task.queued event after unblock")
}

// eval-5 #1: a task gated behind approval must not be blockable — Block
// forced approval_pending -> blocked and a later Unblock re-queued it past
// the approval gate. Operators who must stop a pending task use Cancel.
func TestTaskService_Block_ApprovalPendingRefused_PG(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	approvalRepo := postgres.NewApprovalRepo(env.pool)
	approvalSvc := NewApprovalService(approvalRepo, env.taskSvc, nil).WithTxPath(env.lifecycle, env.outboxRepo)

	task := createTestTask(t, env, "task-block-appr-1")
	_, err := env.pool.Exec(ctx,
		`UPDATE tasks SET status = 'approval_pending' WHERE tenant_id = 'acme' AND id = $1`, task.ID)
	require.NoError(t, err)
	_, aerr := approvalSvc.RequestApproval(ctx, core.Approval{
		TenantID: "acme", TaskID: task.ID, RequestedBy: "agent-1",
	})
	require.NoError(t, aerr)

	err = env.taskSvc.Block(ctx, "acme", task.ID, "operator pause")
	require.Error(t, err, "Block must refuse an approval_pending task")
	require.Contains(t, err.Error(), "approval")

	got, gerr := env.taskRepo.Get(ctx, "acme", task.ID)
	require.NoError(t, gerr)
	assert.Equal(t, core.TaskStatusApprovalPending, got.Status, "refused block must leave the task gated")

	var attempts int
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_attempts WHERE tenant_id='acme' AND task_id=$1`, task.ID).Scan(&attempts))
	assert.Zero(t, attempts, "refused block must leave no attempt side effects")
}

// eval-5 #1 (unblock side): a blocked task whose approval verdict is still
// pending or already rejected must not be unblockable into the queue — the
// pre-tx re-check mirrors Replay's gate.
func TestTaskService_Unblock_ApprovalVerdictRefused_PG(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	approvalRepo := postgres.NewApprovalRepo(env.pool)
	approvalSvc := NewApprovalService(approvalRepo, env.taskSvc, nil).WithTxPath(env.lifecycle, env.outboxRepo)
	env.taskSvc.WithApproval(approvalSvc)

	for _, tc := range []struct {
		name     string
		status   string
		decision string
	}{
		{"pending", "pending", ""},
		{"rejected", "rejected", "rejected"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task := createTestTask(t, env, "task-unblock-appr-"+tc.name)
			require.NoError(t, env.taskSvc.Block(ctx, "acme", task.ID, "review"))
			require.NoError(t, approvalRepo.Create(ctx, core.Approval{
				TenantID: "acme", ID: "appr-unblock-" + tc.name, TaskID: task.ID,
				RequestedBy: "agent-1", Status: tc.status, Decision: tc.decision,
				ExpiresAt: time.Now().Add(24 * time.Hour),
			}))

			err := env.taskSvc.Unblock(ctx, "acme", task.ID)
			require.Error(t, err, "unblock must respect the %s approval verdict", tc.name)
			require.Contains(t, err.Error(), "approval")

			got, gerr := env.taskRepo.Get(ctx, "acme", task.ID)
			require.NoError(t, gerr)
			assert.Equal(t, core.TaskStatusBlocked, got.Status, "refused unblock must leave the task blocked")

			var publishes int
			require.NoError(t, env.pool.QueryRow(ctx,
				`SELECT count(*) FROM outbox_events WHERE tenant_id='acme' AND kind='task_publish'`).Scan(&publishes))
			assert.Zero(t, publishes, "refused unblock must not enqueue a delivery")
		})
	}
}

// eval-5 #2: the unblock task_publish used a FIXED dedupe key
// (task_publish:<tenant>:<task>:unblock); outbox rows are retained forever,
// so every unblock round after the first was silently swallowed by
// ON CONFLICT DO NOTHING and the re-queued task never delivered again.
func TestTaskService_Unblock_SecondRoundDelivers_PG(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	_, _ = env.pool.Exec(ctx, `INSERT INTO agents (tenant_id, id, display_name, protocol, status) VALUES ('acme','blk-agent','B','a2a','online') ON CONFLICT DO NOTHING`)
	_, _ = env.pool.Exec(ctx, `INSERT INTO mailboxes (tenant_id, id, agent_id, status, retry_policy) VALUES ('acme','blk-mb','blk-agent','active','{"max_attempts":3,"backoff_type":"exponential","initial_seconds":1,"max_seconds":60,"jitter":false}') ON CONFLICT DO NOTHING`)

	task := core.Task{
		ID: "unblock-round-2", TenantID: "acme", MailboxID: "blk-mb", SourceAgent: "agent-1",
		TargetType: core.TargetTypeMailbox, TargetValue: "blk-mb",
		Status: core.TaskStatusQueued, Priority: core.PriorityNormal,
		Envelope: core.TaskEnvelope{JanusVersion: "1", TaskID: "unblock-round-2", TenantID: "acme",
			SourceAgent: "agent-1", Target: core.Target{Type: "mailbox", Value: "blk-mb"},
			Payload: core.Payload{Type: "text", Content: "blk"}},
	}
	require.NoError(t, env.taskRepo.Create(ctx, task))

	drainPublishes := func() int {
		entries, _ := env.outboxRepo.FetchPending(ctx, 100)
		n := 0
		for _, e := range entries {
			if e.Kind != "task_publish" {
				continue
			}
			var msg core.TaskMessage
			if json.Unmarshal(e.Payload, &msg) == nil && msg.TaskID == task.ID {
				n++
			}
		}
		return n
	}

	// Round 1: block -> drain -> unblock -> drain.
	require.NoError(t, env.taskSvc.Block(ctx, "acme", task.ID, "round 1"))
	_, _ = env.outboxRepo.FetchPending(ctx, 100)
	require.NoError(t, env.taskSvc.Unblock(ctx, "acme", task.ID))
	require.Equal(t, 1, drainPublishes(), "round 1's unblock enqueues its task_publish")

	// Round 2: block again -> drain -> unblock again.
	require.NoError(t, env.taskSvc.Block(ctx, "acme", task.ID, "round 2"))
	_, _ = env.outboxRepo.FetchPending(ctx, 100)
	require.NoError(t, env.taskSvc.Unblock(ctx, "acme", task.ID))
	require.Equal(t, 1, drainPublishes(), "round 2's unblock must enqueue a NEW task_publish, not be swallowed by the fixed dedupe key")

	got, gerr := env.taskRepo.Get(ctx, "acme", task.ID)
	require.NoError(t, gerr)
	require.Equal(t, core.TaskStatusQueued, got.Status)

	// End-to-end delivery of round 2 with no recovery scanner running:
	// the drained task_publish feeds the consumer, which pulls a lease.
	env.dispatch = env.dispatch.WithQueueDriver(&perCallDriver{queue: []core.TaskDelivery{
		{TaskID: task.ID, DeliveryRef: core.DeliveryRef("blk-ref-2"), Payload: []byte(`{}`)},
	}})
	res, perr := env.dispatch.PullTask(ctx, "acme", "blk-mb", "blk-agent")
	require.NoError(t, perr)
	require.NotNil(t, res)
	require.NoError(t, env.dispatch.StartTask(ctx, "acme", task.ID, res.LeaseID))
	require.NoError(t, env.dispatch.AckTask(ctx, "acme", task.ID, res.LeaseID, "s3://blk-r2", nil))

	final, ferr := env.taskRepo.Get(ctx, "acme", task.ID)
	require.NoError(t, ferr)
	assert.Equal(t, core.TaskStatusCompleted, final.Status)
}

// TestTaskService_Block_TerminalStateRefusal_PG verifies that Block refuses
// to operate on terminal-state tasks (completed/failed/cancelled/expired/dead_lettered).
// This pins the eval-3 W2 fix: Block must not revive terminal tasks.
func TestTaskService_Block_TerminalStateRefusal_PG(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	terminalStates := []core.TaskStatus{
		core.TaskStatusCompleted,
		core.TaskStatusFailed,
		core.TaskStatusCancelled,
		core.TaskStatusExpired,
		core.TaskStatusDeadLettered,
	}

	for _, status := range terminalStates {
		t.Run(string(status), func(t *testing.T) {
			task := createTestTask(t, env, "task-block-terminal-"+string(status))
			// Force the task into the terminal state.
			_, err := env.pool.Exec(ctx,
				`UPDATE tasks SET status = $1 WHERE tenant_id = $2 AND id = $3`,
				status, "acme", task.ID)
			require.NoError(t, err)

			// Block should refuse.
			err = env.taskSvc.Block(ctx, "acme", task.ID, "test")
			assert.Error(t, err, "Block should refuse terminal state %s", status)
			assert.Contains(t, err.Error(), "terminal", "error should mention terminal state")

			// Verify status unchanged.
			got, _ := env.taskRepo.Get(ctx, "acme", task.ID)
			assert.Equal(t, status, got.Status, "status should remain %s", status)
		})
	}
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

// perCallDriver hands out ONE delivery per FetchTasks call (mutex-guarded):
// concurrent pulls each observe a claimable candidate, so the capacity
// recheck inside the claim transaction is what decides admission.
type perCallDriver struct {
	mockDispatchQueueDriver
	mu    sync.Mutex
	queue []core.TaskDelivery
}

func (d *perCallDriver) FetchTasks(ctx context.Context, tenantID, mailbox string, opts core.FetchOptions) ([]core.TaskDelivery, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.queue) == 0 {
		return nil, nil
	}
	one := d.queue[0]
	d.queue = d.queue[1:]
	return []core.TaskDelivery{one}, nil
}

func TestPull_ClaimRace_CancelWins_RollsBackAttempt_PG(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	createTestTask(t, env, "race-task")

	drv := &mockDispatchQueueDriver{}
	drv.deliveries = []core.TaskDelivery{{
		TaskID: "race-task", DeliveryRef: core.DeliveryRef("race-ref"),
		Payload: []byte(`{"task_id":"race-task"}`),
	}}
	env.dispatch = env.dispatch.WithQueueDriver(drv)

	// GA-04 (a): the barrier sits at the actual race point — after the pull's
	// task read ('queued') and attempt insert, immediately before the claim
	// CAS. Cancel is released exactly inside that window, so the CAS
	// deterministically misses and the claim rolls back.
	gate := &claimGate{entered: make(chan struct{}), release: make(chan struct{})}
	gate.TaskAttemptRepository = env.attemptRepo
	env.dispatch.attemptTx = gate

	type pullOutcome struct {
		res *PullResult
		err error
	}
	done := make(chan pullOutcome, 1)
	go func() {
		r, perr := env.dispatch.PullTask(ctx, "acme", "mb-1", "agent-1")
		done <- pullOutcome{res: r, err: perr}
	}()

	<-gate.entered
	require.NoError(t, env.taskSvc.Cancel(ctx, "acme", "race-task"))
	close(gate.release)

	out := <-done
	require.Nil(t, out.res, "no lease may be handed out for a cancelled task")
	require.EqualError(t, out.err,
		"claim race: task race-task status changed concurrently, expected queued")

	task, gerr := env.taskRepo.Get(ctx, "acme", "race-task")
	require.NoError(t, gerr)
	require.Equal(t, core.TaskStatusCancelled, task.Status, "task stays cancelled")

	var attempts int
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_attempts WHERE tenant_id='acme' AND task_id='race-task'`).Scan(&attempts))
	require.Zero(t, attempts, "the racing claim's attempt must roll back with the transaction")
	require.Zero(t, countOutboxEvents(t, env, "race-task", core.EventTaskClaimed),
		"no task.claimed event may be published by the losing claim")
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
	_, _ = env.pool.Exec(ctx, `INSERT INTO task_attempts (tenant_id, task_id, attempt, agent_id, lease_id, status, delivery_ref)
		VALUES ('acme','lg-task',1,'lg-agent','lease-lg1','claimed','dr-lg1')`)

	u1 := &core.TokenUsage{PromptTokens: 100, CompletionTokens: 100, TotalTokens: 200}
	require.NoError(t, env.dispatch.StartTask(ctx, "acme", "lg-task", "lease-lg1"))
	require.NoError(t, env.dispatch.AckTask(ctx, "acme", "lg-task", "lease-lg1", "s3://lg1", u1))

	_, err := env.taskSvc.Replay(ctx, "acme", "lg-task")
	require.NoError(t, err)

	// Round 2: attempt numbering restarted at 1.
	_, _ = env.pool.Exec(ctx, `UPDATE tasks SET status='claimed' WHERE tenant_id='acme' AND id='lg-task'`)
	_, _ = env.pool.Exec(ctx, `INSERT INTO task_attempts (tenant_id, task_id, attempt, agent_id, lease_id, status, delivery_ref)
		VALUES ('acme','lg-task',1,'lg-agent','lease-lg2','claimed','dr-lg2')`)
	u2 := &core.TokenUsage{PromptTokens: 50, CompletionTokens: 50, TotalTokens: 100}
	require.NoError(t, env.dispatch.StartTask(ctx, "acme", "lg-task", "lease-lg2"))
	require.NoError(t, env.dispatch.AckTask(ctx, "acme", "lg-task", "lease-lg2", "s3://lg2", u2))

	// Replay clears the task's ledger rows and re-arms settlement: the
	// replay round's usage must be billed (previously swallowed by ON
	// CONFLICT on the attempt-numbered key).
	var rows, totalTokens int
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT count(*), COALESCE(sum(total_tokens),0) FROM budget_usage_ledger
		 WHERE tenant_id='acme' AND task_id='lg-task' AND scope_type='tenant'`).Scan(&rows, &totalTokens))
	require.Equal(t, 1, rows, "the replay round settles a fresh ledger row")
	require.Equal(t, 100, totalTokens, "round 2's 100 tokens must be billed, not swallowed")
	// The aggregate already counted round 1; round 2 adds on top.
	var aggTokens int
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT COALESCE(sum(tokens_used),0) FROM budget_usage
		 WHERE tenant_id='acme' AND scope_type='tenant'`).Scan(&aggTokens))
	require.Equal(t, 300, aggTokens, "aggregate bills round 1 (200) + round 2 (100)")
}

// B3 regression: GetTx once returned the spec without filling the nullable
// columns, so CheckConcurrencyTx read every cap as zero. A tenant cap of 1
// with two racing pulls must admit exactly one claim — the in-transaction
// recheck under the concurrency lock is the only guard that matters here.
func TestPull_TenantCap1_TwoRacingPulls_OneClaims_PG(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	_, cerr := env.pool.Exec(ctx,
		`INSERT INTO budgets (tenant_id, scope_type, scope_id, max_concurrency) VALUES ('acme','tenant','acme',1)`)
	require.NoError(t, cerr)

	mkTask := func(id string) {
		_, err := env.pool.Exec(ctx, `INSERT INTO tasks (tenant_id, id, source_agent, target_type, target_value, mailbox_id, status, priority, envelope)
			VALUES ('acme',$1,'agent-1','mailbox','mb-1','mb-1','queued','normal',
			jsonb_build_object('task_id',$1::text,'tenant_id','acme','source_agent','agent-1'))`, id)
		require.NoError(t, err)
	}
	mkTask("cap-task-1")
	mkTask("cap-task-2")

	drv := &perCallDriver{queue: []core.TaskDelivery{
		{TaskID: "cap-task-1", DeliveryRef: core.DeliveryRef("cap-ref-1"), Payload: []byte(`{}`)},
		{TaskID: "cap-task-2", DeliveryRef: core.DeliveryRef("cap-ref-2"), Payload: []byte(`{}`)},
	}}
	env.dispatch = env.dispatch.WithQueueDriver(drv)

	var wg sync.WaitGroup
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := env.dispatch.PullTask(ctx, "acme", "mb-1", "agent-1")
			results <- err
		}()
	}
	wg.Wait()
	close(results)

	var backpressure int
	for err := range results {
		if _, ok := err.(*core.BackpressureError); ok {
			backpressure++
		}
	}
	var claimed int
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT count(*) FROM tasks WHERE tenant_id='acme' AND id LIKE 'cap-task-%' AND status='claimed'`).Scan(&claimed))
	require.Equal(t, 1, claimed, "cap=1 must admit exactly one claim")
	require.Equal(t, 1, backpressure, "the second pull must be backpressured")

	var attempts int
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_attempts WHERE tenant_id='acme' AND task_id LIKE 'cap-task-%'`).Scan(&attempts))
	require.Equal(t, 1, attempts, "exactly one attempt row may exist")
}

// A1 regression: owner verification must come from the authenticated
// principal, not the self-reported agent_id. A key bound to agent A must
// not finish agent B's attempt — with the field omitted AND when forged.
func TestAckAs_BoundPrincipalCannotFinishForeignAttempt_PG(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	_, _ = env.pool.Exec(ctx, `INSERT INTO tasks (tenant_id, id, source_agent, target_type, target_value, mailbox_id, status, priority, envelope)
		VALUES ('acme','own-task','agent-b','mailbox','mb-1','mb-1','claimed','normal',
		jsonb_build_object('task_id','own-task','tenant_id','acme','source_agent','agent-b'))`)
	_, _ = env.pool.Exec(ctx, `INSERT INTO task_attempts (tenant_id, task_id, attempt, agent_id, lease_id, status, delivery_ref)
		VALUES ('acme','own-task',1,'agent-b','lease-b','running','dr-own')`)

	boundCtx := context.WithValue(ctx, auth.PrincipalCtxKey, auth.Principal{
		TenantID: "acme", BoundAgentID: "agent-a",
	})

	for _, tc := range []struct {
		name  string
		agent string
	}{
		{"omitted", ""},
		{"forged", "agent-b"},
	} {
		err := env.dispatch.AckTaskAs(boundCtx, "acme", "own-task", "lease-b", tc.agent, "s3://stolen", nil)
		require.Error(t, err, "agent-a's key must not finish agent-b's attempt (%s)", tc.name)
		require.Contains(t, err.Error(), "does not own")
	}

	var status string
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT status FROM task_attempts WHERE tenant_id='acme' AND task_id='own-task' AND attempt=1`).Scan(&status))
	require.Equal(t, "running", status, "foreign ack must leave the attempt untouched (zero side effects)")

	var taskStatus string
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT status FROM tasks WHERE tenant_id='acme' AND id='own-task'`).Scan(&taskStatus))
	require.Equal(t, "claimed", taskStatus, "foreign ack must not complete the task")

	// The rightful owner still succeeds through the same path.
	require.NoError(t, env.dispatch.AckTaskAs(ctx, "acme", "own-task", "lease-b", "agent-b", "s3://ok", nil))
}

// A2 regression: replay must not bypass the original approval verdict.
// pending -> Cancel -> Replay and Reject -> Replay both used to re-queue.
func TestReplay_ApprovalVerdictBlocksRequeue_PG(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	approvalRepo := postgres.NewApprovalRepo(env.pool)
	approvalSvc := NewApprovalService(approvalRepo, env.taskSvc, nil).WithTxPath(env.lifecycle, env.outboxRepo)
	env.taskSvc.WithApproval(approvalSvc)

	mk := func(id string) {
		_, err := env.pool.Exec(ctx, `INSERT INTO tasks (tenant_id, id, source_agent, target_type, target_value, mailbox_id, status, priority, envelope)
			VALUES ('acme',$1,'agent-1','mailbox','mb-1','mb-1','approval_pending','normal',
			jsonb_build_object('task_id',$1::text,'tenant_id','acme','source_agent','agent-1'))`, id)
		require.NoError(t, err)
	}
	request := func(id string) *core.Approval {
		a, err := approvalSvc.RequestApproval(ctx, core.Approval{
			TenantID: "acme", TaskID: id, RequestedBy: "agent-1",
		})
		require.NoError(t, err)
		return a
	}

	mk("rej-task")
	a := request("rej-task")
	require.NoError(t, approvalSvc.Reject(ctx, "acme", a.ID, "admin", "not allowed"))
	_, err := env.taskSvc.Replay(ctx, "acme", "rej-task")
	require.Error(t, err)
	require.Contains(t, err.Error(), "rejected")

	mk("pend-task")
	request("pend-task")
	_, err = env.taskSvc.Replay(ctx, "acme", "pend-task")
	require.Error(t, err)
	require.Contains(t, err.Error(), "pending")

	// Approved tasks replay after completing a full run.
	mk("ok-task")
	a2 := request("ok-task")
	require.NoError(t, approvalSvc.Approve(ctx, "acme", a2.ID, "admin", ""))
	_, uerr := env.pool.Exec(ctx, `UPDATE tasks SET status='completed' WHERE tenant_id='acme' AND id='ok-task'`)
	require.NoError(t, uerr)
	_, err = env.taskSvc.Replay(ctx, "acme", "ok-task")
	require.NoError(t, err)
}

// B2 regression: attempt numbers restart at 1 after a replay. An ACK that
// read the OLD attempt (pre-replay) and resumes after a replay + fresh pull
// must NOT finish the new round's same-numbered attempt — lease_id is part
// of the finish CAS.
func TestAck_ReplayRace_OldAckCannotFinishNewAttempt_PG(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	_, _ = env.pool.Exec(ctx, `INSERT INTO tasks (tenant_id, id, source_agent, target_type, target_value, mailbox_id, status, priority, envelope)
		VALUES ('acme','rr-task','agent-1','mailbox','mb-1','mb-1','claimed','normal',
		jsonb_build_object('task_id','rr-task','tenant_id','acme','source_agent','agent-1'))`)
	_, _ = env.pool.Exec(ctx, `INSERT INTO task_attempts (tenant_id, task_id, attempt, agent_id, lease_id, status, delivery_ref)
		VALUES ('acme','rr-task',1,'agent-1','lease-old','running','dr-old')`)

	// Complete round 1 so Replay is legal, then replay: attempts are wiped.
	_, err := env.pool.Exec(ctx, `UPDATE tasks SET status='completed' WHERE tenant_id='acme' AND id='rr-task'`)
	require.NoError(t, err)
	_, err = env.taskSvc.Replay(ctx, "acme", "rr-task")
	require.NoError(t, err)

	// Round 2 pull creates a NEW attempt=1 under a NEW lease.
	drv := &perCallDriver{queue: []core.TaskDelivery{
		{TaskID: "rr-task", DeliveryRef: core.DeliveryRef("rr-ref-2"), Payload: []byte(`{}`)},
	}}
	env.dispatch = env.dispatch.WithQueueDriver(drv)
	res, perr := env.dispatch.PullTask(ctx, "acme", "mb-1", "agent-1")
	require.NoError(t, perr)
	require.NotNil(t, res)
	newLease := res.LeaseID
	require.NotEqual(t, "lease-old", newLease)

	// The stale ACK (round-1 lease) must not finish the round-2 attempt.
	err = env.dispatch.AckTask(ctx, "acme", "rr-task", "lease-old", "s3://stale", nil)
	require.Error(t, err, "stale-lease ack must be rejected")
	require.Contains(t, err.Error(), "lease mismatch")

	var aStatus string
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT status FROM task_attempts WHERE tenant_id='acme' AND task_id='rr-task' AND attempt=1`).Scan(&aStatus))
	require.Equal(t, "claimed", aStatus, "the new round's attempt must be untouched by the stale ack")

	var tStatus string
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT status FROM tasks WHERE tenant_id='acme' AND id='rr-task'`).Scan(&tStatus))
	require.Equal(t, "claimed", tStatus, "the new round's task must stay claimed")

	// The new round's own lease still acks fine.
	require.NoError(t, env.dispatch.AckTask(ctx, "acme", "rr-task", newLease, "s3://fresh", nil))
}

// B1 regression: StartTaskAs validated lease+owner on the POOL before the
// transaction. Between that read and the tx body, Cancel -> Replay -> a
// fresh Pull swaps the attempt (same attempt NUMBER, new lease) and the
// stale start used to transition the task to running inside the tx without
// revalidation. The tx now re-reads the latest attempt via GetLatestTx and
// that check is authoritative.
func TestStart_ReplayRace_OldLeaseCannotStartNewRound_PG(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	_, _ = env.pool.Exec(ctx, `INSERT INTO tasks (tenant_id, id, source_agent, target_type, target_value, mailbox_id, status, priority, envelope)
		VALUES ('acme','sr-task','agent-1','mailbox','mb-1','mb-1','queued','normal',
		jsonb_build_object('task_id','sr-task','tenant_id','acme','source_agent','agent-1'))`)

	// Round 1: pull hands out lease A on attempt 1.
	env.dispatch = env.dispatch.WithQueueDriver(&perCallDriver{queue: []core.TaskDelivery{
		{TaskID: "sr-task", DeliveryRef: core.DeliveryRef("sr-ref-1"), Payload: []byte(`{}`)},
	}})
	res1, perr := env.dispatch.PullTask(ctx, "acme", "mb-1", "agent-1")
	require.NoError(t, perr)
	require.NotNil(t, res1)
	oldLease := res1.LeaseID

	// Cancel closes round 1 (terminal, attempt finished); Replay wipes the
	// attempts and resets attempt_count to 0.
	require.NoError(t, env.taskSvc.Cancel(ctx, "acme", "sr-task"))
	_, rerr := env.taskSvc.Replay(ctx, "acme", "sr-task")
	require.NoError(t, rerr)

	// Round 2: pull again — attempt 1 AGAIN, under a NEW lease.
	env.dispatch = env.dispatch.WithQueueDriver(&perCallDriver{queue: []core.TaskDelivery{
		{TaskID: "sr-task", DeliveryRef: core.DeliveryRef("sr-ref-2"), Payload: []byte(`{}`)},
	}})
	res2, perr2 := env.dispatch.PullTask(ctx, "acme", "mb-1", "agent-1")
	require.NoError(t, perr2)
	require.NotNil(t, res2)
	newLease := res2.LeaseID
	require.NotEqual(t, oldLease, newLease)

	var tStatus, leaseRow string
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT status FROM tasks WHERE tenant_id='acme' AND id='sr-task'`).Scan(&tStatus))
	require.Equal(t, "claimed", tStatus, "round 2 must be claimed")

	// Simulate the race window: the pre-tx GetLatest read the pre-replay
	// attempt (old lease) just before the swap committed. The transaction
	// re-reads through GetLatestTx — the stale start must be rejected INSIDE
	// the tx and leave the round-2 state untouched.
	real := env.dispatch.attemptRepo
	env.dispatch.attemptRepo = &staleLatestAttemptRepo{
		TaskAttemptRepo: real,
		stale: core.TaskAttempt{
			TenantID: "acme", TaskID: "sr-task", Attempt: 1,
			AgentID: "agent-1", LeaseID: oldLease, DeliveryRef: "sr-ref-1", Status: "claimed",
		},
	}
	serr := env.dispatch.StartTask(ctx, "acme", "sr-task", oldLease)
	env.dispatch.attemptRepo = real
	require.Error(t, serr, "stale-lease start must be rejected even when its pre-check read the old attempt")
	assert.Contains(t, serr.Error(), "lease mismatch")

	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT status FROM tasks WHERE tenant_id='acme' AND id='sr-task'`).Scan(&tStatus))
	require.Equal(t, "claimed", tStatus, "the new round's task must stay claimed, startable only by the new lease")

	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT lease_id FROM task_attempts WHERE tenant_id='acme' AND task_id='sr-task' AND attempt=1`).Scan(&leaseRow))
	require.Equal(t, newLease, leaseRow, "the new round's attempt must be untouched by the stale start")

	// The new lease's start succeeds.
	require.NoError(t, env.dispatch.StartTask(ctx, "acme", "sr-task", newLease))
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT status FROM tasks WHERE tenant_id='acme' AND id='sr-task'`).Scan(&tStatus))
	require.Equal(t, "running", tStatus)
}

// B1 residual: the legacy Complete/Fail terminal paths must close open
// attempts in the same transaction (a live lease used to survive them),
// and heartbeats on finished attempts must be no-ops.
func TestComplete_FinishesOpenAttempt_Atomic_PG(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	_, _ = env.pool.Exec(ctx, `INSERT INTO tasks (tenant_id, id, source_agent, target_type, target_value, mailbox_id, status, priority, envelope)
		VALUES ('acme','cf-task','agent-1','mailbox','mb-1','mb-1','running','normal',
		jsonb_build_object('task_id','cf-task','tenant_id','acme','source_agent','agent-1'))`)
	_, _ = env.pool.Exec(ctx, `INSERT INTO task_attempts (tenant_id, task_id, attempt, agent_id, lease_id, status, delivery_ref)
		VALUES ('acme','cf-task',1,'agent-1','lease-cf','running','dr-cf')`)

	require.NoError(t, env.taskSvc.Complete(ctx, "acme", "cf-task"))

	var aStatus string
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT status FROM task_attempts WHERE tenant_id='acme' AND task_id='cf-task' AND attempt=1`).Scan(&aStatus))
	require.Equal(t, "completed", aStatus, "legacy Complete must finish the open attempt in the same tx")

	// Heartbeats on the finished attempt must be rejected (worker stops).
	hbErr := env.dispatch.TaskHeartbeat(ctx, "acme", "cf-task", "lease-cf")
	require.Error(t, hbErr, "heartbeat on a finished attempt must error")
	require.Contains(t, hbErr.Error(), "no longer active")
	var hb *time.Time
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT heartbeat_at FROM task_attempts WHERE tenant_id='acme' AND task_id='cf-task' AND attempt=1`).Scan(&hb))
	require.Nil(t, hb, "no heartbeat may be stamped on a finished attempt")
}

// B4 acceptance: the FULL dead-letter chain against a real broker and PG —
// non-retriable NACK -> task dead_lettered (no illegal status overwrite)
// -> DLQ message published -> Replay -> re-queued -> delivered again.
func TestB4_NonRetriableNack_To_DLQ_To_Replay_FullChain_PG(t *testing.T) {
	natsURL := os.Getenv("JANUS_NATS_URL")
	if natsURL == "" {
		if _, err := exec.LookPath(os.ExpandEnv("$HOME/go/bin/nats-server")); err != nil {
			t.Skip("no nats-server and JANUS_NATS_URL unset; skipping full-chain DLQ test")
		}
		srv := startNATSIfNeeded(t)
		natsURL = srv
	}

	env := setupServiceTestEnv(t)
	ctx := context.Background()
	nc, nerr := natsgo.Connect(natsURL)
	require.NoError(t, nerr)
	defer nc.Close()
	js, jerr := nc.JetStream()
	require.NoError(t, jerr)

	drv, derr := natsdriver.NewDriver(natsdriver.Config{URL: natsURL})
	require.NoError(t, derr)
	defer drv.Close()
	require.NoError(t, drv.EnsureTenant(ctx, "acme"))
	require.NoError(t, drv.EnsureMailbox(ctx, core.MailboxSpec{
		TenantID: "acme", MailboxID: "mb-1", AgentID: "agent-1", ACKWaitSeconds: 30,
	}))
	require.NoError(t, drv.EnsureConsumer(ctx, core.ConsumerSpec{
		TenantID: "acme", MailboxID: "mb-1", DurableName: "mb-1", ACKWaitSeconds: 30, MaxDeliver: 5,
	}))
	env.dispatch = env.dispatch.WithQueueDriver(&driverCloser{QueueDriver: drv})
	env.taskSvc.WithQueueDriver(drv)

	// Publish a real task through the broker-backed pipeline.
	task := core.Task{
		ID: "b4-task", TenantID: "acme", SourceAgent: "agent-1",
		TargetType: core.TargetTypeMailbox, TargetValue: "mb-1", MailboxID: "mb-1",
		Status: core.TaskStatusQueued, Priority: core.PriorityNormal,
		Envelope: core.TaskEnvelope{JanusVersion: "0.3", TaskID: "b4-task", TenantID: "acme", SourceAgent: "agent-1",
			Target: core.Target{Type: "mailbox", Value: "mb-1"}, Payload: core.Payload{Type: "text", Content: "b4"},
			Trace: core.TraceContext{TraceID: "b4-trace"}},
	}
	_, cerr2 := env.taskSvc.Create(ctx, task)
	require.NoError(t, cerr2)

	// Pull through the service (claim + attempt). The outbox publisher is
	// not running in this harness, so drain it manually: PG outbox -> broker.
	drainOutbox := func() {
		entries, _ := env.outboxRepo.FetchPending(ctx, 10)
		for _, e := range entries {
			switch e.Kind {
			case "task_publish":
				var msg core.TaskMessage
				if json.Unmarshal(e.Payload, &msg) == nil {
					_ = drv.PublishTask(ctx, msg)
				}
			case "dlq_publish":
				var msg core.TaskMessage
				if json.Unmarshal(e.Payload, &msg) == nil {
					_ = drv.PublishDLQ(ctx, msg, []byte(msg.Headers["error"]))
				}
			}
		}
	}
	var res *PullResult
	require.NoError(t, withDeadline(t, 10*time.Second, func() error {
		drainOutbox()
		r, err := env.dispatch.PullTask(ctx, "acme", "mb-1", "agent-1")
		if err != nil {
			return err
		}
		if r == nil {
			return fmt.Errorf("not delivered yet")
		}
		res = r
		return nil
	}), "task must deliver through the broker")

	// Non-retriable NACK: task must go dead_lettered, never an illegal
	// status overwrite, and a DLQ message must exist.
	nerr2 := env.dispatch.NackTask(ctx, "acme", "b4-task", res.LeaseID, false,
		&core.TaskError{Code: "PERMANENT", Message: "b4"})
	require.NoError(t, nerr2)

	got, gerr := env.taskRepo.Get(ctx, "acme", "b4-task")
	require.NoError(t, gerr)
	require.Equal(t, core.TaskStatusDeadLettered, got.Status, "non-retriable nack must dead-letter the task")

	require.NoError(t, withDeadline(t, 10*time.Second, func() error {
		drainOutbox()
		_, _, cerr := fetchDLQ(t, js, "acme", "mb-1")
		return cerr
	}), "a DLQ message must be published")

	// Replay: dead_lettered -> queued, and the task delivers again.
	_, rerr := env.taskSvc.Replay(ctx, "acme", "b4-task")
	require.NoError(t, rerr)
	got2, _ := env.taskRepo.Get(ctx, "acme", "b4-task")
	require.Equal(t, core.TaskStatusQueued, got2.Status)

	var res2 *PullResult
	require.NoError(t, withDeadline(t, 10*time.Second, func() error {
		drainOutbox()
		r, err := env.dispatch.PullTask(ctx, "acme", "mb-1", "agent-1")
		if err != nil {
			return err
		}
		if r == nil {
			return fmt.Errorf("replayed task not delivered yet")
		}
		res2 = r
		return nil
	}), "replayed task must redeliver")
	require.NoError(t, env.dispatch.AckTask(ctx, "acme", "b4-task", res2.LeaseID, "s3://b4", nil))
	got3, _ := env.taskRepo.Get(ctx, "acme", "b4-task")
	require.Equal(t, core.TaskStatusCompleted, got3.Status)
}

func startNATSIfNeeded(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	// Fixed high port: other packages spawn their own servers on 4222
	// during full-suite runs and this nats-server build binds -p 0 there.
	cmd := exec.Command(os.ExpandEnv("$HOME/go/bin/nats-server"), "-p", "4299", "-js", "--store_dir", dir)
	stderr, err := cmd.StderrPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() { cmd.Process.Signal(os.Interrupt); cmd.Wait() })
	addrCh := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(stderr)
		for sc.Scan() {
			if m := regexp.MustCompile(`Listening for client connections on (.+)`).FindStringSubmatch(sc.Text()); len(m) > 1 {
				addrCh <- m[1]
				return
			}
		}
	}()
	select {
	case addr := <-addrCh:
		return "nats://" + addr
	case <-time.After(5 * time.Second):
		t.Fatal("nats-server did not start")
		return ""
	}
}

func withDeadline(t *testing.T, d time.Duration, fn func() error) error {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		err := fn()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return err
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func fetchDLQ(t *testing.T, js natsgo.JetStream, tenant, mailbox string) (string, []byte, error) {
	t.Helper()
	sub, err := js.PullSubscribe(fmt.Sprintf("janus.%s.tasks_dlq.%s", tenant, mailbox),
		"", natsgo.Durable("b4-dlq-consumer-"+mailbox))
	if err != nil {
		return "", nil, err
	}
	defer sub.Unsubscribe()
	msgs, err := sub.Fetch(1, natsgo.MaxWait(2*time.Second))
	if err != nil || len(msgs) == 0 {
		return "", nil, fmt.Errorf("no DLQ message: %v", err)
	}
	return string(msgs[0].Subject), msgs[0].Data, nil
}

// driverCloser keeps the QueueDriver interface while owning the real NATS
// driver's lifecycle in the test.
type driverCloser struct {
	QueueDriver
}

// B3 regression (advisory-lock self-starvation): the mailbox advisory lock
// holds a dedicated pooled connection, and the version-check read under the
// lock used to borrow a SECOND pool connection. On a size-1 pool the locked
// read waits for the very connection the lock pins — deadlock. The locked
// read now runs on the lock's own connection, so UpdateConfig completes on
// a single-connection pool.
func TestMailboxService_UpdateConfigSingleConnPoolNoSelfStarvation(t *testing.T) {
	pool := openServiceTestDB(t)
	ctx := context.Background()

	// A second pool over the same test DB with exactly ONE connection.
	var dbName string
	require.NoError(t, pool.QueryRow(ctx, `SELECT current_database()`).Scan(&dbName))
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
	password := os.Getenv("JANUS_PG_PASSWORD")
	dsn := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable", host, port, user, dbName)
	if password != "" {
		dsn = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbName)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	cfg.MaxConns = 1
	small, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(small.Close)

	// Seed via the normal pool so the size-1 pool starts idle.
	_, err = pool.Exec(ctx, `INSERT INTO tenants (id, name) VALUES ('sc', 'SC') ON CONFLICT DO NOTHING`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO mailboxes (id, tenant_id, agent_id, status) VALUES ('mb-sc', 'sc', 'a1', 'active')`)
	require.NoError(t, err)

	repo := postgres.NewMailboxRepository(small)
	drv := &reconcileRecorder{apply: func(core.ConsumerSpec) error { return nil }}
	svc := NewMailboxService(repo, drv)

	done := make(chan error, 1)
	go func() {
		done <- svc.UpdateConfig(ctx, "sc", "mb-sc", 2, 30, 5, 3600)
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("UpdateConfig starved: the version-gate read under the mailbox lock waited for a second pooled connection (self-starvation regression)")
	}

	drv.mu.Lock()
	require.Len(t, drv.specs, 1, "broker reconcile ran under the lock")
	drv.mu.Unlock()

	mb, err := repo.Get(ctx, "sc", "mb-sc")
	require.NoError(t, err)
	assert.Equal(t, 1, mb.ConfigVersion, "config version bumped")
}

// eval-6 claim 2: Unblock republished without re-running the CURRENT
// task.publish policy — a deny rule added after Block sailed through on the
// strength of the earlier create-time allowance. The gate must fire BEFORE
// the lifecycle tx: refusal leaves the task blocked, no task_publish outbox
// row, no attempt side effects.
func TestTaskService_Unblock_PublishPolicyDeny_PG(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	task := core.Task{
		ID: "unblock-policy-deny", TenantID: "acme", MailboxID: "mb-1", SourceAgent: "agent-1",
		TargetType: core.TargetTypeMailbox, TargetValue: "mb-1",
		Status: core.TaskStatusQueued, Priority: core.PriorityNormal,
		Envelope: core.TaskEnvelope{JanusVersion: "1", TaskID: "unblock-policy-deny", TenantID: "acme",
			SourceAgent: "agent-1", Target: core.Target{Type: "mailbox", Value: "mb-1"},
			Payload: core.Payload{Type: "text", Content: "deny"}},
	}
	require.NoError(t, env.taskRepo.Create(ctx, task))
	require.NoError(t, env.taskSvc.Block(ctx, "acme", task.ID, "policy review"))

	// The deny rule for the task's target lands AFTER the block.
	require.NoError(t, postgres.NewPolicyRuleRepository(env.pool).Create(ctx, core.PolicyRule{
		TenantID:  "acme",
		ID:        "rule-unblock-deny",
		Name:      "deny publish to mb-1",
		Status:    "active",
		Priority:  10,
		Condition: json.RawMessage(`{"resource.value":"mb-1"}`),
		Action:    json.RawMessage(`{"decision":"deny"}`),
	}))

	err := env.taskSvc.Unblock(ctx, "acme", task.ID)
	require.Error(t, err, "unblock must re-run the current task.publish policy")
	require.Contains(t, err.Error(), "unblock denied")
	require.Contains(t, err.Error(), "deny publish to mb-1", "refusal must name the matched policy rule")

	got, gerr := env.taskRepo.Get(ctx, "acme", task.ID)
	require.NoError(t, gerr)
	assert.Equal(t, core.TaskStatusBlocked, got.Status, "refused unblock must leave the task blocked")

	var publishes int
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox_events WHERE tenant_id='acme' AND kind='task_publish'`).Scan(&publishes))
	assert.Zero(t, publishes, "refused unblock must not enqueue a delivery")

	var attempts int
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_attempts WHERE tenant_id='acme' AND task_id=$1`, task.ID).Scan(&attempts))
	assert.Zero(t, attempts, "refused unblock must leave no attempt side effects")
}

// eval-6 claim 2 (admission side): Unblock must also re-pass the final-target
// mailbox admission — a mailbox owner that became policy-denied after Block
// must not receive the republish. Uses the same fake-router harness
// TestReplay_MailboxAdmissionGate uses (service package), over real PG state.
func TestTaskService_Unblock_MailboxAdmissionDeny_PG(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	owner := &routing.AgentCandidate{AgentID: "agent-1", MailboxID: "mb-1", TeamID: "team-a"}
	env.taskSvc.WithRouter(routing.NewRouter(admissionLookup(owner), &admitPolicy{denied: map[string]bool{"agent-1": true}}, nil))

	task := core.Task{
		ID: "unblock-admit-deny", TenantID: "acme", MailboxID: "mb-1", SourceAgent: "agent-1",
		TargetType: core.TargetTypeMailbox, TargetValue: "mb-1",
		Status: core.TaskStatusQueued, Priority: core.PriorityNormal,
		Envelope: core.TaskEnvelope{JanusVersion: "1", TaskID: "unblock-admit-deny", TenantID: "acme",
			SourceAgent: "agent-1", Target: core.Target{Type: "mailbox", Value: "mb-1"},
			Payload: core.Payload{Type: "text", Content: "admit"}},
	}
	require.NoError(t, env.taskRepo.Create(ctx, task))
	require.NoError(t, env.taskSvc.Block(ctx, "acme", task.ID, "admission review"))

	// No policy rules exist: the refusal can only come from admission.
	err := env.taskSvc.Unblock(ctx, "acme", task.ID)
	require.Error(t, err, "unblock must re-pass final-target mailbox admission")
	require.Contains(t, err.Error(), "unblock admission")
	require.Contains(t, err.Error(), "denied by policy")

	got, gerr := env.taskRepo.Get(ctx, "acme", task.ID)
	require.NoError(t, gerr)
	assert.Equal(t, core.TaskStatusBlocked, got.Status, "refused unblock must leave the task blocked")

	var publishes int
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox_events WHERE tenant_id='acme' AND kind='task_publish'`).Scan(&publishes))
	assert.Zero(t, publishes, "refused unblock must not enqueue a delivery")
}
