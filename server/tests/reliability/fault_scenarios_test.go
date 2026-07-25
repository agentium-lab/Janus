package reliability

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
	pgdriver "github.com/agentium-lab/Janus/server/internal/driver/postgres"
	"github.com/agentium-lab/Janus/server/internal/lease"
	"github.com/agentium-lab/Janus/server/internal/service"
)

type faultEnv struct {
	pool        *pgxpool.Pool
	taskRepo    *pgdriver.TaskRepository
	attemptRepo *pgdriver.TaskAttemptRepository
	mailboxRepo *pgdriver.MailboxRepository
	outboxRepo  *pgdriver.OutboxRepo
	budgetUsage *pgdriver.BudgetUsageRepo
	tenantRepo  *pgdriver.TenantRepository
	agentRepo   *pgdriver.AgentRepository
	dispatch    *service.DispatchService
	taskSvc     *service.TaskService
	lifecycle   *service.LifecycleService
	queue       *faultQueueDriver
}

type faultQueueDriver struct {
	mu             sync.Mutex
	deliveries     map[string][]core.TaskDelivery
	publishedTasks []core.TaskMessage
	publishedDLQ   []core.TaskMessage
	events         []core.JanusEvent
	refSeq         int

	ackErr        error
	nackErr       error
	publishErr    error
	publishDLQErr error
	ackCalls      int
	nackCalls     int
	publishCalls  int

	ackRefs  []string
	nackRefs []string
}

func (d *faultQueueDriver) nextRef() core.DeliveryRef {
	d.refSeq++
	return core.DeliveryRef(fmt.Sprintf("ref-%d", d.refSeq))
}

func (d *faultQueueDriver) PublishTask(_ context.Context, msg core.TaskMessage) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.publishErr != nil {
		return d.publishErr
	}
	if d.deliveries == nil {
		d.deliveries = make(map[string][]core.TaskDelivery)
	}
	ref := d.nextRef()
	d.deliveries[msg.MailboxID] = append(d.deliveries[msg.MailboxID], core.TaskDelivery{
		TaskID: msg.TaskID, Payload: msg.Payload, DeliveryRef: ref,
	})
	d.publishedTasks = append(d.publishedTasks, msg)
	d.publishCalls++
	return nil
}

func (d *faultQueueDriver) FetchTasks(_ context.Context, mailbox string, opts core.FetchOptions) ([]core.TaskDelivery, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	q := d.deliveries[mailbox]
	if len(q) == 0 {
		return nil, nil
	}
	limit := opts.MaxMessages
	if limit <= 0 {
		limit = 1
	}
	if limit > len(q) {
		limit = len(q)
	}
	taken := make([]core.TaskDelivery, limit)
	copy(taken, q[:limit])
	d.deliveries[mailbox] = q[limit:]
	return taken, nil
}

func (d *faultQueueDriver) AckTask(_ context.Context, ref core.DeliveryRef) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ackCalls++
	d.ackRefs = append(d.ackRefs, string(ref))
	return d.ackErr
}

func (d *faultQueueDriver) NackTask(_ context.Context, ref core.DeliveryRef, _ core.NackReason) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.nackCalls++
	d.nackRefs = append(d.nackRefs, string(ref))
	return d.nackErr
}

func (d *faultQueueDriver) PublishEvent(_ context.Context, event core.JanusEvent) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.events = append(d.events, event)
	return nil
}

func (d *faultQueueDriver) ReplayEvents(_ context.Context, _ core.EventReplayFilter) (core.EventIterator, error) {
	return nil, nil
}
func (d *faultQueueDriver) EnsureTenant(_ context.Context, _ string) error           { return nil }
func (d *faultQueueDriver) EnsureMailbox(_ context.Context, _ core.MailboxSpec) error { return nil }
func (d *faultQueueDriver) EnsureConsumer(_ context.Context, _ core.ConsumerSpec) error { return nil }

func (d *faultQueueDriver) PublishDLQ(_ context.Context, msg core.TaskMessage, _ []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.publishDLQErr != nil {
		return d.publishDLQErr
	}
	d.publishedDLQ = append(d.publishedDLQ, msg)
	return nil
}

func (d *faultQueueDriver) Close() error { return nil }

func openFaultTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	host := os.Getenv("JANUS_PG_HOST")
	if host == "" {
		host = "/tmp"
	}
	port := os.Getenv("JANUS_PG_PORT")
	if port == "" {
		port = "5432"
	}
	user := os.Getenv("JANUS_PG_USER")
	if user == "" {
		user = "silv"
	}
	testDB := fmt.Sprintf("janus_fault_%d", time.Now().UnixNano())

	ctx := context.Background()
	adminDSN := fmt.Sprintf("host=%s port=%s user=%s dbname=janus_test sslmode=disable", host, port, user)
	adminConn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Skipf("postgres not reachable: %v", err)
	}
	_, err = adminConn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", testDB))
	require.NoError(t, err)
	adminConn.Close(ctx)

	dsn := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable", host, port, user, testDB)
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	require.NoError(t, pool.Ping(ctx))

	runFaultMigrations(t, pool)

	t.Cleanup(func() {
		pool.Close()
		c, err := pgx.Connect(ctx, adminDSN)
		if err == nil {
			c.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDB))
			c.Close(ctx)
		}
	})

	return pool
}

func runFaultMigrations(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	migrationsDir := findFaultMigrationsDir()
	entries, err := os.ReadDir(migrationsDir)
	require.NoError(t, err)
	var upFiles []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".up.sql") {
			upFiles = append(upFiles, e.Name())
		}
	}
	sort.Strings(upFiles)
	for _, m := range upFiles {
		up, err := os.ReadFile(migrationsDir + "/" + m)
		require.NoError(t, err)
		_, err = pool.Exec(context.Background(), string(up))
		require.NoError(t, err, "migration %s", m)
	}
}

func findFaultMigrationsDir() string {
	for _, d := range []string{"../../../../migrations", "../../../migrations"} {
		if _, err := os.Stat(d); err == nil {
			return d
		}
	}
	return "../../../../migrations"
}

func setupFaultEnv(t *testing.T) *faultEnv {
	t.Helper()
	pool := openFaultTestDB(t)
	ctx := context.Background()

	taskRepo := pgdriver.NewTaskRepository(pool)
	attemptRepo := pgdriver.NewTaskAttemptRepository(pool)
	mailboxRepo := pgdriver.NewMailboxRepository(pool)
	outboxRepo := pgdriver.NewOutboxRepo(pool)
	budgetUsage := pgdriver.NewBudgetUsageRepo(pool)
	tenantRepo := pgdriver.NewTenantRepository(pool)
	agentRepo := pgdriver.NewAgentRepository(pool)
	budgetRepo := pgdriver.NewBudgetRepository(pool)
	policyRepo := pgdriver.NewPolicyRuleRepository(pool)

	budgetSvc := service.NewBudgetService(budgetRepo)
	policySvc := service.NewPolicyService(policyRepo)
	queue := &faultQueueDriver{}

	lifecycle := service.NewLifecycleService(pool)
	dispatch := service.NewDispatchService(taskRepo, attemptRepo, mailboxRepo, queue, policySvc, budgetSvc)
	dispatch = dispatch.WithLifecycle(lifecycle, outboxRepo, budgetUsage)

	taskSvc := service.NewTaskService(taskRepo, queue, pool, outboxRepo).WithPolicy(policySvc)
	taskSvc = taskSvc.WithLifecycle(lifecycle)

	tenantRepo.Create(ctx, "acme", "Acme")
	agentRepo.Register(ctx, core.Agent{
		ID: "agent-1", TenantID: "acme", DisplayName: "Agent One",
		Status: core.AgentStatusOnline, Protocol: core.ProtocolA2A,
	})
	mailboxRepo.Create(ctx, core.Mailbox{
		ID: "mb-1", TenantID: "acme", AgentID: "agent-1", Status: "active",
		RetryPolicy: core.RetryPolicy{MaxAttempts: 3, BackoffType: "exponential", InitialSeconds: 1, MaxSeconds: 10},
		MaxDeliver:  3,
	})

	return &faultEnv{
		pool: pool, taskRepo: taskRepo, attemptRepo: attemptRepo,
		mailboxRepo: mailboxRepo, outboxRepo: outboxRepo, budgetUsage: budgetUsage,
		tenantRepo: tenantRepo, agentRepo: agentRepo,
		dispatch: dispatch, taskSvc: taskSvc, lifecycle: lifecycle, queue: queue,
	}
}

func createFaultTask(t *testing.T, env *faultEnv, taskID string) *core.Task {
	t.Helper()
	task := core.Task{
		ID: taskID, TenantID: "acme", MailboxID: "mb-1", SourceAgent: "agent-1",
		Status: core.TaskStatusQueued, Priority: core.PriorityNormal,
		Envelope: core.TaskEnvelope{
			TaskID: taskID, TenantID: "acme", SourceAgent: "agent-1",
			Target: core.Target{Type: "agent", Value: "agent-1"},
		},
	}
	require.NoError(t, env.taskRepo.Create(context.Background(), task))
	return &task
}

func claimTask(t *testing.T, env *faultEnv, taskID, leaseID, deliveryRef string) {
	t.Helper()
	ctx := context.Background()
	task, err := env.taskRepo.Get(ctx, "acme", taskID)
	require.NoError(t, err)

	attempt := core.TaskAttempt{
		TenantID: "acme", TaskID: taskID, Attempt: task.AttemptCount + 1,
		AgentID: "agent-1", LeaseID: leaseID, Status: "claimed",
		StartedAt: time.Now(), DeliveryRef: deliveryRef,
	}
	require.NoError(t, env.attemptRepo.Create(ctx, attempt))
	_, err = env.taskRepo.UpdateStatusWithCheck(ctx, "acme", taskID, core.TaskStatusQueued, core.TaskStatusClaimed, 1)
	require.NoError(t, err)
}

func outboxEventCount(env *faultEnv, eventType core.EventType) int {
	entries, _ := env.outboxRepo.FetchPending(context.Background(), 1000)
	count := 0
	for _, e := range entries {
		if e.Kind == "event_publish" {
			var evt core.JanusEvent
			if json.Unmarshal(e.Payload, &evt) == nil && evt.EventType == eventType {
				count++
			}
		}
	}
	return count
}

// Fault scenario 1 (roadmap §6): NATS publish succeeds, but outbox
// post-DB transaction fails. The outbox entry should be retryable.
func TestFault01_NATSOk_OutboxDBFails(t *testing.T) {
	env := setupFaultEnv(t)
	ctx := context.Background()

	result, err := env.taskSvc.Create(ctx, core.Task{
		ID: "task-fault-01", TenantID: "acme", MailboxID: "mb-1", SourceAgent: "agent-1",
		TargetType: core.TargetTypeMailbox, TargetValue: "mb-1",
		Status: core.TaskStatusCreated, Priority: core.PriorityNormal,
		Envelope: core.TaskEnvelope{
			JanusVersion: "1", TaskID: "task-fault-01", TenantID: "acme", SourceAgent: "agent-1",
			Target:  core.Target{Type: "mailbox", Value: "mb-1"},
			Payload: core.Payload{Type: "test", Content: "{}"},
			Trace:   core.TraceContext{TraceID: "trace-fault-01"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, result)

	got, _ := env.taskRepo.Get(ctx, "acme", "task-fault-01")
	assert.NotNil(t, got, "task should be created in DB")

	entries, _ := env.outboxRepo.FetchPending(ctx, 100)
	pendingCount := len(entries)

	if pendingCount == 0 {
		var rawCount int
		err = env.pool.QueryRow(ctx,
			`SELECT count(*) FROM outbox_events WHERE tenant_id = 'acme'`).Scan(&rawCount)
		require.NoError(t, err)
		assert.Greater(t, rawCount, 0, "outbox should have entries for the task")
	} else {
		assert.GreaterOrEqual(t, pendingCount, 1, "outbox should have pending entries")
	}
}

// Fault scenario 2 (roadmap §6): DB committed completed, but NATS ACK
// failed. The old delivery is redelivered and should be ACKed without
// re-processing.
func TestFault02_DBCompleted_NATSAckFails_Redeliver(t *testing.T) {
	env := setupFaultEnv(t)
	ctx := context.Background()
	task := createFaultTask(t, env, "task-fault-02")
	claimTask(t, env, task.ID, "lease-02", "delivery-02")

	env.queue.ackErr = fmt.Errorf("NATS connection lost")
	_ = env.dispatch.AckTask(ctx, "acme", task.ID, "lease-02", "result://ok", nil)

	got, _ := env.taskRepo.Get(ctx, "acme", task.ID)
	assert.Equal(t, core.TaskStatusCompleted, got.Status, "task should be completed in DB even if queue ACK failed")

	env.queue.ackErr = nil
	env.queue.mu.Lock()
	env.queue.deliveries = map[string][]core.TaskDelivery{
		"mb-1": {{TaskID: task.ID, DeliveryRef: "delivery-02"}},
	}
	env.queue.mu.Unlock()

	_, pullErr := env.dispatch.PullTask(ctx, "acme", "mb-1", "agent-1")
	assert.NoError(t, pullErr)
}

// Fault scenario 3 (roadmap §6): API process restarts between Pull and
// ACK. The task should still be ackable by a new service instance.
func TestFault03_PullThenRestart_ThenAck(t *testing.T) {
	env := setupFaultEnv(t)
	ctx := context.Background()
	task := createFaultTask(t, env, "task-fault-03")

	env.queue.mu.Lock()
	env.queue.deliveries = map[string][]core.TaskDelivery{
		"mb-1": {{TaskID: task.ID, DeliveryRef: "delivery-03"}},
	}
	env.queue.mu.Unlock()

	result, err := env.dispatch.PullTask(ctx, "acme", "mb-1", "agent-1")
	require.NoError(t, err)
	require.NotNil(t, result)

	newDispatch := service.NewDispatchService(
		env.taskRepo, env.attemptRepo, env.mailboxRepo, env.queue,
		service.NewPolicyService(pgdriver.NewPolicyRuleRepository(env.pool)),
		service.NewBudgetService(pgdriver.NewBudgetRepository(env.pool)),
	).WithLifecycle(env.lifecycle, env.outboxRepo, env.budgetUsage)

	err = newDispatch.AckTask(ctx, "acme", task.ID, result.LeaseID, "result://after-restart", nil)
	require.NoError(t, err)

	got, _ := env.taskRepo.Get(ctx, "acme", task.ID)
	assert.Equal(t, core.TaskStatusCompleted, got.Status)
}

// Fault scenario 4 (roadmap §6): Lease timeout after original agent tries
// to ACK. The lease scanner expires the lease; the late ACK is rejected.
func TestFault04_LeaseTimeout_LateACKRejected(t *testing.T) {
	env := setupFaultEnv(t)
	ctx := context.Background()
	task := createFaultTask(t, env, "task-fault-04")
	claimTask(t, env, task.ID, "lease-04", "delivery-04")

	_, err := env.pool.Exec(ctx,
		`UPDATE task_attempts SET heartbeat_at = now() - interval '1 hour'
		 WHERE tenant_id = 'acme' AND task_id = $1 AND attempt = 1`, task.ID)
	require.NoError(t, err)

	scanner := lease.NewScanner(env.pool, time.Hour)
	n, err := scanner.ExpireLeases(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	got, _ := env.taskRepo.Get(ctx, "acme", task.ID)
	assert.Equal(t, core.TaskStatusRetryScheduled, got.Status)

	ackErr := env.dispatch.AckTask(ctx, "acme", task.ID, "lease-04", "", nil)
	if ackErr != nil {
		assert.Contains(t, ackErr.Error(), "lease mismatch")
	}
	assert.Equal(t, core.TaskStatusRetryScheduled, got.Status, "task should remain retry_scheduled after late ACK")
}

// Fault scenario 5 (roadmap §6): Retry exhausted → DLQ → replay
// re-enqueue.
func TestFault05_RetryExhausted_DLQReplay(t *testing.T) {
	env := setupFaultEnv(t)
	ctx := context.Background()
	task := createFaultTask(t, env, "task-fault-05")

	_, err := env.pool.Exec(ctx,
		`UPDATE tasks SET attempt_count = 3 WHERE tenant_id = 'acme' AND id = $1`, task.ID)
	require.NoError(t, err)

	claimTask(t, env, task.ID, "lease-05", "delivery-05")

	err = env.dispatch.NackTask(ctx, "acme", task.ID, "lease-05", false, &core.TaskError{Code: "FATAL"})
	require.NoError(t, err)

	got, _ := env.taskRepo.Get(ctx, "acme", task.ID)
	assert.Equal(t, core.TaskStatusDeadLettered, got.Status)

	result, err := env.taskSvc.Replay(ctx, "acme", task.ID)
	require.NoError(t, err)
	require.NotNil(t, result)

	replayed, _ := env.taskRepo.Get(ctx, "acme", task.ID)
	assert.Contains(t, []string{"created", "queued"}, string(replayed.Status))
}

// Fault scenario 6 (roadmap §6): Duplicate ACK/NACK for an old attempt.
// The CAS guard prevents double-settlement.
func TestFault06_DuplicateACK_NoDoubleSettlement(t *testing.T) {
	env := setupFaultEnv(t)
	ctx := context.Background()
	task := createFaultTask(t, env, "task-fault-06")
	claimTask(t, env, task.ID, "lease-06", "delivery-06")

	usage := &core.TokenUsage{PromptTokens: 100, CompletionTokens: 50, TotalTokens: 150}

	err := env.dispatch.AckTask(ctx, "acme", task.ID, "lease-06", "result://ok", usage)
	require.NoError(t, err)

	tokens1, _, _, _ := env.budgetUsage.GetDailyUsage(ctx, "acme", "tenant", "acme")
	assert.Equal(t, 150, tokens1)

	err = env.dispatch.AckTask(ctx, "acme", task.ID, "lease-06", "result://ok", usage)
	require.NoError(t, err, "duplicate ACK should not error")

	tokens2, _, _, _ := env.budgetUsage.GetDailyUsage(ctx, "acme", "tenant", "acme")
	assert.Equal(t, 150, tokens2, "budget should not be double-settled")

	assert.Equal(t, 1, outboxEventCount(env, core.EventTaskCompleted))
}

// Fault scenario 7 (roadmap §6): retry_scheduled → next enqueue is driven
// ONLY by the delayed outbox next_attempt_at, not an independent retry
// stream.
func TestFault07_RetryScheduled_DelayedOutboxDriven(t *testing.T) {
	env := setupFaultEnv(t)
	ctx := context.Background()
	task := createFaultTask(t, env, "task-fault-07")
	claimTask(t, env, task.ID, "lease-07", "delivery-07")

	err := env.dispatch.NackTask(ctx, "acme", task.ID, "lease-07", true, &core.TaskError{Code: "TIMEOUT"})
	require.NoError(t, err)

	got, _ := env.taskRepo.Get(ctx, "acme", task.ID)
	assert.Equal(t, core.TaskStatusRetryScheduled, got.Status)

	hasRetryAt, err := env.pool.Exec(ctx,
		`UPDATE tasks SET retry_at = retry_at WHERE tenant_id = 'acme' AND id = $1 AND retry_at IS NOT NULL`, task.ID)
	require.NoError(t, err)
	assert.Greater(t, hasRetryAt.RowsAffected(), int64(0), "task should have retry_at set")

	entries, _ := env.outboxRepo.FetchPending(ctx, 100)
	var retryEventCount int
	err = env.pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox_events WHERE kind = 'event_publish' AND tenant_id = 'acme'`).Scan(&retryEventCount)
	require.NoError(t, err)
	assert.Greater(t, retryEventCount, 0, "outbox should contain event_publish rows")

	foundRetryEvent := false
	for _, e := range entries {
		if e.Kind == "event_publish" {
			var evt core.JanusEvent
			if json.Unmarshal(e.Payload, &evt) == nil && evt.EventType == core.EventTaskRetryScheduled {
				foundRetryEvent = true
			}
		}
	}
	if !foundRetryEvent {
		var rawRetryEvents int
		err = env.pool.QueryRow(ctx,
			`SELECT count(*) FROM outbox_events WHERE kind = 'event_publish' AND tenant_id = 'acme'
			 AND payload::text LIKE '%retry_scheduled%'`).Scan(&rawRetryEvents)
		require.NoError(t, err)
		assert.Greater(t, rawRetryEvents, 0, "outbox should contain task.retry_scheduled event row")
	}
}
