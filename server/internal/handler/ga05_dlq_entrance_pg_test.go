package handler

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/driver/postgres"
	"github.com/agentium-lab/Janus/server/internal/service"
)

type ga05dlqRig struct {
	pool       *pgxpool.Pool
	taskRepo   *postgres.TaskRepository
	dlq        *DLQServiceAdapter
	taskSvc    *service.TaskService
	dispatch   *service.DispatchService
	mbSvc      *service.MailboxService
	mailbox    string
	tenant     string
	taskID     string
	currentLease string
	pendingDLQ int
}

type ga05dlqQueue struct {
	published []core.TaskMessage
	events    []core.JanusEvent
}

func (q *ga05dlqQueue) PublishTask(_ context.Context, msg core.TaskMessage) error {
	q.published = append(q.published, msg)
	return nil
}

func (q *ga05dlqQueue) PublishEvent(_ context.Context, e core.JanusEvent) error {
	q.events = append(q.events, e)
	return nil
}

func (q *ga05dlqQueue) FetchTasks(_ context.Context, _ string, _ string, _ core.FetchOptions) ([]core.TaskDelivery, error) {
	return nil, nil
}

func (q *ga05dlqQueue) AckTask(_ context.Context, _ string, _ core.DeliveryRef) error  { return nil }
func (q *ga05dlqQueue) NackTask(_ context.Context, _ string, _ core.DeliveryRef, _ core.NackReason) error {
	return nil
}
func (q *ga05dlqQueue) PublishDLQ(_ context.Context, _ core.TaskMessage, _ []byte) error { return nil }
func (q *ga05dlqQueue) ReplayEvents(_ context.Context, _ core.EventReplayFilter) (core.EventIterator, error) {
	return nil, nil
}
func (q *ga05dlqQueue) EnsureTenant(_ context.Context, _ string) error                   { return nil }
func (q *ga05dlqQueue) EnsureMailbox(_ context.Context, _ core.MailboxSpec) error        { return nil }
func (q *ga05dlqQueue) EnsureConsumer(_ context.Context, _ core.ConsumerSpec) error      { return nil }
func (q *ga05dlqQueue) Close() error                                                     { return nil }

func ga05dlqOpenDB(t *testing.T) *pgxpool.Pool {
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
	password := os.Getenv("JANUS_PG_PASSWORD")
	testDB := fmt.Sprintf("janus_ga05dlq_%d", time.Now().UnixNano())
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
	require.NoError(t, err)
	adminConn.Close(ctx)
	t.Cleanup(func() {
		c, cerr := pgx.Connect(context.Background(), adminDSN)
		if cerr == nil {
			_, _ = c.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDB))
			_ = c.Close(context.Background())
		}
	})
	dsn := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable", host, port, user, testDB)
	if password != "" {
		dsn = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, testDB)
	}
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	require.NoError(t, pool.Ping(ctx))
	t.Cleanup(pool.Close)

	dir := ga05dlqMigrationsDir()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := []string{}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".up.sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	for _, m := range names {
		up, rerr := os.ReadFile(filepath.Join(dir, m))
		require.NoError(t, rerr)
		_, err = pool.Exec(ctx, string(up))
		require.NoError(t, err, "migration %s", m)
	}
	return pool
}

func ga05dlqMigrationsDir() string {
	wd, _ := os.Getwd()
	for {
		if _, err := os.Stat(filepath.Join(wd, "migrations", "000001_initial_schema.up.sql")); err == nil {
			return filepath.Join(wd, "migrations")
		}
		parent := filepath.Dir(wd)
		if parent == wd {
			break
		}
		wd = parent
	}
	return "migrations"
}

func ga05dlqRigFor(t *testing.T, tenant, taskID string) (*ga05dlqRig, *ga05dlqQueue) {
	t.Helper()
	pool := ga05dlqOpenDB(t)
	queue := &ga05dlqQueue{}

	taskRepo := postgres.NewTaskRepository(pool)
	attemptRepo := postgres.NewTaskAttemptRepository(pool)
	mailboxRepo := postgres.NewMailboxRepository(pool)
	outboxRepo := postgres.NewOutboxRepo(pool)
	budgetRepo := postgres.NewBudgetRepository(pool)
	policySvc := service.NewPolicyService(postgres.NewPolicyRuleRepository(pool))
	budgetSvc := service.NewBudgetService(budgetRepo)
	lifecycle := service.NewPGLifecycle(pool)

	taskSvc := service.NewTaskService(taskRepo, queue, pool, outboxRepo).
		WithPolicy(policySvc).WithLifecycle(lifecycle).WithAttemptRepo(attemptRepo)
	dispatch := service.NewDispatchService(taskRepo, attemptRepo, mailboxRepo, queue, policySvc, budgetSvc).
		WithTxPath(lifecycle, outboxRepo, service.PGBudgetLedger(postgres.NewBudgetUsageRepo(pool)))
	mbSvc := service.NewMailboxService(mailboxRepo, queue)
	agent := tenant + "-agent"
	mailbox := tenant + "-mb"
	require.NoError(t, mbSvc.Create(context.Background(), core.Mailbox{
		TenantID: tenant, ID: mailbox, AgentID: agent, Status: core.MailboxStatusActive,
		RetryPolicy: core.RetryPolicy{MaxAttempts: 5, BackoffType: "exponential", InitialSeconds: 1, MaxSeconds: 10},
	}))

	// The REAL DLQ entrance: outbox-atomic replay + the task-service gate,
	// wired exactly like cmd/janus-api/main.go.
	dlqSvc := NewDLQServiceAdapter(taskRepo, queue).
		WithOutbox(outboxRepo, pool).
		WithReplayGate(dlqGate{taskSvc.ReplayGate()})

	return &ga05dlqRig{
		pool: pool, taskRepo: taskRepo, dlq: dlqSvc, taskSvc: taskSvc,
		dispatch: dispatch, mbSvc: mbSvc, mailbox: mailbox, tenant: tenant, taskID: taskID,
	}, queue
}

type dlqGate struct{ inner interface{} }

func (g dlqGate) CheckApproval(ctx context.Context, tenantID, taskID string) error {
	type gate interface {
		CheckApproval(ctx context.Context, tenantID, taskID string) error
	}
	return g.inner.(gate).CheckApproval(ctx, tenantID, taskID)
}

func (g dlqGate) CheckPolicy(ctx context.Context, tenantID, sourceAgent, targetType, targetValue string) error {
	type gate interface {
		CheckPolicy(ctx context.Context, tenantID, sourceAgent, targetType, targetValue string) error
	}
	return g.inner.(gate).CheckPolicy(ctx, tenantID, sourceAgent, targetType, targetValue)
}

func (g dlqGate) CheckAdmission(ctx context.Context, tenantID, mailboxID string, envelope core.TaskEnvelope) error {
	type gate interface {
		CheckAdmission(ctx context.Context, tenantID, mailboxID string, envelope core.TaskEnvelope) error
	}
	return g.inner.(gate).CheckAdmission(ctx, tenantID, mailboxID, envelope)
}

func (r *ga05dlqRig) status(t *testing.T) (core.TaskStatus, int) {
	t.Helper()
	var st string
	var rc int
	require.NoError(t, r.pool.QueryRow(context.Background(),
		`SELECT status, replay_count FROM tasks WHERE tenant_id=$1 AND id=$2`, r.tenant, r.taskID).Scan(&st, &rc))
	return core.TaskStatus(st), rc
}

func (r *ga05dlqRig) createQueuedTask(t *testing.T, ctx context.Context) {
	t.Helper()
	require.NoError(t, r.taskRepo.Create(ctx, core.Task{
		TenantID: r.tenant, ID: r.taskID, SourceAgent: r.tenant + "-agent",
		TargetType: core.TargetTypeMailbox, TargetValue: r.mailbox,
		MailboxID: r.mailbox,
		Status:    core.TaskStatusQueued, Priority: core.PriorityNormal,
		Envelope: core.TaskEnvelope{
			JanusVersion: "1.0",
			TaskID:       r.taskID, TenantID: r.tenant, SourceAgent: r.tenant + "-agent",
			Target:       core.Target{Type: core.TargetTypeMailbox, Value: r.mailbox},
			Payload:      core.Payload{Type: "ga05-t2b", Content: "dlq entrance"},
			Trace:        core.TraceContext{TraceID: "ga05-t2b-trace"},
		},
	}))
}

// GA-05 T2b: two dead-letter/replay rounds through the REAL DLQ adapter
// entrance (DLQServiceAdapter.ReplayDLQ with the outbox-atomic path and
// the task-service replay gate — the exact wiring main.go uses), with
// per-round retry and exactly-once ledger assertions.
func TestGA05_T2b_DLQEntranceTwoRounds_PG(t *testing.T) {
	r, _ := ga05dlqRigFor(t, "ga05t2b", "ga05-t2b-task")
	ctx := context.Background()
	r.createQueuedTask(t, ctx)

	// Round 1: dead-letter via non-retriable NACK.
	ga05dlqSetRunning(t, r, ctx)
	require.NoError(t, r.dispatch.NackTask(ctx, r.tenant, r.taskID, r.currentLease, false,
		&core.TaskError{Code: "PERMANENT", Message: "t2b round 1"}))
	st, rc := r.status(t)
	require.Equal(t, core.TaskStatusDeadLettered, st, "round 1 must settle dead_lettered")
	require.Equal(t, 0, rc)

	// The entrance under test: QueryDLQ returns the dead-lettered task,
	// ReplayDLQ re-queues it through the outbox-atomic path.
	dlqTasks, err := r.dlq.QueryDLQ(ctx, r.tenant, r.mailbox, 10)
	require.NoError(t, err)
	require.Len(t, dlqTasks, 1, "the dead-lettered task must be queryable via the entrance")
	require.Equal(t, r.taskID, dlqTasks[0].ID)

	_, err = r.dlq.ReplayDLQ(ctx, r.tenant, r.taskID)
	require.NoError(t, err, "dlq replay 1 via the real entrance")
	st, rc = r.status(t)
	require.Equal(t, core.TaskStatusQueued, st)
	require.Equal(t, 1, rc, "generation advances through the entrance")

	// The entrance's atomic path must have written a task_publish outbox row.
	var pubRows int
	require.NoError(t, r.pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox_events WHERE tenant_id=$1 AND kind='task_publish' AND dedupe_key LIKE '%replay%'`,
		r.tenant).Scan(&pubRows))
	require.Greater(t, pubRows, 0, "the entrance's replay must write a task_publish outbox row")

	// Round 2: dead-letter again, replay again — generation 2.
	ga05dlqSetRunning(t, r, ctx)
	require.NoError(t, r.dispatch.NackTask(ctx, r.tenant, r.taskID, r.currentLease, false,
		&core.TaskError{Code: "PERMANENT", Message: "t2b round 2"}))
	st, _ = r.status(t)
	require.Equal(t, core.TaskStatusDeadLettered, st, "round 2 must settle dead_lettered again")

	_, err = r.dlq.ReplayDLQ(ctx, r.tenant, r.taskID)
	require.NoError(t, err, "dlq replay 2 via the real entrance")
	st, rc = r.status(t)
	require.Equal(t, core.TaskStatusQueued, st)
	require.Equal(t, 2, rc, "second entrance replay advances the generation again")

	// Round 3 settles via the task service ack path; exactly-once ledger.
	ga05dlqSetRunning(t, r, ctx)
	require.NoError(t, r.dispatch.AckTask(ctx, r.tenant, r.taskID, r.currentLease, "s3://ga05/t2b", nil))
	st, _ = r.status(t)
	require.Equal(t, core.TaskStatusCompleted, st)

	var ledgerRows, ledgerAttempt int
	require.NoError(t, r.pool.QueryRow(ctx,
		`SELECT count(*), coalesce(min(attempt),0) FROM budget_usage_ledger WHERE tenant_id=$1 AND task_id=$2`,
		r.tenant, r.taskID).Scan(&ledgerRows, &ledgerAttempt))
	require.Equal(t, 2, ledgerRows, "ledger settles exactly once (two scope rows)")
	require.GreaterOrEqual(t, ledgerAttempt, 1, "the ledger's attempt is the settling attempt")

	// Discard on a nonexistent task must error (fail-closed).
	require.Error(t, r.dlq.DiscardDLQ(ctx, r.tenant, "ga05-nonexistent"),
		"discarding a nonexistent task must error")
}

func ga05dlqSetRunning(t *testing.T, r *ga05dlqRig, ctx context.Context) {
	ga05dlqSetRunningAttempt(t, r, ctx, time.Now().UnixNano())
}

func ga05dlqSetRunningAttempt(t *testing.T, r *ga05dlqRig, ctx context.Context, leaseSeq int64) {
	t.Helper()
	leaseID := fmt.Sprintf("ga05dlq-lease-%d", leaseSeq)
	_, err := r.pool.Exec(ctx,
		`INSERT INTO task_attempts (tenant_id, task_id, attempt, agent_id, lease_id, status, started_at, delivery_ref)
		VALUES ($1, $2, (SELECT coalesce(max(attempt),0)+1 FROM task_attempts WHERE tenant_id=$1 AND task_id=$2), $3, $4, 'running', now(), $5)
		ON CONFLICT DO NOTHING`,
		r.tenant, r.taskID, r.tenant+"-agent", leaseID, leaseID)
	require.NoError(t, err)
	_, err = r.pool.Exec(ctx,
		`UPDATE tasks SET status='running' WHERE tenant_id=$1 AND id=$2`, r.tenant, r.taskID)
	require.NoError(t, err)
	// Stash the lease for the caller to use in Nack/Ack.
	r.currentLease = leaseID
}
