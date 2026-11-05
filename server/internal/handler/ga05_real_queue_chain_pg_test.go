package handler

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/driver/postgres"
	"github.com/agentium-lab/Janus/server/internal/driver/pgqueue"
	"github.com/agentium-lab/Janus/server/internal/service"
)

// GA-05 T2c: the full DLQ recovery chain on the REAL pgqueue driver — no
// fake FetchTasks, no SQL-constructed running/attempt state. Every claim
// goes through pgqueue.Driver.FetchTasks (the FOR UPDATE SKIP LOCKED claim
// query), every delivery side effect through the driver's receipt-guarded
// Ack/Nack, and the ledger settles exactly once at the end:
//
//	pull (real claim) -> non-retriable NACK -> dead_lettered
//	-> ReplayDLQ (real entrance) -> retriable NACK round -> retry_scheduled
//	-> promotion -> pull (real claim, new round) -> start -> ack -> completed
func TestGA05_T2c_RealQueueReplayDeliveryChain_PG(t *testing.T) {
	pool := ga05dlqOpenDB(t)
	queue := pgqueue.NewDriver(pool)

	taskRepo := postgres.NewTaskRepository(pool)
	attemptRepo := postgres.NewTaskAttemptRepository(pool)
	mailboxRepo := postgres.NewMailboxRepository(pool)
	outboxRepo := postgres.NewOutboxRepo(pool)
	policySvc := service.NewPolicyService(postgres.NewPolicyRuleRepository(pool))
	budgetSvc := service.NewBudgetService(postgres.NewBudgetRepository(pool))
	lifecycle := service.NewPGLifecycle(pool)

	taskSvc := service.NewTaskService(taskRepo, queue, pool, outboxRepo).
		WithPolicy(policySvc).WithLifecycle(lifecycle).WithAttemptRepo(attemptRepo)
	dispatch := service.NewDispatchService(taskRepo, attemptRepo, mailboxRepo, queue, policySvc, budgetSvc).
		WithTxPath(lifecycle, outboxRepo, service.PGBudgetLedger(postgres.NewBudgetUsageRepo(pool)))
	mbSvc := service.NewMailboxService(mailboxRepo, queue)
	dlqSvc := NewDLQServiceAdapter(taskRepo, queue).
		WithOutbox(outboxRepo, pool).
		WithReplayGate(dlqGate{taskSvc.ReplayGate()})

	const tenant = "ga05t2c"
	const agent = tenant + "-agent"
	const mailbox = tenant + "-mb"
	const taskID = "ga05-t2c-task"
	ctx := context.Background()

	require.NoError(t, mbSvc.Create(ctx, core.Mailbox{
		TenantID: tenant, ID: mailbox, AgentID: agent, Status: core.MailboxStatusActive,
		RetryPolicy: core.RetryPolicy{MaxAttempts: 5, BackoffType: "exponential", InitialSeconds: 1, MaxSeconds: 10},
	}))
	require.NoError(t, taskRepo.Create(ctx, core.Task{
		TenantID: tenant, ID: taskID, SourceAgent: agent,
		TargetType: core.TargetTypeMailbox, TargetValue: mailbox, MailboxID: mailbox,
		Status: core.TaskStatusQueued, Priority: core.PriorityNormal,
		Envelope: core.TaskEnvelope{
			JanusVersion: "1.0", TaskID: taskID, TenantID: tenant, SourceAgent: agent,
			Target: core.Target{Type: core.TargetTypeMailbox, Value: mailbox},
			Payload: core.Payload{Type: "ga05-t2c", Content: "real queue chain"},
			Trace:   core.TraceContext{TraceID: "ga05-t2c-trace"},
		},
	}))

	status := func() core.TaskStatus {
		var st string
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT status FROM tasks WHERE tenant_id=$1 AND id=$2`, tenant, taskID).Scan(&st))
		return core.TaskStatus(st)
	}
	var _ *pgxpool.Pool = pool // keeps the pool import if constructors change

	// Round 1: real pull claims the task, non-retriable NACK dead-letters it.
	res1, err := dispatch.PullTask(ctx, tenant, mailbox, agent)
	require.NoError(t, err)
	require.NotNil(t, res1, "real queue must deliver the queued task")
	require.Equal(t, taskID, res1.Task.ID)
	require.NotEmpty(t, res1.LeaseID, "the real claim mints a lease")
	require.NoError(t, dispatch.StartTaskAs(ctx, tenant, taskID, res1.LeaseID, agent))
	require.Equal(t, core.TaskStatusRunning, status())
	require.NoError(t, dispatch.NackTaskAs(ctx, tenant, taskID, res1.LeaseID, agent, false,
		&core.TaskError{Code: "PERMANENT", Message: "t2c round 1"}))
	require.Equal(t, core.TaskStatusDeadLettered, status())

	// The real entrance replays it: queued again, generation 1, outbox row.
	_, err = dlqSvc.ReplayDLQ(ctx, tenant, taskID)
	require.NoError(t, err, "replay via the real DLQ entrance")
	require.Equal(t, core.TaskStatusQueued, status())
	var rc int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT replay_count FROM tasks WHERE tenant_id=$1 AND id=$2`, tenant, taskID).Scan(&rc))
	require.Equal(t, 1, rc)

	// Round 2: real pull delivers the replayed round; a RETRIABLE nack moves
	// it to retry_scheduled with a future retry_at (the backoff the retry
	// scheduler honors).
	res2, err := dispatch.PullTask(ctx, tenant, mailbox, agent)
	require.NoError(t, err)
	require.NotNil(t, res2, "the replayed round must be deliverable through the real queue")
	require.NoError(t, dispatch.StartTaskAs(ctx, tenant, taskID, res2.LeaseID, agent))
	require.NoError(t, dispatch.NackTaskAs(ctx, tenant, taskID, res2.LeaseID, agent, true,
		&core.TaskError{Code: "TRANSIENT", Message: "t2c retry round"}))
	require.Equal(t, core.TaskStatusRetryScheduled, status())

	// Promotion: apply the retry scanner's own transition once the backoff
	// window has elapsed (the scheduler loop itself is exercised in the
	// retry package's tests; this test targets the delivery chain).
	time.Sleep(1200 * time.Millisecond)
	_, err = pool.Exec(ctx,
		`UPDATE tasks SET status='queued', updated_at=now()
		 WHERE tenant_id=$1 AND id=$2 AND status='retry_scheduled' AND retry_at <= now()`,
		tenant, taskID)
	require.NoError(t, err)
	require.Equal(t, core.TaskStatusQueued, status(), "the promoted retry must be pullable again")

	// Round 3: real pull of the retried round, then a SECOND full DLQ
	// round on the real chain: non-retriable NACK -> dead_lettered again
	// -> ReplayDLQ #2 -> final delivery settles.
	res3, err := dispatch.PullTask(ctx, tenant, mailbox, agent)
	require.NoError(t, err)
	require.NotNil(t, res3, "the promoted retry must deliver")
	require.NoError(t, dispatch.StartTaskAs(ctx, tenant, taskID, res3.LeaseID, agent))
	require.NoError(t, dispatch.NackTaskAs(ctx, tenant, taskID, res3.LeaseID, agent, false,
		&core.TaskError{Code: "PERMANENT", Message: "t2c round 3"}))
	require.Equal(t, core.TaskStatusDeadLettered, status())
	_, err = dlqSvc.ReplayDLQ(ctx, tenant, taskID)
	require.NoError(t, err, "second replay via the real DLQ entrance")
	require.Equal(t, core.TaskStatusQueued, status())
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT replay_count FROM tasks WHERE tenant_id=$1 AND id=$2`, tenant, taskID).Scan(&rc))
	require.Equal(t, 2, rc, "second entrance replay advances the generation to 2")

	// Round 4: the second replay's delivery settles the chain.
	res4, err := dispatch.PullTask(ctx, tenant, mailbox, agent)
	require.NoError(t, err)
	require.NotNil(t, res4, "the second replay round must deliver")
	require.NoError(t, dispatch.StartTaskAs(ctx, tenant, taskID, res4.LeaseID, agent))
	require.NoError(t, dispatch.AckTaskAs(ctx, tenant, taskID, res4.LeaseID, agent, "s3://ga05/t2c", nil))
	require.Equal(t, core.TaskStatusCompleted, status())

	// The queue lease is cleared by the receipt-guarded ack of the FINAL round.
	var leaseUntil *time.Time
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT queue_lease_until FROM tasks WHERE tenant_id=$1 AND id=$2`, tenant, taskID).Scan(&leaseUntil))
	require.Nil(t, leaseUntil, "the settling round's ack must clear the queue lease")

	// Attempt history: each Replay reset deleted prior attempts in the same
	// tx as the counter reset (ResetForReplay), so only the post-replay-2
	// rounds' rows remain.
	var attempts int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM task_attempts WHERE tenant_id=$1 AND task_id=$2`, tenant, taskID).Scan(&attempts))
	require.Equal(t, 1, attempts)

	// Ledger settles EXACTLY once for the whole chain: replay #2 reset the
	// ledger with the counters, and only the final ack re-settles it.
	var ledgerRows int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM budget_usage_ledger WHERE tenant_id=$1 AND task_id=$2`, tenant, taskID).Scan(&ledgerRows))
	require.Equal(t, 2, ledgerRows, "two scope rows, settled once after the final round")
}
