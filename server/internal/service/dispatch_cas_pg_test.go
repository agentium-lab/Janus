package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/agentium-lab/Janus/core"
	"github.com/stretchr/testify/require"
)

// GA-04 (c): the three CAS-miss tests previously pre-set the task to
// cancelled, so the CanTransition guard rejected before the CAS-miss branch
// ever ran. The reworked tests force the CAS-miss branch itself:
//   - NACK retry/DLQ: a stale pool-side task read sees 'running'; an
//     in-window task-row-only writer commits 'blocked' (non-terminal,
//     passes the guard on the stale status). The writer must NOT finish
//     the attempt (TaskService.Block would, tripping the attempt CAS first
//     and degrading the nack to the duplicate-NACK no-op) — the task CAS
//     branch is only reachable while the attempt is still open.
//   - ACK: AckTaskAs re-reads under FOR UPDATE, so a real concurrent swap
//     is impossible; the wrapper synthesizes the exact row state the
//     defensive CAS branch guards against inside the same transaction.

// TestACK_TaskCASFailed_RollsBackAttempt_PG: a stale ACK whose task CAS
// expected 'claimed' but the row is 'blocked' (non-terminal, passes the
// CanTransition guard on the stale read). The CAS misses and the whole
// transaction rolls back: the attempt is not finished, no completed event
// is published, the ledger is not settled, completed_at stays NULL.
func TestACK_TaskCASFailed_RollsBackAttempt_PG(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	taskID := fmt.Sprintf("ack-task-%d", time.Now().UnixNano())
	createTestTask(t, env, taskID)
	_, err := env.pool.Exec(ctx,
		`UPDATE tasks SET status='claimed' WHERE tenant_id='acme' AND id=$1`, taskID)
	require.NoError(t, err)
	_, err = env.pool.Exec(ctx,
		`INSERT INTO task_attempts (tenant_id, task_id, attempt, agent_id, lease_id, delivery_ref, status)
		 VALUES ('acme', $1, 1, 'agent-1', 'lease-1', 'delivery-ref-1', 'claimed')`, taskID)
	require.NoError(t, err)

	// GA-04 (c): synthesize the CAS-miss condition on the task row inside
	// the same transaction as the ACK. SELECT ... FOR UPDATE makes a real
	// concurrent swap impossible; the wrapper pins the defensive branch.
	env.dispatch.taskTx = swapAfterLockTx{TaskTxRepo: env.dispatch.taskTx}

	err = env.dispatch.AckTask(ctx, "acme", taskID, "lease-1", "result-1", nil)
	require.Error(t, err)
	require.EqualError(t, err, "task CAS failed: concurrent modification detected")

	var attemptStatus string
	var finishedAt *time.Time
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT status, finished_at FROM task_attempts WHERE tenant_id='acme' AND task_id=$1 AND attempt=1`, taskID).
		Scan(&attemptStatus, &finishedAt))
	require.Equal(t, "claimed", attemptStatus, "attempt must stay claimed (tx rolled back)")
	require.Nil(t, finishedAt)

	var taskStatus string
	var completedAt *time.Time
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT status, completed_at FROM tasks WHERE tenant_id='acme' AND id=$1`, taskID).
		Scan(&taskStatus, &completedAt))
	require.Equal(t, string(core.TaskStatusClaimed), taskStatus,
		"task is back to claimed: the wrapper's in-tx swap rolled back with the tx")
	require.Nil(t, completedAt)

	require.Zero(t, countOutboxEvents(t, env, taskID, core.EventTaskCompleted))
	var ledgerRows int
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT count(*) FROM budget_usage_ledger WHERE tenant_id='acme' AND task_id=$1`, taskID).
		Scan(&ledgerRows))
	require.Zero(t, ledgerRows)
}

// TestNACK_Retry_TaskCASFailed_RollsBackAttempt_PG: a stale NACK retry
// whose pool-side task read saw 'running'; an in-window task-row-only
// writer commits 'blocked' (non-terminal, passes the guard on the stale
// status); the CAS expected 'running' vs actual 'blocked' misses and the
// retry transaction rolls back.
func TestNACK_Retry_TaskCASFailed_RollsBackAttempt_PG(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	taskID := fmt.Sprintf("nack-retry-task-%d", time.Now().UnixNano())
	createTestTask(t, env, taskID)
	_, err := env.pool.Exec(ctx,
		`UPDATE tasks SET status='running' WHERE tenant_id='acme' AND id=$1`, taskID)
	require.NoError(t, err)
	_, err = env.pool.Exec(ctx,
		`INSERT INTO task_attempts (tenant_id, task_id, attempt, agent_id, lease_id, delivery_ref, status)
		 VALUES ('acme', $1, 1, 'agent-1', 'lease-1', 'delivery-ref-1', 'running')`, taskID)
	require.NoError(t, err)

	// GA-04 (c): the stale read is gated so the in-window writer commits
	// before the nack transaction opens. The CAS then sees the mismatched
	// 'blocked' status.
	gate := &staleTaskReadGate{entered: make(chan struct{}), release: make(chan struct{})}
	gate.TaskRepository = env.taskRepo
	env.dispatch.taskRepo = gate

	nackErr := make(chan error, 1)
	go func() {
		nackErr <- env.dispatch.NackTask(ctx, "acme", taskID, "lease-1", true,
			&core.TaskError{Code: "ERR", Message: "test error"})
	}()

	<-gate.entered
	// GA-04 (c): in-window writer — a task-row-only CAS running -> blocked
	// (non-terminal, mismatches the stale read). It must leave the attempt
	// open: a writer that also finishes the attempt (Block/Cancel) trips the
	// attempt CAS first and this branch never runs.
	moved, merr := env.taskRepo.UpdateStatusWithCheck(ctx, "acme", taskID,
		core.TaskStatusRunning, core.TaskStatusBlocked, 0)
	require.NoError(t, merr)
	require.True(t, moved, "in-window writer committed blocked")
	close(gate.release)

	err = <-nackErr
	require.Error(t, err)
	require.EqualError(t, err, "task CAS failed: concurrent modification detected")

	var attemptStatus string
	var finishedAt *time.Time
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT status, finished_at FROM task_attempts WHERE tenant_id='acme' AND task_id=$1 AND attempt=1`, taskID).
		Scan(&attemptStatus, &finishedAt))
	require.Equal(t, "running", attemptStatus, "attempt must stay running (tx rolled back)")
	require.Nil(t, finishedAt)

	var taskStatus string
	var retryAt *time.Time
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT status, retry_at FROM tasks WHERE tenant_id='acme' AND id=$1`, taskID).
		Scan(&taskStatus, &retryAt))
	require.Equal(t, string(core.TaskStatusBlocked), taskStatus,
		"task is blocked (the in-window writer committed)")
	require.Nil(t, retryAt)

	require.Zero(t, countOutboxEvents(t, env, taskID, core.EventTaskRetryScheduled))
}

// TestNACK_DeadLetter_TaskCASFailed_RollsBackAttempt_PG: same stale-NACK
// shape as the retry variant, but the dead-letter branch is taken
// (retriable=false) and the in-window task-row-only writer commits
// 'blocked'. The CAS expected 'running' vs actual 'blocked' misses and the
// dead-letter transaction rolls back: no dlq_publish row, no
// dead_lettered event, attempt stays running, task stays blocked.
func TestNACK_DeadLetter_TaskCASFailed_RollsBackAttempt_PG(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	taskID := fmt.Sprintf("nack-dlq-task-%d", time.Now().UnixNano())
	createTestTask(t, env, taskID)
	_, err := env.pool.Exec(ctx,
		`UPDATE tasks SET status='running' WHERE tenant_id='acme' AND id=$1`, taskID)
	require.NoError(t, err)
	_, err = env.pool.Exec(ctx,
		`INSERT INTO task_attempts (tenant_id, task_id, attempt, agent_id, lease_id, delivery_ref, status)
		 VALUES ('acme', $1, 1, 'agent-1', 'lease-1', 'delivery-ref-1', 'running')`, taskID)
	require.NoError(t, err)

	gate := &staleTaskReadGate{entered: make(chan struct{}), release: make(chan struct{})}
	gate.TaskRepository = env.taskRepo
	env.dispatch.taskRepo = gate

	nackErr := make(chan error, 1)
	go func() {
		nackErr <- env.dispatch.NackTask(ctx, "acme", taskID, "lease-1", false,
			&core.TaskError{Code: "ERR", Message: "test error"})
	}()

	<-gate.entered
	// GA-04 (c): same task-row-only in-window writer as the retry variant —
	// running -> blocked, attempt left open so the task CAS branch runs.
	moved, merr := env.taskRepo.UpdateStatusWithCheck(ctx, "acme", taskID,
		core.TaskStatusRunning, core.TaskStatusBlocked, 0)
	require.NoError(t, merr)
	require.True(t, moved, "in-window writer committed blocked")
	close(gate.release)

	err = <-nackErr
	require.Error(t, err)
	require.EqualError(t, err, "task CAS failed: concurrent modification detected")

	var attemptStatus string
	var finishedAt *time.Time
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT status, finished_at FROM task_attempts WHERE tenant_id='acme' AND task_id=$1 AND attempt=1`, taskID).
		Scan(&attemptStatus, &finishedAt))
	require.Equal(t, "running", attemptStatus, "attempt must stay running (tx rolled back)")
	require.Nil(t, finishedAt)

	var taskStatus string
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT status FROM tasks WHERE tenant_id='acme' AND id=$1`, taskID).
		Scan(&taskStatus))
	require.Equal(t, string(core.TaskStatusBlocked), taskStatus)

	require.Zero(t, countOutboxEvents(t, env, taskID, core.EventTaskDeadLettered))
	require.Zero(t, countOutboxPublishes(t, env, taskID, "dlq_publish"))
}
