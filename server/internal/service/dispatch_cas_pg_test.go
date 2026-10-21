package service

import (
	"context"
	"testing"

	"github.com/agentium-lab/Janus/core"
	"github.com/stretchr/testify/require"
)

// TestACK_TaskCASFailed_RollsBackAttempt_PG verifies that when ACK's task CAS fails,
// the entire transaction rolls back (attempt is NOT closed).
func TestACK_TaskCASFailed_RollsBackAttempt_PG(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	// Setup: task in claimed state with active attempt
	createTestTask(t, env, "ack-task")
	_, err := env.pool.Exec(ctx,
		`UPDATE tasks SET status='claimed' WHERE tenant_id='acme' AND id='ack-task'`)
	require.NoError(t, err)

	_, err = env.pool.Exec(ctx,
		`INSERT INTO task_attempts (tenant_id, task_id, attempt, agent_id, lease_id, status, delivery_ref)
		 VALUES ('acme', 'ack-task', 1, 'agent-1', 'lease-1', 'claimed', 'delivery-ref-1')`)
	require.NoError(t, err)

	// Simulate concurrent modification: task status changes before ACK transaction
	drv := &cancelWinningDriver{}
	drv.onFetch = func() {
		// Change task status to running (simulating concurrent StartTask)
		_, cerr := env.pool.Exec(ctx,
			`UPDATE tasks SET status='running', updated_at=now() WHERE tenant_id='acme' AND id='ack-task'`)
		require.NoError(t, cerr)
	}
	env.dispatch = env.dispatch.WithQueueDriver(drv)

	// Execute ACK
	err = env.dispatch.AckTask(ctx, "acme", "ack-task", "lease-1", "result-1", nil)

	// Verify: CAS failure should return error
	require.Error(t, err)
	require.Contains(t, err.Error(), "task CAS failed")

	// Verify: attempt should NOT be closed (transaction rolled back)
	var attemptStatus string
	err = env.pool.QueryRow(ctx,
		`SELECT status FROM task_attempts WHERE tenant_id='acme' AND task_id='ack-task' AND attempt=1`).Scan(&attemptStatus)
	require.NoError(t, err)
	require.Equal(t, "claimed", attemptStatus, "attempt should still be claimed (rolled back)")

	// Verify: task status should be running (concurrent modification)
	var taskStatus string
	err = env.pool.QueryRow(ctx,
		`SELECT status FROM tasks WHERE tenant_id='acme' AND id='ack-task'`).Scan(&taskStatus)
	require.NoError(t, err)
	require.Equal(t, core.TaskStatusRunning, taskStatus, "task should be running (concurrent modification)")
}

// TestNACK_Retry_TaskCASFailed_RollsBackAttempt_PG verifies that when NACK's task CAS fails
// during retry path, the entire transaction rolls back.
func TestNACK_Retry_TaskCASFailed_RollsBackAttempt_PG(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	// Setup: task in running state with active attempt
	createTestTask(t, env, "nack-retry-task")
	_, err := env.pool.Exec(ctx,
		`UPDATE tasks SET status='running' WHERE tenant_id='acme' AND id='nack-retry-task'`)
	require.NoError(t, err)

	_, err = env.pool.Exec(ctx,
		`INSERT INTO task_attempts (tenant_id, task_id, attempt, agent_id, lease_id, status, delivery_ref)
		 VALUES ('acme', 'nack-retry-task', 1, 'agent-1', 'lease-1', 'running', 'delivery-ref-1')`)
	require.NoError(t, err)

	// Simulate concurrent modification
	drv := &cancelWinningDriver{}
	drv.onFetch = func() {
		_, cerr := env.pool.Exec(ctx,
			`UPDATE tasks SET status='cancelled', updated_at=now() WHERE tenant_id='acme' AND id='nack-retry-task'`)
		require.NoError(t, cerr)
	}
	env.dispatch = env.dispatch.WithQueueDriver(drv)

	// Execute NACK with retriable=true
	err = env.dispatch.NackTask(ctx, "acme", "nack-retry-task", "lease-1", true, &core.TaskError{Code: "ERR", Message: "test error"})

	// Verify: CAS failure should return error
	require.Error(t, err)
	require.Contains(t, err.Error(), "task CAS failed")

	// Verify: attempt should NOT be closed
	var attemptStatus string
	err = env.pool.QueryRow(ctx,
		`SELECT status FROM task_attempts WHERE tenant_id='acme' AND task_id='nack-retry-task' AND attempt=1`).Scan(&attemptStatus)
	require.NoError(t, err)
	require.Equal(t, "running", attemptStatus, "attempt should still be running (rolled back)")
}

// TestNACK_DeadLetter_TaskCASFailed_RollsBackAttempt_PG verifies that when NACK's task CAS fails
// during dead letter path, the entire transaction rolls back.
func TestNACK_DeadLetter_TaskCASFailed_RollsBackAttempt_PG(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	// Setup: task in running state with active attempt
	createTestTask(t, env, "nack-dlq-task")
	_, err := env.pool.Exec(ctx,
		`UPDATE tasks SET status='running' WHERE tenant_id='acme' AND id='nack-dlq-task'`)
	require.NoError(t, err)

	_, err = env.pool.Exec(ctx,
		`INSERT INTO task_attempts (tenant_id, task_id, attempt, agent_id, lease_id, status, delivery_ref)
		 VALUES ('acme', 'nack-dlq-task', 1, 'agent-1', 'lease-1', 'running', 'delivery-ref-1')`)
	require.NoError(t, err)

	// Simulate concurrent modification
	drv := &cancelWinningDriver{}
	drv.onFetch = func() {
		_, cerr := env.pool.Exec(ctx,
			`UPDATE tasks SET status='cancelled', updated_at=now() WHERE tenant_id='acme' AND id='nack-dlq-task'`)
		require.NoError(t, cerr)
	}
	env.dispatch = env.dispatch.WithQueueDriver(drv)

	// Execute NACK with retriable=false (dead letter path)
	err = env.dispatch.NackTask(ctx, "acme", "nack-dlq-task", "lease-1", false, &core.TaskError{Code: "ERR", Message: "test error"})

	// Verify: CAS failure should return error
	require.Error(t, err)
	require.Contains(t, err.Error(), "task CAS failed")

	// Verify: attempt should NOT be closed
	var attemptStatus string
	err = env.pool.QueryRow(ctx,
		`SELECT status FROM task_attempts WHERE tenant_id='acme' AND task_id='nack-dlq-task' AND attempt=1`).Scan(&attemptStatus)
	require.NoError(t, err)
	require.Equal(t, "running", attemptStatus, "attempt should still be running (rolled back)")
}
