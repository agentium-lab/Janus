package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/agentium-lab/Janus/core"
	"github.com/stretchr/testify/require"
)

// TestACK_TaskCASFailed_RollsBackAttempt_PG verifies that when ACK's task CAS fails,
// the entire transaction rolls back (attempt is NOT closed).
func TestACK_TaskCASFailed_RollsBackAttempt_PG(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	// Setup: task in claimed state with active attempt
	taskID := fmt.Sprintf("ack-task-%d", time.Now().UnixNano())
	createTestTask(t, env, taskID)
	_, err := env.pool.Exec(ctx,
		`UPDATE tasks SET status='claimed' WHERE tenant_id='acme' AND id=$1`, taskID)
	require.NoError(t, err)

	_, err = env.pool.Exec(ctx,
		`INSERT INTO task_attempts (tenant_id, task_id, attempt, agent_id, lease_id, status, delivery_ref)
		 VALUES ('acme', $1, 1, 'agent-1', 'lease-1', 'claimed', 'delivery-ref-1')`, taskID)
	require.NoError(t, err)

	// Simulate concurrent modification: task status changes to cancelled before ACK
	// Cancelled is terminal, so the CanTransition check will fail
	_, err = env.pool.Exec(ctx,
		`UPDATE tasks SET status='cancelled', updated_at=now() WHERE tenant_id='acme' AND id=$1`, taskID)
	require.NoError(t, err)

	// Execute ACK - should fail because task is now cancelled (terminal)
	err = env.dispatch.AckTask(ctx, "acme", taskID, "lease-1", "result-1", nil)

	// Verify: transition failure should return error
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid state transition")

	// Verify: attempt should NOT be closed (transaction rolled back)
	var attemptStatus string
	err = env.pool.QueryRow(ctx,
		`SELECT status FROM task_attempts WHERE tenant_id='acme' AND task_id=$1 AND attempt=1`, taskID).Scan(&attemptStatus)
	require.NoError(t, err)
	require.Equal(t, "claimed", attemptStatus, "attempt should still be claimed (rolled back)")

	// Verify: task status should be cancelled (concurrent modification)
	var taskStatus string
	err = env.pool.QueryRow(ctx,
		`SELECT status FROM tasks WHERE tenant_id='acme' AND id=$1`, taskID).Scan(&taskStatus)
	require.NoError(t, err)
	require.Equal(t, string(core.TaskStatusCancelled), taskStatus, "task should be cancelled (concurrent modification)")
}

// TestNACK_Retry_TaskCASFailed_RollsBackAttempt_PG verifies that when NACK's task CAS fails
// during retry path, the entire transaction rolls back.
func TestNACK_Retry_TaskCASFailed_RollsBackAttempt_PG(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	// Setup: task in running state with active attempt
	taskID := fmt.Sprintf("nack-retry-task-%d", time.Now().UnixNano())
	createTestTask(t, env, taskID)
	_, err := env.pool.Exec(ctx,
		`UPDATE tasks SET status='running' WHERE tenant_id='acme' AND id=$1`, taskID)
	require.NoError(t, err)

	_, err = env.pool.Exec(ctx,
		`INSERT INTO task_attempts (tenant_id, task_id, attempt, agent_id, lease_id, status, delivery_ref)
		 VALUES ('acme', $1, 1, 'agent-1', 'lease-1', 'running', 'delivery-ref-1')`, taskID)
	require.NoError(t, err)

	// Simulate concurrent modification: task status changes to cancelled before NACK
	// Cancelled is terminal, so the CanTransition check will fail
	_, err = env.pool.Exec(ctx,
		`UPDATE tasks SET status='cancelled', updated_at=now() WHERE tenant_id='acme' AND id=$1`, taskID)
	require.NoError(t, err)

	// Execute NACK with retriable=true - should fail because task is now cancelled
	err = env.dispatch.NackTask(ctx, "acme", taskID, "lease-1", true, &core.TaskError{Code: "ERR", Message: "test error"})

	// Verify: transition failure should return error
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid state transition")

	// Verify: attempt should NOT be closed (transaction rolled back)
	var attemptStatus string
	err = env.pool.QueryRow(ctx,
		`SELECT status FROM task_attempts WHERE tenant_id='acme' AND task_id=$1 AND attempt=1`, taskID).Scan(&attemptStatus)
	require.NoError(t, err)
	require.Equal(t, "running", attemptStatus, "attempt should still be running (rolled back)")
}

// TestNACK_DeadLetter_TaskCASFailed_RollsBackAttempt_PG verifies that when NACK's task CAS fails
// during dead letter path, the entire transaction rolls back.
func TestNACK_DeadLetter_TaskCASFailed_RollsBackAttempt_PG(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	// Setup: task in running state with active attempt
	taskID := fmt.Sprintf("nack-dlq-task-%d", time.Now().UnixNano())
	createTestTask(t, env, taskID)
	_, err := env.pool.Exec(ctx,
		`UPDATE tasks SET status='running' WHERE tenant_id='acme' AND id=$1`, taskID)
	require.NoError(t, err)

	_, err = env.pool.Exec(ctx,
		`INSERT INTO task_attempts (tenant_id, task_id, attempt, agent_id, lease_id, status, delivery_ref)
		 VALUES ('acme', $1, 1, 'agent-1', 'lease-1', 'running', 'delivery-ref-1')`, taskID)
	require.NoError(t, err)

	// Simulate concurrent modification: task status changes to cancelled before NACK
	// Cancelled is terminal, so the CanTransition check will fail
	_, err = env.pool.Exec(ctx,
		`UPDATE tasks SET status='cancelled', updated_at=now() WHERE tenant_id='acme' AND id=$1`, taskID)
	require.NoError(t, err)

	// Execute NACK with retriable=false (dead letter path) - should fail because task is now cancelled
	err = env.dispatch.NackTask(ctx, "acme", taskID, "lease-1", false, &core.TaskError{Code: "ERR", Message: "test error"})

	// Verify: transition failure should return error
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid state transition")

	// Verify: attempt should NOT be closed (transaction rolled back)
	var attemptStatus string
	err = env.pool.QueryRow(ctx,
		`SELECT status FROM task_attempts WHERE tenant_id='acme' AND task_id=$1 AND attempt=1`, taskID).Scan(&attemptStatus)
	require.NoError(t, err)
	require.Equal(t, "running", attemptStatus, "attempt should still be running (rolled back)")
}
