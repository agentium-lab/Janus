package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/agentium-lab/Janus/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConcurrent_StartCancelReplay(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	tenantID := "acme"
	taskID := "concurrent-start-cancel-replay"

	task := core.Task{
		ID:        taskID,
		TenantID:  tenantID,
		Status:    core.TaskStatusClaimed,
		MailboxID: "mb-1",
		Envelope: core.TaskEnvelope{
			TaskID:      taskID,
			TenantID:    tenantID,
			SourceAgent: "agent-1",
			Target:      core.Target{Type: core.TargetTypeAgent, Value: "agent-2"},
		},
	}
	err := env.taskRepo.Create(ctx, task)
	require.NoError(t, err)

	attempt := core.TaskAttempt{
		TenantID: tenantID,
		TaskID:   taskID,
		Attempt:  1,
		AgentID:  "agent-1",
		LeaseID:  "lease-1",
		Status:   "claimed",
	}
	err = env.attemptRepo.Create(ctx, attempt)
	require.NoError(t, err)

	// GA-04 (d): all three operations really race on a single claimed task.
	// The start gun releases them simultaneously; assertions cover the
	// invariant set over whatever interleaving occurs.
	gun := make(chan struct{})
	var wg sync.WaitGroup
	var startErr, cancelErr, replayErr error
	var replayed *core.Task

	wg.Add(3)
	go func() { defer wg.Done(); <-gun; startErr = env.dispatch.StartTask(ctx, tenantID, taskID, "lease-1") }()
	go func() { defer wg.Done(); <-gun; cancelErr = env.taskSvc.Cancel(ctx, tenantID, taskID) }()
	go func() { defer wg.Done(); <-gun; replayed, replayErr = env.taskSvc.Replay(ctx, tenantID, taskID) }()
	close(gun)
	wg.Wait()

	got, gerr := env.taskRepo.Get(ctx, tenantID, taskID)
	require.NoError(t, gerr)

	// Cancel is valid from every non-terminal status this race can pass
	// through, so it must always win its transition.
	require.NoError(t, cancelErr, "cancel must succeed from claimed/running/queued")

	// Exactly one lifecycle outcome wins: the final status (plus replay_count)
	// determines which writes survived.
	switch got.Status {
	case core.TaskStatusCancelled:
		// Replay could not have committed (it requires a terminal task, and
		// a single cancel already produced the terminal state; a second
		// cancel does not run).
		require.Error(t, replayErr, "replay must fail: task was not terminal when it read")
		require.Equal(t, 0, got.ReplayCount)
		require.Equal(t, 0, got.AttemptCount, "attempt_count was never incremented (seeded directly)")
		var attemptStatus string
		var finishedAt *time.Time
		require.NoError(t, env.pool.QueryRow(ctx,
			`SELECT status, finished_at FROM task_attempts WHERE tenant_id='acme' AND task_id=$1 AND attempt=1`,
			taskID).Scan(&attemptStatus, &finishedAt))
		require.Equal(t, "cancelled", attemptStatus, "Cancel closed the open attempt")
		require.NotNil(t, finishedAt)
	case core.TaskStatusQueued:
		// Replay committed last: it required the cancelled state, so the
		// sequence was Cancel -> Replay (Start either lost or committed
		// before cancel and was then cancelled and replayed).
		require.NoError(t, replayErr)
		require.NotNil(t, replayed)
		require.Equal(t, 1, got.ReplayCount)
		require.Equal(t, 0, got.AttemptCount, "replay reset the attempt counter")
		var attempts int
		require.NoError(t, env.pool.QueryRow(ctx,
			`SELECT count(*) FROM task_attempts WHERE tenant_id='acme' AND task_id=$1`,
			taskID).Scan(&attempts))
		require.Zero(t, attempts, "replay cleared all attempts")
	default:
		t.Fatalf("final status %s not in {cancelled, queued}", got.Status)
	}

	// Universal invariants: no orphan attempt/lease; event counts bounded;
	// no double-publish.
	var liveAttempts int
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_attempts WHERE tenant_id='acme' AND task_id=$1 AND status IN ('claimed','running')`,
		taskID).Scan(&liveAttempts))
	require.Zero(t, liveAttempts, "no orphan attempt/lease in any final state")

	cancelledEvents := countOutboxEvents(t, env, taskID, core.EventTaskCancelled)
	require.Equal(t, 1, cancelledEvents, "exactly one task.cancelled event (cancel always wins)")

	startedEvents := countOutboxEvents(t, env, taskID, core.EventTaskStarted)
	require.LessOrEqual(t, startedEvents, 1, "at most one task.started event")
	if startErr == nil {
		require.Equal(t, 1, startedEvents, "start success implies exactly one started event")
	}

	replayPublishes := countOutboxPublishes(t, env, taskID, "task_publish")
	require.LessOrEqual(t, replayPublishes, 1, "replay is deduped; no double-publish")
	if got.Status == core.TaskStatusQueued {
		require.Equal(t, 1, replayPublishes, "replay committed: exactly one task_publish row")
	} else {
		require.Zero(t, replayPublishes, "replay lost: no task_publish row")
	}

	require.Zero(t, countOutboxPublishes(t, env, taskID, "dlq_publish"),
		"no dlq_publish row in any final state of this race")
}

func TestConcurrent_BlockACK(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	tenantID := "acme"
	taskID := "concurrent-block-ack"

	task := core.Task{
		ID:        taskID,
		TenantID:  tenantID,
		Status:    core.TaskStatusRunning,
		MailboxID: "mb-1",
		Envelope: core.TaskEnvelope{
			TaskID:      taskID,
			TenantID:    tenantID,
			SourceAgent: "agent-1",
			Target:      core.Target{Type: core.TargetTypeAgent, Value: "agent-2"},
		},
	}
	err := env.taskRepo.Create(ctx, task)
	require.NoError(t, err)

	attempt := core.TaskAttempt{
		TenantID: tenantID,
		TaskID:   taskID,
		Attempt:  1,
		AgentID:  "agent-1",
		LeaseID:  "lease-1",
		Status:   "running",
	}
	err = env.attemptRepo.Create(ctx, attempt)
	require.NoError(t, err)

	var wg sync.WaitGroup
	var blockErr, ackErr error

	wg.Add(2)
	go func() {
		defer wg.Done()
		blockErr = env.taskSvc.Block(ctx, tenantID, taskID, "test")
	}()

	go func() {
		defer wg.Done()
		ackErr = env.dispatch.AckTask(ctx, tenantID, taskID, "lease-1", "", nil)
	}()

	wg.Wait()

	got, err := env.taskRepo.Get(ctx, tenantID, taskID)
	require.NoError(t, err)

	if blockErr == nil && ackErr == nil {
		if got.Status != core.TaskStatusBlocked && got.Status != core.TaskStatusCompleted {
			t.Errorf("Both succeeded but task is in unexpected state: %s", got.Status)
		}
	} else if blockErr == nil {
		assert.Equal(t, core.TaskStatusBlocked, got.Status)
	} else if ackErr == nil {
		assert.Equal(t, core.TaskStatusCompleted, got.Status)
	}
}
