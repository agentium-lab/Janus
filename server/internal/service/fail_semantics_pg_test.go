package service

// Fail semantics regression suite — real PostgreSQL.
//
// Contract under test (see the Fail doc-comment in task_service.go): Fail is
// the worker's explicit unrecoverable declaration. The task ends up `failed`
// (terminal), the open attempt and its lease are closed in the same
// transaction, no automatic retry ever follows (that is Nack / lease-expiry
// business), and the task.failed event carrying the marshalled TaskError is
// durable in outbox_events. These tests assert real SQL state and real
// scheduler behavior — never mock-call assertions.

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/retry"
)

// startRunningTaskWithLease drives a queued task into running with a live
// claimed attempt — the exact state a worker holds when it declares Fail.
func startRunningTaskWithLease(t *testing.T, env *serviceTestEnv, taskID, leaseID string) {
	t.Helper()
	ctx := context.Background()
	task := createTestTask(t, env, taskID)
	require.NoError(t, env.attemptRepo.Create(ctx, core.TaskAttempt{
		TenantID: "acme", TaskID: task.ID, Attempt: 1, AgentID: "agent-1",
		LeaseID: leaseID, Status: "claimed", StartedAt: time.Now(),
		DeliveryRef: "delivery-" + taskID,
	}))
	_, err := env.taskRepo.UpdateStatusWithCheck(ctx, "acme", task.ID, core.TaskStatusQueued, core.TaskStatusClaimed, 1)
	require.NoError(t, err, "claim task")
	require.NoError(t, env.dispatch.StartTask(ctx, "acme", task.ID, leaseID), "start task")
}

// (a) Fail closes the open attempt with status "failed" in the same
// transaction and kills its lease: a heartbeat on the old lease id fails,
// and no claimed/running attempt row survives.
func TestFail_PG_ClosesAttemptAndKillsLease(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	const taskID, leaseID = "task-fail-a-1", "lease-fail-a-1"
	startRunningTaskWithLease(t, env, taskID, leaseID)

	// Control: the lease was live before Fail.
	require.NoError(t, env.dispatch.TaskHeartbeat(ctx, "acme", taskID, leaseID),
		"heartbeat must succeed on the live lease before Fail")

	require.NoError(t, env.taskSvc.Fail(ctx, "acme", taskID, &core.TaskError{
		Code: "UNRECOVERABLE", Message: "worker gave up",
	}))

	var attemptStatus string
	var finishedAt *time.Time
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT status, finished_at FROM task_attempts
		 WHERE tenant_id = $1 AND task_id = $2
		 ORDER BY attempt DESC LIMIT 1`,
		"acme", taskID).Scan(&attemptStatus, &finishedAt))
	assert.Equal(t, "failed", attemptStatus, "Fail must close the open attempt with status failed")
	require.NotNil(t, finishedAt, "Fail must stamp finished_at on the attempt")

	err := env.dispatch.TaskHeartbeat(ctx, "acme", taskID, leaseID)
	require.Error(t, err, "heartbeat on the lease closed by Fail must fail")
	assert.Contains(t, err.Error(), "no longer active")

	var live int
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_attempts
		 WHERE tenant_id = $1 AND task_id = $2 AND status IN ('claimed', 'running')`,
		"acme", taskID).Scan(&live))
	assert.Zero(t, live, "no claimed/running attempt row may survive Fail")

	got, gerr := env.taskRepo.Get(ctx, "acme", taskID)
	require.NoError(t, gerr)
	assert.Equal(t, core.TaskStatusFailed, got.Status)
}

// (e) Fail is terminal against broker redelivery too: a stale delivery for
// an explicitly failed task must be ACKed and dropped, never re-claimed.
// Pre-fix: the PullTask reconciliation switch fell through for `failed`,
// minting a new attempt and CAS-ing the task failed -> claimed off a plain
// redelivery — resurrecting work the worker declared unrecoverable.
func TestFail_PG_RedeliveryDoesNotResurrect(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	const taskID, leaseID = "task-fail-e-1", "lease-fail-e-1"
	startRunningTaskWithLease(t, env, taskID, leaseID)

	require.NoError(t, env.taskSvc.Fail(ctx, "acme", taskID, &core.TaskError{
		Code: "UNRECOVERABLE", Message: "worker gave up",
	}))

	// The broker redelivers the original (never-ACKed) delivery.
	env.dispatch = env.dispatch.WithQueueDriver(&perCallDriver{queue: []core.TaskDelivery{
		{TaskID: taskID, DeliveryRef: core.DeliveryRef("delivery-" + taskID), Payload: []byte(`{}`)},
	}})
	res, perr := env.dispatch.PullTask(ctx, "acme", "mb-1", "agent-1")
	require.NoError(t, perr)
	assert.Nil(t, res, "a redelivery for a failed task must not be claimable")

	got, gerr := env.taskRepo.Get(ctx, "acme", taskID)
	require.NoError(t, gerr)
	assert.Equal(t, core.TaskStatusFailed, got.Status, "redelivery must not resurrect a failed task")

	var attempts int
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_attempts WHERE tenant_id = $1 AND task_id = $2`,
		"acme", taskID).Scan(&attempts))
	assert.Equal(t, 1, attempts, "no new attempt may be minted for a failed task's redelivery")
}

// (b) The retry Scheduler never requeues an explicitly failed task. A ready
// retry_scheduled CONTROL task proves a full processReadyRetries pass ran
// after the Fail — the subject assertions are not timing luck.
func TestFail_PG_RetrySchedulerNeverRequeues(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	const taskID, leaseID = "task-fail-sched-1", "lease-fail-sched-1"
	startRunningTaskWithLease(t, env, taskID, leaseID)
	require.NoError(t, env.taskSvc.Fail(ctx, "acme", taskID, &core.TaskError{
		Code: "UNRECOVERABLE", Message: "worker gave up",
	}))

	// Control: a genuinely retry-scheduled task whose retry_at is due. The
	// scheduler pass that promotes it is proven to have run after the Fail.
	_, cerr := env.pool.Exec(ctx,
		`INSERT INTO tasks (tenant_id, id, source_agent, target_type, target_value,
		                    mailbox_id, status, priority, envelope, retry_at)
		 VALUES ('acme', 'task-fail-ctl-1', 'agent-1', 'mailbox', 'mb-1', 'mb-1',
		         'retry_scheduled', 'normal',
		         jsonb_build_object('task_id', 'task-fail-ctl-1', 'tenant_id', 'acme',
			                            'source_agent', 'agent-1'),
		         now() - interval '1 second')`)
	require.NoError(t, cerr, "insert control task")

	sched := retry.NewScheduler(env.pool, nil).WithOutbox()
	schedCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(func() {
		cancel()
		sched.Stop()
	})
	go sched.Start(schedCtx, 20*time.Millisecond)

	// Bounded wait for the control's promotion — the observable proof that
	// processReadyRetries executed at least once after the Fail.
	deadline := time.Now().Add(15 * time.Second)
	for {
		var ctlStatus string
		require.NoError(t, env.pool.QueryRow(ctx,
			`SELECT status FROM tasks WHERE tenant_id = 'acme' AND id = 'task-fail-ctl-1'`).
			Scan(&ctlStatus))
		if ctlStatus == string(core.TaskStatusQueued) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("retry scheduler never promoted the ready control task — scheduler pass not proven")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Subject: the failed task was NOT picked up — still failed, retry_at
	// never armed, and no task_publish outbox row (a requeue writes one).
	var status string
	var retryAt *time.Time
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT status, retry_at FROM tasks WHERE tenant_id = 'acme' AND id = $1`,
		taskID).Scan(&status, &retryAt))
	assert.Equal(t, string(core.TaskStatusFailed), status, "failed task must stay failed across scheduler passes")
	assert.Nil(t, retryAt, "Fail must not arm retry_at")

	var republished int
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox_events
		 WHERE tenant_id = 'acme' AND kind = 'task_publish' AND payload->>'TaskID' = $1`,
		taskID).Scan(&republished))
	assert.Zero(t, republished, "the retry scheduler must not re-publish an explicitly failed task")

	var ctlPublished int
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox_events
		 WHERE tenant_id = 'acme' AND kind = 'task_publish' AND payload->>'TaskID' = 'task-fail-ctl-1'`).
		Scan(&ctlPublished))
	assert.GreaterOrEqual(t, ctlPublished, 1, "control task must have been requeued by the scheduler")
}

// (c) Terminal means terminal: a second Fail on the failed task is rejected
// as an invalid transition, Replay cannot resurrect it, and neither rejected
// call leaves a trace (no state change, no error event).
func TestFail_PG_SecondFailAndReplayRejected(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	const taskID, leaseID = "task-fail-c-1", "lease-fail-c-1"
	startRunningTaskWithLease(t, env, taskID, leaseID)
	require.NoError(t, env.taskSvc.Fail(ctx, "acme", taskID, &core.TaskError{
		Code: "UNRECOVERABLE", Message: "first declaration wins",
	}))

	err := env.taskSvc.Fail(ctx, "acme", taskID, &core.TaskError{Code: "LATE", Message: "second call"})
	require.Error(t, err, "a second Fail must be rejected")
	assert.True(t, errors.Is(err, core.ErrInvalidTransition),
		"second Fail must be rejected as an invalid transition, got: %v", err)

	_, rerr := env.taskSvc.Replay(ctx, "acme", taskID)
	require.Error(t, rerr, "Replay must not resurrect an explicitly failed task")
	assert.Contains(t, rerr.Error(), "only terminal tasks can be replayed")

	var status string
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT status FROM tasks WHERE tenant_id = 'acme' AND id = $1`, taskID).Scan(&status))
	assert.Equal(t, string(core.TaskStatusFailed), status, "rejected calls must leave the task failed")

	// The inner payload is base64 []byte on the wire — inspect in Go, rows from SQL.
	lateRows, lqerr := env.pool.Query(ctx,
		`SELECT payload FROM outbox_events
		 WHERE tenant_id = 'acme' AND kind = 'event_publish'
		   AND payload->>'event_type' = 'task.failed'
		   AND payload->>'task_id' = $1`, taskID)
	require.NoError(t, lqerr)
	defer lateRows.Close()
	lateEvents := 0
	for lateRows.Next() {
		var payload []byte
		require.NoError(t, lateRows.Scan(&payload))
		var evt core.JanusEvent
		require.NoError(t, json.Unmarshal(payload, &evt))
		var te core.TaskError
		if json.Unmarshal(evt.Payload, &te) == nil && te.Code == "LATE" {
			lateEvents++
		}
	}
	require.NoError(t, lateRows.Err())
	assert.Zero(t, lateEvents, "the rejected second Fail must not emit its TaskError event")
}

// (d) The task.failed event carrying the marshalled TaskError payload lands
// durably in outbox_events — a SQL row assertion on real PG, not a mock
// call — under the same event id that fans out on the live queue.
func TestFail_PG_TaskErrorEventInOutbox(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	const taskID, leaseID = "task-fail-d-1", "lease-fail-d-1"
	startRunningTaskWithLease(t, env, taskID, leaseID)

	taskErr := &core.TaskError{Code: "UNRECOVERABLE", Message: "bad prompt, worse luck"}
	require.NoError(t, env.taskSvc.Fail(ctx, "acme", taskID, taskErr))

	// The inner payload is []byte on the wire (base64 inside the event JSON),
	// so the TaskError is inspected after unmarshalling — the same decode the
	// audit projector performs. The rows themselves come from real SQL.
	rows, qerr := env.pool.Query(ctx,
		`SELECT payload FROM outbox_events
		 WHERE tenant_id = 'acme' AND kind = 'event_publish'
		   AND payload->>'event_type' = 'task.failed'
		   AND payload->>'task_id' = $1`, taskID)
	require.NoError(t, qerr)
	defer rows.Close()
	var events []core.JanusEvent
	for rows.Next() {
		var payload []byte
		require.NoError(t, rows.Scan(&payload))
		var evt core.JanusEvent
		require.NoError(t, json.Unmarshal(payload, &evt))
		events = append(events, evt)
	}
	require.NoError(t, rows.Err())
	require.NotEmpty(t, events, "Fail must leave at least one durable task.failed outbox row")

	var errorEvents []core.JanusEvent
	lifecycleEvents := 0
	for _, e := range events {
		var te core.TaskError
		if json.Unmarshal(e.Payload, &te) == nil && te.Code == taskErr.Code {
			errorEvents = append(errorEvents, e)
			continue
		}
		var inner map[string]string
		if json.Unmarshal(e.Payload, &inner) == nil && inner["status"] == string(core.TaskStatusFailed) {
			lifecycleEvents++
		}
	}
	require.Len(t, errorEvents, 1, "exactly one durable task.failed event must carry the TaskError payload")

	evt := errorEvents[0]
	assert.Equal(t, core.EventTaskFailed, evt.EventType)
	assert.Equal(t, taskID, evt.TaskID)
	assert.NotEmpty(t, evt.EventID, "the durable error event must carry a stable event_id")
	assert.False(t, evt.Timestamp.IsZero(), "the durable error event must carry a timestamp")
	var gotErr core.TaskError
	require.NoError(t, json.Unmarshal(evt.Payload, &gotErr))
	assert.Equal(t, *taskErr, gotErr, "outbox payload must round-trip the exact TaskError")

	// The lifecycle transition event ({"status":"failed"}) is a separate,
	// coexisting row — pin the two-row shape so the error event is never
	// "deduplicated away" as an apparent duplicate.
	assert.GreaterOrEqual(t, lifecycleEvents, 1, "the transition's task.failed lifecycle event must stay durable")

	// Same identity on the live lane: the fast-path publish carries the
	// same event_id as the durable outbox row.
	foundLive := false
	for _, e := range env.driver.events {
		if e.EventType == core.EventTaskFailed && e.TaskID == taskID && e.EventID == evt.EventID {
			foundLive = true
		}
	}
	assert.True(t, foundLive, "the live-queue task.failed event must share the outbox row's event_id")
}
