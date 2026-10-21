package retry

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/agentium-lab/Janus/core"
	"github.com/stretchr/testify/require"
)

// TestScheduler_ReplayThenRetry_NoConflict_PG verifies that after a replay,
// the second round's retry does not conflict with the first round's retry.
func TestScheduler_ReplayThenRetry_NoConflict_PG(t *testing.T) {
	pool := openRetryTestDB(t)
	defer pool.Close()

	ctx := context.Background()

	// Create task with replay_count=0, attempt_count=1
	taskID := "replay-retry-task"
	envelope := core.TaskEnvelope{
		JanusVersion: "0.1", TaskID: taskID, TenantID: "acme",
		SourceAgent: "agent-1",
		Target:      core.Target{Type: "mailbox", Value: "mb-1"},
		Payload:     core.Payload{Type: "text", Content: "test"},
		Trace:       core.TraceContext{TraceID: "trace_1"},
	}
	envJSON, _ := json.Marshal(envelope)

	_, err := pool.Exec(ctx,
		`INSERT INTO tasks (tenant_id, id, source_agent, target_type, target_value, mailbox_id, status, priority, envelope, attempt_count, replay_count, retry_at)
		 VALUES ('acme', $1, 'agent-1', 'mailbox', 'mb-1', 'mb-1', 'retry_scheduled', 'normal', $2, 1, 0, now() - interval '1 minute')`,
		taskID, envJSON)
	require.NoError(t, err)

	// Run scheduler (first round retry)
	sched := NewScheduler(pool, nil).WithOutbox()
	sched.processReadyRetries(ctx)

	// Verify: task is queued
	var status string
	err = pool.QueryRow(ctx, `SELECT status FROM tasks WHERE id = $1`, taskID).Scan(&status)
	require.NoError(t, err)
	require.Equal(t, "queued", status, "task should be queued after first retry")

	// Verify: outbox has task_publish with replay:0:attempt:1
	var dedupeKey string
	err = pool.QueryRow(ctx, `SELECT dedupe_key FROM outbox_events WHERE tenant_id = 'acme' AND kind = 'task_publish' AND payload->>'TaskID' = $1`, taskID).Scan(&dedupeKey)
	require.NoError(t, err)
	require.Equal(t, "task_publish:acme:"+taskID+":replay:0:attempt:1", dedupeKey, "first retry should have replay:0:attempt:1")

	// Simulate: task runs, fails, scheduled for retry again (still replay 0, attempt 2)
	_, err = pool.Exec(ctx, `UPDATE tasks SET status = 'retry_scheduled', attempt_count = 2, retry_at = now() - interval '1 minute' WHERE id = $1`, taskID)
	require.NoError(t, err)

	// Clear outbox
	_, err = pool.Exec(ctx, `DELETE FROM outbox_events WHERE tenant_id = 'acme'`)
	require.NoError(t, err)

	// Run scheduler again (second round retry)
	sched.processReadyRetries(ctx)

	// Verify: outbox has new task_publish with replay:0:attempt:2
	err = pool.QueryRow(ctx, `SELECT dedupe_key FROM outbox_events WHERE tenant_id = 'acme' AND kind = 'task_publish' AND payload->>'TaskID' = $1`, taskID).Scan(&dedupeKey)
	require.NoError(t, err)
	require.Equal(t, "task_publish:acme:"+taskID+":replay:0:attempt:2", dedupeKey, "second retry should have replay:0:attempt:2")

	// Simulate: task completes, then replayed (replay_count = 1, attempt_count reset to 0)
	_, err = pool.Exec(ctx, `UPDATE tasks SET status = 'completed', replay_count = 1 WHERE id = $1`, taskID)
	require.NoError(t, err)

	// Replay: reset to retry_scheduled with attempt_count = 1, replay_count = 1
	_, err = pool.Exec(ctx, `UPDATE tasks SET status = 'retry_scheduled', attempt_count = 1, retry_at = now() - interval '1 minute' WHERE id = $1`, taskID)
	require.NoError(t, err)

	// Clear outbox
	_, err = pool.Exec(ctx, `DELETE FROM outbox_events WHERE tenant_id = 'acme'`)
	require.NoError(t, err)

	// Run scheduler (third round retry, first round of replay 1)
	sched.processReadyRetries(ctx)

	// Verify: outbox has new task_publish with replay:1:attempt:1 (different from replay:0:attempt:1)
	err = pool.QueryRow(ctx, `SELECT dedupe_key FROM outbox_events WHERE tenant_id = 'acme' AND kind = 'task_publish' AND payload->>'TaskID' = $1`, taskID).Scan(&dedupeKey)
	require.NoError(t, err)
	require.Equal(t, "task_publish:acme:"+taskID+":replay:1:attempt:1", dedupeKey, "replay 1 should have replay:1:attempt:1")
	require.NotEqual(t, "task_publish:acme:"+taskID+":replay:0:attempt:1", dedupeKey, "replay 1 should have different dedupe key than replay 0")
}
