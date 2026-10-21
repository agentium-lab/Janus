package lease

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/agentium-lab/Janus/core"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fetchEventPublishRows returns every 'event_publish' outbox row for the
// seeded tenant, decoded as JanusEvent. Filtering happens in Go so the helper
// does not depend on the payload column's SQL type.
func fetchEventPublishRows(t *testing.T, pool *pgxpool.Pool, taskID string) []core.JanusEvent {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT payload FROM outbox_events WHERE tenant_id = 'acme' AND kind = 'event_publish'`)
	require.NoError(t, err)
	defer rows.Close()

	var events []core.JanusEvent
	for rows.Next() {
		var raw []byte
		require.NoError(t, rows.Scan(&raw))
		var evt core.JanusEvent
		require.NoError(t, json.Unmarshal(raw, &evt), "outbox event_publish payload must be a JanusEvent")
		if evt.TaskID == taskID {
			events = append(events, evt)
		}
	}
	require.NoError(t, rows.Err())
	return events
}

// TestExpireLeases_EmitsLeaseLostOnRequeue proves the requeue/redelivery
// branch (retries remain) persists a task.lease_lost event in the outbox,
// in the same transaction as the retry_scheduled transition.
func TestExpireLeases_EmitsLeaseLostOnRequeue(t *testing.T) {
	pool := openLeaseTestDB(t)
	seedLeaseTestData(t, pool)

	scanner := NewScanner(pool, 0)
	n, err := scanner.ExpireLeases(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, n)

	var taskStatus string
	err = pool.QueryRow(context.Background(),
		`SELECT status FROM tasks WHERE tenant_id='acme' AND id='task-1'`).Scan(&taskStatus)
	require.NoError(t, err)
	assert.Equal(t, "retry_scheduled", taskStatus)

	events := fetchEventPublishRows(t, pool, "task-1")
	require.Len(t, events, 1, "requeue must emit exactly one event_publish row")
	evt := events[0]
	assert.Equal(t, core.EventLeaseLost, evt.EventType)
	assert.Equal(t, "acme", evt.TenantID)
	assert.Equal(t, "task-1", evt.TaskID)
	assert.Equal(t, "agent-1", evt.SourceAgent, "the lease holder that lost the lease")
	assert.NotEmpty(t, evt.EventID)
	assert.False(t, evt.Timestamp.IsZero())

	var payload struct {
		Reason  string `json:"reason"`
		Attempt int    `json:"attempt"`
		AgentID string `json:"agent_id"`
	}
	require.NoError(t, json.Unmarshal(evt.Payload, &payload))
	assert.Equal(t, "lease_expired", payload.Reason)
	assert.Equal(t, 1, payload.Attempt)
	assert.Equal(t, "agent-1", payload.AgentID)
}

func TestExpireLeases_LeaseLostOutboxInsertError(t *testing.T) {
	pool := openLeaseTestDB(t)
	seedLeaseExpired(t, pool, "task-ll-oerr", 1, "")
	execLease(t, pool, raiseTriggerFn)
	execLease(t, pool, `CREATE TRIGGER lease_cov_ll_boom BEFORE INSERT ON outbox_events
		FOR EACH ROW WHEN (NEW.kind = 'event_publish') EXECUTE FUNCTION lease_cov_raise()`)

	n, err := NewScanner(pool, time.Hour).ExpireLeases(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, n)

	var taskStatus string
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT status FROM tasks WHERE id='task-ll-oerr'`).Scan(&taskStatus))
	assert.Equal(t, "claimed", taskStatus, "lease_lost insert failure must roll back the requeue")
}

// TestExpireLeases_DeadLetterDoesNotEmitLeaseLost pins the contract split:
// lease loss that dead-letters emits task.dead_lettered (existing behavior),
// NOT task.lease_lost — lease_lost belongs to the requeue/redelivery branch.
func TestExpireLeases_DeadLetterDoesNotEmitLeaseLost(t *testing.T) {
	pool := openLeaseTestDB(t)
	seedLeaseTestData(t, pool)

	// Default mailbox retry_policy max_attempts=5: attempt_count=5 exceeds it.
	_, err := pool.Exec(context.Background(),
		`UPDATE tasks SET attempt_count = 5 WHERE tenant_id='acme' AND id='task-1'`)
	require.NoError(t, err)

	scanner := NewScanner(pool, 0)
	n, err := scanner.ExpireLeases(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, n)

	events := fetchEventPublishRows(t, pool, "task-1")
	require.NotEmpty(t, events, "dead-letter branch emits its own event")
	sawDeadLettered := false
	for _, evt := range events {
		assert.NotEqual(t, core.EventLeaseLost, evt.EventType,
			"dead-letter branch must not emit task.lease_lost")
		if evt.EventType == core.EventTaskDeadLettered {
			sawDeadLettered = true
		}
	}
	assert.True(t, sawDeadLettered, "dead-letter branch still emits task.dead_lettered")
}
