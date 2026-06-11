package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/agentium-lab/Janus/core"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func insertTestAuditEvent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tenantID string, evt core.JanusEvent) {
	t.Helper()
	taskID := evt.TaskID
	_, err := pool.Exec(ctx,
		`INSERT INTO audit_event_projection (tenant_id, event_id, event_type, task_id, agent_id, trace_id, occurred_at, payload)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		tenantID, evt.EventID, evt.EventType, taskID, evt.SourceAgent, evt.TraceID, evt.Timestamp, evt.Payload,
	)
	require.NoError(t, err)
}

func TestEventRepo_Insert(t *testing.T) {
	ctx := context.Background()
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "t1")

	repo := NewEventRepo(pool)
	evt := core.JanusEvent{
		EventID:     "evt-1",
		EventType:   core.EventTaskCreated,
		TenantID:    "t1",
		TaskID:      "task-1",
		SourceAgent: "agent-1",
		TraceID:     "trace-1",
		Timestamp:   time.Now().UTC().Truncate(time.Microsecond),
		Payload:     []byte(`{"priority":"normal"}`),
	}
	err := repo.Insert(ctx, evt)
	assert.NoError(t, err)
}

func TestEventRepo_Insert_Duplicate(t *testing.T) {
	ctx := context.Background()
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "t1")

	repo := NewEventRepo(pool)
	evt := core.JanusEvent{
		EventID:   "evt-dup",
		EventType: core.EventTaskCompleted,
		TenantID:  "t1",
		Timestamp: time.Now().UTC(),
		Payload:   []byte(`{}`),
	}
	require.NoError(t, repo.Insert(ctx, evt))
	err := repo.Insert(ctx, evt)
	assert.Error(t, err)
}

func TestEventRepo_ListByTask(t *testing.T) {
	ctx := context.Background()
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "t1")

	now := time.Now().UTC().Truncate(time.Microsecond)
	insertTestAuditEvent(t, ctx, pool, "t1", core.JanusEvent{
		EventID: "e1", EventType: core.EventTaskCreated, TenantID: "t1",
		TaskID: "task-1", Timestamp: now, Payload: []byte(`{}`),
	})
	insertTestAuditEvent(t, ctx, pool, "t1", core.JanusEvent{
		EventID: "e2", EventType: core.EventTaskClaimed, TenantID: "t1",
		TaskID: "task-1", Timestamp: now.Add(time.Second), Payload: []byte(`{}`),
	})
	insertTestAuditEvent(t, ctx, pool, "t1", core.JanusEvent{
		EventID: "e3", EventType: core.EventTaskCreated, TenantID: "t1",
		TaskID: "task-2", Timestamp: now.Add(2*time.Second), Payload: []byte(`{}`),
	})

	repo := NewEventRepo(pool)
	events, err := repo.ListByTask(ctx, "t1", "task-1", 10)
	require.NoError(t, err)
	assert.Len(t, events, 2)
	assert.Equal(t, "e1", events[0].EventID)
	assert.Equal(t, "e2", events[1].EventID)
}

func TestEventRepo_ListByTask_Empty(t *testing.T) {
	ctx := context.Background()
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "t1")

	repo := NewEventRepo(pool)
	events, err := repo.ListByTask(ctx, "t1", "nonexistent", 10)
	require.NoError(t, err)
	assert.Len(t, events, 0)
}

func TestEventRepo_ListByTask_DefaultLimit(t *testing.T) {
	ctx := context.Background()
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "t1")

	repo := NewEventRepo(pool)
	events, err := repo.ListByTask(ctx, "t1", "nope", 0)
	require.NoError(t, err)
	assert.Len(t, events, 0)
}

func TestEventRepo_ListByTrace(t *testing.T) {
	ctx := context.Background()
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "t1")

	now := time.Now().UTC().Truncate(time.Microsecond)
	insertTestAuditEvent(t, ctx, pool, "t1", core.JanusEvent{
		EventID: "e1", EventType: core.EventTaskCreated, TenantID: "t1",
		TaskID: "task-1", TraceID: "trace-abc", Timestamp: now, Payload: []byte(`{}`),
	})
	insertTestAuditEvent(t, ctx, pool, "t1", core.JanusEvent{
		EventID: "e2", EventType: core.EventTaskCompleted, TenantID: "t1",
		TaskID: "task-1", TraceID: "trace-abc", Timestamp: now.Add(time.Second), Payload: []byte(`{}`),
	})
	insertTestAuditEvent(t, ctx, pool, "t1", core.JanusEvent{
		EventID: "e3", EventType: core.EventTaskCreated, TenantID: "t1",
		TaskID: "task-2", TraceID: "trace-xyz", Timestamp: now.Add(2*time.Second), Payload: []byte(`{}`),
	})

	repo := NewEventRepo(pool)
	events, err := repo.ListByTrace(ctx, "t1", "trace-abc", 10)
	require.NoError(t, err)
	assert.Len(t, events, 2)
	assert.Equal(t, "e1", events[0].EventID)
	assert.Equal(t, "e2", events[1].EventID)
}

func TestEventRepo_ListByTrace_Empty(t *testing.T) {
	ctx := context.Background()
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "t1")

	repo := NewEventRepo(pool)
	events, err := repo.ListByTrace(ctx, "t1", "nonexistent", 10)
	require.NoError(t, err)
	assert.Len(t, events, 0)
}

func TestEventRepo_ListByTrace_DefaultLimit(t *testing.T) {
	ctx := context.Background()
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "t1")

	repo := NewEventRepo(pool)
	events, err := repo.ListByTrace(ctx, "t1", "nope", -1)
	require.NoError(t, err)
	assert.Len(t, events, 0)
}
