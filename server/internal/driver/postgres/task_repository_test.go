package postgres

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

func TestTaskRepo_CreateAndGet(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "acme")
	repo := NewTaskRepository(pool)
	ctx := context.Background()

	task := core.Task{
		TenantID:    "acme",
		ID:          "task_001",
		SourceAgent: "agent-a",
		TargetType:  core.TargetTypeCapability,
		TargetValue: "code_review",
		Status:      core.TaskStatusCreated,
		Priority:    core.PriorityNormal,
		Envelope: core.TaskEnvelope{
			JanusVersion: "0.1", TaskID: "task_001", TenantID: "acme",
			SourceAgent: "agent-a",
			Target:      core.Target{Type: core.TargetTypeCapability, Value: "code_review"},
			Priority:    core.PriorityNormal,
			Payload:     core.Payload{Type: "review", Content: "review this"},
			Trace:       core.TraceContext{TraceID: "trace_001"},
		},
	}

	err := repo.Create(ctx, task)
	require.NoError(t, err)

	got, err := repo.Get(ctx, "acme", "task_001")
	require.NoError(t, err)
	assert.Equal(t, "task_001", got.ID)
	assert.Equal(t, core.TaskStatusCreated, got.Status)
	assert.Equal(t, "agent-a", got.SourceAgent)
	assert.Equal(t, core.TargetTypeCapability, got.TargetType)
	assert.Equal(t, "code_review", got.TargetValue)
}

func TestTaskRepo_GetByIdempotencyKey(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "acme")
	repo := NewTaskRepository(pool)
	ctx := context.Background()

	task := core.Task{
		TenantID:       "acme",
		ID:             "task_001",
		IdempotencyKey: "repo-123-pr-456",
		SourceAgent:    "agent-a",
		TargetType:     core.TargetTypeCapability,
		TargetValue:    "code_review",
		Status:         core.TaskStatusCreated,
		Priority:       core.PriorityNormal,
		Envelope: core.TaskEnvelope{
			JanusVersion: "0.1", TaskID: "task_001", TenantID: "acme",
			SourceAgent: "agent-a",
			Target:      core.Target{Type: core.TargetTypeCapability, Value: "code_review"},
			Priority:    core.PriorityNormal,
			Payload:     core.Payload{Type: "review", Content: "x"},
			Trace:       core.TraceContext{TraceID: "trace_001"},
		},
	}
	require.NoError(t, repo.Create(ctx, task))

	got, err := repo.GetByIdempotencyKey(ctx, "acme", "repo-123-pr-456")
	require.NoError(t, err)
	assert.Equal(t, "task_001", got.ID)

	_, err = repo.GetByIdempotencyKey(ctx, "acme", "nonexistent")
	assert.Error(t, err)
}

func TestTaskRepo_UpdateStatus(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "acme")
	repo := NewTaskRepository(pool)
	ctx := context.Background()

	task := makeTestTask("acme", "task_001")
	require.NoError(t, repo.Create(ctx, task))

	err := repo.UpdateStatus(ctx, "acme", "task_001", core.TaskStatusQueued, 0)
	require.NoError(t, err)

	got, err := repo.Get(ctx, "acme", "task_001")
	require.NoError(t, err)
	assert.Equal(t, core.TaskStatusQueued, got.Status)
}

func TestTaskRepo_IncrementAttempt(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "acme")
	repo := NewTaskRepository(pool)
	ctx := context.Background()

	task := makeTestTask("acme", "task_001")
	require.NoError(t, repo.Create(ctx, task))

	err := repo.UpdateStatus(ctx, "acme", "task_001", core.TaskStatusFailed, 1)
	require.NoError(t, err)

	got, err := repo.Get(ctx, "acme", "task_001")
	require.NoError(t, err)
	assert.Equal(t, 1, got.AttemptCount)
}

func TestTaskRepo_Complete(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "acme")
	repo := NewTaskRepository(pool)
	ctx := context.Background()

	task := makeTestTask("acme", "task_001")
	require.NoError(t, repo.Create(ctx, task))

	err := repo.UpdateStatus(ctx, "acme", "task_001", core.TaskStatusCompleted, 0)
	require.NoError(t, err)

	got, err := repo.Get(ctx, "acme", "task_001")
	require.NoError(t, err)
	assert.Equal(t, core.TaskStatusCompleted, got.Status)
	assert.NotNil(t, got.CompletedAt)
}

func TestTaskRepo_ListByStatus(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "acme")
	repo := NewTaskRepository(pool)
	ctx := context.Background()

	for i, status := range []core.TaskStatus{core.TaskStatusCreated, core.TaskStatusCreated, core.TaskStatusQueued} {
		task := makeTestTask("acme", fmt.Sprintf("task_%03d", i))
		task.Status = status
		require.NoError(t, repo.Create(ctx, task))
	}

	tasks, err := repo.ListByStatus(ctx, "acme", core.TaskStatusCreated, 10)
	require.NoError(t, err)
	assert.Len(t, tasks, 2)
}

func makeTestTask(tenantID, taskID string) core.Task {
	return core.Task{
		TenantID:    tenantID,
		ID:          taskID,
		SourceAgent: "agent-a",
		TargetType:  core.TargetTypeCapability,
		TargetValue: "code_review",
		Status:      core.TaskStatusCreated,
		Priority:    core.PriorityNormal,
		Envelope: core.TaskEnvelope{
			JanusVersion: "0.1", TaskID: taskID, TenantID: tenantID,
			SourceAgent: "agent-a",
			Target:      core.Target{Type: core.TargetTypeCapability, Value: "code_review"},
			Payload:     core.Payload{Type: "review", Content: "x"},
			Trace:       core.TraceContext{TraceID: "trace_" + taskID},
		},
	}
}
