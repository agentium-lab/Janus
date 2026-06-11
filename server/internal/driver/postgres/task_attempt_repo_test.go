package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jackc/pgx/v5"

	"github.com/agentium-lab/Janus/core"
)

func TestTaskAttemptRepo_CreateAndGetLatest(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "acme")
	repo := NewTaskAttemptRepository(pool)
	ctx := context.Background()

	now := time.Now()
	attempt := core.TaskAttempt{
		TenantID:  "acme",
		TaskID:    "task_001",
		Attempt:   1,
		AgentID:   "agent-a",
		LeaseID:   "lease_abc123",
		Status:    "running",
		StartedAt: now,
	}

	require.NoError(t, repo.Create(ctx, attempt))

	got, err := repo.GetLatest(ctx, "acme", "task_001")
	require.NoError(t, err)
	assert.Equal(t, "acme", got.TenantID)
	assert.Equal(t, "task_001", got.TaskID)
	assert.Equal(t, 1, got.Attempt)
	assert.Equal(t, "agent-a", got.AgentID)
	assert.Equal(t, "lease_abc123", got.LeaseID)
	assert.Equal(t, "running", got.Status)
	assert.Nil(t, got.HeartbeatAt)
	assert.Nil(t, got.FinishedAt)
	assert.Nil(t, got.Error)
	assert.Nil(t, got.TokenUsage)
}

func TestTaskAttemptRepo_MultipleAttempts(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "acme")
	repo := NewTaskAttemptRepository(pool)
	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		require.NoError(t, repo.Create(ctx, core.TaskAttempt{
			TenantID: "acme", TaskID: "task_002", Attempt: i,
			AgentID: "agent-a", LeaseID: fmt.Sprintf("lease_%d", i),
			Status: "running", StartedAt: time.Now(),
		}))
	}

	got, err := repo.GetLatest(ctx, "acme", "task_002")
	require.NoError(t, err)
	assert.Equal(t, 3, got.Attempt)
	assert.Equal(t, "lease_3", got.LeaseID)
}

func TestTaskAttemptRepo_GetLatestNotFound(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewTaskAttemptRepository(pool)
	ctx := context.Background()

	_, err := repo.GetLatest(ctx, "acme", "nonexistent")
	assert.Equal(t, pgx.ErrNoRows, err)
}

func TestTaskAttemptRepo_UpdateHeartbeat(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "acme")
	repo := NewTaskAttemptRepository(pool)
	ctx := context.Background()

	require.NoError(t, repo.Create(ctx, core.TaskAttempt{
		TenantID: "acme", TaskID: "task_003", Attempt: 1,
		AgentID: "agent-a", LeaseID: "lease_hb", Status: "running", StartedAt: time.Now(),
	}))

	require.NoError(t, repo.UpdateHeartbeat(ctx, "acme", "task_003", 1))

	got, err := repo.GetLatest(ctx, "acme", "task_003")
	require.NoError(t, err)
	assert.NotNil(t, got.HeartbeatAt)
	assert.True(t, got.HeartbeatAt.After(time.Now().Add(-5*time.Second)))
}

func TestTaskAttemptRepo_UpdateFinished(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "acme")
	repo := NewTaskAttemptRepository(pool)
	ctx := context.Background()

	require.NoError(t, repo.Create(ctx, core.TaskAttempt{
		TenantID: "acme", TaskID: "task_004", Attempt: 1,
		AgentID: "agent-a", LeaseID: "lease_fin", Status: "running", StartedAt: time.Now(),
	}))

	errJSON, _ := json.Marshal(core.TaskError{Code: "TIMEOUT", Message: "agent timed out"})
	usageJSON, _ := json.Marshal(core.TokenUsage{PromptTokens: 100, CompletionTokens: 50, TotalTokens: 150})

	require.NoError(t, repo.UpdateFinished(ctx, "acme", "task_004", 1, "failed", errJSON, usageJSON))

	got, err := repo.GetLatest(ctx, "acme", "task_004")
	require.NoError(t, err)
	assert.Equal(t, "failed", got.Status)
	assert.NotNil(t, got.FinishedAt)
	require.NotNil(t, got.Error)
	assert.Equal(t, "TIMEOUT", got.Error.Code)
	require.NotNil(t, got.TokenUsage)
	assert.Equal(t, 150, got.TokenUsage.TotalTokens)
}

func TestTaskAttemptRepo_UpdateFinishedWithoutError(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "acme")
	repo := NewTaskAttemptRepository(pool)
	ctx := context.Background()

	require.NoError(t, repo.Create(ctx, core.TaskAttempt{
		TenantID: "acme", TaskID: "task_005", Attempt: 1,
		AgentID: "agent-a", LeaseID: "lease_ok", Status: "running", StartedAt: time.Now(),
	}))

	require.NoError(t, repo.UpdateFinished(ctx, "acme", "task_005", 1, "completed", nil, nil))

	got, err := repo.GetLatest(ctx, "acme", "task_005")
	require.NoError(t, err)
	assert.Equal(t, "completed", got.Status)
	assert.Nil(t, got.Error)
	assert.Nil(t, got.TokenUsage)
}

func TestTaskAttemptRepo_UpdateHeartbeatNotFound(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewTaskAttemptRepository(pool)
	ctx := context.Background()

	err := repo.UpdateHeartbeat(ctx, "acme", "nonexistent", 1)
	assert.NoError(t, err)
}

func TestTaskAttemptRepo_UpdateFinishedNotFound(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewTaskAttemptRepository(pool)
	ctx := context.Background()

	err := repo.UpdateFinished(ctx, "acme", "nonexistent", 1, "failed", nil, nil)
	assert.NoError(t, err)
}

func TestTaskAttemptRepo_CreateWithErrorAndUsage(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "acme")
	repo := NewTaskAttemptRepository(pool)
	ctx := context.Background()

	require.NoError(t, repo.Create(ctx, core.TaskAttempt{
		TenantID: "acme", TaskID: "task_006", Attempt: 1,
		AgentID: "agent-a", LeaseID: "lease_eu", Status: "failed", StartedAt: time.Now(),
		Error:      &core.TaskError{Code: "OOM", Message: "out of memory"},
		TokenUsage: &core.TokenUsage{PromptTokens: 200, CompletionTokens: 0, TotalTokens: 200},
	}))

	got, err := repo.GetLatest(ctx, "acme", "task_006")
	require.NoError(t, err)
	require.NotNil(t, got.Error)
	assert.Equal(t, "OOM", got.Error.Code)
	require.NotNil(t, got.TokenUsage)
	assert.Equal(t, 200, got.TokenUsage.TotalTokens)
}
