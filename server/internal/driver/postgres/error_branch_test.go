package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

func TestAgentRepo_GetNotFound(t *testing.T) {
	db := openTestDB(t)
	runMigration(t, db)
	insertTestTenant(t, db, "acme")
	repo := NewAgentRepository(db)
	ctx := context.Background()

	_, err := repo.Get(ctx, "acme", "nonexistent")
	assert.Equal(t, sql.ErrNoRows, err)
}

func TestAgentRepo_ListEmpty(t *testing.T) {
	db := openTestDB(t)
	runMigration(t, db)
	insertTestTenant(t, db, "acme")
	repo := NewAgentRepository(db)
	ctx := context.Background()

	agents, err := repo.List(ctx, "acme")
	require.NoError(t, err)
	assert.Len(t, agents, 0)
}

func TestAgentRepo_ListByStatusEmpty(t *testing.T) {
	db := openTestDB(t)
	runMigration(t, db)
	insertTestTenant(t, db, "acme")
	repo := NewAgentRepository(db)
	ctx := context.Background()

	agents, err := repo.ListByStatus(ctx, "acme", core.AgentStatusDegraded)
	require.NoError(t, err)
	assert.Len(t, agents, 0)
}

func TestAgentRepo_WithNullFields(t *testing.T) {
	db := openTestDB(t)
	runMigration(t, db)
	insertTestTenant(t, db, "acme")
	repo := NewAgentRepository(db)
	ctx := context.Background()

	agent := core.Agent{
		ID:             "minimal",
		TenantID:       "acme",
		DisplayName:    "Minimal Agent",
		Protocol:       core.ProtocolA2A,
		Status:         core.AgentStatusOnline,
		MaxConcurrency: 1,
	}
	require.NoError(t, repo.Register(ctx, agent))

	got, err := repo.Get(ctx, "acme", "minimal")
	require.NoError(t, err)
	assert.Equal(t, "", got.Endpoint)
	assert.Equal(t, "", got.Description)
	assert.Equal(t, 0, got.RPM)
	assert.Equal(t, 0, got.TPM)
	assert.Nil(t, got.LastHeartbeatAt)
}

func TestTaskRepo_GetNotFound(t *testing.T) {
	db := openTestDB(t)
	runMigration(t, db)
	repo := NewTaskRepository(db)
	ctx := context.Background()

	_, err := repo.Get(ctx, "acme", "nonexistent")
	assert.Equal(t, sql.ErrNoRows, err)
}

func TestTaskRepo_WithDeadlineAndTTL(t *testing.T) {
	db := openTestDB(t)
	runMigration(t, db)
	insertTestTenant(t, db, "acme")
	repo := NewTaskRepository(db)
	ctx := context.Background()

	deadline := time.Date(2026, 12, 31, 23, 59, 59, 0, time.UTC)
	task := core.Task{
		TenantID:    "acme",
		ID:          "task_deadline",
		SourceAgent: "agent-a",
		TargetType:  core.TargetTypeCapability,
		TargetValue: "code_review",
		Status:      core.TaskStatusCreated,
		Priority:    core.PriorityNormal,
		Deadline:    &deadline,
		TTLSeconds:  3600,
		Envelope: core.TaskEnvelope{
			JanusVersion: "0.1", TaskID: "task_deadline", TenantID: "acme",
			SourceAgent: "agent-a",
			Target:      core.Target{Type: core.TargetTypeCapability, Value: "code_review"},
			Payload:     core.Payload{Type: "review", Content: "x"},
			Trace:       core.TraceContext{TraceID: "trace_deadline"},
		},
	}

	require.NoError(t, repo.Create(ctx, task))

	got, err := repo.Get(ctx, "acme", "task_deadline")
	require.NoError(t, err)
	require.NotNil(t, got.Deadline)
	// PG may convert UTC to session timezone (Asia/Shanghai = UTC+8)
	// 2026-12-31T23:59:59 UTC → 2027-01-01T07:59:59+08:00 — same instant, different wall clock
	assert.False(t, got.Deadline.Before(deadline) || got.Deadline.After(deadline.Add(time.Second)),
		"deadline should be within 1s of expected, got %v", got.Deadline)
	assert.Equal(t, 3600, got.TTLSeconds)
}

func TestTaskRepo_WithMailboxAndResult(t *testing.T) {
	db := openTestDB(t)
	runMigration(t, db)
	insertTestTenant(t, db, "acme")
	agentRepo := NewAgentRepository(db)
	mailboxRepo := NewMailboxRepository(db)
	taskRepo := NewTaskRepository(db)
	ctx := context.Background()

	require.NoError(t, agentRepo.Register(ctx, core.Agent{
		ID: "reviewer", TenantID: "acme", DisplayName: "Reviewer",
		Protocol: core.ProtocolA2A, Status: core.AgentStatusOnline,
	}))
	require.NoError(t, mailboxRepo.Create(ctx, core.Mailbox{
		TenantID: "acme", ID: "reviewer.default", AgentID: "reviewer",
		Status: core.MailboxStatusActive, Priority: core.PriorityNormal,
		MaxConcurrency: 1, ACKWaitSeconds: 300, MaxDeliver: 5,
		RetentionSeconds: 604800, RetryPolicy: core.DefaultRetryPolicy(),
	}))

	task := makeTestTask("acme", "task_mb")
	task.MailboxID = "reviewer.default"
	require.NoError(t, taskRepo.Create(ctx, task))

	got, err := taskRepo.Get(ctx, "acme", "task_mb")
	require.NoError(t, err)
	assert.Equal(t, "reviewer.default", got.MailboxID)
	assert.Equal(t, "", got.ResultRef)
}

func TestTaskRepo_ListByStatusEmpty(t *testing.T) {
	db := openTestDB(t)
	runMigration(t, db)
	insertTestTenant(t, db, "acme")
	repo := NewTaskRepository(db)
	ctx := context.Background()

	tasks, err := repo.ListByStatus(ctx, "acme", core.TaskStatusRunning, 10)
	require.NoError(t, err)
	assert.Len(t, tasks, 0)
}

func TestTaskRepo_DuplicateID(t *testing.T) {
	db := openTestDB(t)
	runMigration(t, db)
	insertTestTenant(t, db, "acme")
	repo := NewTaskRepository(db)
	ctx := context.Background()

	task := makeTestTask("acme", "task_dup")
	require.NoError(t, repo.Create(ctx, task))

	err := repo.Create(ctx, task)
	assert.Error(t, err, "duplicate task ID should fail")
}

func TestMailboxRepo_GetNotFound(t *testing.T) {
	db := openTestDB(t)
	runMigration(t, db)
	repo := NewMailboxRepository(db)
	ctx := context.Background()

	_, err := repo.Get(ctx, "acme", "nonexistent")
	assert.Equal(t, sql.ErrNoRows, err)
}

func TestMailboxRepo_ListByAgentEmpty(t *testing.T) {
	db := openTestDB(t)
	runMigration(t, db)
	repo := NewMailboxRepository(db)
	ctx := context.Background()

	mailboxes, err := repo.ListByAgent(ctx, "acme", "nonexistent")
	require.NoError(t, err)
	assert.Len(t, mailboxes, 0)
}

func TestTenantRepo_GetNotFound(t *testing.T) {
	db := openTestDB(t)
	runMigration(t, db)
	repo := NewTenantRepository(db)
	ctx := context.Background()

	_, err := repo.GetName(ctx, "nonexistent")
	assert.Equal(t, sql.ErrNoRows, err)
}

func TestAgentRepo_UpdateStatusNotFound(t *testing.T) {
	db := openTestDB(t)
	runMigration(t, db)
	repo := NewAgentRepository(db)
	ctx := context.Background()

	err := repo.UpdateStatus(ctx, "acme", "nonexistent", core.AgentStatusOffline)
	assert.NoError(t, err)
}

func TestAgentRepo_UpdateHeartbeatNotFound(t *testing.T) {
	db := openTestDB(t)
	runMigration(t, db)
	repo := NewAgentRepository(db)
	ctx := context.Background()

	err := repo.UpdateHeartbeat(ctx, "acme", "nonexistent")
	assert.NoError(t, err)
}

func TestTaskRepo_UpdateStatusNotFound(t *testing.T) {
	db := openTestDB(t)
	runMigration(t, db)
	repo := NewTaskRepository(db)
	ctx := context.Background()

	err := repo.UpdateStatus(ctx, "acme", "nonexistent", core.TaskStatusQueued, 0)
	assert.NoError(t, err)
}

func TestAgentRepo_ListMultipleWithHeartbeats(t *testing.T) {
	db := openTestDB(t)
	runMigration(t, db)
	insertTestTenant(t, db, "acme")
	repo := NewAgentRepository(db)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		require.NoError(t, repo.Register(ctx, core.Agent{
			ID: fmt.Sprintf("agent_%d", i), TenantID: "acme",
			DisplayName: fmt.Sprintf("Agent %d", i),
			Protocol:    core.ProtocolA2A, Status: core.AgentStatusOnline,
		}))
		require.NoError(t, repo.UpdateHeartbeat(ctx, "acme", fmt.Sprintf("agent_%d", i)))
	}

	agents, err := repo.List(ctx, "acme")
	require.NoError(t, err)
	assert.Len(t, agents, 5)
	for _, a := range agents {
		assert.NotNil(t, a.LastHeartbeatAt)
	}
}

func TestTaskRepo_FullLifecycle(t *testing.T) {
	db := openTestDB(t)
	runMigration(t, db)
	insertTestTenant(t, db, "acme")
	repo := NewTaskRepository(db)
	ctx := context.Background()

	task := makeTestTask("acme", "task_lifecycle")
	require.NoError(t, repo.Create(ctx, task))

	require.NoError(t, repo.UpdateStatus(ctx, "acme", "task_lifecycle", core.TaskStatusQueued, 0))
	got, _ := repo.Get(ctx, "acme", "task_lifecycle")
	assert.Equal(t, core.TaskStatusQueued, got.Status)

	require.NoError(t, repo.UpdateStatus(ctx, "acme", "task_lifecycle", core.TaskStatusFailed, 1))
	got, _ = repo.Get(ctx, "acme", "task_lifecycle")
	assert.Equal(t, core.TaskStatusFailed, got.Status)
	assert.Equal(t, 1, got.AttemptCount)

	require.NoError(t, repo.UpdateStatus(ctx, "acme", "task_lifecycle", core.TaskStatusRetryScheduled, 0))
	got, _ = repo.Get(ctx, "acme", "task_lifecycle")
	assert.Equal(t, core.TaskStatusRetryScheduled, got.Status)

	require.NoError(t, repo.UpdateStatus(ctx, "acme", "task_lifecycle", core.TaskStatusQueued, 0))
	require.NoError(t, repo.UpdateStatus(ctx, "acme", "task_lifecycle", core.TaskStatusCompleted, 0))
	got, _ = repo.Get(ctx, "acme", "task_lifecycle")
	assert.Equal(t, core.TaskStatusCompleted, got.Status)
	assert.NotNil(t, got.CompletedAt)
}
