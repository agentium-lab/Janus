package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

func TestAgentRepo_RegisterAndGet(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "acme")
	repo := NewAgentRepository(pool)
	ctx := context.Background()

	agent := core.Agent{
		ID:             "code-reviewer.team-a",
		TenantID:       "acme",
		DisplayName:    "Code Reviewer",
		Protocol:       core.ProtocolA2A,
		Endpoint:       "https://reviewer.internal/a2a",
		Status:         core.AgentStatusOnline,
		MaxConcurrency: 4,
		RPM:            60,
		TPM:            200000,
	}

	err := repo.Register(ctx, agent)
	require.NoError(t, err)

	got, err := repo.Get(ctx, "acme", "code-reviewer.team-a")
	require.NoError(t, err)
	assert.Equal(t, "Code Reviewer", got.DisplayName)
	assert.Equal(t, core.AgentStatusOnline, got.Status)
	assert.Equal(t, 4, got.MaxConcurrency)
	assert.Equal(t, 60, got.RPM)
	assert.Equal(t, 200000, got.TPM)
}

func TestAgentRepo_RegisterDuplicate(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "acme")
	repo := NewAgentRepository(pool)
	ctx := context.Background()

	agent := core.Agent{
		ID: "reviewer", TenantID: "acme", DisplayName: "R1",
		Protocol: core.ProtocolA2A, Status: core.AgentStatusOnline,
	}
	require.NoError(t, repo.Register(ctx, agent))
	err := repo.Register(ctx, agent)
	assert.Error(t, err)
}

func TestAgentRepo_UpdateStatus(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "acme")
	repo := NewAgentRepository(pool)
	ctx := context.Background()

	agent := core.Agent{
		ID: "reviewer", TenantID: "acme", DisplayName: "R1",
		Protocol: core.ProtocolA2A, Status: core.AgentStatusOnline,
	}
	require.NoError(t, repo.Register(ctx, agent))

	err := repo.UpdateStatus(ctx, "acme", "reviewer", core.AgentStatusOffline)
	require.NoError(t, err)

	got, err := repo.Get(ctx, "acme", "reviewer")
	require.NoError(t, err)
	assert.Equal(t, core.AgentStatusOffline, got.Status)
}

func TestAgentRepo_UpdateHeartbeat(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "acme")
	repo := NewAgentRepository(pool)
	ctx := context.Background()

	agent := core.Agent{
		ID: "reviewer", TenantID: "acme", DisplayName: "R1",
		Protocol: core.ProtocolA2A, Status: core.AgentStatusOnline,
	}
	require.NoError(t, repo.Register(ctx, agent))

	before := time.Now()
	err := repo.UpdateHeartbeat(ctx, "acme", "reviewer")
	require.NoError(t, err)

	got, err := repo.Get(ctx, "acme", "reviewer")
	require.NoError(t, err)
	assert.NotNil(t, got.LastHeartbeatAt)
	assert.True(t, got.LastHeartbeatAt.After(before.Add(-5*time.Second)))
}

func TestAgentRepo_List(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "acme")
	repo := NewAgentRepository(pool)
	ctx := context.Background()

	for _, a := range []core.Agent{
		{ID: "a1", TenantID: "acme", DisplayName: "Agent 1", Protocol: core.ProtocolA2A, Status: core.AgentStatusOnline},
		{ID: "a2", TenantID: "acme", DisplayName: "Agent 2", Protocol: core.ProtocolA2A, Status: core.AgentStatusOffline},
		{ID: "a3", TenantID: "acme", DisplayName: "Agent 3", Protocol: core.ProtocolA2A, Status: core.AgentStatusOnline},
	} {
		require.NoError(t, repo.Register(ctx, a))
	}

	all, err := repo.List(ctx, "acme")
	require.NoError(t, err)
	assert.Len(t, all, 3)

	online, err := repo.ListByStatus(ctx, "acme", core.AgentStatusOnline)
	require.NoError(t, err)
	assert.Len(t, online, 2)
}
