package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

func TestMailboxRepo_CreateAndGet(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "acme")
	agentRepo := NewAgentRepository(pool)
	repo := NewMailboxRepository(pool)
	ctx := context.Background()

	require.NoError(t, agentRepo.Register(ctx, core.Agent{
		ID: "reviewer", TenantID: "acme", DisplayName: "Reviewer",
		Protocol: core.ProtocolA2A, Status: core.AgentStatusOnline,
	}))

	mb := core.Mailbox{
		TenantID:         "acme",
		ID:               "reviewer.default",
		AgentID:          "reviewer",
		Status:           core.MailboxStatusActive,
		Priority:         core.PriorityNormal,
		MaxConcurrency:   4,
		ACKWaitSeconds:   300,
		MaxDeliver:       5,
		RetentionSeconds: 604800,
		RetryPolicy:      core.DefaultRetryPolicy(),
	}

	err := repo.Create(ctx, mb)
	require.NoError(t, err)

	got, err := repo.Get(ctx, "acme", "reviewer.default")
	require.NoError(t, err)
	assert.Equal(t, "reviewer.default", got.ID)
	assert.Equal(t, "reviewer", got.AgentID)
	assert.Equal(t, 4, got.MaxConcurrency)
	assert.Equal(t, 300, got.ACKWaitSeconds)
	assert.Equal(t, 5, got.MaxDeliver)
	assert.Equal(t, 604800, got.RetentionSeconds)
	assert.Equal(t, 5, got.RetryPolicy.MaxAttempts)
}

func TestMailboxRepo_ListByAgent(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "acme")
	agentRepo := NewAgentRepository(pool)
	repo := NewMailboxRepository(pool)
	ctx := context.Background()

	require.NoError(t, agentRepo.Register(ctx, core.Agent{
		ID: "reviewer", TenantID: "acme", DisplayName: "Reviewer",
		Protocol: core.ProtocolA2A, Status: core.AgentStatusOnline,
	}))

	for _, mbID := range []string{"reviewer.default", "reviewer.high-priority"} {
		require.NoError(t, repo.Create(ctx, core.Mailbox{
			TenantID: "acme", ID: mbID, AgentID: "reviewer",
			Status: core.MailboxStatusActive, Priority: core.PriorityNormal,
			MaxConcurrency: 1, ACKWaitSeconds: 300, MaxDeliver: 5,
			RetentionSeconds: 604800, RetryPolicy: core.DefaultRetryPolicy(),
		}))
	}

	mailboxes, err := repo.ListByAgent(ctx, "acme", "reviewer")
	require.NoError(t, err)
	assert.Len(t, mailboxes, 2)
}
