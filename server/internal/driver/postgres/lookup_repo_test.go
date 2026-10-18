package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A3 fix: ListOnlineByCapability must return governance attributes from
// trusted records (team_id, metadata.data_classification) and REAL
// running/backlog counts — not hollow candidates that silently pass every
// router filter.
func TestAgentLookupRepo_ListOnlineByCapability_RealGovernanceAttributes(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "acme")
	ctx := context.Background()

	// Agent trusted record: team + data classification in metadata.
	_, err := pool.Exec(ctx, `
		INSERT INTO agents (id, tenant_id, display_name, protocol, status, metadata, max_concurrency, team_id)
		VALUES ('gov-agent', 'acme', 'Gov Agent', 'a2a', 'online',
		        '{"data_classification":"confidential"}'::jsonb, 2, 'team-x')`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO agent_capabilities (tenant_id, agent_id, capability)
		VALUES ('acme', 'gov-agent', 'code-review')`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO mailboxes (id, tenant_id, agent_id, status)
		VALUES ('mb-gov', 'acme', 'gov-agent', 'active')`)
	require.NoError(t, err)

	// 1 running + 2 queued tasks on the agent's mailbox.
	for i, status := range []string{"running", "queued", "queued"} {
		_, err = pool.Exec(ctx, `
			INSERT INTO tasks (id, tenant_id, source_agent, target_type, target_value,
			                   mailbox_id, status, priority, envelope, created_at, updated_at)
			VALUES ($1, 'acme', 'src', 'mailbox', 'mb-gov',
			        'mb-gov', $2, 'normal', '{}'::jsonb, now(), now())`,
			"task-"+string(rune('a'+i)), status)
		require.NoError(t, err)
	}

	repo := NewAgentLookupRepo(pool)
	candidates, err := repo.ListOnlineByCapability(ctx, "acme", "code-review")
	require.NoError(t, err)
	require.Len(t, candidates, 1)

	c := candidates[0]
	assert.Equal(t, "gov-agent", c.AgentID)
	assert.Equal(t, "mb-gov", c.MailboxID)
	assert.Equal(t, "team-x", c.TeamID, "team_id must come from the trusted agent record")
	assert.Equal(t, "confidential", c.DataClass, "data_classification must come from agents.metadata")
	assert.Equal(t, 1, c.RunningCount, "running count must come from the tasks table")
	assert.Equal(t, 2, c.Backlog, "backlog must come from the tasks table")
	assert.Equal(t, 2, c.MaxConcurrency)
}

// B7 regression: the expiry outbox event ID must be tenant-qualified —
// outbox_events.id is globally unique while task IDs are only
// tenant-scoped; two tenants expiring the same task ID used to collide
// on 'exp-<taskID>' and roll back the whole expiry transaction.
func TestTaskRepo_ExpireTasks_SameTaskIDAcrossTenants(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	ctx := context.Background()
	repo := NewTaskRepository(pool)

	insertExpired := func(tenantID string) {
		_, err := pool.Exec(ctx, `INSERT INTO tenants (id, name) VALUES ($1, $1)`, tenantID)
		require.NoError(t, err)
		_, err = pool.Exec(ctx, `
			INSERT INTO tasks (id, tenant_id, source_agent, target_type, target_value,
			                   status, priority, ttl_seconds, envelope, created_at, updated_at)
			VALUES ('same-task', $1, 'src', 'mailbox', 'mb', 'queued', 'normal', 1,
			        '{}'::jsonb, now() - interval '1 hour', now() - interval '1 hour')`, tenantID)
		require.NoError(t, err)
	}
	insertExpired("ten-a")
	insertExpired("ten-b")

	n, err := repo.ExpireTasks(ctx)
	require.NoError(t, err, "expiry must succeed across tenants with colliding task IDs")
	assert.GreaterOrEqual(t, n, int64(2))

	var events int
	err = pool.QueryRow(ctx, `
		SELECT count(*) FROM outbox_events
		WHERE payload->>'event_type' = 'task.expired' AND payload->>'task_id' = 'same-task'`).Scan(&events)
	require.NoError(t, err)
	assert.Equal(t, 2, events, "both tenants must get their own task.expired outbox event")
}

// B7: the tenant/task concatenation must be injective — plain dashes let
// tenant=a-b/task=c collide with tenant=a/task=b-c on the global outbox PK.
func TestTaskRepo_ExpireTasks_AmbiguousNamesDoNotCollide(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	ctx := context.Background()
	repo := NewTaskRepository(pool)

	for _, tn := range []string{"a-b", "a"} {
		_, err := pool.Exec(ctx, `INSERT INTO tenants (id, name) VALUES ($1, $1)`, tn)
		require.NoError(t, err)
	}
	for _, pair := range []struct{ tenant, task string }{{"a-b", "c"}, {"a", "b-c"}} {
		_, err := pool.Exec(ctx, `
			INSERT INTO tasks (id, tenant_id, source_agent, target_type, target_value,
			                   status, priority, ttl_seconds, envelope, created_at, updated_at)
			VALUES ($1, $2, 'src', 'mailbox', 'mb', 'queued', 'normal', 1,
			        '{}'::jsonb, now() - interval '1 hour', now() - interval '1 hour')`,
			pair.task, pair.tenant)
		require.NoError(t, err)
	}

	n, err := repo.ExpireTasks(ctx)
	require.NoError(t, err, "ambiguous tenant/task names must not collide on the outbox PK")
	require.Equal(t, int64(2), n)
}
