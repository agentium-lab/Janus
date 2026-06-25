package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

func TestApprovalRepo_CreateAndGet(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewApprovalRepo(pool)
	ctx := context.Background()

	// Seed prerequisites.
	pool.Exec(ctx, "INSERT INTO tenants (id, name) VALUES ($1, $2)", "t1", "T1")
	pool.Exec(ctx, `INSERT INTO agents (id, tenant_id, display_name, protocol, status) VALUES ($1,$2,$3,$4,$5)`,
		"a1", "t1", "A1", "a2a", "online")
	pool.Exec(ctx, `INSERT INTO mailboxes (id, tenant_id, agent_id, status) VALUES ($1,$2,$3,$4)`,
		"mb1", "t1", "a1", "active")
	pool.Exec(ctx, `INSERT INTO tasks (id, tenant_id, source_agent, target_type, target_value, mailbox_id, status, priority)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, "task-ap-1", "t1", "a1", "agent", "a1", "mb1", "approval_pending", "normal")

	approval := core.Approval{
		ID: "ap-1", TenantID: "t1", TaskID: "task-ap-1",
		Reason: "needs review", Status: "pending",
	}
	err := repo.Create(ctx, approval)
	require.NoError(t, err)

	got, err := repo.Get(ctx, "t1", "ap-1")
	require.NoError(t, err)
	assert.Equal(t, "ap-1", got.ID)
	assert.Equal(t, "task-ap-1", got.TaskID)
	assert.Equal(t, "pending", got.Status)
}

func TestApprovalRepo_UpdateDecision(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewApprovalRepo(pool)
	ctx := context.Background()

	pool.Exec(ctx, "INSERT INTO tenants (id, name) VALUES ($1, $2)", "t2", "T2")
	pool.Exec(ctx, `INSERT INTO agents (id, tenant_id, display_name, protocol, status) VALUES ($1,$2,$3,$4,$5)`,
		"a2", "t2", "A2", "a2a", "online")
	pool.Exec(ctx, `INSERT INTO mailboxes (id, tenant_id, agent_id, status) VALUES ($1,$2,$3,$4)`,
		"mb2", "t2", "a2", "active")
	pool.Exec(ctx, `INSERT INTO tasks (id, tenant_id, source_agent, target_type, target_value, mailbox_id, status, priority)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, "task-ap-2", "t2", "a2", "agent", "a2", "mb2", "approval_pending", "normal")

	repo.Create(ctx, core.Approval{ID: "ap-2", TenantID: "t2", TaskID: "task-ap-2", Status: "pending"})

	err := repo.UpdateDecision(ctx, "t2", "ap-2", "approved", "reviewer-1", "looks good")
	require.NoError(t, err)

	got, _ := repo.Get(ctx, "t2", "ap-2")
	assert.Equal(t, "approved", got.Status)
}

func TestApprovalRepo_ListPending(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewApprovalRepo(pool)
	ctx := context.Background()

	pool.Exec(ctx, "INSERT INTO tenants (id, name) VALUES ($1, $2)", "t3", "T3")
	pool.Exec(ctx, `INSERT INTO agents (id, tenant_id, display_name, protocol, status) VALUES ($1,$2,$3,$4,$5)`,
		"a3", "t3", "A3", "a2a", "online")
	pool.Exec(ctx, `INSERT INTO mailboxes (id, tenant_id, agent_id, status) VALUES ($1,$2,$3,$4)`,
		"mb3", "t3", "a3", "active")
	pool.Exec(ctx, `INSERT INTO tasks (id, tenant_id, source_agent, target_type, target_value, mailbox_id, status, priority)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, "task-ap-3", "t3", "a3", "agent", "a3", "mb3", "approval_pending", "normal")

	repo.Create(ctx, core.Approval{ID: "ap-3", TenantID: "t3", TaskID: "task-ap-3", Status: "pending"})

	pending, err := repo.ListPending(ctx, "t3", 10)
	require.NoError(t, err)
	assert.Len(t, pending, 1)
	assert.Equal(t, "ap-3", pending[0].ID)
}

func TestBudgetUsageRepo_ReserveSettleRelease(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewBudgetUsageRepo(pool)
	ctx := context.Background()
	pool.Exec(ctx, "INSERT INTO tenants (id, name) VALUES ($1, $2)", "t4", "T4")

	// Reserve: increments task_count.
	err := repo.ReserveTask(ctx, "t4", "tenant", "t4")
	require.NoError(t, err)
	err = repo.ReserveTask(ctx, "t4", "tenant", "t4")
	require.NoError(t, err)

	_, _, taskCount, _ := repo.GetDailyUsage(ctx, "t4", "tenant", "t4")
	assert.Equal(t, 2, taskCount, "task_count should be 2 after 2 reserves")

	// Settle: adds tokens.
	err = repo.SettleUsage(ctx, "t4", "tenant", "t4", 500, 0.5)
	require.NoError(t, err)

	tokens, cost, _, _ := repo.GetDailyUsage(ctx, "t4", "tenant", "t4")
	assert.Equal(t, 500, tokens)
	if cost < 0.49 || cost > 0.51 {
		t.Fatalf("cost should be ~0.5, got %v", cost)
	}

	// Release: decrements task_count.
	err = repo.ReleaseTask(ctx, "t4", "tenant", "t4")
	require.NoError(t, err)
	_, _, taskCount2, _ := repo.GetDailyUsage(ctx, "t4", "tenant", "t4")
	assert.Equal(t, 1, taskCount2, "task_count should be 1 after release")
}

func TestBudgetUsageRepo_InsertLedger_NonTx(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewBudgetUsageRepo(pool)
	ctx := context.Background()
	pool.Exec(ctx, "INSERT INTO tenants (id, name) VALUES ($1, $2)", "t5", "T5")

	entry := core.LedgerEntry{
		TenantID: "t5", TaskID: "task-ledger-1", Attempt: 1,
		ScopeType: "tenant", ScopeID: "t5",
		TotalTokens: 100, CostUSD: 0.1,
	}
	inserted, err := repo.InsertLedger(ctx, entry)
	require.NoError(t, err)
	assert.True(t, inserted)

	// Duplicate → not inserted.
	inserted2, err := repo.InsertLedger(ctx, entry)
	require.NoError(t, err)
	assert.False(t, inserted2)
}

func TestBudgetUsageRepo_IncrementUsage_NonTx(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewBudgetUsageRepo(pool)
	ctx := context.Background()
	pool.Exec(ctx, "INSERT INTO tenants (id, name) VALUES ($1, $2)", "t6", "T6")

	err := repo.IncrementUsage(ctx, "t6", "tenant", "t6", 10, 5, 15, 0.2)
	require.NoError(t, err)
	tokens, _, _, _ := repo.GetDailyUsage(ctx, "t6", "tenant", "t6")
	assert.Equal(t, 15, tokens)
}

func TestContextRefRepo_InsertAndGet(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewContextRefRepo(pool)
	ctx := context.Background()
	pool.Exec(ctx, "INSERT INTO tenants (id, name) VALUES ($1, $2)", "t7", "T7")

	ref := core.ContextRef{
		ID: "ctxref-1", TenantID: "t7", Type: "file", URI: "file:///x",
		Hash: "abc123", Classification: "internal",
	}
	err := repo.Insert(ctx, ref)
	require.NoError(t, err)

	got, err := repo.Get(ctx, "t7", "ctxref-1")
	require.NoError(t, err)
	assert.Equal(t, "ctxref-1", got.ID)
	assert.Equal(t, "file", got.Type)
	assert.Equal(t, "internal", got.Classification)
}

func TestAgentRepo_FindByCapability(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewAgentRepository(pool)
	ctx := context.Background()
	pool.Exec(ctx, "INSERT INTO tenants (id, name) VALUES ($1, $2)", "t8", "T8")

	// Register an agent, then manually insert its capability.
	repo.Register(ctx, core.Agent{
		ID: "cap-agent-1", TenantID: "t8", DisplayName: "Cap", Protocol: "a2a", Status: "online",
	})
	pool.Exec(ctx, `INSERT INTO agent_capabilities (tenant_id, agent_id, capability) VALUES ($1, $2, $3)`,
		"t8", "cap-agent-1", "code_review")

	agents, err := repo.FindByCapability(ctx, "t8", "code_review")
	require.NoError(t, err)
	assert.Len(t, agents, 1)
	assert.Equal(t, "cap-agent-1", agents[0].ID)
}

func TestAgentRepo_ListAllByStatus(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewAgentRepository(pool)
	ctx := context.Background()
	pool.Exec(ctx, "INSERT INTO tenants (id, name) VALUES ($1, $2)", "t9", "T9")

	repo.Register(ctx, core.Agent{ID: "ls-1", TenantID: "t9", DisplayName: "L1", Protocol: "a2a", Status: "offline"})
	repo.Register(ctx, core.Agent{ID: "ls-2", TenantID: "t9", DisplayName: "L2", Protocol: "a2a", Status: "offline"})

	agents, err := repo.ListAllByStatus(ctx, core.AgentStatusOffline)
	require.NoError(t, err)
	// May include agents from other test DBs; just verify our agents are present.
	found := 0
	for _, a := range agents {
		if a.ID == "ls-1" || a.ID == "ls-2" {
			found++
		}
	}
	assert.Equal(t, 2, found)
}

func TestTaskRepo_SetResultRef(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewTaskRepository(pool)
	ctx := context.Background()
	pool.Exec(ctx, "INSERT INTO tenants (id, name) VALUES ($1, $2)", "t10", "T10")
	pool.Exec(ctx, `INSERT INTO agents (id, tenant_id, display_name, protocol, status) VALUES ($1,$2,$3,$4,$5)`,
		"a10", "t10", "A10", "a2a", "online")
	pool.Exec(ctx, `INSERT INTO mailboxes (id, tenant_id, agent_id, status) VALUES ($1,$2,$3,$4)`,
		"mb10", "t10", "a10", "active")

	repo.Create(ctx, core.Task{
		ID: "task-ref-1", TenantID: "t10", SourceAgent: "a10",
		TargetType: "agent", TargetValue: "a10", MailboxID: "mb10",
		Status: "queued", Priority: "normal",
	})

	err := repo.SetResultRef(ctx, "t10", "task-ref-1", "result://test")
	require.NoError(t, err)

	got, _ := repo.Get(ctx, "t10", "task-ref-1")
	assert.Equal(t, "result://test", got.ResultRef)
}

func TestTaskRepo_UpdateRetryAt(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewTaskRepository(pool)
	ctx := context.Background()
	pool.Exec(ctx, "INSERT INTO tenants (id, name) VALUES ($1, $2)", "t11", "T11")
	pool.Exec(ctx, `INSERT INTO agents (id, tenant_id, display_name, protocol, status) VALUES ($1,$2,$3,$4,$5)`,
		"a11", "t11", "A11", "a2a", "online")
	pool.Exec(ctx, `INSERT INTO mailboxes (id, tenant_id, agent_id, status) VALUES ($1,$2,$3,$4)`,
		"mb11", "t11", "a11", "active")

	repo.Create(ctx, core.Task{
		ID: "task-retry-1", TenantID: "t11", SourceAgent: "a11",
		TargetType: "agent", TargetValue: "a11", MailboxID: "mb11",
		Status: "queued", Priority: "normal",
	})

	retryAt := time.Now().Add(30 * time.Second)
	err := repo.UpdateRetryAt(ctx, "t11", "task-retry-1", retryAt)
	require.NoError(t, err)

	got, _ := repo.Get(ctx, "t11", "task-retry-1")
	assert.Equal(t, core.TaskStatus("retry_scheduled"), got.Status)
}
