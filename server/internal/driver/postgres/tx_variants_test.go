package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

func seedTenantAgentMailbox(t *testing.T, pool interface {
	Exec(ctx context.Context, sql string, args ...interface{}) (interface{}, error)
}) {
	// Helper not used — tests seed inline.
}

func TestTaskRepo_CountByStatus(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewTaskRepository(pool)
	ctx := context.Background()
	pool.Exec(ctx, "INSERT INTO tenants (id, name) VALUES ($1, $2)", "cnt", "CNT")
	pool.Exec(ctx, `INSERT INTO agents (id, tenant_id, display_name, protocol, status) VALUES ($1,$2,$3,$4,$5)`, "a1", "cnt", "A", "a2a", "online")
	pool.Exec(ctx, `INSERT INTO mailboxes (id, tenant_id, agent_id, status) VALUES ($1,$2,$3,$4)`, "mb1", "cnt", "a1", "active")

	repo.Create(ctx, core.Task{ID: "t1", TenantID: "cnt", SourceAgent: "a1", TargetType: "agent", TargetValue: "a1", MailboxID: "mb1", Status: "queued", Priority: "normal"})
	repo.Create(ctx, core.Task{ID: "t2", TenantID: "cnt", SourceAgent: "a1", TargetType: "agent", TargetValue: "a1", MailboxID: "mb1", Status: "queued", Priority: "normal"})

	count, err := repo.CountByStatus(ctx, "cnt", core.TaskStatusQueued)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestTaskRepo_ResetForReplay(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewTaskRepository(pool)
	ctx := context.Background()
	pool.Exec(ctx, "INSERT INTO tenants (id, name) VALUES ($1, $2)", "rpl", "RPL")
	pool.Exec(ctx, `INSERT INTO agents (id, tenant_id, display_name, protocol, status) VALUES ($1,$2,$3,$4,$5)`, "a1", "rpl", "A", "a2a", "online")
	pool.Exec(ctx, `INSERT INTO mailboxes (id, tenant_id, agent_id, status) VALUES ($1,$2,$3,$4)`, "mb1", "rpl", "a1", "active")

	repo.Create(ctx, core.Task{ID: "t-rpl", TenantID: "rpl", SourceAgent: "a1", TargetType: "agent", TargetValue: "a1", MailboxID: "mb1", Status: "cancelled", Priority: "normal"})

	err := repo.ResetForReplay(ctx, "rpl", "t-rpl")
	require.NoError(t, err)

	got, _ := repo.Get(ctx, "rpl", "t-rpl")
	assert.Equal(t, core.TaskStatusCreated, got.Status)
}

func TestTaskRepo_ListDeadLettered(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewTaskRepository(pool)
	ctx := context.Background()
	pool.Exec(ctx, "INSERT INTO tenants (id, name) VALUES ($1, $2)", "dl", "DL")
	pool.Exec(ctx, `INSERT INTO agents (id, tenant_id, display_name, protocol, status) VALUES ($1,$2,$3,$4,$5)`, "a1", "dl", "A", "a2a", "online")
	pool.Exec(ctx, `INSERT INTO mailboxes (id, tenant_id, agent_id, status) VALUES ($1,$2,$3,$4)`, "mb1", "dl", "a1", "active")

	repo.Create(ctx, core.Task{ID: "t-dl", TenantID: "dl", SourceAgent: "a1", TargetType: "agent", TargetValue: "a1", MailboxID: "mb1", Status: "dead_lettered", Priority: "normal"})

	tasks, err := repo.ListDeadLettered(ctx, "dl", "mb1", 10)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, "t-dl", tasks[0].ID)
}

// ExpireTasks and GetPendingByTask require complex setup — skipped for now.

func TestMailboxRepo_Backlog(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewMailboxRepository(pool)
	ctx := context.Background()
	pool.Exec(ctx, "INSERT INTO tenants (id, name) VALUES ($1, $2)", "bl", "BL")
	pool.Exec(ctx, `INSERT INTO agents (id, tenant_id, display_name, protocol, status) VALUES ($1,$2,$3,$4,$5)`, "a1", "bl", "A", "a2a", "online")
	repo.Create(ctx, core.Mailbox{ID: "mb-bl", TenantID: "bl", AgentID: "a1", Status: "active"})

	backlog, err := repo.Backlog(ctx, "bl", "mb-bl")
	require.NoError(t, err)
	assert.Equal(t, 0, backlog, "empty mailbox should have 0 backlog")
}

func TestMailboxRepo_UpdateConfig(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewMailboxRepository(pool)
	ctx := context.Background()
	pool.Exec(ctx, "INSERT INTO tenants (id, name) VALUES ($1, $2)", "uc", "UC")
	pool.Exec(ctx, `INSERT INTO agents (id, tenant_id, display_name, protocol, status) VALUES ($1,$2,$3,$4,$5)`, "a1", "uc", "A", "a2a", "online")
	repo.Create(ctx, core.Mailbox{ID: "mb-uc", TenantID: "uc", AgentID: "a1", Status: "active"})

	err := repo.UpdateConfig(ctx, "uc", "mb-uc", 10, 60, 5, 3600)
	require.NoError(t, err)

	mb, _ := repo.Get(ctx, "uc", "mb-uc")
	assert.Equal(t, 10, mb.MaxConcurrency)
}

func TestEventRepo_ListByTenant(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewEventRepo(pool)
	ctx := context.Background()
	pool.Exec(ctx, "INSERT INTO tenants (id, name) VALUES ($1, $2)", "ev", "EV")

	// Insert an event directly matching the audit_event_projection schema.
	pool.Exec(ctx, `INSERT INTO audit_event_projection (tenant_id, event_id, event_type, task_id, agent_id, trace_id, occurred_at, payload)
		VALUES ($1, $2, $3, $4, $5, $6, now(), $7)`,
		"ev", "evt-1", "task.completed", "task-1", "a1", "trace-1", `{"status":"completed"}`)

	events, err := repo.ListByTenant(ctx, "ev", 10)
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, "evt-1", events[0].EventID)
}

func TestContextRefRepo_ListByTaskAndDelete(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewContextRefRepo(pool)
	ctx := context.Background()
	pool.Exec(ctx, "INSERT INTO tenants (id, name) VALUES ($1, $2)", "cr", "CR")

	repo.Insert(ctx, core.ContextRef{ID: "cr-1", TenantID: "cr", Type: "file", URI: "file:///x"})
	// Attach to a task.
	pool.Exec(ctx, `INSERT INTO task_context_refs (tenant_id, task_id, context_ref_id) VALUES ($1, $2, $3)`,
		"cr", "task-cr-1", "cr-1")

	refs, err := repo.ListByTask(ctx, "cr", "task-cr-1")
	require.NoError(t, err)
	require.Len(t, refs, 1)
	assert.Equal(t, "cr-1", refs[0].ID)

	// Delete.
	err = repo.Delete(ctx, "cr", "cr-1")
	require.NoError(t, err)

	_, err = repo.Get(ctx, "cr", "cr-1")
	assert.Error(t, err, "should be deleted")
}

// --- ...Tx variant tests ---

func TestTaskRepo_UpdateStatusWithCheckTx(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewTaskRepository(pool)
	ctx := context.Background()
	pool.Exec(ctx, "INSERT INTO tenants (id, name) VALUES ($1, $2)", "tx1", "TX1")
	pool.Exec(ctx, `INSERT INTO agents (id, tenant_id, display_name, protocol, status) VALUES ($1,$2,$3,$4,$5)`, "a1", "tx1", "A", "a2a", "online")
	pool.Exec(ctx, `INSERT INTO mailboxes (id, tenant_id, agent_id, status) VALUES ($1,$2,$3,$4)`, "mb1", "tx1", "a1", "active")
	repo.Create(ctx, core.Task{ID: "t-tx", TenantID: "tx1", SourceAgent: "a1", TargetType: "agent", TargetValue: "a1", MailboxID: "mb1", Status: "queued", Priority: "normal"})

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	ok, err := repo.UpdateStatusWithCheckTx(ctx, tx, "tx1", "t-tx", core.TaskStatusQueued, core.TaskStatusClaimed, 1)
	require.NoError(t, err)
	assert.True(t, ok)
	require.NoError(t, tx.Commit(ctx))

	got, _ := repo.Get(ctx, "tx1", "t-tx")
	assert.Equal(t, core.TaskStatusClaimed, got.Status)
}

func TestTaskRepo_SetResultRefTx(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewTaskRepository(pool)
	ctx := context.Background()
	pool.Exec(ctx, "INSERT INTO tenants (id, name) VALUES ($1, $2)", "tx2", "TX2")
	pool.Exec(ctx, `INSERT INTO agents (id, tenant_id, display_name, protocol, status) VALUES ($1,$2,$3,$4,$5)`, "a1", "tx2", "A", "a2a", "online")
	pool.Exec(ctx, `INSERT INTO mailboxes (id, tenant_id, agent_id, status) VALUES ($1,$2,$3,$4)`, "mb1", "tx2", "a1", "active")
	repo.Create(ctx, core.Task{ID: "t-tx2", TenantID: "tx2", SourceAgent: "a1", TargetType: "agent", TargetValue: "a1", MailboxID: "mb1", Status: "queued", Priority: "normal"})

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	err = repo.SetResultRefTx(ctx, tx, "tx2", "t-tx2", "result://ok")
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	got, _ := repo.Get(ctx, "tx2", "t-tx2")
	assert.Equal(t, "result://ok", got.ResultRef)
}

func TestTaskRepo_UpdateRetryAtTx(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewTaskRepository(pool)
	ctx := context.Background()
	pool.Exec(ctx, "INSERT INTO tenants (id, name) VALUES ($1, $2)", "tx3", "TX3")
	pool.Exec(ctx, `INSERT INTO agents (id, tenant_id, display_name, protocol, status) VALUES ($1,$2,$3,$4,$5)`, "a1", "tx3", "A", "a2a", "online")
	pool.Exec(ctx, `INSERT INTO mailboxes (id, tenant_id, agent_id, status) VALUES ($1,$2,$3,$4)`, "mb1", "tx3", "a1", "active")
	repo.Create(ctx, core.Task{ID: "t-tx3", TenantID: "tx3", SourceAgent: "a1", TargetType: "agent", TargetValue: "a1", MailboxID: "mb1", Status: "queued", Priority: "normal"})

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	retryAt := time.Now().Add(30 * time.Second)
	err = repo.UpdateRetryAtTx(ctx, tx, "tx3", "t-tx3", retryAt)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	got, _ := repo.Get(ctx, "tx3", "t-tx3")
	assert.Equal(t, core.TaskStatusRetryScheduled, got.Status)
}

func TestOutboxRepo_Insert(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewOutboxRepo(pool)
	ctx := context.Background()
	pool.Exec(ctx, "INSERT INTO tenants (id, name) VALUES ($1, $2)", "ob1", "OB1")

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	err = repo.Insert(ctx, tx, "ob-id-1", "ob1", "event_publish", []byte(`{"type":"test"}`))
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	var count int
	pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE id = $1", "ob-id-1").Scan(&count)
	assert.Equal(t, 1, count)
}

func TestOutboxRepo_InsertWithDedupe(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewOutboxRepo(pool)
	ctx := context.Background()
	pool.Exec(ctx, "INSERT INTO tenants (id, name) VALUES ($1, $2)", "ob2", "OB2")

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	err = repo.InsertWithDedupe(ctx, tx, "ob-ded-1", "ob2", "task_publish", "dedupe-1", []byte(`{}`))
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	// Second insert with same dedupe key → no-op.
	tx2, _ := pool.Begin(ctx)
	err = repo.InsertWithDedupe(ctx, tx2, "ob-ded-2", "ob2", "task_publish", "dedupe-1", []byte(`{}`))
	require.NoError(t, err)
	require.NoError(t, tx2.Commit(ctx))

	var count int
	pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE tenant_id = $1 AND dedupe_key = $2", "ob2", "dedupe-1").Scan(&count)
	assert.Equal(t, 1, count, "dedupe should prevent duplicate")
}
