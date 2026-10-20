package service

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/driver/postgres"
)

// Real-PostgreSQL acceptance tests for the context-ref write plan.
//
// The regression being pinned here: NormalizeAndBindTx used to infer
// "isNew" from CreatedAt == "", but prepareRef stamps a NON-empty
// CreatedAt on every generated ref, so new inline refs skipped InsertTx.
// context_refs has no foreign key, so the task committed with bind rows
// pointing at missing entities; ListByTask (inner join) then found
// nothing and idempotent retries failed not-found.
//
// PG-gated per repo convention: SKIP (not FAIL) when JANUS_PG_* is unset
// or no server is reachable. Each test uses the package's existing
// harness (openServiceTestDB): a throwaway database per test with all
// migrations/*.up.sql applied.

// failBindTxRepo forwards inserts to the real PG tx repo but fails every
// bind, forcing a mid-transaction failure inside createAtomic to prove
// the task, its context_refs and its outbox rows all roll back together.
type failBindTxRepo struct {
	*postgres.ContextRefRepo
	err error
}

func (f failBindTxRepo) InsertTx(ctx context.Context, tx pgx.Tx, ref core.ContextRef) error {
	return f.ContextRefRepo.InsertTx(ctx, tx, ref)
}

func (f failBindTxRepo) BindToTaskTx(_ context.Context, _ pgx.Tx, _, _, _ string) error {
	return f.err
}

// setupCtxRefEnv reuses the standard PG harness but rebuilds taskSvc with
// the context-ref service attached (and without a policy service, so the
// create path under test stays minimal).
func setupCtxRefEnv(t *testing.T) (*serviceTestEnv, *ContextRefService) {
	t.Helper()
	env := setupServiceTestEnv(t)
	ctxSvc := NewContextRefService(postgres.NewContextRefRepo(env.pool))
	env.taskSvc = NewTaskService(env.taskRepo, env.driver, env.pool, env.outboxRepo).
		WithAttemptRepo(env.attemptRepo).
		WithContextRefService(ctxSvc)
	return env, ctxSvc
}

func ctxRefTask(id string, refs []core.ContextRef) core.Task {
	envelope := makeTestEnvelope(id, "acme")
	envelope.ContextRefs = refs
	return core.Task{
		TenantID: "acme", ID: id, SourceAgent: "agent-1",
		TargetType: core.TargetTypeCapability, TargetValue: "review",
		Priority: core.PriorityNormal,
		Envelope: envelope,
	}
}

func countRows(t *testing.T, env *serviceTestEnv, query string, args ...interface{}) int {
	t.Helper()
	var n int
	require.NoError(t, env.pool.QueryRow(context.Background(), query, args...).Scan(&n))
	return n
}

// (a) Create with an inline envelope ref must persist the ENTITY: a
// context_refs row the ListByTask inner join can find — not just a bind.
func TestContextRefPG_Create_InlineRefPersistsEntity(t *testing.T) {
	env, ctxSvc := setupCtxRefEnv(t)
	ctx := context.Background()

	task := ctxRefTask("t-inline", []core.ContextRef{{Type: "file", URI: "s3://x"}})
	created, err := env.taskSvc.Create(ctx, task)
	require.NoError(t, err)
	require.NotNil(t, created)
	require.Len(t, created.Envelope.ContextRefs, 1, "generated ref ID must ride in the persisted envelope")
	refID := created.Envelope.ContextRefs[0].ID
	require.NotEmpty(t, refID)

	assert.Equal(t, 1, countRows(t, env,
		`SELECT count(*) FROM context_refs WHERE tenant_id = 'acme' AND id = $1`, refID),
		"inline ref must insert a context_refs entity row (the regression wrote only the bind row)")

	refs, err := ctxSvc.ListByTask(ctx, "acme", "t-inline")
	require.NoError(t, err)
	require.Len(t, refs, 1, "ListByTask (inner join) must find the ref")
	assert.Equal(t, refID, refs[0].ID)
	assert.Equal(t, "s3://x", refs[0].URI)
}

// (b) A second task reusing an existing ref ID must bind it WITHOUT
// inserting another context_refs row, and must not duplicate the entity.
func TestContextRefPG_Create_ReuseRefBindsWithoutDuplicateEntity(t *testing.T) {
	env, ctxSvc := setupCtxRefEnv(t)
	ctx := context.Background()

	shared, err := ctxSvc.Attach(ctx, "acme", "file", "s3://shared", "", "", nil)
	require.NoError(t, err)
	before := countRows(t, env, `SELECT count(*) FROM context_refs WHERE tenant_id = 'acme'`)
	require.Equal(t, 1, before)

	created, err := env.taskSvc.Create(ctx, ctxRefTask("t-reuse", []core.ContextRef{{ID: shared.ID}}))
	require.NoError(t, err)
	require.NotNil(t, created)

	assert.Equal(t, before, countRows(t, env, `SELECT count(*) FROM context_refs WHERE tenant_id = 'acme'`),
		"reusing an existing ref ID must NOT insert another entity row")
	assert.Equal(t, 1, countRows(t, env,
		`SELECT count(*) FROM task_context_refs WHERE tenant_id = 'acme' AND task_id = 't-reuse' AND context_ref_id = $1`, shared.ID),
		"reused ref must be bound to the new task")

	refs, err := ctxSvc.ListByTask(ctx, "acme", "t-reuse")
	require.NoError(t, err)
	require.Len(t, refs, 1)
	assert.Equal(t, shared.ID, refs[0].ID)
}

// (c) Retrying the same idempotency key must succeed and return the
// existing task, with its context ref still resolvable.
func TestContextRefPG_Create_IdempotentRetryReturnsExisting(t *testing.T) {
	env, ctxSvc := setupCtxRefEnv(t)
	ctx := context.Background()

	first := ctxRefTask("t-idem-1", []core.ContextRef{{Type: "file", URI: "s3://idem"}})
	first.IdempotencyKey = "idem-key-1"
	first.Envelope.IdempotencyKey = "idem-key-1"
	created, err := env.taskSvc.Create(ctx, first)
	require.NoError(t, err)
	require.NotNil(t, created)
	refID := created.Envelope.ContextRefs[0].ID
	require.NotEmpty(t, refID)

	retry := ctxRefTask("t-idem-2", []core.ContextRef{{Type: "file", URI: "s3://idem"}})
	retry.IdempotencyKey = "idem-key-1"
	retry.Envelope.IdempotencyKey = "idem-key-1"
	again, err := env.taskSvc.Create(ctx, retry)
	require.NoError(t, err, "idempotent retry must succeed (the regression failed not-found)")
	require.NotNil(t, again)
	assert.Equal(t, "t-idem-1", again.ID, "retry must return the existing task")

	assert.Equal(t, 1, countRows(t, env, `SELECT count(*) FROM context_refs WHERE tenant_id = 'acme'`),
		"retry must not duplicate the ref entity")
	refs, err := ctxSvc.ListByTask(ctx, "acme", "t-idem-1")
	require.NoError(t, err)
	require.Len(t, refs, 1)
	assert.Equal(t, refID, refs[0].ID)
}

// (d) A forced bind failure mid-transaction must roll back the WHOLE
// create: no task row, no context_refs row, no outbox row, no bind row.
func TestContextRefPG_Create_BindFailureRollsBackAll(t *testing.T) {
	env, _ := setupCtxRefEnv(t)
	ctx := context.Background()

	pgRepo := postgres.NewContextRefRepo(env.pool)
	failing := &ContextRefService{repo: pgRepo, txRepo: failBindTxRepo{ContextRefRepo: pgRepo, err: errors.New("forced bind failure")}}
	env.taskSvc = NewTaskService(env.taskRepo, env.driver, env.pool, env.outboxRepo).
		WithAttemptRepo(env.attemptRepo).
		WithContextRefService(failing)

	refs := []core.ContextRef{{Type: "file", URI: "s3://doom"}}
	_, err := env.taskSvc.Create(ctx, ctxRefTask("t-bindfail", refs))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "context ref bind")

	// Prepare ran before the transaction and mutated the caller's slice,
	// so the generated (never-committed) ref ID is observable here.
	generatedID := refs[0].ID
	require.NotEmpty(t, generatedID)

	assert.Zero(t, countRows(t, env, `SELECT count(*) FROM tasks WHERE tenant_id = 'acme' AND id = 't-bindfail'`),
		"task row must roll back")
	assert.Zero(t, countRows(t, env, `SELECT count(*) FROM context_refs WHERE tenant_id = 'acme' AND id = $1`, generatedID),
		"context_refs row must roll back")
	assert.Zero(t, countRows(t, env, `SELECT count(*) FROM outbox_events WHERE tenant_id = 'acme'`),
		"outbox row must roll back")
	assert.Zero(t, countRows(t, env, `SELECT count(*) FROM task_context_refs WHERE tenant_id = 'acme' AND task_id = 't-bindfail'`),
		"bind row must roll back")
}
