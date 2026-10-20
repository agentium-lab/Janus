package service

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

// gateApprovalRepo drives the Replay approval gate with a controllable
// latest verdict or lookup error.
type gateApprovalRepo struct {
	mockApprovalRepo
	latest *core.Approval
	err    error
}

func (m *gateApprovalRepo) GetLatestByTask(_ context.Context, _, _ string) (*core.Approval, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.latest, nil
}

func newReplayGateSvc(repo *gateApprovalRepo) *TaskService {
	svc := NewTaskService(&cbTaskRepo{tasks: map[string]*core.Task{
		"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusCompleted, MailboxID: "mb1"},
	}}, &mockQueueDriver{}, nil, nil)
	return svc.WithApproval(NewApprovalService(repo, nil, nil))
}

func TestCov_TaskService_Replay_ApprovalGate_FailClosed(t *testing.T) {
	ctx := context.Background()

	t.Run("lookup error fails closed", func(t *testing.T) {
		svc := newReplayGateSvc(&gateApprovalRepo{err: errors.New("db down")})
		_, err := svc.Replay(ctx, "acme", "t1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "replay approval check")
	})

	t.Run("expired verdict denies replay", func(t *testing.T) {
		svc := newReplayGateSvc(&gateApprovalRepo{latest: &core.Approval{Decision: "expired"}})
		_, err := svc.Replay(ctx, "acme", "t1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "approval expired")
	})

	t.Run("pending verdict denies replay", func(t *testing.T) {
		svc := newReplayGateSvc(&gateApprovalRepo{latest: &core.Approval{Status: "pending"}})
		_, err := svc.Replay(ctx, "acme", "t1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "still pending")
	})

	t.Run("approved verdict proceeds", func(t *testing.T) {
		svc := newReplayGateSvc(&gateApprovalRepo{latest: &core.Approval{Decision: "approved"}})
		_, err := svc.Replay(ctx, "acme", "t1")
		require.NoError(t, err)
	})
}

func TestCov_ContextRef_PrepareAndBind_Paths(t *testing.T) {
	ctx := context.Background()

	t.Run("prepare stub ref stays unbound", func(t *testing.T) {
		repo := &cbCtxRefRepo{}
		svc := NewContextRefService(repo)
		refs := []core.ContextRef{{TenantID: "acme"}}
		plan, err := svc.Prepare(ctx, "acme", refs)
		require.NoError(t, err)
		assert.Empty(t, refs[0].ID, "stub ref must not gain an ID")
		assert.Empty(t, plan, "stub ref must not be planned")
	})

	t.Run("prepare new ref generates id and created_at", func(t *testing.T) {
		repo := &cbCtxRefRepo{}
		svc := NewContextRefService(repo)
		refs := []core.ContextRef{{TenantID: "acme", Type: "file", URI: "s3://x"}}
		plan, err := svc.Prepare(ctx, "acme", refs)
		require.NoError(t, err)
		assert.NotEmpty(t, refs[0].ID)
		assert.NotEmpty(t, refs[0].CreatedAt)
		require.Len(t, plan, 1)
		assert.True(t, plan[0].IsNew, "generated ref must be planned as new (insert + bind)")
		assert.Nil(t, plan[0].Trusted)
	})

	t.Run("prepare existing ref plans reuse with trusted record", func(t *testing.T) {
		repo := &cbCtxRefRepo{getResult: &core.ContextRef{ID: "r1", TenantID: "acme", Type: "file", URI: "s3://trusted"}}
		svc := NewContextRefService(repo)
		refs := []core.ContextRef{{TenantID: "acme", ID: "r1", Type: "file", URI: "s3://caller-supplied"}}
		plan, err := svc.Prepare(ctx, "acme", refs)
		require.NoError(t, err)
		require.Len(t, plan, 1)
		assert.False(t, plan[0].IsNew, "existing ref must be planned as reuse (bind only)")
		require.NotNil(t, plan[0].Trusted)
		assert.Equal(t, "s3://trusted", plan[0].Ref.URI, "reuse must bind the trusted record, not caller fields")
		assert.Equal(t, "s3://trusted", refs[0].URI, "caller ref must be backfilled from the trusted record")
	})

	t.Run("prepare tenant mismatch rejected", func(t *testing.T) {
		svc := NewContextRefService(&cbCtxRefRepo{})
		refs := []core.ContextRef{{TenantID: "other", Type: "file", URI: "s3://x"}}
		_, err := svc.Prepare(ctx, "acme", refs)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "tenant mismatch")
	})

	t.Run("pool bind inserts new refs and binds them", func(t *testing.T) {
		repo := newMockContextRefRepo()
		svc := NewContextRefService(repo)
		refs := []core.ContextRef{{TenantID: "acme", Type: "file", URI: "s3://x"}}
		require.NoError(t, svc.NormalizeAndBind(ctx, "acme", "t-1", refs))
		require.Len(t, repo.binds, 1)
		assert.Equal(t, "t-1:"+refs[0].ID, repo.binds[0])
		assert.Contains(t, repo.refs, refs[0].ID, "new ref must be INSERTED, not only bound")
	})

	t.Run("tx plan of prepared refs inserts new and binds", func(t *testing.T) {
		repo := newMockContextRefRepo()
		svc := NewContextRefService(repo)
		refs := []core.ContextRef{{TenantID: "acme", Type: "file", URI: "s3://x"}}
		plan, err := svc.Prepare(ctx, "acme", refs)
		require.NoError(t, err)
		require.Len(t, plan, 1)
		assert.True(t, plan[0].IsNew)
		// MemoryLifecycle passes nil tx; the adapter degrades to pool calls.
		require.NoError(t, svc.ApplyPlanTx(ctx, nil, "acme", "t-1", plan))
		assert.Equal(t, []string{"t-1:" + refs[0].ID}, repo.binds)
		assert.Contains(t, repo.refs, refs[0].ID, "planned new ref must be INSERTED inside the tx path")
	})

	t.Run("tx plan of reused refs binds without insert", func(t *testing.T) {
		repo := newMockContextRefRepo()
		repo.refs["r1"] = &core.ContextRef{ID: "r1", TenantID: "acme", Type: "file", URI: "s3://trusted"}
		svc := NewContextRefService(repo)
		plan, err := svc.Prepare(ctx, "acme", []core.ContextRef{{TenantID: "acme", ID: "r1"}})
		require.NoError(t, err)
		require.Len(t, plan, 1)
		assert.False(t, plan[0].IsNew)
		require.NoError(t, svc.ApplyPlanTx(ctx, nil, "acme", "t-2", plan))
		assert.Equal(t, []string{"t-2:r1"}, repo.binds)
		assert.Len(t, repo.refs, 1, "reused ref must NOT be re-inserted")
	})
}

func TestCov_TaskService_Create_NewContextRefs_TxBind(t *testing.T) {
	ctx := context.Background()
	repo := newMockContextRefRepo()
	svc := NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil).
		WithContextRefService(NewContextRefService(repo))
	task := core.Task{
		TenantID: "acme", ID: "t-newref", SourceAgent: "agent-a",
		TargetType: core.TargetTypeCapability, TargetValue: "review",
		Envelope: makeTestEnvelope("t-newref", "acme"),
	}
	task.Envelope.ContextRefs = []core.ContextRef{{Type: "file", URI: "s3://x"}}
	created, err := svc.Create(ctx, task)
	require.NoError(t, err)
	require.NotNil(t, created)
	// Prepare mutates the caller's slice in place: the generated ID is
	// visible here even though Create takes the task by value.
	refID := task.Envelope.ContextRefs[0].ID
	require.NotEmpty(t, refID, "Prepare must generate the ref ID")
	assert.Contains(t, repo.refs, refID, "create must INSERT the generated context ref entity (regression: bind-only left ListByTask empty)")
	assert.Equal(t, []string{"t-newref:" + refID}, repo.binds, "create must bind the generated context ref to the task")
}
