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
		require.NoError(t, svc.Prepare(ctx, "acme", refs))
		assert.Empty(t, refs[0].ID, "stub ref must not gain an ID")
	})

	t.Run("prepare new ref generates id and created_at", func(t *testing.T) {
		repo := &cbCtxRefRepo{}
		svc := NewContextRefService(repo)
		refs := []core.ContextRef{{TenantID: "acme", Type: "file", URI: "s3://x"}}
		require.NoError(t, svc.Prepare(ctx, "acme", refs))
		assert.NotEmpty(t, refs[0].ID)
		assert.NotEmpty(t, refs[0].CreatedAt)
	})

	t.Run("prepare tenant mismatch rejected", func(t *testing.T) {
		svc := NewContextRefService(&cbCtxRefRepo{})
		refs := []core.ContextRef{{TenantID: "other", Type: "file", URI: "s3://x"}}
		err := svc.Prepare(ctx, "acme", refs)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "tenant mismatch")
	})

	t.Run("pool bind inserts new refs and binds them", func(t *testing.T) {
		repo := &cbCtxRefRepo{}
		svc := NewContextRefService(repo)
		refs := []core.ContextRef{{TenantID: "acme", Type: "file", URI: "s3://x"}}
		require.NoError(t, svc.NormalizeAndBind(ctx, "acme", "t-1", refs))
		require.Len(t, repo.binds, 1)
		assert.Equal(t, refs[0].ID, repo.binds[0])
	})

	t.Run("tx bind of prepared refs binds without lookup", func(t *testing.T) {
		repo := &cbCtxRefRepo{}
		svc := NewContextRefService(repo)
		refs := []core.ContextRef{{TenantID: "acme", Type: "file", URI: "s3://x"}}
		require.NoError(t, svc.Prepare(ctx, "acme", refs))
		// MemoryLifecycle passes nil tx; the adapter degrades to pool calls.
		require.NoError(t, svc.NormalizeAndBindTx(ctx, nil, "acme", "t-1", refs))
		assert.Equal(t, []string{refs[0].ID}, repo.binds)
	})
}

func TestCov_TaskService_Create_NewContextRefs_TxBind(t *testing.T) {
	ctx := context.Background()
	svc := NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil).
		WithContextRefService(NewContextRefService(&cbCtxRefRepo{}))
	task := core.Task{
		TenantID: "acme", ID: "t-newref", SourceAgent: "agent-a",
		TargetType: core.TargetTypeCapability, TargetValue: "review",
		Envelope: makeTestEnvelope("t-newref", "acme"),
	}
	task.Envelope.ContextRefs = []core.ContextRef{{Type: "file", URI: "s3://x"}}
	created, err := svc.Create(ctx, task)
	require.NoError(t, err)
	require.NotNil(t, created)
}
