package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/auth"
	"github.com/agentium-lab/Janus/server/internal/driver/postgres"
)

// cgBudgetTxRepo serves per-scope specs to CheckConcurrencyTx.
type cgBudgetTxRepo struct {
	mockBudgetRepo
	agentSpec  *core.BudgetSpec
	tenantSpec *core.BudgetSpec
	agentErr   error
	tenantErr  error
}

func (m *cgBudgetTxRepo) GetTx(_ context.Context, _ pgx.Tx, _ string, scopeType core.BudgetScopeType, _ string) (*core.BudgetSpec, error) {
	if scopeType == core.BudgetScopeAgent {
		if m.agentErr != nil {
			return nil, m.agentErr
		}
		if m.agentSpec == nil {
			return nil, pgx.ErrNoRows
		}
		return m.agentSpec, nil
	}
	if m.tenantErr != nil {
		return nil, m.tenantErr
	}
	if m.tenantSpec == nil {
		return nil, pgx.ErrNoRows
	}
	return m.tenantSpec, nil
}

func TestCov_CheckConcurrencyTx(t *testing.T) {
	ctx := context.Background()

	t.Run("non-tx repo falls back to pool path", func(t *testing.T) {
		svc := NewBudgetService(&mockBudgetRepo{})
		require.NoError(t, svc.CheckConcurrencyTx(ctx, nil, "acme", "a1", 0, 0))
	})

	t.Run("tenant id required", func(t *testing.T) {
		svc := NewBudgetService(&cgBudgetTxRepo{})
		require.ErrorContains(t, svc.CheckConcurrencyTx(ctx, nil, "", "a1", 0, 0), "tenant id is required")
	})

	t.Run("agent cap enforced", func(t *testing.T) {
		svc := NewBudgetService(&cgBudgetTxRepo{agentSpec: &core.BudgetSpec{MaxConcurrency: 2}})
		require.NoError(t, svc.CheckConcurrencyTx(ctx, nil, "acme", "a1", 1, 0))
		var bpe *core.BackpressureError
		err := svc.CheckConcurrencyTx(ctx, nil, "acme", "a1", 2, 0)
		require.ErrorAs(t, err, &bpe)
		assert.Equal(t, core.ReasonAgentConcurrencyExceeded, bpe.Reason)
		assert.Contains(t, bpe.Message, "agent a1: 2/2")
	})

	t.Run("tenant cap enforced", func(t *testing.T) {
		svc := NewBudgetService(&cgBudgetTxRepo{tenantSpec: &core.BudgetSpec{MaxConcurrency: 3}})
		var bpe *core.BackpressureError
		err := svc.CheckConcurrencyTx(ctx, nil, "acme", "a1", 0, 3)
		require.ErrorAs(t, err, &bpe)
		assert.Contains(t, bpe.Message, "tenant acme: 3/3")
	})

	t.Run("lookup failures propagate", func(t *testing.T) {
		svc := NewBudgetService(&cgBudgetTxRepo{agentErr: errors.New("boom")})
		require.ErrorContains(t, svc.CheckConcurrencyTx(ctx, nil, "acme", "a1", 0, 0), "agent budget lookup")
		svc = NewBudgetService(&cgBudgetTxRepo{tenantErr: errors.New("boom")})
		require.ErrorContains(t, svc.CheckConcurrencyTx(ctx, nil, "acme", "a1", 0, 0), "tenant budget lookup")
	})

	t.Run("no rows means no cap", func(t *testing.T) {
		svc := NewBudgetService(&cgBudgetTxRepo{})
		require.NoError(t, svc.CheckConcurrencyTx(ctx, nil, "acme", "a1", 99, 99))
	})
}

type cgUsageRepo struct {
	settled   []string
	released  []string
	settleErr error
}

func (m *cgUsageRepo) ReserveTask(context.Context, string, string, string) error { return nil }

func (m *cgUsageRepo) SettleUsage(_ context.Context, _, _, scopeID string, tokens int, costUSD float64) error {
	if m.settleErr != nil {
		return m.settleErr
	}
	m.settled = append(m.settled, fmt.Sprintf("%s:%d:%.5f", scopeID, tokens, costUSD))
	return nil
}

func (m *cgUsageRepo) ReleaseTask(_ context.Context, _, _, scopeID string) error {
	m.released = append(m.released, scopeID)
	return nil
}

func (m *cgUsageRepo) GetDailyUsage(context.Context, string, string, string) (int, float64, int, error) {
	return 0, 0, 0, nil
}

func TestCov_SettleScope(t *testing.T) {
	ctx := context.Background()

	t.Run("nil usage repo is a no-op", func(t *testing.T) {
		require.NoError(t, NewBudgetService(&mockBudgetRepo{}).SettleScope(ctx, "acme", core.BudgetScopeAgent, "a1", &core.TokenUsage{TotalTokens: 10}))
	})

	usage := &cgUsageRepo{}
	svc := NewBudgetServiceWithUsage(&mockBudgetRepo{}, usage)

	t.Run("agent scope settles usage and releases the reservation", func(t *testing.T) {
		require.NoError(t, svc.SettleScope(ctx, "acme", core.BudgetScopeAgent, "a1", &core.TokenUsage{PromptTokens: 3, CompletionTokens: 7, TotalTokens: 10}))
		require.Len(t, usage.settled, 1)
		assert.Contains(t, usage.settled[0], "a1:10:")
		assert.Equal(t, []string{"a1"}, usage.released)
	})

	t.Run("tenant scope settles but never releases", func(t *testing.T) {
		require.NoError(t, svc.SettleScope(ctx, "acme", core.BudgetScopeTenant, "acme", nil))
		assert.Len(t, usage.settled, 1, "nil usage settles nothing")
		assert.Len(t, usage.released, 1, "tenant scope holds no reservation")
	})

	t.Run("settle failure propagates", func(t *testing.T) {
		failing := NewBudgetServiceWithUsage(&mockBudgetRepo{}, &cgUsageRepo{settleErr: errors.New("ledger down")})
		require.Error(t, failing.SettleScope(ctx, "acme", core.BudgetScopeAgent, "a1", &core.TokenUsage{TotalTokens: 1}))
	})
}

func TestCov_MemoryBudgetLedger(t *testing.T) {
	usage := &cgUsageRepo{}
	ledger := MemoryBudgetLedger(NewBudgetServiceWithUsage(&mockBudgetRepo{}, usage))
	require.NoError(t, ledger.SettleTx(context.Background(), nil, core.LedgerEntry{
		TenantID: "acme", ScopeType: "agent", ScopeID: "a1",
		PromptTokens: 2, CompletionTokens: 3, TotalTokens: 5,
	}))
	require.Len(t, usage.settled, 1)
	assert.Contains(t, usage.settled[0], "a1:5:")
	assert.Equal(t, []string{"a1"}, usage.released, "agent ledger entries release the reservation")
}

func TestCov_LogOutboxWrite(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)
	logOutboxWrite("task-9", errors.New("disk full"))
	assert.Contains(t, buf.String(), "task-9")
	assert.Contains(t, buf.String(), "disk full")
}

func TestCov_ContextRefRepoTxAdapter(t *testing.T) {
	ctx := context.Background()
	inner := &cbCtxRefRepo{}
	adapter := ContextRefRepoTxAdapter{ContextRefRepo: inner}
	require.NoError(t, adapter.InsertTx(ctx, nil, core.ContextRef{ID: "r1"}))
	require.NoError(t, adapter.BindToTaskTx(ctx, nil, "acme", "t1", "r1"))
	assert.Equal(t, []string{"r1"}, inner.binds)

	failing := ContextRefRepoTxAdapter{ContextRefRepo: &cbCtxRefRepo{insertErr: errors.New("nope")}}
	require.Error(t, failing.InsertTx(ctx, nil, core.ContextRef{}))
}

func TestCov_ApprovalRepoTxAdapter_GetLatestByTask(t *testing.T) {
	adapter := ApprovalRepoTxAdapter{ApprovalRepo: &gateApprovalRepo{latest: &core.Approval{ID: "ap-1", Decision: "approved"}}}
	got, err := adapter.GetLatestByTask(context.Background(), "acme", "t1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "ap-1", got.ID)
}

type cgExec struct{}

func (cgExec) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}
func (cgExec) Query(context.Context, string, ...any) (pgx.Rows, error) { return nil, nil }
func (cgExec) QueryRow(context.Context, string, ...any) pgx.Row      { return nil }

type cgEventRepo struct {
	mockEventRepo
	insertedOn []core.JanusEvent
	execUsed   postgres.Executor
}

func (m *cgEventRepo) InsertOn(_ context.Context, exec postgres.Executor, evt core.JanusEvent) error {
	m.execUsed = exec
	m.insertedOn = append(m.insertedOn, evt)
	return nil
}

func TestCov_EventService_RecordIdempotentPaths(t *testing.T) {
	ctx := context.Background()

	t.Run("RecordIdempotent inserts through the repo", func(t *testing.T) {
		repo := &mockEventRepo{}
		svc := NewEventService(repo)
		evt := core.JanusEvent{EventType: core.EventTaskCompleted, TenantID: "acme", TaskID: "t1"}
		require.NoError(t, svc.RecordIdempotent(ctx, evt))
		require.Len(t, repo.events, 1)
		assert.NotEmpty(t, repo.events[0].EventID)
		assert.Equal(t, []byte(`{}`), repo.events[0].Payload, "nil payload normalized to empty object")
	})

	t.Run("RecordIdempotentOn uses the executor-scoped repo", func(t *testing.T) {
		repo := &cgEventRepo{}
		svc := NewEventService(repo)
		exec := cgExec{}
		require.NoError(t, svc.RecordIdempotentOn(ctx, exec, core.JanusEvent{EventType: core.EventTaskCompleted, TenantID: "acme"}))
		require.Len(t, repo.insertedOn, 1)
		assert.Equal(t, postgres.Executor(exec), repo.execUsed)
	})

	t.Run("RecordIdempotentOn falls back without executor support", func(t *testing.T) {
		repo := &mockEventRepo{}
		svc := NewEventService(repo)
		require.NoError(t, svc.RecordIdempotentOn(ctx, cgExec{}, core.JanusEvent{TenantID: "acme"}))
		require.Len(t, repo.events, 1)
		require.NoError(t, svc.RecordIdempotentOn(ctx, nil, core.JanusEvent{TenantID: "acme"}))
		assert.Len(t, repo.events, 2, "nil executor also falls back to the pool path")
	})
}

func TestCov_TaskService_Cancel(t *testing.T) {
	ctx := context.Background()

	t.Run("ids required", func(t *testing.T) {
		svc := NewTaskService(&cbTaskRepo{tasks: map[string]*core.Task{}}, &mockQueueDriver{}, nil, nil)
		require.ErrorContains(t, svc.Cancel(ctx, "", "t1"), "tenant id and task id are required")
		require.ErrorContains(t, svc.Cancel(ctx, "acme", ""), "tenant id and task id are required")
	})

	t.Run("repo lookup failure surfaces", func(t *testing.T) {
		svc := NewTaskService(&cbTaskRepo{getErr: errors.New("db down")}, &mockQueueDriver{}, nil, nil)
		require.ErrorContains(t, svc.Cancel(ctx, "acme", "t1"), "get task for transition")
	})

	t.Run("terminal task rejected", func(t *testing.T) {
		repo := &cbTaskRepo{tasks: map[string]*core.Task{
			"acme:done": {ID: "done", TenantID: "acme", Status: core.TaskStatusCompleted},
		}}
		svc := NewTaskService(repo, &mockQueueDriver{}, nil, nil)
		require.ErrorIs(t, svc.Cancel(ctx, "acme", "done"), core.ErrTaskInTerminalState)
	})

	t.Run("invalid transition rejected", func(t *testing.T) {
		repo := &cbTaskRepo{tasks: map[string]*core.Task{
			"acme:fresh": {ID: "fresh", TenantID: "acme", Status: core.TaskStatusCreated},
		}}
		svc := NewTaskService(repo, &mockQueueDriver{}, nil, nil)
		require.ErrorIs(t, svc.Cancel(ctx, "acme", "fresh"), core.ErrInvalidTransition)
	})

	t.Run("queued task cancels", func(t *testing.T) {
		repo := &cbTaskRepo{tasks: map[string]*core.Task{
			"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusQueued},
		}}
		drv := &mockQueueDriver{}
		svc := NewTaskService(repo, drv, nil, nil)
		require.NoError(t, svc.Cancel(ctx, "acme", "t1"))
		assert.Equal(t, core.TaskStatusCancelled, repo.tasks["acme:t1"].Status)
		assert.NotEmpty(t, drv.publishedEvents, "cancelled event published")
	})

	t.Run("open-attempt finish failure rolls into the error", func(t *testing.T) {
		repo := &cbTaskRepo{tasks: map[string]*core.Task{
			"acme:t2": {ID: "t2", TenantID: "acme", Status: core.TaskStatusQueued},
		}}
		svc := NewTaskService(repo, &mockQueueDriver{}, nil, nil).
			WithAttemptRepo(&cgFinishErrAttemptRepo{finishErr: errors.New("attempt store down")})
		require.ErrorContains(t, svc.Cancel(ctx, "acme", "t2"), "finish open attempts for cancel")
	})
}

type cgFinishErrAttemptRepo struct {
	mockDispatchAttemptRepo
	finishErr error
}

func (m *cgFinishErrAttemptRepo) FinishOpenAttempts(context.Context, string, string, string) (int64, error) {
	return 0, m.finishErr
}

func TestCov_ErrorMessages(t *testing.T) {
	assert.Contains(t, (&ReplayConflictError{TaskID: "t-409"}).Error(), "t-409")
	assert.Contains(t, (&IdempotentError{ExistingTaskID: "t-1"}).Error(), "t-1")
}

func TestCov_Create_OverwritesSourceAgentFromBoundPrincipal(t *testing.T) {
	repo := &cbTaskRepo{tasks: map[string]*core.Task{}}
	svc := NewTaskService(repo, &mockQueueDriver{}, nil, nil)
	ctx := context.WithValue(context.Background(), auth.PrincipalCtxKey, auth.Principal{BoundAgentID: "a1"})

	created, err := svc.Create(ctx, core.Task{
		TenantID: "acme", ID: "t-auth", SourceAgent: "imposter",
		TargetType: core.TargetTypeCapability, TargetValue: "code_review",
		Envelope: core.TaskEnvelope{
			JanusVersion: "1.0", TaskID: "t-auth", TenantID: "acme",
			SourceAgent: "imposter",
			Target:   core.Target{Type: core.TargetTypeCapability, Value: "code_review"},
			Trace:    core.TraceContext{TraceID: "ctx-t-auth"},
			Payload:  core.Payload{Type: "text", Content: "hi"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created)
	assert.Equal(t, "a1", created.SourceAgent, "bound principal identity wins over caller declaration")
	assert.Equal(t, "a1", created.Envelope.SourceAgent)
}

func TestCov_Create_ToolPolicyEventCarriesClaimedActor(t *testing.T) {
	drv := &mockQueueDriver{}
	repo := &cbTaskRepo{tasks: map[string]*core.Task{}}
	svc := NewTaskService(repo, drv, nil, nil).WithPolicy(NewPolicyService(&mockPolicyRuleRepo{}))
	ctx := context.WithValue(context.Background(), auth.ActingUserCtxKey, "alice")

	created, err := svc.Create(ctx, core.Task{
		TenantID: "acme", ID: "t-tool", SourceAgent: "a1",
		TargetType: core.TargetTypeCapability, TargetValue: "code_review",
		Envelope: core.TaskEnvelope{
			JanusVersion: "1.0", TaskID: "t-tool", TenantID: "acme", SourceAgent: "a1",
			Target:        core.Target{Type: core.TargetTypeCapability, Value: "code_review"},
			Trace:         core.TraceContext{TraceID: "ctx-t-tool"},
			Payload:        core.Payload{Type: "text", Content: "hi"},
			ToolInvocation: &core.ToolInvocation{Name: "run_tests"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created)

	found := false
	for _, evt := range drv.publishedEvents {
		if evt.EventType == core.EventToolInvocationAllowed {
			found = true
			assert.Contains(t, string(evt.Payload), `"claimed_actor":"alice"`)
		}
	}
	assert.True(t, found, "expected a tool.invocation_allowed event carrying the claimed actor")
}
