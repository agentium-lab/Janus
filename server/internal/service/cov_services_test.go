package service

import (
	"os"
	"sync"
	"sync/atomic"
	"time"

	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/bootstrap"
	natsdriver "github.com/agentium-lab/Janus/server/internal/driver/nats"
	redisdriver "github.com/agentium-lab/Janus/server/internal/driver/redis"
)

// Remaining service behavior coverage: API keys, agents, tenants,
// policies, context refs, events, condition matching helpers.

func TestCov_APIKeyService_List(t *testing.T) {
	repo := &cbAPIKeyRepo{list: []core.APIKey{{TenantID: "acme", Name: "k1"}}}
	svc := NewAPIKeyService(repo)
	keys, err := svc.List(context.Background(), "acme")
	require.NoError(t, err)
	assert.Len(t, keys, 1)
}

func TestCov_APIKeyService_CreateRepoError(t *testing.T) {
	svc := NewAPIKeyService(&cbAPIKeyRepo{createErr: errors.New("db down")})
	_, _, err := svc.Create(context.Background(), "acme", "k", []string{"task:write"}, "")
	require.Error(t, err)
}

// --- approval ---

func TestCov_ContextRefService_AttachInsertError(t *testing.T) {
	svc := NewContextRefService(&cbCtxRefRepo{insertErr: errors.New("disk full")})
	_, err := svc.Attach(context.Background(), "acme", "file", "s3://x", "h", "public", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "attach context ref")
}

func TestCov_ContextRefService_NormalizeAndBind_Branches(t *testing.T) {
	ctx := context.Background()

	t.Run("tenant mismatch", func(t *testing.T) {
		svc := NewContextRefService(&cbCtxRefRepo{})
		err := svc.NormalizeAndBind(ctx, "acme", "t1", []core.ContextRef{{TenantID: "other", ID: "r1"}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "tenant mismatch")
	})

	t.Run("incomplete ref skipped", func(t *testing.T) {
		repo := &cbCtxRefRepo{}
		svc := NewContextRefService(repo)
		err := svc.NormalizeAndBind(ctx, "acme", "t1", []core.ContextRef{{TenantID: "acme"}})
		require.NoError(t, err)
		assert.Empty(t, repo.binds)
	})

	t.Run("new ref insert error", func(t *testing.T) {
		svc := NewContextRefService(&cbCtxRefRepo{insertErr: errors.New("disk full")})
		err := svc.NormalizeAndBind(ctx, "acme", "t1", []core.ContextRef{{TenantID: "acme", Type: "file", URI: "s3://x"}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "insert context ref")
	})

	t.Run("existing ref not found", func(t *testing.T) {
		svc := NewContextRefService(&cbCtxRefRepo{})
		err := svc.NormalizeAndBind(ctx, "acme", "t1", []core.ContextRef{{TenantID: "acme", ID: "missing"}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found in tenant")
	})

	t.Run("cross-tenant denied", func(t *testing.T) {
		svc := NewContextRefService(&cbCtxRefRepo{getResult: &core.ContextRef{ID: "r1", TenantID: "other"}})
		err := svc.NormalizeAndBind(ctx, "acme", "t1", []core.ContextRef{{TenantID: "acme", ID: "r1"}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cross-tenant")
	})

	t.Run("bind error", func(t *testing.T) {
		svc := NewContextRefService(&cbCtxRefRepo{getResult: &core.ContextRef{ID: "r1", TenantID: "acme"}, bindErr: errors.New("bind fail")})
		err := svc.NormalizeAndBind(ctx, "acme", "t1", []core.ContextRef{{TenantID: "acme", ID: "r1"},
			{TenantID: "acme", Type: "file", URI: "s3://x"}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "bind context ref")
	})

	t.Run("mixed refs bound", func(t *testing.T) {
		repo := &cbCtxRefRepo{getResult: &core.ContextRef{ID: "r1", TenantID: "acme"}}
		svc := NewContextRefService(repo)
		err := svc.NormalizeAndBind(ctx, "acme", "t1", []core.ContextRef{
			{TenantID: "acme", ID: "r1"},
			{TenantID: "acme", Type: "file", URI: "s3://y"},
		})
		require.NoError(t, err)
		require.Len(t, repo.binds, 2)
	})
}

// --- policy rule + tenant services ---

func TestCov_PolicyRuleService_List(t *testing.T) {
	repo := &mockPolicyRuleRepo{rules: []*core.PolicyRule{{TenantID: "acme", ID: "r1", Status: "active"}}}
	got, err := NewPolicyRuleService(repo).List(context.Background(), "acme")
	require.NoError(t, err)
	assert.Len(t, got, 1)
}

func TestCov_TenantService_List(t *testing.T) {
	ctx := context.Background()

	t.Run("success with names", func(t *testing.T) {
		repo := &cbTenantRepo{ids: []string{"acme", "globex"}, names: map[string]string{"acme": "Acme", "globex": "Globex"}}
		tenants, err := NewTenantService(repo).List(ctx)
		require.NoError(t, err)
		require.Len(t, tenants, 2)
		byID := map[string]string{}
		for _, tn := range tenants {
			byID[tn.ID] = tn.Name
		}
		assert.Equal(t, "Acme", byID["acme"])
		assert.Equal(t, "Globex", byID["globex"])
	})

	t.Run("name lookup failure falls back to id", func(t *testing.T) {
		repo := &cbTenantRepo{ids: []string{"acme"}, names: map[string]string{}, nameErr: map[string]bool{"acme": true}}
		tenants, err := NewTenantService(repo).List(ctx)
		require.NoError(t, err)
		require.Len(t, tenants, 1)
		assert.Equal(t, "acme", tenants[0].Name)
	})

	t.Run("list ids error", func(t *testing.T) {
		_, err := NewTenantService(&cbTenantRepo{listErr: errors.New("db down")}).List(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "list tenants")
	})
}

// --- event service ---

func TestCov_EventService_Record(t *testing.T) {
	ctx := context.Background()

	repo := &mockEventRepo{}
	err := NewEventService(repo).Record(ctx, core.JanusEvent{TenantID: "acme", TaskID: "t1"})
	require.NoError(t, err)
	require.Len(t, repo.events, 1)
	assert.Equal(t, []byte(`{}`), repo.events[0].Payload, "nil payload must default to empty JSON object")

	err = NewEventService(&mockEventRepo{err: errors.New("insert fail")}).Record(ctx, core.JanusEvent{})
	require.Error(t, err)
}

// --- agent service ---

func TestCov_AgentService_Heartbeat_NilDriver(t *testing.T) {
	svc := NewAgentService(&mockAgentRepo{}, nil, nil, nil)
	require.NoError(t, svc.Heartbeat(context.Background(), "acme", "a1"),
		"nil heartbeat driver is a guarded no-op")
	require.Error(t, svc.Heartbeat(context.Background(), "", "a1"))
}

func TestCov_AgentService_Heartbeat_PingError(t *testing.T) {
	svc := NewAgentService(&mockAgentRepo{}, nil, &mockHeartbeatDriver{err: errors.New("redis down")}, nil)
	err := svc.Heartbeat(context.Background(), "acme", "a1")
	require.NoError(t, err, "redis TTL mark is best-effort; the PG record is durable")
}

func TestCov_AgentService_Register_NestedErrors(t *testing.T) {
	ctx := context.Background()
	agent := core.Agent{ID: "a1", TenantID: "acme", DisplayName: "A", Capabilities: []core.AgentCapability{{Capability: "x"}}}

	err := NewAgentService(&cbAgentRepo{capErr: errors.New("cap fail")}, nil, &mockHeartbeatDriver{}, nil).
		Register(ctx, agent)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert capabilities")

	// The redis mark at registration is best-effort (PG is the durable
	// record): a redis-down driver no longer fails Register.
	err = NewAgentService(&mockAgentRepo{}, nil, &mockHeartbeatDriver{err: errors.New("redis down")}, nil).
		Register(ctx, agent)
	require.NoError(t, err)

	err = NewAgentService(&cbAgentRepo{statusErr: errors.New("status fail")}, nil, &mockHeartbeatDriver{}, nil).
		Register(ctx, agent)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "set online")
}

// --- dispatch service ---

func TestExtra_GenerateContextRefID(t *testing.T) {
	id, err := generateContextRefID()
	require.NoError(t, err)
	assert.Contains(t, id, "ctxref_")
	assert.Len(t, id, 7+20)
}

func TestExtra_GenerateContextRefID_Uniqueness(t *testing.T) {
	ids := make(map[string]bool)
	for i := 0; i < 100; i++ {
		id, err := generateContextRefID()
		require.NoError(t, err)
		assert.False(t, ids[id], "duplicate id generated: %s", id)
		ids[id] = true
	}
}

func TestExtra_MatchCondition_ValidCondition(t *testing.T) {
	condition := json.RawMessage(`{"actor.type":"agent"}`)
	input := map[string]interface{}{
		"actor": map[string]interface{}{
			"type": "agent",
		},
	}
	assert.True(t, matchCondition(condition, input))
}

func TestExtra_MatchCondition_InvalidJSON(t *testing.T) {
	condition := json.RawMessage(`invalid json`)
	input := map[string]interface{}{}
	assert.False(t, matchCondition(condition, input))
}

func TestExtra_MatchCondition_MissingKey(t *testing.T) {
	condition := json.RawMessage(`{"actor.type":"agent"}`)
	input := map[string]interface{}{
		"actor": map[string]interface{}{
			"type": "user",
		},
	}
	assert.False(t, matchCondition(condition, input))
}

func TestExtra_MatchCondition_MissingNestedKey(t *testing.T) {
	condition := json.RawMessage(`{"actor.type":"agent"}`)
	input := map[string]interface{}{
		"actor": map[string]interface{}{},
	}
	assert.False(t, matchCondition(condition, input))
}

func TestExtra_MatchCondition_MissingTopLevelKey(t *testing.T) {
	condition := json.RawMessage(`{"actor.type":"agent"}`)
	input := map[string]interface{}{}
	assert.False(t, matchCondition(condition, input))
}

func TestExtra_MatchCondition_DifferentValue(t *testing.T) {
	condition := json.RawMessage(`{"count":"100"}`)
	input := map[string]interface{}{
		"count": "200",
	}
	assert.False(t, matchCondition(condition, input))
}

func TestExtra_MatchCondition_MultiKeyMatch(t *testing.T) {
	condition := json.RawMessage(`{"actor.type":"agent","action":"dispatch"}`)
	input := map[string]interface{}{
		"actor":  map[string]interface{}{"type": "agent"},
		"action": "dispatch",
	}
	assert.True(t, matchCondition(condition, input))
}

func TestExtra_LookupNested_SimpleKey(t *testing.T) {
	m := map[string]interface{}{"key": "value"}
	val, ok := lookupNested(m, "key")
	assert.True(t, ok)
	assert.Equal(t, "value", val)
}

func TestExtra_LookupNested_NestedKey(t *testing.T) {
	m := map[string]interface{}{
		"outer": map[string]interface{}{
			"inner": "value",
		},
	}
	val, ok := lookupNested(m, "outer.inner")
	assert.True(t, ok)
	assert.Equal(t, "value", val)
}

func TestExtra_LookupNested_DeeplyNested(t *testing.T) {
	m := map[string]interface{}{
		"a": map[string]interface{}{
			"b": map[string]interface{}{
				"c": map[string]interface{}{
					"d": "value",
				},
			},
		},
	}
	val, ok := lookupNested(m, "a.b.c.d")
	assert.True(t, ok)
	assert.Equal(t, "value", val)
}

func TestExtra_LookupNested_MissingKey(t *testing.T) {
	m := map[string]interface{}{"key": "value"}
	_, ok := lookupNested(m, "nonexistent")
	assert.False(t, ok)
}

func TestExtra_LookupNested_MissingNestedKey(t *testing.T) {
	m := map[string]interface{}{
		"outer": map[string]interface{}{},
	}
	_, ok := lookupNested(m, "outer.inner")
	assert.False(t, ok)
}

func TestExtra_LookupNested_NonMapValue(t *testing.T) {
	m := map[string]interface{}{
		"key": "string not map",
	}
	_, ok := lookupNested(m, "key.nested")
	assert.False(t, ok)
}

func TestExtra_LookupNested_EmptyKey(t *testing.T) {
	m := map[string]interface{}{"": "value"}
	val, ok := lookupNested(m, "")
	assert.True(t, ok)
	assert.Equal(t, "value", val)
}

func TestExtra_ParseAction_ValidAllow(t *testing.T) {
	raw := json.RawMessage(`{"decision":"allow"}`)
	a, ok := parseAction(raw)
	assert.True(t, ok)
	assert.Equal(t, core.PolicyDecisionAllow, a.Decision)
}

func TestExtra_ParseAction_ValidDeny(t *testing.T) {
	raw := json.RawMessage(`{"decision":"deny"}`)
	a, ok := parseAction(raw)
	assert.True(t, ok)
	assert.Equal(t, core.PolicyDecisionDeny, a.Decision)
}

func TestExtra_ParseAction_InvalidJSON(t *testing.T) {
	raw := json.RawMessage(`invalid`)
	_, ok := parseAction(raw)
	assert.False(t, ok)
}

func TestExtra_ParseAction_EmptyDecision(t *testing.T) {
	raw := json.RawMessage(`{"decision":""}`)
	_, ok := parseAction(raw)
	assert.False(t, ok)
}

func TestExtra_ParseAction_MissingDecision(t *testing.T) {
	raw := json.RawMessage(`{"other":"field"}`)
	_, ok := parseAction(raw)
	assert.False(t, ok)
}

func TestExtra_RecordTaskMetric_Completed(t *testing.T) {
	recordTaskMetric("acme", core.TaskStatusCompleted)
}

func TestExtra_RecordTaskMetric_Failed(t *testing.T) {
	recordTaskMetric("acme", core.TaskStatusFailed)
}

func TestExtra_RecordTaskMetric_DeadLettered(t *testing.T) {
	recordTaskMetric("acme", core.TaskStatusDeadLettered)
}

func TestExtra_RecordTaskMetric_OtherStatus(t *testing.T) {
	recordTaskMetric("acme", core.TaskStatusRunning)
	recordTaskMetric("acme", core.TaskStatusQueued)
}

func TestExtra_Fail_WithTaskError(t *testing.T) {
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusRunning},
	}}
	svc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	ctx := context.Background()

	taskErr := &core.TaskError{Code: "ERR", Message: "something went wrong"}
	err := svc.Fail(ctx, "acme", "t1", taskErr)
	require.NoError(t, err)

	got, _ := svc.Get(ctx, "acme", "t1")
	assert.Equal(t, core.TaskStatusFailed, got.Status)
}

func TestExtra_Fail_NilTaskError(t *testing.T) {
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusRunning},
	}}
	svc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	ctx := context.Background()

	err := svc.Fail(ctx, "acme", "t1", nil)
	require.NoError(t, err)
}

func TestExtra_Create_WithIdempotencyKey(t *testing.T) {
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:existing": {ID: "existing", TenantID: "acme", IdempotencyKey: "idem-1"},
	}}
	svc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	ctx := context.Background()

	result, err := svc.Create(ctx, core.Task{
		TenantID:       "acme",
		ID:             "new-task",
		SourceAgent:    "agent-a",
		TargetType:     core.TargetTypeCapability,
		TargetValue:    "test",
		IdempotencyKey: "idem-1",
		Envelope:       makeTestEnvelope("new-task", "acme"),
	})
	require.NoError(t, err)
	assert.Equal(t, "existing", result.ID)
}

func TestExtra_TaskHeartbeat_GetLatestError(t *testing.T) {
	svc, _, _, _ := newTestDispatchSvc()
	ctx := context.Background()

	err := svc.TaskHeartbeat(ctx, "acme", "task-1", "lease-abc")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get latest attempt")
}

func TestExtra_Block_RepoError(t *testing.T) {
	taskRepo := &mockTaskRepo{err: fmt.Errorf("db error")}
	svc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	ctx := context.Background()

	err := svc.Block(ctx, "acme", "t1", "reason")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "block task")
}

func TestExtra_MemoryLifecycle_ApplyTx_RunsFn(t *testing.T) {
	ls := NewMemoryLifecycle()
	ran := false
	err := ls.ApplyTx(context.Background(), func(tx pgx.Tx) error {
		ran = true
		assert.Nil(t, tx)
		return nil
	})
	assert.NoError(t, err)
	assert.True(t, ran)
}

func TestExtra_Create_EnvelopeValidationError(t *testing.T) {
	svc := NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil)
	ctx := context.Background()

	_, err := svc.Create(ctx, core.Task{
		TenantID: "acme", ID: "t1", SourceAgent: "a",
		TargetType: core.TargetTypeCapability, TargetValue: "r",
		Envelope: core.TaskEnvelope{},
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "envelope validation")
}

func TestExtra_LifecycleService_ApplyTx_BeginError(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "host=/tmp port=5433 user=silv dbname=nonexistent connect_timeout=2")
	require.NoError(t, err)
	defer pool.Close()
	ls := NewPGLifecycle(pool)
	err = ls.ApplyTx(context.Background(), func(tx pgx.Tx) error { return nil })
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "begin tx")
}

func TestExtra_LifecycleService_ApplyTx_FnError(t *testing.T) {
	pool := openExtraTestPool(t)
	ls := NewPGLifecycle(pool)
	err := ls.ApplyTx(context.Background(), func(tx pgx.Tx) error {
		return fmt.Errorf("fn error")
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "fn error")
}

func TestExtra_LifecycleService_ApplyTx_CommitError(t *testing.T) {
	pool := openExtraTestPool(t)
	ls := NewPGLifecycle(pool)
	err := ls.ApplyTx(context.Background(), func(tx pgx.Tx) error {
		_ = tx.Rollback(context.Background())
		return nil
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "commit lifecycle tx")
}

// fakeReconcileDriver flips between failing and succeeding so the pending
// drain can be observed.
type fakeReconcileDriver struct {
	mockQueueDriver
	failuresLeft int
	calls        int
	specs        []core.ConsumerSpec
}

func (d *fakeReconcileDriver) ReconcileConsumer(_ context.Context, spec core.ConsumerSpec) error {
	d.calls++
	d.specs = append(d.specs, spec)
	if d.failuresLeft > 0 {
		d.failuresLeft--
		return errors.New("broker down")
	}
	return nil
}

func TestMailboxService_UpdateConfig_PendingReconcileDrains(t *testing.T) {
	drv := &fakeReconcileDriver{failuresLeft: 1}
	repo := newVersionRepo()
	repo.setMailbox("acme", "mb-1", 0)
	svc := NewMailboxService(repo, drv)
	ctx := context.Background()

	// First attempt fails: PG commits, spec is parked.
	require.NoError(t, svc.UpdateConfig(ctx, "acme", "mb-1", 4, 120, 5, 3600))
	assert.Equal(t, 1, drv.calls)

	svc.retryPending(ctx) // retry fails again (failuresLeft exhausted on 2nd? no: 0 left -> success)
	// failuresLeft was 1 and consumed by the first call, so this retry succeeds.
	assert.Equal(t, 2, drv.calls)

	svc.pendMu.Lock()
	pending := len(svc.pending)
	svc.pendMu.Unlock()
	assert.Equal(t, 0, pending, "spec must drain once the broker accepts it")
	require.Len(t, drv.specs, 2)
	assert.Equal(t, 120, drv.specs[0].ACKWaitSeconds)
	assert.Equal(t, "mb-1", drv.specs[0].MailboxID)
}

func TestMailboxService_UpdateConfig_NoReconciler_NoPanic(t *testing.T) {
	svc := NewMailboxService(&mockMailboxRepo{}, &mockQueueDriver{})
	require.NoError(t, svc.UpdateConfig(context.Background(), "acme", "mb-1", 4, 120, 5, 3600))
	svc.retryPending(context.Background()) // must no-op cleanly
}

func TestReconnectingLimiter_ReportsUnavailableUntilPromoted(t *testing.T) {
	// The dead-address config never connects; every check must surface the
	// sentinel so a fail-closed policy actually applies.
	u := NewReconnectingLimiter(redisdriver.Config{Addr: "127.0.0.1:59999"}, nil)
	err := u.CheckRPM(context.Background(), "acme", "agent", "a1", 5)
	require.ErrorIs(t, err, redisdriver.ErrThrottleUnavailable)
	err = u.CheckTPM(context.Background(), "acme", "agent", "a1", 100, 10)
	require.ErrorIs(t, err, redisdriver.ErrThrottleUnavailable)
}

func TestBudgetReserve_FailClosedWithUnreachableLimiter(t *testing.T) {
	spec := &core.BudgetSpec{TenantID: "acme", ScopeType: core.BudgetScopeAgent, ScopeID: "agent-1", RPM: 10, TPM: 100}
	repo := &cbBudgetSpecRepo{specs: []*core.BudgetSpec{spec}}
	svc := NewBudgetService(repo).
		WithRateLimiter(NewReconnectingLimiter(redisdriver.Config{Addr: "127.0.0.1:59999"}, nil)).
		WithThrottleFailureMode(ThrottleFailClosed)
	err := svc.Reserve(context.Background(), "acme", "agent-1", nil)
	var bp *core.BackpressureError
	require.ErrorAs(t, err, &bp, "fail-closed + startup-unreachable limiter must reject, not skip")
}

func TestBudgetReserve_UnbudgetedTaskStillCountsTPM(t *testing.T) {
	// TPM admission counts a minimal unit for un-budgeted tasks: a caller
	// omitting max_tokens must not bypass the tenant TPM cap.
	calls := 0
	var lastTokens int
	rl := &recordingTPM{calls: &calls, last: &lastTokens}
	spec := &core.BudgetSpec{TenantID: "acme", ScopeType: core.BudgetScopeAgent, ScopeID: "agent-1", RPM: 10, TPM: 100}
	repo := &cbBudgetSpecRepo{specs: []*core.BudgetSpec{spec}}
	svc := NewBudgetService(repo).WithRateLimiter(rl)
	require.NoError(t, svc.Reserve(context.Background(), "acme", "agent-1", nil))
	assert.Greater(t, *rl.last, 0, "admission estimate must be >= 1 even without a task budget")
}

type recordingTPM struct {
	calls *int
	last  *int
}

func (r *recordingTPM) CheckRPM(context.Context, string, string, string, int) error { return nil }
func (r *recordingTPM) CheckTPM(_ context.Context, _ string, _ string, _ string, _ int, tokens int) error {
	*r.calls++
	*r.last = tokens
	return nil
}

func TestMailboxService_StalePendingDoesNotResurrectAfterSuccess(t *testing.T) {
	// A fails and parks; B (newer revision for the same mailbox) applies
	// successfully; the retry loop must NOT write A back over B.
	var failNext bool
	traffic := 0
	drv := &reconcileRecorder{
		apply: func(spec core.ConsumerSpec) error {
			traffic++
			if failNext {
				failNext = false
				return errors.New("broker down")
			}
			return nil
		},
	}
	repo := newVersionRepo()
	repo.setMailbox("acme", "mb-1", 0)
	svc := NewMailboxService(repo, drv)
	ctx := context.Background()

	failNext = true
	require.NoError(t, svc.UpdateConfig(ctx, "acme", "mb-1", 4, 111, 5, 3600)) // A parks
	svc.pendMu.Lock()
	require.Len(t, svc.pending, 1)
	svc.pendMu.Unlock()

	require.NoError(t, svc.UpdateConfig(ctx, "acme", "mb-1", 8, 222, 9, 3600)) // B applies
	svc.pendMu.Lock()
	require.Empty(t, svc.pending, "successful apply must clear the stale parked spec")
	svc.pendMu.Unlock()

	svc.retryPending(ctx) // nothing parked: no stale A resurrection possible
	assert.Equal(t, 2, traffic)

	var last core.ConsumerSpec
	drv.mu.Lock()
	if len(drv.specs) > 0 {
		last = drv.specs[len(drv.specs)-1]
	}
	drv.mu.Unlock()
	assert.Equal(t, 222, last.ACKWaitSeconds, "newest revision must win")
	assert.Equal(t, 16, last.MaxACKPending, "maxConcurrency*2 parity on updates")
}

type reconcileRecorder struct {
	mu    sync.Mutex
	specs []core.ConsumerSpec
	apply func(core.ConsumerSpec) error
}

func (d *reconcileRecorder) PublishTask(context.Context, core.TaskMessage) error { return nil }
func (d *reconcileRecorder) PublishEvent(context.Context, core.JanusEvent) error { return nil }
func (d *reconcileRecorder) EnsureTenant(context.Context, string) error          { return nil }
func (d *reconcileRecorder) EnsureMailbox(context.Context, core.MailboxSpec) error {
	return nil
}
func (d *reconcileRecorder) EnsureConsumer(context.Context, core.ConsumerSpec) error { return nil }
func (d *reconcileRecorder) Close() error                                            { return nil }
func (d *reconcileRecorder) FetchTasks(context.Context, string, string, core.FetchOptions) ([]core.TaskDelivery, error) {
	return nil, nil
}
func (d *reconcileRecorder) AckTask(context.Context, string, core.DeliveryRef) error { return nil }
func (d *reconcileRecorder) NackTask(context.Context, string, core.DeliveryRef, core.NackReason) error {
	return nil
}
func (d *reconcileRecorder) PublishDLQ(context.Context, core.TaskMessage, []byte) error { return nil }
func (d *reconcileRecorder) ReplayEvents(context.Context, core.EventReplayFilter) (core.EventIterator, error) {
	return nil, nil
}
func (d *reconcileRecorder) SubscribeEvents(context.Context, chan<- core.JanusEvent) error {
	return nil
}
func (d *reconcileRecorder) ReconcileConsumer(ctx context.Context, spec core.ConsumerSpec) error {
	d.mu.Lock()
	d.specs = append(d.specs, spec)
	d.mu.Unlock()
	return d.apply(spec)
}

func TestMailboxService_ReconcileAllConsumers(t *testing.T) {
	drv := &reconcileRecorder{apply: func(core.ConsumerSpec) error { return nil }}
	repo := &listAllMailboxRepo{mailboxes: []*core.Mailbox{
		{TenantID: "acme", ID: "mb-1", AgentID: "a1", ACKWaitSeconds: 60, MaxDeliver: 5, MaxConcurrency: 4},
		{TenantID: "b", ID: "mb-2", AgentID: "a2", ACKWaitSeconds: 120, MaxDeliver: 9, MaxConcurrency: 8},
	}}
	svc := NewMailboxService(repo, drv)
	svc.ReconcileAllConsumers(context.Background())

	drv.mu.Lock()
	defer drv.mu.Unlock()
	require.Len(t, drv.specs, 2, "every durable mailbox config replays onto the broker")
	assert.Equal(t, 60, drv.specs[0].ACKWaitSeconds)
	assert.Equal(t, 8, drv.specs[0].MaxACKPending, "maxConcurrency*2 parity")
	assert.Equal(t, 120, drv.specs[1].ACKWaitSeconds)
}

// Failing entries park for the retry loop; a list error is logged, not fatal.
func TestMailboxService_ReconcileAllConsumers_ParksFailures(t *testing.T) {
	var calls int32
	drv := &reconcileRecorder{apply: func(core.ConsumerSpec) error {
		if atomic.AddInt32(&calls, 1) == 1 {
			return errors.New("broker down")
		}
		return nil
	}}
	repo := &listAllMailboxRepo{mailboxes: []*core.Mailbox{
		{TenantID: "acme", ID: "mb-1", AgentID: "a1", ACKWaitSeconds: 60, MaxConcurrency: 4},
		{TenantID: "acme", ID: "mb-2", AgentID: "a1", ACKWaitSeconds: 90, MaxConcurrency: 4},
	}}
	svc := NewMailboxService(repo, drv)
	svc.ReconcileAllConsumers(context.Background())

	svc.pendMu.Lock()
	defer svc.pendMu.Unlock()
	assert.Len(t, svc.pending, 1, "the failed spec is parked, the successful one is not")
}

type listAllMailboxRepo struct{ mailboxes []*core.Mailbox }

func (r *listAllMailboxRepo) Create(context.Context, core.Mailbox) error { return nil }
func (r *listAllMailboxRepo) Get(_ context.Context, tenantID, mailboxID string) (*core.Mailbox, error) {
	for _, mb := range r.mailboxes {
		if mb.TenantID == tenantID && mb.ID == mailboxID {
			return mb, nil
		}
	}
	return nil, pgx.ErrNoRows
}
func (r *listAllMailboxRepo) ListByAgent(context.Context, string, string) ([]*core.Mailbox, error) {
	return nil, nil
}
func (r *listAllMailboxRepo) ListAll(context.Context) ([]*core.Mailbox, error) {
	return r.mailboxes, nil
}
func (r *listAllMailboxRepo) Backlog(context.Context, string, string) (int, error) {
	return 0, nil
}
func (r *listAllMailboxRepo) UpdateStatus(context.Context, string, string, core.MailboxStatus) error {
	return nil
}
func (r *listAllMailboxRepo) UpdateConfig(context.Context, string, string, int, int, int, int) (int, error) {
	return 1, nil
}

// The startup replay must carry the mailbox's REAL config, not zero
// values: a partial ListAll read used to reset every custom
// ack_wait/max_deliver to the defaults on each boot.
func TestMailboxService_ReconcileAllUsesFullConfig(t *testing.T) {
	drv := &reconcileRecorder{apply: func(core.ConsumerSpec) error { return nil }}
	repo := &listAllMailboxRepo{mailboxes: []*core.Mailbox{
		{TenantID: "acme", ID: "mb-custom", AgentID: "a1",
			ACKWaitSeconds: 45, MaxDeliver: 7, MaxConcurrency: 3},
	}}
	svc := NewMailboxService(repo, drv)
	svc.ReconcileAllConsumers(context.Background())
	drv.mu.Lock()
	defer drv.mu.Unlock()
	require.Len(t, drv.specs, 1)
	assert.Equal(t, 45, drv.specs[0].ACKWaitSeconds, "custom ack_wait must survive the replay")
	assert.Equal(t, 7, drv.specs[0].MaxDeliver)
	assert.Equal(t, 6, drv.specs[0].MaxACKPending)
}

// versionMailboxRepo records UpdateConfig versions and lets tests move the
// durable version independently of the service call.
type versionMailboxRepo struct {
	mockMailboxRepo
	mu          sync.Mutex
	current     map[string]int
	mailboxes   map[string]*core.Mailbox
	getErr      map[string]error
	getOverride map[string]*core.Mailbox
}

func (r *versionMailboxRepo) UpdateConfig(ctx context.Context, tenantID, mailboxID string, mc, aw, md, rs int) (int, error) {
	r.mu.Lock()
	k := tenantID + "/" + mailboxID
	r.current[k]++
	v := r.current[k]
	if mb, ok := r.mailboxes[k]; ok {
		mb.ConfigVersion = v
		mb.MaxConcurrency = mc
		mb.ACKWaitSeconds = aw
		mb.MaxDeliver = md
		mb.RetentionSeconds = rs
	} else {
		r.mailboxes[k] = &core.Mailbox{TenantID: tenantID, ID: mailboxID,
			ConfigVersion: v, MaxConcurrency: mc, ACKWaitSeconds: aw, MaxDeliver: md, RetentionSeconds: rs}
	}
	r.mu.Unlock()
	return v, nil
}

func (r *versionMailboxRepo) Get(_ context.Context, tenantID, mailboxID string) (*core.Mailbox, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := tenantID + "/" + mailboxID
	if err := r.getErr[k]; err != nil {
		return nil, err
	}
	if mb, ok := r.getOverride[k]; ok {
		return mb, nil
	}
	if mb, ok := r.mailboxes[k]; ok {
		return mb, nil
	}
	return nil, pgx.ErrNoRows
}

func (r *versionMailboxRepo) ListAll(_ context.Context) ([]*core.Mailbox, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*core.Mailbox, 0, len(r.mailboxes))
	for _, mb := range r.mailboxes {
		out = append(out, mb)
	}
	return out, nil
}

func newVersionRepo() *versionMailboxRepo {
	return &versionMailboxRepo{
		current:     map[string]int{},
		mailboxes:   map[string]*core.Mailbox{},
		getErr:      map[string]error{},
		getOverride: map[string]*core.Mailbox{},
	}
}

func (r *versionMailboxRepo) setMailbox(tenantID, mailboxID string, v int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mailboxes[tenantID+"/"+mailboxID] = &core.Mailbox{
		TenantID: tenantID, ID: mailboxID, ConfigVersion: v,
		ACKWaitSeconds: 60, MaxConcurrency: 4,
	}
	r.current[tenantID+"/"+mailboxID] = v
}

// Cross-instance interleave: our write of an older revision must be
// skipped when PG has moved on by the time the broker write happens.
func TestMailboxService_UpdateConfig_SkipsWhenSuperseded(t *testing.T) {
	repo := newVersionRepo()
	repo.setMailbox("acme", "mb-1", 2) // a newer replica already committed v2
	var applied []core.ConsumerSpec
	var mu sync.Mutex
	drv := &reconcileRecorder{apply: func(spec core.ConsumerSpec) error {
		mu.Lock()
		applied = append(applied, spec)
		mu.Unlock()
		return nil
	}}
	svc := NewMailboxService(repo, drv)

	// Our commit bumps to v1; by the time the broker-write gate reads PG,
	// a newer replica has committed v2 — the override simulates that
	// interleave. Our v1 write must be skipped.
	repo.setMailbox("acme", "mb-1", 0)
	repo.mu.Lock()
	repo.getOverride["acme/mb-1"] = &core.Mailbox{TenantID: "acme", ID: "mb-1", ConfigVersion: 2}
	repo.mu.Unlock()
	require.NoError(t, svc.UpdateConfig(context.Background(), "acme", "mb-1", 4, 90, 5, 3600))

	mu.Lock()
	defer mu.Unlock()
	assert.Empty(t, applied, "a broker write for a superseded revision must be skipped")
}

// PG unreadable at write time => skip, do not write stale config.
func TestMailboxService_UpdateConfig_SkipsWhenVersionUnreadable(t *testing.T) {
	repo := newVersionRepo()
	repo.setMailbox("acme", "mb-1", 1)
	repo.mu.Lock()
	repo.getErr["acme/mb-1"] = errors.New("db down")
	repo.mu.Unlock()
	drv := &reconcileRecorder{apply: func(core.ConsumerSpec) error { return nil }}
	svc := NewMailboxService(repo, drv)

	require.NoError(t, svc.UpdateConfig(context.Background(), "acme", "mb-1", 4, 90, 5, 3600))
	drv.mu.Lock()
	defer drv.mu.Unlock()
	assert.Empty(t, drv.specs, "version-unreadable broker writes must be skipped")
}

// Retry loop skips (leaves parked) when the version lookup fails.
func TestMailboxService_RetrySkipsOnVersionLookupError(t *testing.T) {
	repo := newVersionRepo()
	repo.setMailbox("acme", "mb-1", 1)
	drv := &reconcileRecorder{apply: func(core.ConsumerSpec) error { return nil }}
	svc := NewMailboxService(repo, drv)
	svc.pendMu.Lock()
	svc.pending["acme/mb-1"] = parkedSpec{version: 1, spec: core.ConsumerSpec{TenantID: "acme", MailboxID: "mb-1"}}
	svc.pendMu.Unlock()

	repo.mu.Lock()
	repo.getErr["acme/mb-1"] = errors.New("db down")
	repo.mu.Unlock()
	svc.retryPending(context.Background())

	drv.mu.Lock()
	defer drv.mu.Unlock()
	assert.Empty(t, drv.specs, "no broker write when the version gate cannot read PG")
	svc.pendMu.Lock()
	defer svc.pendMu.Unlock()
	assert.Len(t, svc.pending, 1, "spec stays parked for the next tick")
}

// Startup-failure self-heal: bootstrap RetryLoop converges failed tenants.
func TestBootstrap_RetryLoop_ConvergesFailedTenants(t *testing.T) {
	fail := map[string]bool{"t1": true, "t2": true}
	var mu sync.Mutex
	var ensured []string
	lister := &retryLister{ids: []string{"t1", "t2"}}
	ensurer := &retryEnsurer{fail: fail, mu: &mu, ensured: &ensured}

	done := bootstrap.RetryLoop(context.Background(), bootstrap.Options{
		TenantLister: lister, QueueEnsurer: ensurer,
	}, []string{"t1", "t2"}, 5*time.Millisecond)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("retry loop did not converge")
	}
	mu.Lock()
	defer mu.Unlock()
	require.Contains(t, ensured, "t1")
	require.Contains(t, ensured, "t2")
}

type retryLister struct{ ids []string }

func (r *retryLister) ListIDs(context.Context) ([]string, error) { return r.ids, nil }

type retryEnsurer struct {
	fail    map[string]bool
	mu      *sync.Mutex
	ensured *[]string
}

func (r *retryEnsurer) EnsureTenant(_ context.Context, tenantID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	*r.ensured = append(*r.ensured, tenantID)
	if r.fail[tenantID] {
		r.fail[tenantID] = false // heal after the first failure
		return errors.New("broker down")
	}
	return nil
}

// Replay write-time gate: a stale snapshot must not roll the broker back.
func TestMailboxService_ReplaySkipsStaleSnapshot(t *testing.T) {
	repo := newVersionRepo()
	repo.setMailbox("acme", "mb-1", 1)
	// Snapshot reads v1; PG moves to v5 before the write (override).
	repo.mu.Lock()
	repo.getOverride["acme/mb-1"] = &core.Mailbox{TenantID: "acme", ID: "mb-1",
		ConfigVersion: 5, ACKWaitSeconds: 300, MaxConcurrency: 8}
	repo.mu.Unlock()
	drv := &reconcileRecorder{apply: func(core.ConsumerSpec) error { return nil }}
	svc := NewMailboxService(repo, drv)

	svc.ReconcileAllConsumers(context.Background())

	drv.mu.Lock()
	defer drv.mu.Unlock()
	assert.Empty(t, drv.specs, "stale snapshot replay must be skipped")
	svc.pendMu.Lock()
	defer svc.pendMu.Unlock()
	require.Len(t, svc.pending, 1, "the CURRENT revision is re-parked for convergence")
	assert.Equal(t, 5, svc.pending["acme/mb-1"].version)
	assert.Equal(t, 300, svc.pending["acme/mb-1"].spec.ACKWaitSeconds)
}

// UpdateConfig with unreadable version parks (no silent success).
func TestMailboxService_UpdateConfig_UnreadableVersionParks(t *testing.T) {
	repo := newVersionRepo()
	repo.setMailbox("acme", "mb-1", 0)
	drv := &reconcileRecorder{apply: func(core.ConsumerSpec) error { return nil }}
	svc := NewMailboxService(repo, drv)

	repo.mu.Lock()
	repo.getErr["acme/mb-1"] = errors.New("db down")
	repo.mu.Unlock()
	require.NoError(t, svc.UpdateConfig(context.Background(), "acme", "mb-1", 4, 90, 5, 3600))

	svc.pendMu.Lock()
	defer svc.pendMu.Unlock()
	assert.Len(t, svc.pending, 1, "unreadable version must park the spec, not return silent success")
}

// busyLockRepo always reports the cross-replica lock as held.
type busyLockRepo struct {
	mockMailboxRepo
	mu          sync.Mutex
	mailboxes   map[string]*core.Mailbox
	getOverride map[string]*core.Mailbox
}

func (r *busyLockRepo) AcquireMailboxLock(context.Context, string, string) (func(), bool, error) {
	return nil, false, nil // busy: another replica holds it
}
func (r *busyLockRepo) Get(_ context.Context, tenantID, mailboxID string) (*core.Mailbox, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if mb, ok := r.getOverride[tenantID+"/"+mailboxID]; ok {
		return mb, nil
	}
	return nil, pgx.ErrNoRows
}
func (r *busyLockRepo) UpdateConfig(_ context.Context, _, _ string, _, _, _, _ int) (int, error) {
	return 1, nil
}

// When the cross-replica lock is held, the write MUST NOT happen and the
// spec must be parked — writing under a local-only mutex is exactly the
// interleave the lock exists to prevent.
func TestMailboxService_LockBusy_ParksInsteadOfWriting(t *testing.T) {
	repo := &busyLockRepo{getOverride: map[string]*core.Mailbox{
		"acme/mb-1": {TenantID: "acme", ID: "mb-1", ConfigVersion: 1, MaxConcurrency: 4},
	}}
	drv := &reconcileRecorder{apply: func(core.ConsumerSpec) error { return nil }}
	svc := NewMailboxService(repo, drv)

	require.NoError(t, svc.UpdateConfig(context.Background(), "acme", "mb-1", 4, 90, 5, 3600))

	drv.mu.Lock()
	assert.Empty(t, drv.specs, "no broker write while the cross-replica lock is held")
	drv.mu.Unlock()
	svc.pendMu.Lock()
	defer svc.pendMu.Unlock()
	assert.Len(t, svc.pending, 1, "busy lock parks the spec for retry")
}

// errLockRepo simulates lock-infrastructure failure: no blind writes.
type errLockRepo struct {
	mockMailboxRepo
	getOverride map[string]*core.Mailbox
}

func (r *errLockRepo) AcquireMailboxLock(context.Context, string, string) (func(), bool, error) {
	return nil, false, errors.New("pg unavailable")
}
func (r *errLockRepo) Get(_ context.Context, tenantID, mailboxID string) (*core.Mailbox, error) {
	if mb, ok := r.getOverride[tenantID+"/"+mailboxID]; ok {
		return mb, nil
	}
	return nil, pgx.ErrNoRows
}
func (r *errLockRepo) UpdateConfig(_ context.Context, _, _ string, _, _, _, _ int) (int, error) {
	return 1, nil
}

func TestMailboxService_LockError_ParksInsteadOfWriting(t *testing.T) {
	repo := &errLockRepo{getOverride: map[string]*core.Mailbox{
		"acme/mb-1": {TenantID: "acme", ID: "mb-1", ConfigVersion: 1, MaxConcurrency: 4},
	}}
	drv := &reconcileRecorder{apply: func(core.ConsumerSpec) error { return nil }}
	svc := NewMailboxService(repo, drv)

	require.NoError(t, svc.UpdateConfig(context.Background(), "acme", "mb-1", 4, 90, 5, 3600))
	drv.mu.Lock()
	defer drv.mu.Unlock()
	assert.Empty(t, drv.specs, "no broker write when the lock infrastructure fails")
}

// Covers the tenant/DLQ reconcile driver loop: one ListAll, tenants
// reconciled once per distinct tenant, DLQ per mailbox.
type recordingTenantReconciler struct {
	reconcileRecorder
	tenants []string
	dlqs    []string
}

func (d *recordingTenantReconciler) ReconcileTenant(_ context.Context, tenantID string) error {
	d.mu.Lock()
	d.tenants = append(d.tenants, tenantID)
	d.mu.Unlock()
	return nil
}

func (d *recordingTenantReconciler) ReconcileMailboxDLQ(_ context.Context, tenantID, mailboxID string) error {
	d.mu.Lock()
	d.dlqs = append(d.dlqs, tenantID+"/"+mailboxID)
	d.mu.Unlock()
	return nil
}

func TestMailboxService_ReconcileAllTenants_DeduplicatesAndCovers(t *testing.T) {
	drv := &recordingTenantReconciler{}
	repo := newVersionRepo()
	repo.setMailbox("acme", "mb-1", 0)
	repo.setMailbox("acme", "mb-2", 0)
	repo.setMailbox("b", "mb-1", 0)
	svc := NewMailboxService(repo, drv)

	svc.ReconcileAllTenants(context.Background())

	drv.mu.Lock()
	defer drv.mu.Unlock()
	assert.ElementsMatch(t, []string{"acme", "b"}, drv.tenants, "tenant streams reconciled once per distinct tenant")
	assert.Len(t, drv.dlqs, 3, "DLQ verified for every mailbox")
}

func TestMailboxService_StartReconcileRetryLoop_TicksAndStops(t *testing.T) {
	repo := newVersionRepo()
	repo.setMailbox("acme", "mb-1", 0)
	svc := NewMailboxService(repo, &mockQueueDriver{})
	ctx, cancel := context.WithCancel(context.Background())
	go svc.StartReconcileRetryLoop(ctx, 10*time.Millisecond, 25*time.Millisecond)
	time.Sleep(60 * time.Millisecond)
	cancel()
}

// Cross-replica cold-cache pull: replica A creates the mailbox+consumer
// with the CREATE-path spec (MaxACKPending = MaxConcurrency*2); a FRESH
// driver (replica B, empty cache, same NATS) builds the spec exactly like
// DispatchService.ensureMailboxConsumer does (consumerSpecFor) — the specs
// must agree or NATS answers ErrConsumerExists and pulls fail.
func TestCrossReplica_ColdCacheConsumerSpecMatches(t *testing.T) {
	d := openNATSOverlayDriver(t)
	ctx := context.Background()
	tenant := "cc-" + fmt.Sprintf("%d", time.Now().UnixNano())
	if err := d.EnsureTenant(ctx, tenant); err != nil {
		t.Fatalf("A ensure tenant: %v", err)
	}
	mb := core.Mailbox{TenantID: tenant, ID: "mb-cc", AgentID: "a1",
		MaxConcurrency: 1, ACKWaitSeconds: 45, MaxDeliver: 5}
	if err := d.EnsureMailbox(ctx, core.MailboxSpec{
		TenantID: tenant, MailboxID: mb.ID, AgentID: mb.AgentID,
		MaxConcurrency: mb.MaxConcurrency, ACKWaitSeconds: mb.ACKWaitSeconds,
	}); err != nil {
		t.Fatalf("A ensure mailbox: %v", err)
	}
	// Create path spec (what MailboxService.Create drives).
	createSpec := core.ConsumerSpec{TenantID: tenant, MailboxID: mb.ID, DurableName: mb.ID,
		ACKWaitSeconds: mb.ACKWaitSeconds, MaxDeliver: mb.MaxDeliver, MaxACKPending: 2}
	if err := d.EnsureConsumer(ctx, createSpec); err != nil {
		t.Fatalf("A ensure consumer: %v", err)
	}

	// Replica B: fresh driver, same broker; the pull-path spec must match.
	b := openNATSOverlayDriver(t)
	if err := b.EnsureTenant(ctx, tenant); err != nil {
		t.Fatalf("B ensure tenant: %v", err)
	}
	pullSpec := consumerSpecFor(mb)
	assert.Equal(t, createSpec.MaxACKPending, pullSpec.MaxACKPending,
		"create and pull spec builders must agree on MaxACKPending")
	if err := b.EnsureConsumer(ctx, pullSpec); err != nil {
		t.Fatalf("B cold-cache ensure consumer (spec mismatch -> ErrConsumerExists): %v", err)
	}
}

// openNATSOverlayDriver builds a Driver with a COLD cache over the shared
// test NATS (JANUS_NATS_URL when set; otherwise a self-started server whose
// URL is stashed in the package-level overlay var).
var overlayNATSURL string

func openNATSOverlayDriver(t *testing.T) *natsdriver.Driver {
	t.Helper()
	url := os.Getenv("JANUS_NATS_URL")
	if url == "" {
		if overlayNATSURL == "" {
			t.Skip("no shared NATS URL available (set JANUS_NATS_URL for cross-replica coverage)")
		}
		url = overlayNATSURL
	}
	d, err := natsdriver.NewDriver(natsdriver.Config{URL: url})
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close() })
	return d
}
