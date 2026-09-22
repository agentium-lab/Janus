package service

import (
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
	require.Error(t, err)
	assert.Contains(t, err.Error(), "heartbeat")
}

func TestCov_AgentService_Register_NestedErrors(t *testing.T) {
	ctx := context.Background()
	agent := core.Agent{ID: "a1", TenantID: "acme", DisplayName: "A", Capabilities: []core.AgentCapability{{Capability: "x"}}}

	err := NewAgentService(&cbAgentRepo{capErr: errors.New("cap fail")}, nil, &mockHeartbeatDriver{}, nil).
		Register(ctx, agent)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert capabilities")

	err = NewAgentService(&mockAgentRepo{}, nil, &mockHeartbeatDriver{err: errors.New("redis down")}, nil).
		Register(ctx, agent)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "initial heartbeat")

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
