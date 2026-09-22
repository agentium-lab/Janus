package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/auth"
	"github.com/agentium-lab/Janus/server/internal/service/routing"
)

// TaskService behavior coverage: creation branches, transitions, replay,
// progress reporting, event identity helpers.

func TestCov_TaskService_BuilderWiring(t *testing.T) {
	svc := NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil)
	assert.Same(t, svc, svc.WithIntentResolver(&cbIntentResolver{}))
	assert.NotNil(t, svc.intentResolver)
	assert.Same(t, svc, svc.WithContextRefService(NewContextRefService(&cbCtxRefRepo{})))
	assert.NotNil(t, svc.contextRefSvc)
	assert.Same(t, svc, svc.WithAttemptRepo(&mockDispatchAttemptRepo{}))
	assert.NotNil(t, svc.attemptRepo)
	router := routing.NewRouter(&cbRouterLookup{mailbox: "mb-routed"}, nil, nil)
	assert.Same(t, svc, svc.WithRouter(router))
	assert.NotNil(t, svc.router)
}

func TestCov_TaskService_Create_IntentBranches(t *testing.T) {
	ctx := context.Background()
	intentTask := func() core.Task {
		return core.Task{
			TenantID: "acme", ID: "t-intent", SourceAgent: "agent-a",
			TargetType: core.TargetType("intent"), TargetValue: "review the code",
			Envelope: makeTestEnvelope("t-intent", "acme"),
		}
	}

	t.Run("intent without resolver", func(t *testing.T) {
		svc := NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil)
		_, err := svc.Create(ctx, intentTask())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "intent routing not available")
	})

	t.Run("resolver error", func(t *testing.T) {
		svc := NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil).
			WithIntentResolver(&cbIntentResolver{err: errors.New("llm down")})
		_, err := svc.Create(ctx, intentTask())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "intent resolution failed")
	})

	t.Run("resolver returns empty capability", func(t *testing.T) {
		svc := NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil).
			WithIntentResolver(&cbIntentResolver{result: &IntentResolveResult{Reason: "no match"}})
		_, err := svc.Create(ctx, intentTask())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no match")
	})

	t.Run("resolver rewrites target", func(t *testing.T) {
		repo := &mockTaskRepo{}
		svc := NewTaskService(repo, &mockQueueDriver{}, nil, nil).
			WithIntentResolver(&cbIntentResolver{result: &IntentResolveResult{ResolvedCapability: "code_review", Confidence: 0.9}})
		created, err := svc.Create(ctx, intentTask())
		require.NoError(t, err)
		assert.Equal(t, core.TargetTypeCapability, created.TargetType)
		assert.Equal(t, "code_review", created.TargetValue)
		assert.Equal(t, "code_review", created.Envelope.Target.Value)
	})
}

func TestCov_TaskService_Create_RouterBranches(t *testing.T) {
	ctx := context.Background()

	t.Run("router error", func(t *testing.T) {
		svc := NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil).
			WithRouter(routing.NewRouter(&cbRouterLookup{err: errors.New("lookup down")}, nil, nil))
		_, err := svc.Create(ctx, core.Task{
			TenantID: "acme", ID: "t-r", SourceAgent: "agent-a",
			TargetType: core.TargetTypeAgent, TargetValue: "agent-1",
			Envelope: makeTestEnvelope("t-r", "acme"),
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "routing")
	})

	t.Run("router assigns mailbox", func(t *testing.T) {
		repo := &mockTaskRepo{}
		qd := &mockQueueDriver{}
		svc := NewTaskService(repo, qd, nil, nil).
			WithRouter(routing.NewRouter(&cbRouterLookup{mailbox: "mb-routed"}, nil, nil))
		created, err := svc.Create(ctx, core.Task{
			TenantID: "acme", ID: "t-r2", SourceAgent: "agent-a",
			TargetType: core.TargetTypeAgent, TargetValue: "agent-1",
			Envelope: makeTestEnvelope("t-r2", "acme"),
		})
		require.NoError(t, err)
		assert.Equal(t, "mb-routed", created.MailboxID)
		assert.Equal(t, core.TaskStatusQueued, repo.tasks["acme:t-r2"].Status)
		assert.Len(t, qd.publishedTasks, 1)
	})
}

func TestCov_TaskService_Create_AgentExistenceBranches(t *testing.T) {
	ctx := context.Background()
	task := core.Task{
		TenantID: "acme", ID: "t-ex", SourceAgent: "ghost",
		TargetType: core.TargetTypeCapability, TargetValue: "review",
		Envelope: makeTestEnvelope("t-ex", "acme"),
	}

	t.Run("existence check error", func(t *testing.T) {
		svc := NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil).
			WithAgentExistence(&cbAgentExistence{err: errors.New("agents db down")})
		_, err := svc.Create(ctx, task)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "verify source agent")
	})

	t.Run("unknown source agent", func(t *testing.T) {
		svc := NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil).
			WithAgentExistence(&cbAgentExistence{exists: false})
		_, err := svc.Create(ctx, task)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `unknown source_agent "ghost"`)
	})

	t.Run("known source agent", func(t *testing.T) {
		svc := NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil).
			WithAgentExistence(&cbAgentExistence{exists: true})
		_, err := svc.Create(ctx, task)
		require.NoError(t, err)
	})
}

func TestCov_TaskService_Create_ApprovalRequired(t *testing.T) {
	ctx := context.Background()

	makeSvc := func(approvalRepo ApprovalRepo) (*TaskService, *mockQueueDriver) {
		repo := &mockTaskRepo{}
		qd := &mockQueueDriver{}
		policySvc := NewPolicyService(&mockPolicyRuleRepo{rules: []*core.PolicyRule{{
			TenantID: "acme", ID: "need-approval", Name: "approval", Status: "active", Priority: 10,
			Condition: json.RawMessage(`{"action":"task.publish"}`),
			Action:    json.RawMessage(`{"decision":"approval_required"}`),
		}}})
		approvalSvc := NewApprovalService(approvalRepo, nil, qd)
		svc := NewTaskService(repo, qd, nil, nil).WithPolicy(policySvc).WithApproval(approvalSvc)
		return svc, qd
	}

	task := core.Task{
		TenantID: "acme", ID: "t-ap", SourceAgent: "agent-a",
		TargetType: core.TargetTypeCapability, TargetValue: "review",
		Envelope: makeTestEnvelope("t-ap", "acme"),
	}

	t.Run("request approval on approval_required", func(t *testing.T) {
		approvalRepo := &mockApprovalRepo{}
		svc, _ := makeSvc(approvalRepo)
		created, err := svc.Create(ctx, task)
		require.NoError(t, err)
		assert.Equal(t, core.TaskStatusApprovalPending, created.Status)
		require.Len(t, approvalRepo.approvals, 1)
	})

	t.Run("approval request failure is logged not fatal", func(t *testing.T) {
		svc, _ := makeSvc(&mockApprovalRepo{err: errors.New("approval db down")})
		_, err := svc.Create(ctx, task)
		require.NoError(t, err)
	})
}

func TestCov_TaskService_Create_IdempotencyLookupError(t *testing.T) {
	svc := NewTaskService(&cbTaskRepo{idemErr: errors.New("idem db down")}, &mockQueueDriver{}, nil, nil)
	_, err := svc.Create(context.Background(), core.Task{
		TenantID: "acme", ID: "t-idem", SourceAgent: "agent-a",
		TargetType: core.TargetTypeCapability, TargetValue: "review",
		IdempotencyKey: "key-1",
		Envelope:       makeTestEnvelope("t-idem", "acme"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "idempotency key lookup")
}

func TestCov_TaskService_Create_GetAfterCreateFailure(t *testing.T) {
	repo := &cbTaskRepo{getFailAfter: 1}
	svc := NewTaskService(repo, &mockQueueDriver{}, nil, nil)
	created, err := svc.Create(context.Background(), core.Task{
		TenantID: "acme", ID: "t-fallback", SourceAgent: "agent-a",
		TargetType: core.TargetTypeCapability, TargetValue: "review",
		Envelope: makeTestEnvelope("t-fallback", "acme"),
	})
	require.NoError(t, err)
	assert.Equal(t, "t-fallback", created.ID, "must fall back to the in-memory task when re-get fails")
}

func TestCov_TaskService_Create_ContextRefBind(t *testing.T) {
	ctx := context.Background()
	task := core.Task{
		TenantID: "acme", ID: "t-ctx", SourceAgent: "agent-a",
		TargetType: core.TargetTypeCapability, TargetValue: "review",
		Envelope: makeTestEnvelope("t-ctx", "acme"),
	}
	task.Envelope.ContextRefs = []core.ContextRef{{TenantID: "acme", ID: "r1"}}

	t.Run("bind error returned with result", func(t *testing.T) {
		svc := NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil).
			WithContextRefService(NewContextRefService(&cbCtxRefRepo{bindErr: errors.New("bind fail")}))
		result, err := svc.Create(ctx, task)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "context ref bind")
		assert.NotNil(t, result, "created task is returned alongside the bind error")
	})

	t.Run("bind success", func(t *testing.T) {
		repo := &cbCtxRefRepo{getResult: &core.ContextRef{ID: "r1", TenantID: "acme"}}
		svc := NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil).
			WithContextRefService(NewContextRefService(repo))
		_, err := svc.Create(ctx, task)
		require.NoError(t, err)
		assert.Equal(t, []string{"r1"}, repo.binds)
	})
}

func TestCov_TaskService_Transition_Branches(t *testing.T) {
	ctx := context.Background()

	t.Run("update with check repo error", func(t *testing.T) {
		repo := &cbTaskRepo{tasks: map[string]*core.Task{
			"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusRunning},
		}, updateCheckErr: errors.New("cas fail")}
		svc := NewTaskService(repo, &mockQueueDriver{}, nil, nil)
		err := svc.Complete(ctx, "acme", "t1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "update task status to completed")
	})

	t.Run("concurrent status change conflict", func(t *testing.T) {
		repo := &cbTaskRepo{tasks: map[string]*core.Task{
			"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusRunning},
		}, updateCheckFalse: true}
		svc := NewTaskService(repo, &mockQueueDriver{}, nil, nil)
		err := svc.Complete(ctx, "acme", "t1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "status changed concurrently")
	})

	t.Run("lifecycle with non-pg repo falls back", func(t *testing.T) {
		repo := &mockTaskRepo{tasks: map[string]*core.Task{
			"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusRunning},
		}}
		qd := &mockQueueDriver{}
		svc := NewTaskService(repo, qd, nil, nil).WithLifecycle(NewMemoryLifecycle())
		err := svc.Complete(ctx, "acme", "t1")
		require.NoError(t, err)
		assert.Equal(t, core.TaskStatusCompleted, repo.tasks["acme:t1"].Status)
		assert.Len(t, qd.publishedEvents, 1)
	})
}

func TestCov_TaskService_ReportProgress(t *testing.T) {
	ctx := context.Background()

	t.Run("task not found", func(t *testing.T) {
		svc := NewTaskService(&mockTaskRepo{err: errors.New("db down")}, &mockQueueDriver{}, nil, nil)
		_, err := svc.ReportProgress(ctx, "acme", "missing", "a1", core.TaskProgress{Message: "working"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "task not found")
	})

	t.Run("wrong status", func(t *testing.T) {
		repo := &mockTaskRepo{tasks: map[string]*core.Task{
			"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusQueued},
		}}
		svc := NewTaskService(repo, &mockQueueDriver{}, nil, nil)
		_, err := svc.ReportProgress(ctx, "acme", "t1", "a1", core.TaskProgress{Message: "working"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "progress only accepted while claimed or running")
	})

	t.Run("agent does not hold attempt", func(t *testing.T) {
		repo := &mockTaskRepo{tasks: map[string]*core.Task{
			"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusRunning},
		}}
		aRepo := &mockDispatchAttemptRepo{attempts: []*core.TaskAttempt{{
			TenantID: "acme", TaskID: "t1", Attempt: 1, AgentID: "agent-owner", LeaseID: "l",
		}}}
		svc := NewTaskService(repo, &mockQueueDriver{}, nil, nil).WithAttemptRepo(aRepo)
		_, err := svc.ReportProgress(ctx, "acme", "t1", "agent-impostor", core.TaskProgress{Message: "working"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not hold the latest attempt")
	})

	t.Run("attempt lookup error", func(t *testing.T) {
		repo := &mockTaskRepo{tasks: map[string]*core.Task{
			"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusRunning},
		}}
		svc := NewTaskService(repo, &mockQueueDriver{}, nil, nil).WithAttemptRepo(&mockDispatchAttemptRepo{})
		_, err := svc.ReportProgress(ctx, "acme", "t1", "a1", core.TaskProgress{Message: "working"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not hold the latest attempt")
	})

	t.Run("success without attempt repo and without outbox", func(t *testing.T) {
		repo := &mockTaskRepo{tasks: map[string]*core.Task{
			"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusClaimed},
		}}
		svc := NewTaskService(repo, &mockQueueDriver{}, nil, nil)
		_, err := svc.ReportProgress(ctx, "acme", "t1", "any-agent", core.TaskProgress{Message: "working"})
		require.NoError(t, err)
	})
}

func TestCov_TaskService_Replay_ErrorBranches(t *testing.T) {
	ctx := context.Background()

	t.Run("reset error", func(t *testing.T) {
		repo := &cbTaskRepo{tasks: map[string]*core.Task{"acme:t1": &core.Task{}}}
		repo.tasks["acme:t1"] = &core.Task{ID: "t1", TenantID: "acme", Status: core.TaskStatusCompleted, MailboxID: "mb1"}
		repo.resetErr = errors.New("reset fail")
		svc := NewTaskService(repo, &mockQueueDriver{}, nil, nil)
		_, err := svc.Replay(ctx, "acme", "t1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "reset task")
	})

	t.Run("update to queued error", func(t *testing.T) {
		repo := &cbTaskRepo{tasks: map[string]*core.Task{"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusCompleted, MailboxID: "mb1"}}, updateErr: errors.New("update fail")}
		svc := NewTaskService(repo, &mockQueueDriver{}, nil, nil)
		_, err := svc.Replay(ctx, "acme", "t1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "update task queued after replay")
	})

	t.Run("final get error", func(t *testing.T) {
		repo := &cbTaskRepo{getFailAfter: 1}
		repo.tasks = map[string]*core.Task{"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusCompleted, MailboxID: "mb1"}}
		svc := NewTaskService(repo, &mockQueueDriver{}, nil, nil)
		_, err := svc.Replay(ctx, "acme", "t1")
		require.Error(t, err)
	})
}

func TestCov_TaskService_TransitionInTx_MemoryRepo(t *testing.T) {
	repo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusQueued},
	}}
	qd := &mockQueueDriver{}
	svc := NewTaskService(repo, qd, nil, nil)
	err := svc.TransitionInTx(context.Background(), nil, "acme", "t1",
		core.TaskStatusQueued, core.TaskStatusClaimed, core.EventTaskClaimed, 0)
	require.NoError(t, err)
	assert.Equal(t, core.TaskStatusClaimed, repo.tasks["acme:t1"].Status)
	require.Len(t, qd.publishedEvents, 1)
}

func TestCov_TaskService_PublishEvent_ActingUser(t *testing.T) {
	repo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusRunning},
	}}
	qd := &mockQueueDriver{}
	svc := NewTaskService(repo, qd, nil, nil)
	ctx := context.WithValue(context.Background(), auth.ActingUserCtxKey, "user-42")

	err := svc.Complete(ctx, "acme", "t1")
	require.NoError(t, err)
	require.Len(t, qd.publishedEvents, 1)
	var payload map[string]string
	require.NoError(t, json.Unmarshal(qd.publishedEvents[0].Payload, &payload))
	assert.Equal(t, "user-42", payload["claimed_actor"])
}

func TestCov_TaskService_EmitToolPolicyEvent_Decisions(t *testing.T) {
	qd := &mockQueueDriver{}
	svc := NewTaskService(&mockTaskRepo{}, qd, nil, nil)
	ctx := context.Background()
	task := &core.Task{
		TenantID: "acme", ID: "t-tool",
		Envelope: core.TaskEnvelope{ToolInvocation: &core.ToolInvocation{Name: "search"}},
	}

	svc.emitToolPolicyEvent(ctx, task, core.PolicyDecisionDeny, "nope")
	svc.emitToolPolicyEvent(ctx, task, core.PolicyDecisionAllow, "ok")
	svc.emitToolPolicyEvent(ctx, task, core.PolicyDecisionApprovalRequired, "ask")
	svc.emitToolPolicyEvent(ctx, task, core.PolicyDecisionType("bogus"), "ignored")
	svc.emitToolPolicyEvent(ctx, &core.Task{TenantID: "acme", ID: "no-tool"}, core.PolicyDecisionAllow, "ok")

	types := map[core.EventType]int{}
	for _, evt := range qd.publishedEvents {
		types[evt.EventType]++
	}
	assert.Equal(t, 1, types[core.EventToolInvocationDenied])
	assert.Equal(t, 2, types[core.EventToolInvocationAllowed])
	assert.Len(t, qd.publishedEvents, 3, "bogus decision and nil tool invocation must not publish")
}

func TestExtra_GenerateEventID(t *testing.T) {
	id, err := generateEventID()
	require.NoError(t, err)
	assert.Contains(t, id, "evt_")
	assert.Len(t, id, 4+20)
}

func TestExtra_GenerateEventID_Uniqueness(t *testing.T) {
	ids := make(map[string]bool)
	for i := 0; i < 100; i++ {
		id, err := generateEventID()
		require.NoError(t, err)
		assert.False(t, ids[id], "duplicate id generated: %s", id)
		ids[id] = true
	}
}

func TestExtra_PublishEvent_EnrichesEvent(t *testing.T) {
	qd := &mockQueueDriver{}
	taskRepo := &mockTaskRepo{}
	svc := NewTaskService(taskRepo, qd, nil, nil)
	ctx := context.Background()

	_, err := svc.Create(ctx, core.Task{
		TenantID:    "acme",
		ID:          "task-pub",
		SourceAgent: "agent-a",
		TargetType:  core.TargetTypeCapability,
		TargetValue: "test",
		MailboxID:   "mb-1",
		Envelope:    makeTestEnvelope("task-pub", "acme"),
	})
	require.NoError(t, err)

	require.Len(t, qd.publishedEvents, 2)
	for _, evt := range qd.publishedEvents {
		assert.NotEmpty(t, evt.EventID)
		assert.Contains(t, evt.EventID, "evt_")
		assert.False(t, evt.Timestamp.IsZero())
	}
}

func TestExtra_Transition_InvalidStatus(t *testing.T) {
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusCompleted},
	}}
	svc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	ctx := context.Background()

	err := svc.Start(ctx, "acme", "t1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "task is in a terminal state")
}

func TestExtra_Transition_CannotTransition(t *testing.T) {
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusCreated},
	}}
	svc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	ctx := context.Background()

	err := svc.Complete(ctx, "acme", "t1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid state transition")
}

func TestExtra_Transition_GetTaskError(t *testing.T) {
	taskRepo := &mockTaskRepo{err: errors.New("db error")}
	svc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	ctx := context.Background()

	err := svc.Start(ctx, "acme", "t1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get task for transition")
}

func TestExtra_Replay_NonTerminalTask(t *testing.T) {
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusRunning},
	}}
	svc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	ctx := context.Background()

	_, err := svc.Replay(ctx, "acme", "t1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "only terminal tasks can be replayed")
}

func TestExtra_Replay_GetTaskError(t *testing.T) {
	taskRepo := &mockTaskRepo{err: errors.New("db error")}
	svc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	ctx := context.Background()

	_, err := svc.Replay(ctx, "acme", "t1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get task")
}

func TestExtra_Replay_WithMailbox(t *testing.T) {
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:t1": {
			ID:        "t1",
			TenantID:  "acme",
			Status:    core.TaskStatusCompleted,
			MailboxID: "mb1",
			Envelope:  core.TaskEnvelope{TaskID: "t1"},
		},
	}}
	qd := &mockQueueDriver{}
	svc := NewTaskService(taskRepo, qd, nil, nil)
	ctx := context.Background()

	_, err := svc.Replay(ctx, "acme", "t1")
	require.NoError(t, err)
	assert.Len(t, qd.publishedTasks, 1)
}

func TestExtra_Replay_WithoutMailbox(t *testing.T) {
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:t1": {
			ID:       "t1",
			TenantID: "acme",
			Status:   core.TaskStatusCompleted,
			Envelope: core.TaskEnvelope{TaskID: "t1"},
		},
	}}
	qd := &mockQueueDriver{}
	svc := NewTaskService(taskRepo, qd, nil, nil)
	ctx := context.Background()

	_, err := svc.Replay(ctx, "acme", "t1")
	require.NoError(t, err)
	assert.Len(t, qd.publishedTasks, 0)
}

func TestExtra_TaskService_Block(t *testing.T) {
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusRunning},
	}}
	svc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	ctx := context.Background()

	err := svc.Block(ctx, "acme", "t1", "manual block")
	require.NoError(t, err)

	got, _ := taskRepo.Get(ctx, "acme", "t1")
	assert.Equal(t, core.TaskStatusBlocked, got.Status)
}

func TestExtra_TaskService_Block_Validation(t *testing.T) {
	svc := NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil)
	ctx := context.Background()

	err := svc.Block(ctx, "", "t1", "x")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "tenant id and task id are required")

	err = svc.Block(ctx, "acme", "", "x")
	assert.Error(t, err)
}

func TestExtra_TaskService_Unblock(t *testing.T) {
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusBlocked},
	}}
	svc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	ctx := context.Background()

	err := svc.Unblock(ctx, "acme", "t1")
	require.NoError(t, err)

	got, _ := taskRepo.Get(ctx, "acme", "t1")
	assert.Equal(t, core.TaskStatusRunning, got.Status)
}

func TestExtra_TaskService_Cancel(t *testing.T) {
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusQueued},
	}}
	svc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	ctx := context.Background()

	err := svc.Cancel(ctx, "acme", "t1")
	require.NoError(t, err)

	got, _ := taskRepo.Get(ctx, "acme", "t1")
	assert.Equal(t, core.TaskStatusCancelled, got.Status)
}

func TestExtra_TaskService_Get(t *testing.T) {
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:t1": {ID: "t1", TenantID: "acme"},
	}}
	svc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	ctx := context.Background()

	got, err := svc.Get(ctx, "acme", "t1")
	require.NoError(t, err)
	assert.Equal(t, "t1", got.ID)
}

func TestExtra_TaskService_Get_NotFound(t *testing.T) {
	svc := NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil)
	ctx := context.Background()

	_, err := svc.Get(ctx, "acme", "nonexistent")
	assert.Error(t, err)
}

func TestExtra_TaskService_ListByStatus(t *testing.T) {
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusRunning},
		"acme:t2": {ID: "t2", TenantID: "acme", Status: core.TaskStatusRunning},
		"acme:t3": {ID: "t3", TenantID: "acme", Status: core.TaskStatusCompleted},
	}}
	svc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	ctx := context.Background()

	tasks, err := svc.ListByStatus(ctx, "acme", core.TaskStatusRunning, 10)
	require.NoError(t, err)
	assert.Len(t, tasks, 2)
}

func TestExtra_TaskService_ListByStatus_DefaultLimit(t *testing.T) {
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusRunning},
	}}
	svc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	ctx := context.Background()

	tasks, err := svc.ListByStatus(ctx, "acme", core.TaskStatusRunning, 0)
	require.NoError(t, err)
	assert.Len(t, tasks, 1)
}

func TestExtra_Fail_InvalidTransition(t *testing.T) {
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusCreated},
	}}
	svc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	ctx := context.Background()

	err := svc.Fail(ctx, "acme", "t1", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid state transition")
}

func TestExtra_Fail_TransitionError(t *testing.T) {
	taskRepo := &mockTaskRepo{err: fmt.Errorf("db error")}
	svc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	ctx := context.Background()

	err := svc.Fail(ctx, "acme", "t1", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get task for transition")
}

func TestExtra_Replay_QueueError(t *testing.T) {
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:t1": {
			ID: "t1", TenantID: "acme", Status: core.TaskStatusCompleted,
			MailboxID: "mb1", Envelope: core.TaskEnvelope{TaskID: "t1"},
		},
	}}
	qd := &mockQueueDriver{err: fmt.Errorf("nats down")}
	svc := NewTaskService(taskRepo, qd, nil, nil)
	ctx := context.Background()

	_, err := svc.Replay(ctx, "acme", "t1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "outbox insert replay")
}

func TestExtra_EventService_PublishEvent_MarshalError(t *testing.T) {
	svc := NewEventService(&mockEventRepo{})
	ctx := context.Background()

	err := svc.PublishEvent(ctx, "acme", core.EventTaskCreated, "t1", "", "", make(chan int))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "marshal event payload")
}

func TestExtra_ApprovalService_Reject_TransitionError(t *testing.T) {
	approvalRepo := &mockApprovalRepo{
		approvals: map[string]*core.Approval{
			"acme:appr-1": {ID: "appr-1", TenantID: "acme", Status: "pending", TaskID: "t1"},
		},
	}
	taskRepo := &mockTaskRepo{err: fmt.Errorf("db error")}
	taskSvc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	svc := NewApprovalService(approvalRepo, taskSvc, nil)
	ctx := context.Background()

	err := svc.Reject(ctx, "acme", "appr-1", "approver", "reason")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get task")
}
