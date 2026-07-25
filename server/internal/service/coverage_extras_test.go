package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

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

func TestExtra_ParseAction_ValidApprovalRequired(t *testing.T) {
	raw := json.RawMessage(`{"decision":"approval_required"}`)
	a, ok := parseAction(raw)
	assert.True(t, ok)
	assert.Equal(t, core.PolicyDecisionApprovalRequired, a.Decision)
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
	assert.Contains(t, err.Error(), "terminal state")
}

func TestExtra_Transition_CannotTransition(t *testing.T) {
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusCreated},
	}}
	svc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	ctx := context.Background()

	err := svc.Complete(ctx, "acme", "t1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid transition")
}

func TestExtra_Transition_GetTaskError(t *testing.T) {
	taskRepo := &mockTaskRepo{err: errors.New("db error")}
	svc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	ctx := context.Background()

	err := svc.Start(ctx, "acme", "t1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get task for transition")
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

func TestExtra_Reject_ApprovalAlreadyDecided(t *testing.T) {
	approvalRepo := &mockApprovalRepo{
		approvals: map[string]*core.Approval{
			"acme:appr-1": {ID: "appr-1", TenantID: "acme", Status: "approved"},
		},
	}
	svc := NewApprovalService(approvalRepo, nil, nil)
	ctx := context.Background()

	err := svc.Reject(ctx, "acme", "appr-1", "approver", "reason")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already decided")
}

func TestExtra_Approve_ApprovalAlreadyDecided(t *testing.T) {
	approvalRepo := &mockApprovalRepo{
		approvals: map[string]*core.Approval{
			"acme:appr-1": {ID: "appr-1", TenantID: "acme", Status: "rejected"},
		},
	}
	svc := NewApprovalService(approvalRepo, nil, nil)
	ctx := context.Background()

	err := svc.Approve(ctx, "acme", "appr-1", "approver", "reason")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already decided")
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

func TestExtra_PullTask_TerminalACK(t *testing.T) {
	svc, qDrv, tRepo, _ := newTestDispatchSvc()
	ctx := context.Background()
	task := makeDispatchTestTask("acme", "task-1", "mb-1", 0)
	task.Status = core.TaskStatusCompleted
	tRepo.tasks["acme:task-1"] = task
	qDrv.deliveries = []core.TaskDelivery{{TaskID: "task-1", DeliveryRef: "ref-1"}}

	result, err := svc.PullTask(ctx, "acme", "mb-1", "agent-1")
	require.NoError(t, err)
	assert.Nil(t, result)
	assert.Equal(t, 1, qDrv.ackCalls)
}

func TestExtra_PullTask_RetryScheduledACK(t *testing.T) {
	svc, qDrv, tRepo, _ := newTestDispatchSvc()
	ctx := context.Background()
	task := makeDispatchTestTask("acme", "task-1", "mb-1", 0)
	task.Status = core.TaskStatusRetryScheduled
	tRepo.tasks["acme:task-1"] = task
	qDrv.deliveries = []core.TaskDelivery{{TaskID: "task-1", DeliveryRef: "ref-1"}}

	result, err := svc.PullTask(ctx, "acme", "mb-1", "agent-1")
	require.NoError(t, err)
	assert.Nil(t, result)
	assert.Equal(t, 1, qDrv.ackCalls)
}

func TestExtra_PullTask_CreatedNACK(t *testing.T) {
	svc, qDrv, tRepo, _ := newTestDispatchSvc()
	ctx := context.Background()
	task := makeDispatchTestTask("acme", "task-1", "mb-1", 0)
	task.Status = core.TaskStatusCreated
	tRepo.tasks["acme:task-1"] = task
	qDrv.deliveries = []core.TaskDelivery{{TaskID: "task-1", DeliveryRef: "ref-1"}}

	result, err := svc.PullTask(ctx, "acme", "mb-1", "agent-1")
	require.NoError(t, err)
	assert.Nil(t, result)
	assert.Equal(t, 1, qDrv.nackCalls)
}

func TestExtra_PullTask_SameAttemptACK(t *testing.T) {
	svc, qDrv, tRepo, aRepo := newTestDispatchSvc()
	ctx := context.Background()
	task := makeDispatchTestTask("acme", "task-1", "mb-1", 1)
	task.Status = core.TaskStatusClaimed
	tRepo.tasks["acme:task-1"] = task
	aRepo.attempts = []*core.TaskAttempt{{
		TenantID: "acme", TaskID: "task-1", Attempt: 1,
		AgentID: "agent-1", LeaseID: "lease-abc", DeliveryRef: "ref-1", Status: "claimed",
	}}
	qDrv.deliveries = []core.TaskDelivery{{TaskID: "task-1", DeliveryRef: "ref-1"}}

	result, err := svc.PullTask(ctx, "acme", "mb-1", "agent-1")
	require.NoError(t, err)
	assert.Nil(t, result)
	assert.Equal(t, 1, qDrv.ackCalls)
}

func TestExtra_PullTask_OlderAttemptACK(t *testing.T) {
	svc, qDrv, tRepo, aRepo := newTestDispatchSvc()
	ctx := context.Background()
	task := makeDispatchTestTask("acme", "task-1", "mb-1", 1)
	task.Status = core.TaskStatusRunning
	tRepo.tasks["acme:task-1"] = task
	aRepo.attempts = []*core.TaskAttempt{{
		TenantID: "acme", TaskID: "task-1", Attempt: 1,
		AgentID: "agent-1", LeaseID: "lease-abc", DeliveryRef: "ref-old", Status: "running",
	}}
	qDrv.deliveries = []core.TaskDelivery{{TaskID: "task-1", DeliveryRef: "ref-new"}}

	result, err := svc.PullTask(ctx, "acme", "mb-1", "agent-1")
	require.NoError(t, err)
	assert.Nil(t, result)
	assert.Equal(t, 1, qDrv.ackCalls)
}

func TestExtra_PullTask_BudgetReserveError(t *testing.T) {
	qDrv := &mockDispatchQueueDriver{}
	tRepo := &mockDispatchTaskRepo{tasks: make(map[string]*core.Task)}
	aRepo := &mockDispatchAttemptRepo{}
	mRepo := &mockDispatchMailboxRepo{mailboxes: make(map[string]*core.Mailbox)}
	policySvc := NewPolicyService(&mockPolicyRuleRepo{})
	budgetSvc := NewBudgetServiceWithUsage(&mockBudgetRepo{}, &mockBudgetUsageRepo{reserveErr: fmt.Errorf("reserve fail")})
	svc := NewDispatchService(tRepo, aRepo, mRepo, qDrv, policySvc, budgetSvc)

	ctx := context.Background()
	task := makeDispatchTestTask("acme", "task-1", "mb-1", 0)
	tRepo.tasks["acme:task-1"] = task
	qDrv.deliveries = []core.TaskDelivery{{TaskID: "task-1", DeliveryRef: "ref-1"}}

	_, err := svc.PullTask(ctx, "acme", "mb-1", "agent-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reserve fail")
}

func TestExtra_StartTask_GetLatestError(t *testing.T) {
	svc, _, tRepo, _ := newTestDispatchSvc()
	ctx := context.Background()
	tRepo.tasks["acme:task-1"] = makeDispatchTestTask("acme", "task-1", "mb-1", 1)

	err := svc.StartTask(ctx, "acme", "task-1", "lease-abc")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get latest attempt")
}

func TestExtra_AckTask_GetLatestError(t *testing.T) {
	svc, _, tRepo, _ := newTestDispatchSvc()
	ctx := context.Background()
	tRepo.tasks["acme:task-1"] = makeDispatchTestTask("acme", "task-1", "mb-1", 1)

	err := svc.AckTask(ctx, "acme", "task-1", "lease-abc", "", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get latest attempt")
}

func TestExtra_AckTask_DuplicateACK(t *testing.T) {
	svc, qDrv, tRepo, aRepo := newTestDispatchSvc()
	ctx := context.Background()

	tRepo.tasks["acme:task-1"] = makeDispatchTestTask("acme", "task-1", "mb-1", 1)
	aRepo.attempts = []*core.TaskAttempt{{
		TenantID: "acme", TaskID: "task-1", Attempt: 1,
		LeaseID: "lease-abc", DeliveryRef: "ref-1", Status: "completed",
	}}

	err := svc.AckTask(ctx, "acme", "task-1", "lease-abc", "", nil)
	require.NoError(t, err)
	assert.Equal(t, 0, qDrv.ackCalls)
}

func TestExtra_AckTask_TaskUpdateError(t *testing.T) {
	svc, _, tRepo, aRepo := newTestDispatchSvc()
	ctx := context.Background()

	tRepo.tasks["acme:task-1"] = makeDispatchTestTask("acme", "task-1", "mb-1", 1)
	tRepo.updateErr = fmt.Errorf("update fail")
	aRepo.attempts = []*core.TaskAttempt{{
		TenantID: "acme", TaskID: "task-1", Attempt: 1,
		LeaseID: "lease-abc", DeliveryRef: "ref-1", Status: "running",
	}}

	err := svc.AckTask(ctx, "acme", "task-1", "lease-abc", "", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "complete task")
}

func TestExtra_AckTask_QueueAckError(t *testing.T) {
	svc, qDrv, tRepo, aRepo := newTestDispatchSvc()
	ctx := context.Background()

	tRepo.tasks["acme:task-1"] = makeDispatchTestTask("acme", "task-1", "mb-1", 1)
	aRepo.attempts = []*core.TaskAttempt{{
		TenantID: "acme", TaskID: "task-1", Attempt: 1,
		LeaseID: "lease-abc", DeliveryRef: "ref-1", Status: "running",
	}}
	qDrv.ackErr = fmt.Errorf("ack fail")

	err := svc.AckTask(ctx, "acme", "task-1", "lease-abc", "", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ack queue message")
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

func TestExtra_Fail_InvalidTransition(t *testing.T) {
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusCreated},
	}}
	svc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	ctx := context.Background()

	err := svc.Fail(ctx, "acme", "t1", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid transition")
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
	assert.Contains(t, err.Error(), "re-publish to queue")
}

func TestExtra_LifecycleService_ApplyTx_Nil(t *testing.T) {
	var ls *LifecycleService
	err := ls.ApplyTx(context.Background(), func(tx pgx.Tx) error { return nil })
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not configured")
}

func TestExtra_LifecycleService_ApplyTx_NilPool(t *testing.T) {
	ls := NewLifecycleService(nil)
	err := ls.ApplyTx(context.Background(), func(tx pgx.Tx) error { return nil })
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not configured")
}

func TestExtra_EventService_PublishEvent_MarshalError(t *testing.T) {
	svc := NewEventService(&mockEventRepo{})
	ctx := context.Background()

	err := svc.PublishEvent(ctx, "acme", core.EventTaskCreated, "t1", "", "", make(chan int))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "marshal event payload")
}

func TestExtra_BudgetService_Reserve_AgentUnderLimitThenError(t *testing.T) {
	repo := &mockBudgetRepo{
		budgets: map[string]*core.BudgetSpec{
			"acme:agent:agent-1": {TenantID: "acme", ScopeType: core.BudgetScopeAgent, ScopeID: "agent-1", DailyCostUSD: 5.0},
		},
	}
	usageRepo := &mockBudgetUsageRepo{dailyCost: 1.0, reserveErr: fmt.Errorf("reserve fail")}
	svc := NewBudgetServiceWithUsage(repo, usageRepo)
	ctx := context.Background()

	err := svc.Reserve(ctx, "acme", "agent-1", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "reserve fail")
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
	assert.Contains(t, err.Error(), "cancel task")
}

func TestExtra_NackTask_GetLatestError(t *testing.T) {
	svc, _, _, _ := newTestDispatchSvc()
	ctx := context.Background()

	err := svc.NackTask(ctx, "acme", "task-1", "lease-abc", false, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get latest attempt")
}

func TestExtra_NackTaskDirect_GetTaskError(t *testing.T) {
	svc, _, _, aRepo := newTestDispatchSvc()
	ctx := context.Background()

	aRepo.attempts = []*core.TaskAttempt{{
		TenantID: "acme", TaskID: "task-1", Attempt: 1,
		LeaseID: "lease-abc", DeliveryRef: "ref-1", Status: "running",
	}}

	err := svc.NackTask(ctx, "acme", "task-1", "lease-abc", false, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get task")
}

func TestExtra_NackTaskDirect_UpdateStatusError(t *testing.T) {
	svc, _, tRepo, aRepo := newTestDispatchSvc()
	ctx := context.Background()

	task := makeDispatchTestTask("acme", "task-1", "mb-1", 1)
	tRepo.tasks["acme:task-1"] = task
	tRepo.updateErr = fmt.Errorf("update fail")
	aRepo.attempts = []*core.TaskAttempt{{
		TenantID: "acme", TaskID: "task-1", Attempt: 1,
		LeaseID: "lease-abc", DeliveryRef: "ref-1", Status: "running",
	}}

	err := svc.NackTask(ctx, "acme", "task-1", "lease-abc", false, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "dead letter")
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
	ls := NewLifecycleService(pool)
	err = ls.ApplyTx(context.Background(), func(tx pgx.Tx) error { return nil })
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "begin lifecycle tx")
}

func TestExtra_LifecycleService_ApplyTx_FnError(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "host=/tmp port=5432 user=silv dbname=janus_test")
	require.NoError(t, err)
	defer pool.Close()
	ls := NewLifecycleService(pool)
	err = ls.ApplyTx(context.Background(), func(tx pgx.Tx) error {
		return fmt.Errorf("fn error")
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "fn error")
}

func TestExtra_LifecycleService_ApplyTx_CommitError(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "host=/tmp port=5432 user=silv dbname=janus_test")
	require.NoError(t, err)
	defer pool.Close()
	ls := NewLifecycleService(pool)
	err = ls.ApplyTx(context.Background(), func(tx pgx.Tx) error {
		_ = tx.Rollback(context.Background())
		return nil
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "commit lifecycle tx")
}


