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
)

// DispatchService behavior coverage: pull/claim, ack/nack paths,
// lease and side-effect ordering.

func TestCov_PullTask_MailboxOwnerMismatch(t *testing.T) {
	svc, _, _, _, mRepo := newCovDispatchSvc()
	mRepo.mailboxes["acme:mb-1"] = &core.Mailbox{TenantID: "acme", ID: "mb-1", AgentID: "agent-other"}

	_, err := svc.PullTask(context.Background(), "acme", "mb-1", "agent-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not the owner of mailbox")
}

func TestCov_PullTask_PolicyEvaluateError(t *testing.T) {
	svc, _, _, _, _ := newCovDispatchSvc()
	svc.policySvc = NewPolicyService(&mockPolicyRuleRepo{err: errors.New("rules db down")})

	_, err := svc.PullTask(context.Background(), "acme", "mb-1", "agent-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "policy check")
}

func TestCov_PullTask_ConcurrencyExceeded(t *testing.T) {
	svc, _, tRepo, _, _ := newCovDispatchSvc()
	tRepo.running = 2
	svc.budgetSvc = NewBudgetService(&mockBudgetRepo{budgets: map[string]*core.BudgetSpec{
		"acme:agent:agent-1": {TenantID: "acme", ScopeType: core.BudgetScopeAgent, ScopeID: "agent-1", MaxConcurrency: 1},
	}})

	_, err := svc.PullTask(context.Background(), "acme", "mb-1", "agent-1")
	var bp *core.BackpressureError
	require.ErrorAs(t, err, &bp)
}

func TestCov_PullTask_TaskDispatchPolicyDenied(t *testing.T) {
	svc, qDrv, tRepo, _, _ := newCovDispatchSvc()
	svc.policySvc = NewPolicyService(&mockPolicyRuleRepo{rules: []*core.PolicyRule{{
		TenantID: "acme", ID: "deny-task-dispatch", Name: "deny", Status: "active", Priority: 50,
		Condition: json.RawMessage(`{"action":"dispatch","resource.type":"task"}`),
		Action:    json.RawMessage(`{"decision":"deny"}`),
	}}})
	tRepo.tasks["acme:task-1"] = makeDispatchTestTask("acme", "task-1", "mb-1", 0)
	qDrv.deliveries = []core.TaskDelivery{{TaskID: "task-1", DeliveryRef: "ref-1"}}

	_, err := svc.PullTask(context.Background(), "acme", "mb-1", "agent-1")
	var bp *core.BackpressureError
	require.ErrorAs(t, err, &bp)
	assert.Contains(t, err.Error(), "dispatch policy denied")

	found := false
	for _, evt := range qDrv.events {
		if evt.EventType == core.EventPolicyDenied {
			found = true
		}
	}
	assert.True(t, found, "policy.denied event must be published")
}

func TestCov_PullTask_EnsureMailboxConsumer(t *testing.T) {
	svc, qDrv, tRepo, _, mRepo := newCovDispatchSvc()
	mRepo.mailboxes["acme:mb-1"] = &core.Mailbox{TenantID: "acme", ID: "mb-1", AgentID: "agent-1"}
	tRepo.tasks["acme:task-1"] = makeDispatchTestTask("acme", "task-1", "mb-1", 0)
	qDrv.deliveries = []core.TaskDelivery{{TaskID: "task-1", DeliveryRef: "ref-1"}}

	_, err := svc.PullTask(context.Background(), "acme", "mb-1", "agent-1")
	require.NoError(t, err)
	assert.Equal(t, 1, qDrv.ensureMbx)
	assert.Equal(t, 1, qDrv.ensureCons)
}

func TestCov_PullTask_UpdateStatusError(t *testing.T) {
	svc, qDrv, tRepo, _, _ := newCovDispatchSvc()
	tRepo.tasks["acme:task-1"] = makeDispatchTestTask("acme", "task-1", "mb-1", 0)
	tRepo.updateCheckErr = errors.New("update fail")
	qDrv.deliveries = []core.TaskDelivery{{TaskID: "task-1", DeliveryRef: "ref-1"}}

	_, err := svc.PullTask(context.Background(), "acme", "mb-1", "agent-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "update task claimed")
}

func TestCov_StartTask_ValidationAndUpdateError(t *testing.T) {
	svc, _, tRepo, aRepo := newTestDispatchSvc()
	ctx := context.Background()

	err := svc.StartTask(ctx, "", "task-1", "lease")
	assert.EqualError(t, err, "tenant id, task id, and lease id are required")

	tRepo.tasks["acme:task-1"] = makeDispatchTestTask("acme", "task-1", "mb-1", 1)
	tRepo.updateErr = errors.New("update fail")
	aRepo.attempts = []*core.TaskAttempt{{
		TenantID: "acme", TaskID: "task-1", Attempt: 1,
		LeaseID: "lease-abc", DeliveryRef: "ref-1", Status: "claimed",
	}}
	err = svc.StartTask(ctx, "acme", "task-1", "lease-abc")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "update task running")
}

func TestCov_AckTask_LifecycleWithNonPGRepoFallsBack(t *testing.T) {
	svc, qDrv, tRepo, aRepo := newTestDispatchSvc()
	svc = svc.WithTxPath(NewMemoryLifecycle(), nil, nil)
	ctx := context.Background()

	tRepo.tasks["acme:task-1"] = makeDispatchTestTask("acme", "task-1", "mb-1", 1)
	aRepo.attempts = []*core.TaskAttempt{{
		TenantID: "acme", TaskID: "task-1", Attempt: 1,
		LeaseID: "lease-abc", DeliveryRef: "ref-1", Status: "running",
	}}

	err := svc.AckTask(ctx, "acme", "task-1", "lease-abc", "ref-out", nil)
	require.NoError(t, err)
	assert.Equal(t, core.TaskStatusCompleted, tRepo.tasks["acme:task-1"].Status)
	assert.Equal(t, 1, qDrv.ackCalls)
}

func TestCov_NackTask_LifecycleWithNonPGRepoFallsBack(t *testing.T) {
	svc, qDrv, tRepo, aRepo := newTestDispatchSvc()
	svc = svc.WithTxPath(NewMemoryLifecycle(), nil, nil)
	ctx := context.Background()

	tRepo.tasks["acme:task-1"] = makeDispatchTestTask("acme", "task-1", "mb-1", 1)
	aRepo.attempts = []*core.TaskAttempt{{
		TenantID: "acme", TaskID: "task-1", Attempt: 1,
		LeaseID: "lease-abc", DeliveryRef: "ref-1", Status: "running",
	}}

	err := svc.NackTask(ctx, "acme", "task-1", "lease-abc", false, nil)
	require.NoError(t, err)
	assert.Equal(t, core.TaskStatusDeadLettered, tRepo.tasks["acme:task-1"].Status)
	assert.Equal(t, 1, qDrv.nackCalls)
}

func TestCov_NackTask_QueueSideEffectErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("nack error after dead letter is logged", func(t *testing.T) {
		svc, qDrv, tRepo, aRepo, _ := newCovDispatchSvc()
		qDrv.nackErr = errors.New("nats nack down")
		tRepo.tasks["acme:task-1"] = makeDispatchTestTask("acme", "task-1", "mb-1", 1)
		aRepo.attempts = []*core.TaskAttempt{{
			TenantID: "acme", TaskID: "task-1", Attempt: 1,
			LeaseID: "lease-abc", DeliveryRef: "ref-1", Status: "running",
		}}
		err := svc.NackTask(ctx, "acme", "task-1", "lease-abc", false, nil)
		require.NoError(t, err, "queue nack failure after commit is logged, not returned")
		assert.Equal(t, core.TaskStatusDeadLettered, tRepo.tasks["acme:task-1"].Status)
	})

	t.Run("ack error after retry schedule is logged", func(t *testing.T) {
		svc, qDrv, tRepo, aRepo, mRepo := newCovDispatchSvc()
		qDrv.ackErr = errors.New("nats ack down")
		mRepo.mailboxes["acme:mb-1"] = &core.Mailbox{TenantID: "acme", ID: "mb-1", RetryPolicy: core.DefaultRetryPolicy()}
		tRepo.tasks["acme:task-1"] = makeDispatchTestTask("acme", "task-1", "mb-1", 1)
		aRepo.attempts = []*core.TaskAttempt{{
			TenantID: "acme", TaskID: "task-1", Attempt: 1,
			LeaseID: "lease-abc", DeliveryRef: "ref-1", Status: "running",
		}}
		err := svc.NackTask(ctx, "acme", "task-1", "lease-abc", true, nil)
		require.NoError(t, err)
		assert.Equal(t, core.TaskStatusRetryScheduled, tRepo.tasks["acme:task-1"].Status)
	})
}

// --- task service ---

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
	assert.NoError(t, err, "DB already committed; queue ack failure is logged, not returned")
	assert.Equal(t, core.TaskStatusCompleted, tRepo.tasks["acme:task-1"].Status)
	found := false
	for _, evt := range qDrv.events {
		if evt.EventType == core.EventTaskCompleted {
			var payload map[string]string
			if json.Unmarshal(evt.Payload, &payload) == nil && payload["ack_error"] == "ack fail" {
				found = true
			}
		}
	}
	assert.True(t, found, "ack failure must emit a warn event via outbox")
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
