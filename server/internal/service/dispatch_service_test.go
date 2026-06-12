package service

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

type mockDispatchQueueDriver struct {
	deliveries []core.TaskDelivery
	events     []core.JanusEvent
	fetchErr   error
	ackErr     error
}

func (m *mockDispatchQueueDriver) PublishTask(_ context.Context, msg core.TaskMessage) error {
	return nil
}

func (m *mockDispatchQueueDriver) FetchTasks(_ context.Context, mailbox string, opts core.FetchOptions) ([]core.TaskDelivery, error) {
	if m.fetchErr != nil {
		return nil, m.fetchErr
	}
	result := m.deliveries
	m.deliveries = nil
	return result, nil
}

func (m *mockDispatchQueueDriver) AckTask(_ context.Context, ref core.DeliveryRef) error {
	return m.ackErr
}

func (m *mockDispatchQueueDriver) NackTask(_ context.Context, ref core.DeliveryRef, reason core.NackReason) error {
	return nil
}

func (m *mockDispatchQueueDriver) PublishDLQ(_ context.Context, msg core.TaskMessage, errPayload []byte) error {
	return nil
}

func (m *mockDispatchQueueDriver) PublishEvent(_ context.Context, event core.JanusEvent) error {
	m.events = append(m.events, event)
	return nil
}

func (m *mockDispatchQueueDriver) ReplayEvents(_ context.Context, filter core.EventReplayFilter) (core.EventIterator, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *mockDispatchQueueDriver) EnsureTenant(_ context.Context, tenantID string) error { return nil }
func (m *mockDispatchQueueDriver) EnsureMailbox(_ context.Context, spec core.MailboxSpec) error {
	return nil
}
func (m *mockDispatchQueueDriver) EnsureConsumer(_ context.Context, spec core.ConsumerSpec) error {
	return nil
}
func (m *mockDispatchQueueDriver) Close() error { return nil }

type mockDispatchAttemptRepo struct {
	attempts []*core.TaskAttempt
	createErr error
}

func (m *mockDispatchAttemptRepo) Create(_ context.Context, a core.TaskAttempt) error {
	if m.createErr != nil {
		return m.createErr
	}
	m.attempts = append(m.attempts, &a)
	return nil
}

func (m *mockDispatchAttemptRepo) GetLatest(_ context.Context, tenantID, taskID string) (*core.TaskAttempt, error) {
	for i := len(m.attempts) - 1; i >= 0; i-- {
		a := m.attempts[i]
		if a.TenantID == tenantID && a.TaskID == taskID {
			return a, nil
		}
	}
	return nil, fmt.Errorf("not found")
}

func (m *mockDispatchAttemptRepo) UpdateHeartbeat(_ context.Context, tenantID, taskID string, attempt int) error {
	return nil
}

func (m *mockDispatchAttemptRepo) UpdateFinished(_ context.Context, tenantID, taskID string, attempt int, status string, errJSON []byte, usageJSON []byte) error {
	for i := len(m.attempts) - 1; i >= 0; i-- {
		a := m.attempts[i]
		if a.TaskID == taskID && a.Attempt == attempt {
			a.Status = status
			now := time.Now()
			a.FinishedAt = &now
			return nil
		}
	}
	return nil
}

func (m *mockDispatchAttemptRepo) UpdateFinishedWithCheck(_ context.Context, tenantID, taskID string, attempt int, status string, errJSON []byte, usageJSON []byte) (bool, error) {
	for i := len(m.attempts) - 1; i >= 0; i-- {
		a := m.attempts[i]
		if a.TaskID == taskID && a.Attempt == attempt {
			if a.Status != "claimed" && a.Status != "running" {
				return false, nil
			}
			a.Status = status
			now := time.Now()
			a.FinishedAt = &now
			return true, nil
		}
	}
	return false, nil
}

type mockDispatchTaskRepo struct {
	tasks    map[string]*core.Task
	updateErr error
}

func (m *mockDispatchTaskRepo) Create(_ context.Context, task core.Task) error {
	return nil
}

func (m *mockDispatchTaskRepo) Get(_ context.Context, tenantID, taskID string) (*core.Task, error) {
	key := tenantID + ":" + taskID
	t, ok := m.tasks[key]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return t, nil
}

func (m *mockDispatchTaskRepo) GetByIdempotencyKey(_ context.Context, tenantID, key string) (*core.Task, error) {
	return nil, fmt.Errorf("not found")
}

func (m *mockDispatchTaskRepo) UpdateStatus(_ context.Context, tenantID, taskID string, status core.TaskStatus, attemptInc int) error {
	if m.updateErr != nil {
		return m.updateErr
	}
	key := tenantID + ":" + taskID
	t, ok := m.tasks[key]
	if !ok {
		return fmt.Errorf("not found")
	}
	t.Status = status
	t.AttemptCount += attemptInc
	if status == core.TaskStatusCompleted {
		now := time.Now()
		t.CompletedAt = &now
	}
	return nil
}

func (m *mockDispatchTaskRepo) UpdateStatusWithCheck(_ context.Context, tenantID, taskID string, expectedStatus, newStatus core.TaskStatus, attemptInc int) (bool, error) {
	if m.updateErr != nil {
		return false, m.updateErr
	}
	key := tenantID + ":" + taskID
	t, ok := m.tasks[key]
	if !ok {
		return false, nil
	}
	if t.Status != expectedStatus {
		return false, nil
	}
	t.Status = newStatus
	t.AttemptCount += attemptInc
	if newStatus == core.TaskStatusCompleted {
		now := time.Now()
		t.CompletedAt = &now
	}
	return true, nil
}

func (m *mockDispatchTaskRepo) ListByStatus(_ context.Context, tenantID string, status core.TaskStatus, limit int) ([]*core.Task, error) {
	return nil, nil
}

func (m *mockDispatchTaskRepo) UpdateRetryAt(_ context.Context, tenantID, taskID string, retryAt time.Time) error {
	key := tenantID + ":" + taskID
	if t, ok := m.tasks[key]; ok {
		t.Status = core.TaskStatusRetryScheduled
	}
	return nil
}

func (m *mockDispatchTaskRepo) SetResultRef(_ context.Context, _, _, _ string) error { return nil }
func (m *mockDispatchTaskRepo) CountByStatus(_ context.Context, _ string, _ core.TaskStatus) (int, error) {
	return 0, nil
}
func (m *mockDispatchTaskRepo) ResetForReplay(_ context.Context, _, _ string) error { return nil }

type mockDispatchMailboxRepo struct {
	mailboxes map[string]*core.Mailbox
}

func (m *mockDispatchMailboxRepo) Create(_ context.Context, mailbox core.Mailbox) error { return nil }
func (m *mockDispatchMailboxRepo) Get(_ context.Context, tenantID, mailboxID string) (*core.Mailbox, error) {
	key := tenantID + ":" + mailboxID
	mb, ok := m.mailboxes[key]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return mb, nil
}
func (m *mockDispatchMailboxRepo) ListByAgent(_ context.Context, tenantID, agentID string) ([]*core.Mailbox, error) {
	return nil, nil
}

func (m *mockDispatchMailboxRepo) Backlog(_ context.Context, tenantID, mailboxID string) (int, error) {
	return 0, nil
}

func (m *mockDispatchMailboxRepo) UpdateStatus(_ context.Context, tenantID, mailboxID string, status core.MailboxStatus) error {
	return nil
}

func (m *mockDispatchMailboxRepo) UpdateConfig(_ context.Context, tenantID, mailboxID string, maxConcurrency, ackWaitSeconds, maxDeliver, retentionSeconds int) error {
	return nil
}

func newTestDispatchSvc() (*DispatchService, *mockDispatchQueueDriver, *mockDispatchTaskRepo, *mockDispatchAttemptRepo) {
	qDrv := &mockDispatchQueueDriver{}
	tRepo := &mockDispatchTaskRepo{tasks: make(map[string]*core.Task)}
	aRepo := &mockDispatchAttemptRepo{}
	mRepo := &mockDispatchMailboxRepo{mailboxes: make(map[string]*core.Mailbox)}
	policySvc := NewPolicyService(&mockPolicyRuleRepo{})
	budgetSvc := NewBudgetService(&mockBudgetRepo{})
	svc := NewDispatchService(tRepo, aRepo, mRepo, qDrv, policySvc, budgetSvc)
	return svc, qDrv, tRepo, aRepo
}

func makeDispatchTestTask(tenantID, taskID, mailboxID string, attemptCount int) *core.Task {
	return &core.Task{
		TenantID:     tenantID,
		ID:           taskID,
		MailboxID:    mailboxID,
		Status:       core.TaskStatusQueued,
		Priority:     core.PriorityNormal,
		AttemptCount: attemptCount,
		Envelope: core.TaskEnvelope{
			JanusVersion: "0.1", TaskID: taskID, TenantID: tenantID,
			SourceAgent: "agent-a",
			Target:      core.Target{Type: core.TargetTypeCapability, Value: "code_review"},
			Payload:     core.Payload{Type: "review", Content: "x"},
			Trace:       core.TraceContext{TraceID: "trace_" + taskID},
		},
	}
}

func TestDispatchService_PullTask_Success(t *testing.T) {
	svc, qDrv, tRepo, _ := newTestDispatchSvc()
	ctx := context.Background()

	task := makeDispatchTestTask("acme", "task-1", "mb-1", 0)
	tRepo.tasks["acme:task-1"] = task
	qDrv.deliveries = []core.TaskDelivery{
		{TaskID: "task-1", DeliveryRef: "ref-1", Payload: []byte("{}")},
	}

	result, err := svc.PullTask(ctx, "acme", "mb-1", "agent-1")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "task-1", result.Task.ID)
	assert.NotEmpty(t, result.LeaseID)
	assert.False(t, result.ExpiresAt.IsZero())

	gotTask := tRepo.tasks["acme:task-1"]
	assert.Equal(t, core.TaskStatusClaimed, gotTask.Status)
	assert.Equal(t, 1, gotTask.AttemptCount)

	assert.Len(t, qDrv.events, 1)
	assert.Equal(t, core.EventTaskClaimed, qDrv.events[0].EventType)
}

func TestDispatchService_PullTask_NoMessages(t *testing.T) {
	svc, _, _, _ := newTestDispatchSvc()
	ctx := context.Background()

	result, err := svc.PullTask(ctx, "acme", "mb-1", "agent-1")
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestDispatchService_PullTask_FetchError(t *testing.T) {
	svc, qDrv, _, _ := newTestDispatchSvc()
	ctx := context.Background()
	qDrv.fetchErr = fmt.Errorf("nats down")

	_, err := svc.PullTask(ctx, "acme", "mb-1", "agent-1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "fetch tasks")
}

func TestDispatchService_PullTask_Validation(t *testing.T) {
	svc, _, _, _ := newTestDispatchSvc()
	ctx := context.Background()

	_, err := svc.PullTask(ctx, "", "mb-1", "agent-1")
	assert.EqualError(t, err, "tenant id, mailbox id, and agent id are required")

	_, err = svc.PullTask(ctx, "acme", "", "agent-1")
	assert.EqualError(t, err, "tenant id, mailbox id, and agent id are required")

	_, err = svc.PullTask(ctx, "acme", "mb-1", "")
	assert.EqualError(t, err, "tenant id, mailbox id, and agent id are required")
}

func TestDispatchService_PullTask_PolicyDenied(t *testing.T) {
	svc, _, _, _ := newTestDispatchSvc()
	svc.policySvc = NewPolicyService(&mockPolicyRuleRepo{
		rules: []*core.PolicyRule{{
			TenantID: "acme", ID: "deny-dispatch", Name: "Deny Dispatch",
			Status: "active", Priority: 100,
			Condition: json.RawMessage(`{"action":"dispatch"}`),
			Action:    json.RawMessage(`{"decision":"deny"}`),
		}},
	})
	ctx := context.Background()

	_, err := svc.PullTask(ctx, "acme", "mb-1", "agent-1")
	require.Error(t, err)
	var bpErr *core.BackpressureError
	require.ErrorAs(t, err, &bpErr)
}

func TestDispatchService_StartTask_Success(t *testing.T) {
	svc, qDrv, tRepo, aRepo := newTestDispatchSvc()
	ctx := context.Background()

	tRepo.tasks["acme:task-1"] = makeDispatchTestTask("acme", "task-1", "mb-1", 1)
	aRepo.attempts = []*core.TaskAttempt{{
		TenantID: "acme", TaskID: "task-1", Attempt: 1,
		AgentID: "agent-1", LeaseID: "lease-abc", DeliveryRef: "ref-1", Status: "claimed",
	}}

	err := svc.StartTask(ctx, "acme", "task-1", "lease-abc")
	require.NoError(t, err)
	assert.Equal(t, core.TaskStatusRunning, tRepo.tasks["acme:task-1"].Status)

	assert.Len(t, qDrv.events, 1)
	assert.Equal(t, core.EventTaskStarted, qDrv.events[0].EventType)
}

func TestDispatchService_StartTask_LeaseMismatch(t *testing.T) {
	svc, _, tRepo, aRepo := newTestDispatchSvc()
	ctx := context.Background()

	tRepo.tasks["acme:task-1"] = makeDispatchTestTask("acme", "task-1", "mb-1", 1)
	aRepo.attempts = []*core.TaskAttempt{{
		TenantID: "acme", TaskID: "task-1", Attempt: 1,
		AgentID: "agent-1", LeaseID: "lease-abc", DeliveryRef: "ref-1", Status: "claimed",
	}}

	err := svc.StartTask(ctx, "acme", "task-1", "wrong-lease")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "lease mismatch")
}

func TestDispatchService_StartTask_Validation(t *testing.T) {
	svc, _, _, _ := newTestDispatchSvc()
	ctx := context.Background()

	err := svc.StartTask(ctx, "", "task-1", "lease")
	assert.EqualError(t, err, "tenant id, task id, and lease id are required")
}

func TestDispatchService_TaskHeartbeat_Success(t *testing.T) {
	svc, _, _, aRepo := newTestDispatchSvc()
	ctx := context.Background()

	aRepo.attempts = []*core.TaskAttempt{{
		TenantID: "acme", TaskID: "task-1", Attempt: 1,
		LeaseID: "lease-abc", DeliveryRef: "ref-1", Status: "running",
	}}

	err := svc.TaskHeartbeat(ctx, "acme", "task-1", "lease-abc")
	assert.NoError(t, err)
}

func TestDispatchService_TaskHeartbeat_LeaseMismatch(t *testing.T) {
	svc, _, _, aRepo := newTestDispatchSvc()
	ctx := context.Background()

	aRepo.attempts = []*core.TaskAttempt{{
		TenantID: "acme", TaskID: "task-1", Attempt: 1,
		LeaseID: "lease-abc", DeliveryRef: "ref-1", Status: "running",
	}}

	err := svc.TaskHeartbeat(ctx, "acme", "task-1", "wrong")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "lease mismatch")
}

func TestDispatchService_TaskHeartbeat_Validation(t *testing.T) {
	svc, _, _, _ := newTestDispatchSvc()
	err := svc.TaskHeartbeat(context.Background(), "", "task-1", "lease")
	assert.EqualError(t, err, "tenant id, task id, and lease id are required")
}

func TestDispatchService_AckTask_Success(t *testing.T) {
	svc, qDrv, tRepo, aRepo := newTestDispatchSvc()
	ctx := context.Background()

	tRepo.tasks["acme:task-1"] = makeDispatchTestTask("acme", "task-1", "mb-1", 1)
	aRepo.attempts = []*core.TaskAttempt{{
		TenantID: "acme", TaskID: "task-1", Attempt: 1,
		LeaseID: "lease-abc", DeliveryRef: "ref-1", Status: "running",
	}}

	usage := &core.TokenUsage{PromptTokens: 100, CompletionTokens: 50, TotalTokens: 150}
	err := svc.AckTask(ctx, "acme", "task-1", "lease-abc", "s3://results/1", usage)
	require.NoError(t, err)
	assert.Equal(t, core.TaskStatusCompleted, tRepo.tasks["acme:task-1"].Status)
	assert.NotNil(t, tRepo.tasks["acme:task-1"].CompletedAt)

	assert.Len(t, qDrv.events, 1)
	assert.Equal(t, core.EventTaskCompleted, qDrv.events[0].EventType)
}

func TestDispatchService_AckTask_NoUsage(t *testing.T) {
	svc, _, tRepo, aRepo := newTestDispatchSvc()
	ctx := context.Background()

	tRepo.tasks["acme:task-1"] = makeDispatchTestTask("acme", "task-1", "mb-1", 1)
	aRepo.attempts = []*core.TaskAttempt{{
		TenantID: "acme", TaskID: "task-1", Attempt: 1,
		LeaseID: "lease-abc", DeliveryRef: "ref-1", Status: "running",
	}}

	err := svc.AckTask(ctx, "acme", "task-1", "lease-abc", "", nil)
	require.NoError(t, err)
	assert.Equal(t, core.TaskStatusCompleted, tRepo.tasks["acme:task-1"].Status)
}

func TestDispatchService_AckTask_LeaseMismatch(t *testing.T) {
	svc, _, tRepo, aRepo := newTestDispatchSvc()
	ctx := context.Background()

	tRepo.tasks["acme:task-1"] = makeDispatchTestTask("acme", "task-1", "mb-1", 1)
	aRepo.attempts = []*core.TaskAttempt{{
		TenantID: "acme", TaskID: "task-1", Attempt: 1,
		LeaseID: "lease-abc", DeliveryRef: "ref-1", Status: "running",
	}}

	err := svc.AckTask(ctx, "acme", "task-1", "wrong", "", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "lease mismatch")
}

func TestDispatchService_AckTask_Validation(t *testing.T) {
	svc, _, _, _ := newTestDispatchSvc()
	err := svc.AckTask(context.Background(), "", "task-1", "lease", "", nil)
	assert.EqualError(t, err, "tenant id, task id, and lease id are required")
}

func TestDispatchService_NackTask_Retriable(t *testing.T) {
	svc, qDrv, tRepo, aRepo := newTestDispatchSvc()
	ctx := context.Background()

	mbRepo := svc.mailboxRepo.(*mockDispatchMailboxRepo)
	mbRepo.mailboxes["acme:mb-1"] = &core.Mailbox{
		TenantID: "acme", ID: "mb-1", RetryPolicy: core.DefaultRetryPolicy(),
	}

	task := makeDispatchTestTask("acme", "task-1", "mb-1", 1)
	tRepo.tasks["acme:task-1"] = task
	aRepo.attempts = []*core.TaskAttempt{{
		TenantID: "acme", TaskID: "task-1", Attempt: 1,
		LeaseID: "lease-abc", DeliveryRef: "ref-1", Status: "running",
	}}

	taskErr := &core.TaskError{Code: "TIMEOUT", Message: "agent timed out"}
	err := svc.NackTask(ctx, "acme", "task-1", "lease-abc", true, taskErr)
	require.NoError(t, err)
	assert.Equal(t, core.TaskStatusRetryScheduled, tRepo.tasks["acme:task-1"].Status)

	assert.Len(t, qDrv.events, 1)
	assert.Equal(t, core.EventTaskRetryScheduled, qDrv.events[0].EventType)
}

func TestDispatchService_NackTask_NonRetriable(t *testing.T) {
	svc, qDrv, tRepo, aRepo := newTestDispatchSvc()
	ctx := context.Background()

	task := makeDispatchTestTask("acme", "task-1", "mb-1", 1)
	tRepo.tasks["acme:task-1"] = task
	aRepo.attempts = []*core.TaskAttempt{{
		TenantID: "acme", TaskID: "task-1", Attempt: 1,
		LeaseID: "lease-abc", DeliveryRef: "ref-1", Status: "running",
	}}

	err := svc.NackTask(ctx, "acme", "task-1", "lease-abc", false, &core.TaskError{Code: "FATAL", Message: "unrecoverable"})
	require.NoError(t, err)
	assert.Equal(t, core.TaskStatusDeadLettered, tRepo.tasks["acme:task-1"].Status)

	assert.Len(t, qDrv.events, 1)
	assert.Equal(t, core.EventTaskDeadLettered, qDrv.events[0].EventType)
}

func TestDispatchService_NackTask_RetriesExhausted(t *testing.T) {
	svc, _, tRepo, aRepo := newTestDispatchSvc()
	ctx := context.Background()

	mbRepo := svc.mailboxRepo.(*mockDispatchMailboxRepo)
	rp := core.DefaultRetryPolicy()
	mbRepo.mailboxes["acme:mb-1"] = &core.Mailbox{
		TenantID: "acme", ID: "mb-1", RetryPolicy: rp,
	}

	task := makeDispatchTestTask("acme", "task-1", "mb-1", 5)
	tRepo.tasks["acme:task-1"] = task
	aRepo.attempts = []*core.TaskAttempt{{
		TenantID: "acme", TaskID: "task-1", Attempt: 5,
		LeaseID: "lease-abc", DeliveryRef: "ref-1", Status: "running",
	}}

	err := svc.NackTask(ctx, "acme", "task-1", "lease-abc", true, nil)
	require.NoError(t, err)
	assert.Equal(t, core.TaskStatusDeadLettered, tRepo.tasks["acme:task-1"].Status)
}

func TestDispatchService_NackTask_LeaseMismatch(t *testing.T) {
	svc, _, tRepo, aRepo := newTestDispatchSvc()
	ctx := context.Background()

	tRepo.tasks["acme:task-1"] = makeDispatchTestTask("acme", "task-1", "mb-1", 1)
	aRepo.attempts = []*core.TaskAttempt{{
		TenantID: "acme", TaskID: "task-1", Attempt: 1,
		LeaseID: "lease-abc", DeliveryRef: "ref-1", Status: "running",
	}}

	err := svc.NackTask(ctx, "acme", "task-1", "wrong", false, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "lease mismatch")
}

func TestDispatchService_NackTask_Validation(t *testing.T) {
	svc, _, _, _ := newTestDispatchSvc()
	err := svc.NackTask(context.Background(), "", "task-1", "lease", false, nil)
	assert.EqualError(t, err, "tenant id, task id, and lease id are required")
}

func TestDispatchService_NackTask_NoError(t *testing.T) {
	svc, _, tRepo, aRepo := newTestDispatchSvc()
	ctx := context.Background()

	task := makeDispatchTestTask("acme", "task-1", "mb-1", 1)
	tRepo.tasks["acme:task-1"] = task
	aRepo.attempts = []*core.TaskAttempt{{
		TenantID: "acme", TaskID: "task-1", Attempt: 1,
		LeaseID: "lease-abc", DeliveryRef: "ref-1", Status: "running",
	}}

	err := svc.NackTask(ctx, "acme", "task-1", "lease-abc", false, nil)
	require.NoError(t, err)
	assert.Equal(t, core.TaskStatusDeadLettered, tRepo.tasks["acme:task-1"].Status)
}

func TestDispatchService_PullTask_CreateAttemptError(t *testing.T) {
	svc, qDrv, tRepo, aRepo := newTestDispatchSvc()
	ctx := context.Background()
	aRepo.createErr = fmt.Errorf("db error")

	tRepo.tasks["acme:task-1"] = makeDispatchTestTask("acme", "task-1", "mb-1", 0)
	qDrv.deliveries = []core.TaskDelivery{
		{TaskID: "task-1", DeliveryRef: "ref-1"},
	}

	_, err := svc.PullTask(ctx, "acme", "mb-1", "agent-1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "create attempt")
}

func TestDispatchService_PullTask_TaskNotFound(t *testing.T) {
	svc, qDrv, _, _ := newTestDispatchSvc()
	ctx := context.Background()

	qDrv.deliveries = []core.TaskDelivery{
		{TaskID: "missing", DeliveryRef: "ref-1"},
	}

	_, err := svc.PullTask(ctx, "acme", "mb-1", "agent-1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get task")
}

func TestDispatchService_NackTask_NoMailbox(t *testing.T) {
	svc, _, tRepo, aRepo := newTestDispatchSvc()
	ctx := context.Background()

	task := makeDispatchTestTask("acme", "task-1", "nonexistent-mb", 1)
	tRepo.tasks["acme:task-1"] = task
	aRepo.attempts = []*core.TaskAttempt{{
		TenantID: "acme", TaskID: "task-1", Attempt: 1,
		LeaseID: "lease-abc", DeliveryRef: "ref-1", Status: "running",
	}}

	err := svc.NackTask(ctx, "acme", "task-1", "lease-abc", true, nil)
	require.NoError(t, err)
	assert.Equal(t, core.TaskStatusDeadLettered, tRepo.tasks["acme:task-1"].Status)
}

func TestGenerateLeaseID(t *testing.T) {
	id1 := generateLeaseID()
	id2 := generateLeaseID()
	assert.NotEmpty(t, id1)
	assert.NotEqual(t, id1, id2)
	assert.Len(t, id1, 20)
}
