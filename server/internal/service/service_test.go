package service

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

type mockTenantRepo struct {
	tenants map[string]string
	err     error
}

func (m *mockTenantRepo) Create(_ context.Context, id, name string) error {
	if m.err != nil {
		return m.err
	}
	if m.tenants == nil {
		m.tenants = make(map[string]string)
	}
	m.tenants[id] = name
	return nil
}

func (m *mockTenantRepo) GetName(_ context.Context, id string) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	if m.tenants == nil {
		return "", pgx.ErrNoRows
	}
	name, ok := m.tenants[id]
	if !ok {
		return "", pgx.ErrNoRows
	}
	return name, nil
}

func TestTenantService_Create(t *testing.T) {
	svc := NewTenantService(&mockTenantRepo{})
	ctx := context.Background()

	require.NoError(t, svc.Create(ctx, "acme", "Acme Corp"))
}

func TestTenantService_CreateValidation(t *testing.T) {
	svc := NewTenantService(&mockTenantRepo{})
	ctx := context.Background()

	assert.EqualError(t, svc.Create(ctx, "", "Acme"), "tenant id is required")
	assert.EqualError(t, svc.Create(ctx, "acme", ""), "tenant name is required")
}

func TestTenantService_Get(t *testing.T) {
	repo := &mockTenantRepo{tenants: map[string]string{"acme": "Acme Corp"}}
	svc := NewTenantService(repo)
	ctx := context.Background()

	tenant, err := svc.Get(ctx, "acme")
	require.NoError(t, err)
	assert.Equal(t, "Acme Corp", tenant.Name)
}

func TestTenantService_GetNotFound(t *testing.T) {
	svc := NewTenantService(&mockTenantRepo{})
	ctx := context.Background()

	_, err := svc.Get(ctx, "nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestTenantService_GetValidation(t *testing.T) {
	svc := NewTenantService(&mockTenantRepo{})
	_, err := svc.Get(context.Background(), "")
	assert.EqualError(t, err, "tenant id is required")
}

type mockAgentRepo struct {
	agents map[string]*core.Agent
	err    error
}

func (m *mockAgentRepo) Register(_ context.Context, a core.Agent) error {
	if m.err != nil {
		return m.err
	}
	if m.agents == nil {
		m.agents = make(map[string]*core.Agent)
	}
	m.agents[a.TenantID+":"+a.ID] = &a
	return nil
}

func (m *mockAgentRepo) Get(_ context.Context, tenantID, agentID string) (*core.Agent, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.agents == nil {
		return nil, fmt.Errorf("not found")
	}
	a, ok := m.agents[tenantID+":"+agentID]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return a, nil
}

func (m *mockAgentRepo) UpdateStatus(_ context.Context, tenantID, agentID string, status core.AgentStatus) error {
	if m.err != nil {
		return m.err
	}
	key := tenantID + ":" + agentID
	if a, ok := m.agents[key]; ok {
		a.Status = status
	}
	return nil
}

func (m *mockAgentRepo) UpdateHeartbeat(_ context.Context, tenantID, agentID string) error {
	return m.err
}

func (m *mockAgentRepo) List(_ context.Context, tenantID string) ([]*core.Agent, error) {
	if m.err != nil {
		return nil, m.err
	}
	var result []*core.Agent
	for _, a := range m.agents {
		if a.TenantID == tenantID {
			result = append(result, a)
		}
	}
	return result, nil
}

func (m *mockAgentRepo) ListByStatus(_ context.Context, tenantID string, status core.AgentStatus) ([]*core.Agent, error) {
	if m.err != nil {
		return nil, m.err
	}
	var result []*core.Agent
	for _, a := range m.agents {
		if a.TenantID == tenantID && a.Status == status {
			result = append(result, a)
		}
	}
	return result, nil
}

func (m *mockAgentRepo) ListAllByStatus(_ context.Context, status core.AgentStatus) ([]*core.Agent, error) {
	return m.ListByStatus(nil, "", status)
}

func (m *mockAgentRepo) FindByCapability(_ context.Context, tenantID, capability string) ([]*core.Agent, error) {
	if m.err != nil {
		return nil, m.err
	}
	return nil, nil
}

type mockHeartbeatDriver struct {
	pings map[string]bool
	err   error
}

func (m *mockHeartbeatDriver) Ping(_ context.Context, tenantID, agentID string) error {
	if m.err != nil {
		return m.err
	}
	if m.pings == nil {
		m.pings = make(map[string]bool)
	}
	m.pings[tenantID+":"+agentID] = true
	return nil
}

func (m *mockHeartbeatDriver) GetLastHeartbeat(_ context.Context, tenantID, agentID string) (*time.Time, error) {
	return nil, nil
}

func (m *mockHeartbeatDriver) ScanExpired(_ context.Context, tenantID string) ([]string, error) {
	return nil, nil
}

func (m *mockHeartbeatDriver) Remove(_ context.Context, tenantID, agentID string) error {
	return nil
}

func (m *mockHeartbeatDriver) Close() error { return nil }

type mockQueueDriver struct {
	publishedTasks []core.TaskMessage
	publishedEvents []core.JanusEvent
	mailboxes      map[string]bool
	consumers      map[string]bool
	err            error
}

func (m *mockQueueDriver) PublishTask(_ context.Context, msg core.TaskMessage) error {
	if m.err != nil {
		return m.err
	}
	m.publishedTasks = append(m.publishedTasks, msg)
	return nil
}

func (m *mockQueueDriver) FetchTasks(_ context.Context, _ string, _ core.FetchOptions) ([]core.TaskDelivery, error) {
	return nil, nil
}

func (m *mockQueueDriver) AckTask(_ context.Context, _ core.DeliveryRef) error { return nil }

func (m *mockQueueDriver) NackTask(_ context.Context, _ core.DeliveryRef, _ core.NackReason) error {
	return nil
}

func (m *mockQueueDriver) PublishDLQ(_ context.Context, _ core.TaskMessage, _ []byte) error {
	return nil
}

func (m *mockQueueDriver) PublishEvent(_ context.Context, event core.JanusEvent) error {
	if m.err != nil {
		return m.err
	}
	m.publishedEvents = append(m.publishedEvents, event)
	return nil
}

func (m *mockQueueDriver) ReplayEvents(_ context.Context, _ core.EventReplayFilter) (core.EventIterator, error) {
	return nil, nil
}

func (m *mockQueueDriver) EnsureTenant(_ context.Context, _ string) error { return nil }

func (m *mockQueueDriver) EnsureMailbox(_ context.Context, spec core.MailboxSpec) error {
	if m.mailboxes == nil {
		m.mailboxes = make(map[string]bool)
	}
	m.mailboxes[spec.MailboxID] = true
	return nil
}

func (m *mockQueueDriver) EnsureConsumer(_ context.Context, spec core.ConsumerSpec) error {
	if m.consumers == nil {
		m.consumers = make(map[string]bool)
	}
	m.consumers[spec.MailboxID] = true
	return nil
}

func (m *mockQueueDriver) Close() error { return nil }

func TestAgentService_Register(t *testing.T) {
	agentRepo := &mockAgentRepo{}
	hb := &mockHeartbeatDriver{}
	qd := &mockQueueDriver{}
	svc := NewAgentService(agentRepo, nil, hb, qd)
	ctx := context.Background()

	err := svc.Register(ctx, core.Agent{
		ID: "agent-1", TenantID: "acme", DisplayName: "Agent 1",
		Protocol: core.ProtocolA2A,
	})
	require.NoError(t, err)

	got, err := svc.Get(ctx, "acme", "agent-1")
	require.NoError(t, err)
	assert.Equal(t, core.AgentStatusOnline, got.Status)
}

func TestAgentService_RegisterValidation(t *testing.T) {
	svc := NewAgentService(&mockAgentRepo{}, nil, &mockHeartbeatDriver{}, &mockQueueDriver{})
	ctx := context.Background()

	assert.EqualError(t, svc.Register(ctx, core.Agent{TenantID: "acme", DisplayName: "x"}), "agent id is required")
	assert.EqualError(t, svc.Register(ctx, core.Agent{ID: "a", DisplayName: "x"}), "tenant id is required")
	assert.EqualError(t, svc.Register(ctx, core.Agent{ID: "a", TenantID: "acme"}), "display name is required")
}

func TestAgentService_Heartbeat(t *testing.T) {
	agentRepo := &mockAgentRepo{agents: map[string]*core.Agent{
		"acme:agent-1": {ID: "agent-1", TenantID: "acme"},
	}}
	hb := &mockHeartbeatDriver{}
	svc := NewAgentService(agentRepo, nil, hb, nil)
	ctx := context.Background()

	require.NoError(t, svc.Heartbeat(ctx, "acme", "agent-1"))
	assert.True(t, hb.pings["acme:agent-1"])
}

func TestAgentService_List(t *testing.T) {
	agentRepo := &mockAgentRepo{agents: map[string]*core.Agent{
		"acme:a1": {ID: "a1", TenantID: "acme"},
		"acme:a2": {ID: "a2", TenantID: "acme"},
	}}
	svc := NewAgentService(agentRepo, nil, nil, nil)
	ctx := context.Background()

	agents, err := svc.List(ctx, "acme")
	require.NoError(t, err)
	assert.Len(t, agents, 2)
}

func TestAgentService_UpdateStatus(t *testing.T) {
	agentRepo := &mockAgentRepo{agents: map[string]*core.Agent{
		"acme:a1": {ID: "a1", TenantID: "acme", Status: core.AgentStatusOnline},
	}}
	svc := NewAgentService(agentRepo, nil, nil, nil)
	ctx := context.Background()

	require.NoError(t, svc.UpdateStatus(ctx, "acme", "a1", core.AgentStatusOffline))
	assert.Equal(t, core.AgentStatusOffline, agentRepo.agents["acme:a1"].Status)
}

type mockMailboxRepo struct {
	mailboxes map[string]*core.Mailbox
	err       error
}

func (m *mockMailboxRepo) Create(_ context.Context, mb core.Mailbox) error {
	if m.err != nil {
		return m.err
	}
	if m.mailboxes == nil {
		m.mailboxes = make(map[string]*core.Mailbox)
	}
	m.mailboxes[mb.TenantID+":"+mb.ID] = &mb
	return nil
}

func (m *mockMailboxRepo) Get(_ context.Context, tenantID, mailboxID string) (*core.Mailbox, error) {
	if m.err != nil {
		return nil, m.err
	}
	mb, ok := m.mailboxes[tenantID+":"+mailboxID]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return mb, nil
}

func (m *mockMailboxRepo) ListByAgent(_ context.Context, tenantID, agentID string) ([]*core.Mailbox, error) {
	if m.err != nil {
		return nil, m.err
	}
	var result []*core.Mailbox
	for _, mb := range m.mailboxes {
		if mb.TenantID == tenantID && mb.AgentID == agentID {
			result = append(result, mb)
		}
	}
	return result, nil
}

func (m *mockMailboxRepo) Backlog(_ context.Context, tenantID, mailboxID string) (int, error) {
	if m.err != nil {
		return 0, m.err
	}
	return 0, nil
}

func (m *mockMailboxRepo) UpdateStatus(_ context.Context, tenantID, mailboxID string, status core.MailboxStatus) error {
	return m.err
}

func (m *mockMailboxRepo) UpdateConfig(_ context.Context, tenantID, mailboxID string, maxConcurrency, ackWaitSeconds, maxDeliver, retentionSeconds int) error {
	return m.err
}

type mockTaskRepo struct {
	tasks map[string]*core.Task
	err   error
}

func (m *mockTaskRepo) Create(_ context.Context, task core.Task) error {
	if m.err != nil {
		return m.err
	}
	if m.tasks == nil {
		m.tasks = make(map[string]*core.Task)
	}
	m.tasks[task.TenantID+":"+task.ID] = &task
	return nil
}

func (m *mockTaskRepo) Get(_ context.Context, tenantID, taskID string) (*core.Task, error) {
	if m.err != nil {
		return nil, m.err
	}
	t, ok := m.tasks[tenantID+":"+taskID]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return t, nil
}

func (m *mockTaskRepo) GetByIdempotencyKey(_ context.Context, tenantID, key string) (*core.Task, error) {
	if m.err != nil {
		return nil, m.err
	}
	for _, t := range m.tasks {
		if t.TenantID == tenantID && t.IdempotencyKey == key {
			return t, nil
		}
	}
	return nil, fmt.Errorf("not found")
}

func (m *mockTaskRepo) UpdateStatus(_ context.Context, tenantID, taskID string, status core.TaskStatus, inc int) error {
	if m.err != nil {
		return m.err
	}
	key := tenantID + ":" + taskID
	if t, ok := m.tasks[key]; ok {
		t.Status = status
		t.AttemptCount += inc
	}
	return nil
}

func (m *mockTaskRepo) UpdateRetryAt(_ context.Context, tenantID, taskID string, retryAt time.Time) error {
	if m.err != nil {
		return m.err
	}
	key := tenantID + ":" + taskID
	if t, ok := m.tasks[key]; ok {
		t.Status = core.TaskStatusRetryScheduled
	}
	return nil
}

func (m *mockTaskRepo) ListByStatus(_ context.Context, tenantID string, status core.TaskStatus, limit int) ([]*core.Task, error) {
	if m.err != nil {
		return nil, m.err
	}
	var result []*core.Task
	for _, t := range m.tasks {
		if t.TenantID == tenantID && t.Status == status {
			result = append(result, t)
			if len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}

func makeTestEnvelope(taskID, tenantID string) core.TaskEnvelope {
	return core.TaskEnvelope{
		JanusVersion: "0.1", TaskID: taskID, TenantID: tenantID,
		SourceAgent: "agent-a",
		Target:      core.Target{Type: core.TargetTypeCapability, Value: "review"},
		Payload:     core.Payload{Type: "review", Content: "x"},
		Trace:       core.TraceContext{TraceID: "trace_1"},
	}
}

func TestTaskService_Create(t *testing.T) {
	taskRepo := &mockTaskRepo{}
	qd := &mockQueueDriver{}
	svc := NewTaskService(taskRepo, qd, nil, nil)
	ctx := context.Background()

	_, err := svc.Create(ctx, core.Task{
		TenantID:    "acme",
		ID:          "task-1",
		SourceAgent: "agent-a",
		TargetType:  core.TargetTypeCapability,
		TargetValue: "review",
		MailboxID:   "mb-1",
		Envelope:    makeTestEnvelope("task-1", "acme"),
	})
	require.NoError(t, err)

	got, err := svc.Get(ctx, "acme", "task-1")
	require.NoError(t, err)
	assert.Equal(t, core.TaskStatusQueued, got.Status)

	assert.Len(t, qd.publishedTasks, 1)
	assert.Equal(t, "task-1", qd.publishedTasks[0].TaskID)

	assert.Len(t, qd.publishedEvents, 2)
	assert.Equal(t, core.EventTaskCreated, qd.publishedEvents[0].EventType)
	assert.Equal(t, core.EventTaskQueued, qd.publishedEvents[1].EventType)
}

func TestTaskService_CreateNoMailbox(t *testing.T) {
	taskRepo := &mockTaskRepo{}
	qd := &mockQueueDriver{}
	svc := NewTaskService(taskRepo, qd, nil, nil)
	ctx := context.Background()

	_, err := svc.Create(ctx, core.Task{
		TenantID:    "acme",
		ID:          "task-2",
		SourceAgent: "agent-a",
		TargetType:  core.TargetTypeCapability,
		TargetValue: "review",
		Envelope:    makeTestEnvelope("task-2", "acme"),
	})
	require.NoError(t, err)

	got, _ := svc.Get(ctx, "acme", "task-2")
	assert.Equal(t, core.TaskStatusCreated, got.Status)
	assert.Len(t, qd.publishedTasks, 0)
}

func TestTaskService_CreateValidation(t *testing.T) {
	svc := NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil)
	ctx := context.Background()

	_, err := svc.Create(ctx, core.Task{ID: "x"})
	assert.EqualError(t, err, "tenant id is required")
	_, err = svc.Create(ctx, core.Task{TenantID: "acme"})
	assert.EqualError(t, err, "task id is required")
	_, err = svc.Create(ctx, core.Task{TenantID: "acme", ID: "x"})
	assert.EqualError(t, err, "source agent is required")
	_, err = svc.Create(ctx, core.Task{TenantID: "acme", ID: "x", SourceAgent: "a"})
	assert.EqualError(t, err, "target type is required")
}

func TestTaskService_Idempotency(t *testing.T) {
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:task-1": {ID: "task-1", TenantID: "acme", IdempotencyKey: "key-1"},
	}}
	svc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	ctx := context.Background()

	result, err := svc.Create(ctx, core.Task{
		TenantID: "acme", ID: "task-2", SourceAgent: "a",
		TargetType: core.TargetTypeCapability, TargetValue: "r",
		IdempotencyKey: "key-1",
		Envelope: makeTestEnvelope("task-2", "acme"),
	})
	require.NoError(t, err)
	assert.Equal(t, "task-1", result.ID)
}

func TestTaskService_Lifecycle(t *testing.T) {
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusQueued},
	}}
	qd := &mockQueueDriver{}
	svc := NewTaskService(taskRepo, qd, nil, nil)
	ctx := context.Background()

	require.NoError(t, svc.Start(ctx, "acme", "t1"))
	got, _ := svc.Get(ctx, "acme", "t1")
	assert.Equal(t, core.TaskStatusRunning, got.Status)

	require.NoError(t, svc.Complete(ctx, "acme", "t1"))
	got, _ = svc.Get(ctx, "acme", "t1")
	assert.Equal(t, core.TaskStatusCompleted, got.Status)
}

func TestTaskService_Fail(t *testing.T) {
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusRunning, AttemptCount: 0},
	}}
	svc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	ctx := context.Background()

	require.NoError(t, svc.Fail(ctx, "acme", "t1", &core.TaskError{Code: "ERR", Message: "fail"}))
	got, _ := svc.Get(ctx, "acme", "t1")
	assert.Equal(t, core.TaskStatusFailed, got.Status)
	assert.Equal(t, 1, got.AttemptCount)
}

func TestTaskService_Cancel(t *testing.T) {
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusQueued},
	}}
	svc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	ctx := context.Background()

	require.NoError(t, svc.Cancel(ctx, "acme", "t1"))
	got, _ := svc.Get(ctx, "acme", "t1")
	assert.Equal(t, core.TaskStatusCancelled, got.Status)
}

func TestTaskService_ListByStatus(t *testing.T) {
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusQueued},
		"acme:t2": {ID: "t2", TenantID: "acme", Status: core.TaskStatusQueued},
		"acme:t3": {ID: "t3", TenantID: "acme", Status: core.TaskStatusRunning},
	}}
	svc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	ctx := context.Background()

	tasks, err := svc.ListByStatus(ctx, "acme", core.TaskStatusQueued, 10)
	require.NoError(t, err)
	assert.Len(t, tasks, 2)
}

func TestMailboxService_Create(t *testing.T) {
	mbRepo := &mockMailboxRepo{}
	qd := &mockQueueDriver{}
	svc := NewMailboxService(mbRepo, qd)
	ctx := context.Background()

	err := svc.Create(ctx, core.Mailbox{
		TenantID: "acme", ID: "reviewer_default", AgentID: "agent-1",
		MaxConcurrency: 2,
	})
	require.NoError(t, err)
	assert.True(t, qd.mailboxes["reviewer_default"])
	assert.True(t, qd.consumers["reviewer_default"])

	got, err := svc.Get(ctx, "acme", "reviewer_default")
	require.NoError(t, err)
	assert.Equal(t, core.MailboxStatusActive, got.Status)
	assert.Equal(t, 300, got.ACKWaitSeconds)
}

func TestMailboxService_CreateDefaults(t *testing.T) {
	mbRepo := &mockMailboxRepo{}
	qd := &mockQueueDriver{}
	svc := NewMailboxService(mbRepo, qd)
	ctx := context.Background()

	err := svc.Create(ctx, core.Mailbox{
		TenantID: "acme", ID: "mb1", AgentID: "a1",
	})
	require.NoError(t, err)

	got := mbRepo.mailboxes["acme:mb1"]
	assert.Equal(t, 1, got.MaxConcurrency)
	assert.Equal(t, 300, got.ACKWaitSeconds)
	assert.Equal(t, 5, got.MaxDeliver)
	assert.Equal(t, 604800, got.RetentionSeconds)
}

func TestMailboxService_CreateValidation(t *testing.T) {
	svc := NewMailboxService(&mockMailboxRepo{}, &mockQueueDriver{})
	ctx := context.Background()

	assert.EqualError(t, svc.Create(ctx, core.Mailbox{ID: "x"}), "tenant id is required")
	assert.EqualError(t, svc.Create(ctx, core.Mailbox{TenantID: "acme"}), "mailbox id is required")
	assert.EqualError(t, svc.Create(ctx, core.Mailbox{TenantID: "acme", ID: "x"}), "agent id is required")
}

func TestMailboxService_ListByAgent(t *testing.T) {
	mbRepo := &mockMailboxRepo{mailboxes: map[string]*core.Mailbox{
		"acme:mb1": {ID: "mb1", TenantID: "acme", AgentID: "a1"},
		"acme:mb2": {ID: "mb2", TenantID: "acme", AgentID: "a1"},
		"acme:mb3": {ID: "mb3", TenantID: "acme", AgentID: "a2"},
	}}
	svc := NewMailboxService(mbRepo, nil)
	ctx := context.Background()

	mbs, err := svc.ListByAgent(ctx, "acme", "a1")
	require.NoError(t, err)
	assert.Len(t, mbs, 2)
}

func TestAgentService_ListByStatus(t *testing.T) {
	agentRepo := &mockAgentRepo{agents: map[string]*core.Agent{
		"acme:a1": {ID: "a1", TenantID: "acme", Status: core.AgentStatusOnline},
		"acme:a2": {ID: "a2", TenantID: "acme", Status: core.AgentStatusOffline},
		"acme:a3": {ID: "a3", TenantID: "acme", Status: core.AgentStatusOnline},
	}}
	svc := NewAgentService(agentRepo, nil, nil, nil)
	ctx := context.Background()

	agents, err := svc.ListByStatus(ctx, "acme", core.AgentStatusOnline)
	require.NoError(t, err)
	assert.Len(t, agents, 2)
}

func TestAgentService_GetError(t *testing.T) {
	svc := NewAgentService(&mockAgentRepo{}, nil, nil, nil)
	_, err := svc.Get(context.Background(), "", "x")
	assert.EqualError(t, err, "tenant id and agent id are required")
}

func TestAgentService_HeartbeatValidation(t *testing.T) {
	svc := NewAgentService(&mockAgentRepo{}, nil, &mockHeartbeatDriver{}, nil)
	err := svc.Heartbeat(context.Background(), "", "x")
	assert.EqualError(t, err, "tenant id and agent id are required")
}

func TestAgentService_UpdateStatusValidation(t *testing.T) {
	svc := NewAgentService(&mockAgentRepo{}, nil, nil, nil)
	err := svc.UpdateStatus(context.Background(), "", "x", core.AgentStatusOnline)
	assert.EqualError(t, err, "tenant id and agent id are required")
}

func TestAgentService_ListValidation(t *testing.T) {
	svc := NewAgentService(&mockAgentRepo{}, nil, nil, nil)
	_, err := svc.List(context.Background(), "")
	assert.EqualError(t, err, "tenant id is required")
}

func TestAgentService_ListByStatusValidation(t *testing.T) {
	svc := NewAgentService(&mockAgentRepo{}, nil, nil, nil)
	_, err := svc.ListByStatus(context.Background(), "", core.AgentStatusOnline)
	assert.EqualError(t, err, "tenant id is required")
}

func TestAgentService_RegisterHeartbeatError(t *testing.T) {
	svc := NewAgentService(&mockAgentRepo{}, nil, &mockHeartbeatDriver{err: fmt.Errorf("redis down")}, &mockQueueDriver{})
	err := svc.Register(context.Background(), core.Agent{
		ID: "a1", TenantID: "acme", DisplayName: "A1", Protocol: core.ProtocolA2A,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "initial heartbeat")
}

func TestTaskService_GetValidation(t *testing.T) {
	svc := NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil)
	_, err := svc.Get(context.Background(), "", "x")
	assert.EqualError(t, err, "tenant id and task id are required")
}

func TestTaskService_ListByStatusValidation(t *testing.T) {
	svc := NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil)
	_, err := svc.ListByStatus(context.Background(), "", core.TaskStatusQueued, 0)
	assert.EqualError(t, err, "tenant id is required")
}

func TestTaskService_TransitionValidation(t *testing.T) {
	svc := NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil)
	err := svc.Start(context.Background(), "", "x")
	assert.EqualError(t, err, "tenant id and task id are required")
}

func TestTaskService_CreateRepoError(t *testing.T) {
	svc := NewTaskService(&mockTaskRepo{err: fmt.Errorf("db down")}, &mockQueueDriver{}, nil, nil)
_, err := svc.Create(context.Background(), core.Task{
		TenantID: "acme", ID: "t1", SourceAgent: "a",
		TargetType: core.TargetTypeCapability, TargetValue: "r",
		Envelope: makeTestEnvelope("t1", "acme"),
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "create task")
}

func TestTaskService_CreateQueueError(t *testing.T) {
	taskRepo := &mockTaskRepo{}
	qd := &mockQueueDriver{err: fmt.Errorf("nats down")}
	svc := NewTaskService(taskRepo, qd, nil, nil)
_, err := svc.Create(context.Background(), core.Task{
		TenantID: "acme", ID: "t1", SourceAgent: "a",
		TargetType: core.TargetTypeCapability, TargetValue: "r",
		MailboxID: "mb1", Envelope: makeTestEnvelope("t1", "acme"),
	})
	assert.Error(t, err)
}

func TestTaskService_FailNoError(t *testing.T) {
	taskRepo := &mockTaskRepo{tasks: map[string]*core.Task{
		"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusRunning, AttemptCount: 0},
	}}
	svc := NewTaskService(taskRepo, &mockQueueDriver{}, nil, nil)
	ctx := context.Background()
	err := svc.Fail(ctx, "acme", "t1", nil)
	require.NoError(t, err)
	got, _ := svc.Get(ctx, "acme", "t1")
	assert.Equal(t, core.TaskStatusFailed, got.Status)
}

func TestIdempotentError(t *testing.T) {
	err := &IdempotentError{ExistingTaskID: "task-1"}
	assert.Equal(t, "task already exists with idempotency key: task-1", err.Error())
}

func TestMailboxService_GetValidation(t *testing.T) {
	svc := NewMailboxService(&mockMailboxRepo{}, nil)
	_, err := svc.Get(context.Background(), "", "x")
	assert.EqualError(t, err, "tenant id and mailbox id are required")
}

func TestMailboxService_ListByAgentValidation(t *testing.T) {
	svc := NewMailboxService(&mockMailboxRepo{}, nil)
	_, err := svc.ListByAgent(context.Background(), "", "x")
	assert.EqualError(t, err, "tenant id and agent id are required")
}

func TestMailboxService_CreateRepoError(t *testing.T) {
	svc := NewMailboxService(&mockMailboxRepo{err: fmt.Errorf("db down")}, &mockQueueDriver{})
	err := svc.Create(context.Background(), core.Mailbox{
		TenantID: "acme", ID: "mb1", AgentID: "a1",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "create mailbox")
}

func TestAgentService_RegisterRepoError(t *testing.T) {
	svc := NewAgentService(&mockAgentRepo{err: fmt.Errorf("dup")}, nil, &mockHeartbeatDriver{}, &mockQueueDriver{})
	err := svc.Register(context.Background(), core.Agent{
		ID: "a1", TenantID: "acme", DisplayName: "A1", Protocol: core.ProtocolA2A,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "register agent")
}

type mockAgentRepoFailOn struct {
	agents    map[string]*core.Agent
	failOn    string // "register", "updateStatus", "heartbeat", "get", "list", "listByStatus"
}

func (m *mockAgentRepoFailOn) Register(_ context.Context, a core.Agent) error {
	if m.failOn == "register" {
		return fmt.Errorf("register failed")
	}
	if m.agents == nil {
		m.agents = make(map[string]*core.Agent)
	}
	m.agents[a.TenantID+":"+a.ID] = &a
	return nil
}

func (m *mockAgentRepoFailOn) Get(_ context.Context, tenantID, agentID string) (*core.Agent, error) {
	if m.failOn == "get" {
		return nil, fmt.Errorf("get failed")
	}
	a, ok := m.agents[tenantID+":"+agentID]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return a, nil
}

func (m *mockAgentRepoFailOn) UpdateStatus(_ context.Context, tenantID, agentID string, status core.AgentStatus) error {
	if m.failOn == "updateStatus" {
		return fmt.Errorf("updateStatus failed")
	}
	key := tenantID + ":" + agentID
	if a, ok := m.agents[key]; ok {
		a.Status = status
	}
	return nil
}

func (m *mockAgentRepoFailOn) UpdateHeartbeat(_ context.Context, tenantID, agentID string) error {
	if m.failOn == "heartbeat" {
		return fmt.Errorf("heartbeat failed")
	}
	return nil
}

func (m *mockAgentRepoFailOn) List(_ context.Context, tenantID string) ([]*core.Agent, error) {
	var result []*core.Agent
	for _, a := range m.agents {
		if a.TenantID == tenantID {
			result = append(result, a)
		}
	}
	return result, nil
}

func (m *mockAgentRepoFailOn) ListByStatus(_ context.Context, tenantID string, status core.AgentStatus) ([]*core.Agent, error) {
	var result []*core.Agent
	for _, a := range m.agents {
		if a.TenantID == tenantID && a.Status == status {
			result = append(result, a)
		}
	}
	return result, nil
}

func (m *mockAgentRepoFailOn) ListAllByStatus(_ context.Context, status core.AgentStatus) ([]*core.Agent, error) {
	return m.ListByStatus(nil, "", status)
}

func (m *mockAgentRepoFailOn) FindByCapability(_ context.Context, tenantID, capability string) ([]*core.Agent, error) {
	return nil, nil
}

func TestAgentService_RegisterSetOnlineError(t *testing.T) {
	svc := NewAgentService(&mockAgentRepoFailOn{failOn: "updateStatus"}, nil, &mockHeartbeatDriver{}, &mockQueueDriver{})
	err := svc.Register(context.Background(), core.Agent{
		ID: "a1", TenantID: "acme", DisplayName: "A1", Protocol: core.ProtocolA2A,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "set online")
}

func TestAgentService_GetRepoError(t *testing.T) {
	svc := NewAgentService(&mockAgentRepo{err: fmt.Errorf("db err")}, nil, nil, nil)
	_, err := svc.Get(context.Background(), "acme", "x")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get agent")
}

func TestAgentService_HeartbeatPingError(t *testing.T) {
	svc := NewAgentService(&mockAgentRepo{}, nil, &mockHeartbeatDriver{err: fmt.Errorf("redis down")}, nil)
	err := svc.Heartbeat(context.Background(), "acme", "a1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "heartbeat")
}

func TestTenantService_GetRepoError(t *testing.T) {
	svc := NewTenantService(&mockTenantRepo{err: fmt.Errorf("connection lost")})
	_, err := svc.Get(context.Background(), "acme")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get tenant")
}

func TestTaskService_CreateEventError(t *testing.T) {
	taskRepo := &mockTaskRepo{}
	qd := &mockQueueDriver{err: fmt.Errorf("event fail")}
	svc := NewTaskService(taskRepo, qd, nil, nil)
_, err := svc.Create(context.Background(), core.Task{
		TenantID: "acme", ID: "t1", SourceAgent: "a",
		TargetType: core.TargetTypeCapability, TargetValue: "r",
		Envelope: makeTestEnvelope("t1", "acme"),
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "publish created event")
}

func TestTaskService_CreateQueuePublishError(t *testing.T) {
	taskRepo := &mockTaskRepo{}
	qd := &mockQueueDriverFailPublish{err: fmt.Errorf("nats down")}
	svc := NewTaskService(taskRepo, qd, nil, nil)
	_, err := svc.Create(context.Background(), core.Task{
		TenantID: "acme", ID: "t1", SourceAgent: "a",
		TargetType: core.TargetTypeCapability, TargetValue: "r",
		MailboxID: "mb1", Envelope: makeTestEnvelope("t1", "acme"),
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "publish to queue")
}

type mockQueueDriverFailPublish struct {
	err error
}

func (m *mockQueueDriverFailPublish) PublishTask(_ context.Context, _ core.TaskMessage) error {
	return m.err
}
func (m *mockQueueDriverFailPublish) FetchTasks(_ context.Context, _ string, _ core.FetchOptions) ([]core.TaskDelivery, error) {
	return nil, nil
}
func (m *mockQueueDriverFailPublish) AckTask(_ context.Context, _ core.DeliveryRef) error        { return nil }
func (m *mockQueueDriverFailPublish) NackTask(_ context.Context, _ core.DeliveryRef, _ core.NackReason) error {
	return nil
}
func (m *mockQueueDriverFailPublish) PublishDLQ(_ context.Context, _ core.TaskMessage, _ []byte) error {
	return nil
}
func (m *mockQueueDriverFailPublish) PublishEvent(_ context.Context, _ core.JanusEvent) error     { return nil }
func (m *mockQueueDriverFailPublish) ReplayEvents(_ context.Context, _ core.EventReplayFilter) (core.EventIterator, error) {
	return nil, nil
}
func (m *mockQueueDriverFailPublish) EnsureTenant(_ context.Context, _ string) error              { return nil }
func (m *mockQueueDriverFailPublish) EnsureMailbox(_ context.Context, _ core.MailboxSpec) error    { return nil }
func (m *mockQueueDriverFailPublish) EnsureConsumer(_ context.Context, _ core.ConsumerSpec) error { return nil }
func (m *mockQueueDriverFailPublish) Close() error                                                { return nil }

func TestTaskService_CreateUpdateQueuedError(t *testing.T) {
	taskRepo := &mockTaskRepoFailUpdate{}
	qd := &mockQueueDriver{}
	svc := NewTaskService(taskRepo, qd, nil, nil)
	_, err := svc.Create(context.Background(), core.Task{
		TenantID: "acme", ID: "t1", SourceAgent: "a",
		TargetType: core.TargetTypeCapability, TargetValue: "r",
		MailboxID: "mb1", Envelope: makeTestEnvelope("t1", "acme"),
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "update to queued")
}

type mockTaskRepoFailUpdate struct {
	tasks map[string]*core.Task
}

func (m *mockTaskRepoFailUpdate) Create(_ context.Context, task core.Task) error {
	if m.tasks == nil {
		m.tasks = make(map[string]*core.Task)
	}
	m.tasks[task.TenantID+":"+task.ID] = &task
	return nil
}
func (m *mockTaskRepoFailUpdate) Get(_ context.Context, tenantID, taskID string) (*core.Task, error) {
	t, ok := m.tasks[tenantID+":"+taskID]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return t, nil
}
func (m *mockTaskRepoFailUpdate) GetByIdempotencyKey(_ context.Context, _, _ string) (*core.Task, error) {
	return nil, fmt.Errorf("not found")
}
func (m *mockTaskRepoFailUpdate) UpdateStatus(_ context.Context, _, _ string, _ core.TaskStatus, _ int) error {
	return fmt.Errorf("update fail")
}
func (m *mockTaskRepoFailUpdate) UpdateRetryAt(_ context.Context, _, _ string, _ time.Time) error { return nil }
func (m *mockTaskRepoFailUpdate) ListByStatus(_ context.Context, _ string, _ core.TaskStatus, _ int) ([]*core.Task, error) {
	return nil, nil
}

func TestTaskService_TransitionUpdateError(t *testing.T) {
	svc := NewTaskService(&mockTaskRepo{err: fmt.Errorf("update fail")}, &mockQueueDriver{}, nil, nil)
	err := svc.Start(context.Background(), "acme", "t1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "update task status")
}

func TestTaskService_ListByStatusDefault(t *testing.T) {
	svc := NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil)
	tasks, err := svc.ListByStatus(context.Background(), "acme", core.TaskStatusQueued, -1)
	require.NoError(t, err)
	assert.Len(t, tasks, 0)
}

func TestMailboxService_GetNotFound(t *testing.T) {
	svc := NewMailboxService(&mockMailboxRepo{}, nil)
	_, err := svc.Get(context.Background(), "acme", "nonexistent")
	assert.Error(t, err)
}

func TestAgentService_ResolveCapability(t *testing.T) {
	agentRepo := &mockAgentRepo{agents: map[string]*core.Agent{
		"acme:a1": {ID: "a1", TenantID: "acme", Status: core.AgentStatusOnline},
	}}
	svc := NewAgentService(agentRepo, nil, nil, nil)
	ctx := context.Background()

	agents, err := svc.ResolveCapability(ctx, "acme", "review")
	require.NoError(t, err)
	assert.Empty(t, agents)
}

func TestAgentService_ResolveCapability_Validation(t *testing.T) {
	svc := NewAgentService(&mockAgentRepo{}, nil, nil, nil)
	ctx := context.Background()

	_, err := svc.ResolveCapability(ctx, "", "review")
	assert.EqualError(t, err, "tenant id is required")

	_, err = svc.ResolveCapability(ctx, "acme", "")
	assert.EqualError(t, err, "capability is required")
}

func TestMailboxService_Backlog(t *testing.T) {
	svc := NewMailboxService(&mockMailboxRepo{}, nil)
	ctx := context.Background()

	count, err := svc.Backlog(ctx, "acme", "mb1")
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestMailboxService_Backlog_Validation(t *testing.T) {
	svc := NewMailboxService(&mockMailboxRepo{}, nil)
	ctx := context.Background()

	_, err := svc.Backlog(ctx, "", "mb1")
	assert.EqualError(t, err, "tenant id and mailbox id are required")

	_, err = svc.Backlog(ctx, "acme", "")
	assert.EqualError(t, err, "tenant id and mailbox id are required")
}

func TestMailboxService_Pause(t *testing.T) {
	svc := NewMailboxService(&mockMailboxRepo{}, nil)
	ctx := context.Background()

	err := svc.Pause(ctx, "acme", "mb1")
	require.NoError(t, err)
}

func TestMailboxService_Pause_Validation(t *testing.T) {
	svc := NewMailboxService(&mockMailboxRepo{}, nil)
	ctx := context.Background()

	err := svc.Pause(ctx, "", "mb1")
	assert.EqualError(t, err, "tenant id and mailbox id are required")

	err = svc.Pause(ctx, "acme", "")
	assert.EqualError(t, err, "tenant id and mailbox id are required")
}

func TestMailboxService_Resume(t *testing.T) {
	svc := NewMailboxService(&mockMailboxRepo{}, nil)
	ctx := context.Background()

	err := svc.Resume(ctx, "acme", "mb1")
	require.NoError(t, err)
}

func TestMailboxService_Resume_Validation(t *testing.T) {
	svc := NewMailboxService(&mockMailboxRepo{}, nil)
	ctx := context.Background()

	err := svc.Resume(ctx, "", "mb1")
	assert.EqualError(t, err, "tenant id and mailbox id are required")

	err = svc.Resume(ctx, "acme", "")
	assert.EqualError(t, err, "tenant id and mailbox id are required")
}

func TestTaskService_WithPolicy(t *testing.T) {
	svc := NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil)
	result := svc.WithPolicy(NewPolicyService(&mockPolicyRuleRepo{}))
	assert.Same(t, svc, result)
}

func TestTaskService_CreateWithPolicyDenied(t *testing.T) {
	policySvc := NewPolicyService(&mockPolicyRuleRepo{
		rules: []*core.PolicyRule{
			{
				TenantID: "acme", ID: "deny-all", Status: "active", Priority: 1,
				Condition: json.RawMessage(`{"actor.type":"agent"}`),
				Action:    json.RawMessage(`{"decision":"deny"}`),
			},
		},
	})
	svc := NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil).WithPolicy(policySvc)
	ctx := context.Background()

	_, err := svc.Create(ctx, core.Task{
		TenantID: "acme", ID: "t1", SourceAgent: "a",
		TargetType: core.TargetTypeCapability, TargetValue: "r",
		Envelope: makeTestEnvelope("t1", "acme"),
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "policy denied")
}

func TestTaskService_CreateWithPolicyApprovalRequired(t *testing.T) {
	policySvc := NewPolicyService(&mockPolicyRuleRepo{
		rules: []*core.PolicyRule{
			{
				TenantID: "acme", ID: "require-approval", Status: "active", Priority: 1,
				Condition: json.RawMessage(`{"actor.type":"agent"}`),
				Action:    json.RawMessage(`{"decision":"approval_required"}`),
			},
		},
	})
	taskRepo := &mockTaskRepo{}
	qd := &mockQueueDriver{}
	svc := NewTaskService(taskRepo, qd, nil, nil).WithPolicy(policySvc)
	ctx := context.Background()

	_, err := svc.Create(ctx, core.Task{
		TenantID: "acme", ID: "t1", SourceAgent: "a",
		TargetType: core.TargetTypeCapability, TargetValue: "r",
		Envelope: makeTestEnvelope("t1", "acme"),
	})
	require.NoError(t, err)
	got, _ := svc.Get(ctx, "acme", "t1")
	assert.Equal(t, core.TaskStatusApprovalPending, got.Status)
}

func TestTaskService_CreateWithPolicyAllow(t *testing.T) {
	policySvc := NewPolicyService(&mockPolicyRuleRepo{})
	taskRepo := &mockTaskRepo{}
	qd := &mockQueueDriver{}
	svc := NewTaskService(taskRepo, qd, nil, nil).WithPolicy(policySvc)
	ctx := context.Background()

	_, err := svc.Create(ctx, core.Task{
		TenantID: "acme", ID: "t1", SourceAgent: "a",
		TargetType: core.TargetTypeCapability, TargetValue: "r",
		MailboxID: "mb1", Envelope: makeTestEnvelope("t1", "acme"),
	})
	require.NoError(t, err)
	got, _ := svc.Get(ctx, "acme", "t1")
	assert.Equal(t, core.TaskStatusQueued, got.Status)
}

func TestTaskService_CreateWithPolicyError(t *testing.T) {
	policySvc := NewPolicyService(&mockPolicyRuleRepo{err: fmt.Errorf("policy db down")})
	svc := NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil).WithPolicy(policySvc)
	ctx := context.Background()

	_, err := svc.Create(ctx, core.Task{
		TenantID: "acme", ID: "t1", SourceAgent: "a",
		TargetType: core.TargetTypeCapability, TargetValue: "r",
		Envelope: makeTestEnvelope("t1", "acme"),
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "policy check")
}

func TestMailboxService_Backlog_RepoError(t *testing.T) {
	svc := NewMailboxService(&mockMailboxRepo{err: fmt.Errorf("db down")}, nil)
	ctx := context.Background()

	_, err := svc.Backlog(ctx, "acme", "mb1")
	assert.Error(t, err)
}

func TestMailboxService_Pause_RepoError(t *testing.T) {
	svc := NewMailboxService(&mockMailboxRepo{err: fmt.Errorf("db down")}, nil)
	ctx := context.Background()

	err := svc.Pause(ctx, "acme", "mb1")
	assert.Error(t, err)
}

func TestMailboxService_Resume_RepoError(t *testing.T) {
	svc := NewMailboxService(&mockMailboxRepo{err: fmt.Errorf("db down")}, nil)
	ctx := context.Background()

	err := svc.Resume(ctx, "acme", "mb1")
	assert.Error(t, err)
}

func TestAgentService_ResolveCapability_RepoError(t *testing.T) {
	svc := NewAgentService(&mockAgentRepo{err: fmt.Errorf("db down")}, nil, nil, nil)
	ctx := context.Background()

	_, err := svc.ResolveCapability(ctx, "acme", "review")
	assert.Error(t, err)
}
