package service

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/auth"
	"github.com/agentium-lab/Janus/server/internal/service/routing"
)

// --- compact fakes with granular error knobs ---

type cbQueueDriver struct {
	mu         sync.Mutex
	deliveries []core.TaskDelivery
	events     []core.JanusEvent
	tasks      []core.TaskMessage
	fetchErr   error
	ackErr     error
	nackErr    error
	publishErr error
	ackCalls   int
	nackCalls  int
	ensureMbx  int
	ensureCons int
}

func (m *cbQueueDriver) PublishTask(_ context.Context, msg core.TaskMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.publishErr != nil {
		return m.publishErr
	}
	m.tasks = append(m.tasks, msg)
	return nil
}

func (m *cbQueueDriver) FetchTasks(_ context.Context, _, _ string, _ core.FetchOptions) ([]core.TaskDelivery, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fetchErr != nil {
		return nil, m.fetchErr
	}
	result := m.deliveries
	m.deliveries = nil
	return result, nil
}

func (m *cbQueueDriver) AckTask(_ context.Context, _ string, _ core.DeliveryRef) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ackCalls++
	return m.ackErr
}

func (m *cbQueueDriver) NackTask(_ context.Context, _ string, _ core.DeliveryRef, _ core.NackReason) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nackCalls++
	return m.nackErr
}

func (m *cbQueueDriver) PublishDLQ(_ context.Context, _ core.TaskMessage, _ []byte) error { return nil }

func (m *cbQueueDriver) PublishEvent(_ context.Context, event core.JanusEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.publishErr != nil {
		return m.publishErr
	}
	m.events = append(m.events, event)
	return nil
}

func (m *cbQueueDriver) ReplayEvents(_ context.Context, _ core.EventReplayFilter) (core.EventIterator, error) {
	return nil, errors.New("not implemented")
}

func (m *cbQueueDriver) EnsureTenant(_ context.Context, _ string) error { return nil }

func (m *cbQueueDriver) EnsureMailbox(_ context.Context, _ core.MailboxSpec) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureMbx++
	return nil
}

func (m *cbQueueDriver) EnsureConsumer(_ context.Context, _ core.ConsumerSpec) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureCons++
	return nil
}

func (m *cbQueueDriver) Close() error { return nil }

type cbTaskRepo struct {
	tasks            map[string]*core.Task
	createErr        error
	getErr           error
	idemErr          error
	updateErr        error
	updateCheckErr   error
	updateCheckFalse bool
	resetErr         error
	running          int
	getCalls         int
	getFailAfter     int
}

func cbKey(tenantID, id string) string { return tenantID + ":" + id }

func (m *cbTaskRepo) Create(_ context.Context, task core.Task) error {
	if m.createErr != nil {
		return m.createErr
	}
	if m.tasks == nil {
		m.tasks = make(map[string]*core.Task)
	}
	cp := task
	m.tasks[cbKey(task.TenantID, task.ID)] = &cp
	return nil
}

func (m *cbTaskRepo) Get(_ context.Context, tenantID, taskID string) (*core.Task, error) {
	m.getCalls++
	if m.getFailAfter > 0 && m.getCalls > m.getFailAfter {
		return nil, errors.New("get failed")
	}
	if m.getErr != nil {
		return nil, m.getErr
	}
	t, ok := m.tasks[cbKey(tenantID, taskID)]
	if !ok {
		return nil, errors.New("not found")
	}
	return t, nil
}

func (m *cbTaskRepo) GetByIdempotencyKey(_ context.Context, _, _ string) (*core.Task, error) {
	if m.idemErr != nil {
		return nil, m.idemErr
	}
	return nil, errors.New("not found")
}

func (m *cbTaskRepo) UpdateStatus(_ context.Context, tenantID, taskID string, status core.TaskStatus, inc int) error {
	if m.updateErr != nil {
		return m.updateErr
	}
	if t, ok := m.tasks[cbKey(tenantID, taskID)]; ok {
		t.Status = status
		t.AttemptCount += inc
	}
	return nil
}

func (m *cbTaskRepo) UpdateStatusWithCheck(_ context.Context, tenantID, taskID string, expected, newStatus core.TaskStatus, inc int) (bool, error) {
	if m.updateCheckErr != nil {
		return false, m.updateCheckErr
	}
	if m.updateCheckFalse {
		return false, nil
	}
	t, ok := m.tasks[cbKey(tenantID, taskID)]
	if !ok || t.Status != expected {
		return false, nil
	}
	t.Status = newStatus
	t.AttemptCount += inc
	return true, nil
}

func (m *cbTaskRepo) UpdateRetryAt(_ context.Context, tenantID, taskID string, _ time.Time) error {
	if m.updateErr != nil {
		return m.updateErr
	}
	if t, ok := m.tasks[cbKey(tenantID, taskID)]; ok {
		t.Status = core.TaskStatusRetryScheduled
	}
	return nil
}

func (m *cbTaskRepo) ListByStatus(_ context.Context, _ string, _ core.TaskStatus, _ int) ([]*core.Task, error) {
	return nil, nil
}

func (m *cbTaskRepo) SetResultRef(_ context.Context, _, _, _ string) error { return nil }

func (m *cbTaskRepo) CountByStatus(_ context.Context, _ string, _ core.TaskStatus) (int, error) {
	return 0, nil
}

func (m *cbTaskRepo) CountRunningByAgent(_ context.Context, _, _ string) (int, error) {
	return m.running, nil
}

func (m *cbTaskRepo) ResetForReplay(_ context.Context, _, _ string) error { return m.resetErr }

type cbMailboxRepo struct {
	mailboxes map[string]*core.Mailbox
}

func (m *cbMailboxRepo) Create(_ context.Context, _ core.Mailbox) error { return nil }

func (m *cbMailboxRepo) Get(_ context.Context, tenantID, mailboxID string) (*core.Mailbox, error) {
	mb, ok := m.mailboxes[cbKey(tenantID, mailboxID)]
	if !ok {
		return nil, errors.New("not found")
	}
	return mb, nil
}

func (m *cbMailboxRepo) ListByAgent(_ context.Context, _, _ string) ([]*core.Mailbox, error) {
	return nil, nil
}
func (m *cbMailboxRepo) Backlog(_ context.Context, _, _ string) (int, error) { return 0, nil }
func (m *cbMailboxRepo) UpdateStatus(_ context.Context, _, _ string, _ core.MailboxStatus) error {
	return nil
}
func (m *cbMailboxRepo) UpdateConfig(_ context.Context, _, _ string, _, _, _, _ int) error {
	return nil
}

type cbCtxRefRepo struct {
	insertErr error
	getErr    error
	getResult *core.ContextRef
	bindErr   error
	binds     []string
}

func (m *cbCtxRefRepo) Insert(_ context.Context, _ core.ContextRef) error { return m.insertErr }
func (m *cbCtxRefRepo) Get(_ context.Context, _, _ string) (*core.ContextRef, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	if m.getResult == nil {
		return nil, errors.New("not found")
	}
	return m.getResult, nil
}
func (m *cbCtxRefRepo) ListByTask(_ context.Context, _, _ string) ([]*core.ContextRef, error) {
	return nil, nil
}
func (m *cbCtxRefRepo) Delete(_ context.Context, _, _ string) error { return nil }
func (m *cbCtxRefRepo) BindToTask(_ context.Context, _, _, refID string) error {
	if m.bindErr != nil {
		return m.bindErr
	}
	m.binds = append(m.binds, refID)
	return nil
}
func (m *cbCtxRefRepo) UnbindFromTask(_ context.Context, _, _, _ string) error { return nil }

type cbRateLimiter struct {
	rpmErr error
	tpmErr error
}

func (m *cbRateLimiter) CheckRPM(_ context.Context, _, _, _ string, _ int) error { return m.rpmErr }
func (m *cbRateLimiter) CheckTPM(_ context.Context, _, _, _ string, _, _ int) error {
	return m.tpmErr
}

type cbBudgetUsage struct {
	dailyErr   error
	reserveErr error
}

func (m *cbBudgetUsage) ReserveTask(_ context.Context, _, _, _ string) error { return m.reserveErr }
func (m *cbBudgetUsage) SettleUsage(_ context.Context, _, _, _ string, _ int, _ float64) error {
	return nil
}
func (m *cbBudgetUsage) ReleaseTask(_ context.Context, _, _, _ string) error { return nil }
func (m *cbBudgetUsage) GetDailyUsage(_ context.Context, _, _, _ string) (int, float64, int, error) {
	if m.dailyErr != nil {
		return 0, 0, 0, m.dailyErr
	}
	return 0, 0, 0, nil
}

type cbBudgetSpecRepo struct {
	specs    []*core.BudgetSpec
	getErr   error
	notFound bool
	listErr  error
}

func (m *cbBudgetSpecRepo) Upsert(_ context.Context, _ core.BudgetSpec) error { return nil }
func (m *cbBudgetSpecRepo) Get(_ context.Context, _ string, _ core.BudgetScopeType, _ string) (*core.BudgetSpec, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	if m.notFound || len(m.specs) == 0 {
		return nil, pgx.ErrNoRows
	}
	return m.specs[0], nil
}
func (m *cbBudgetSpecRepo) ListByTenant(_ context.Context, _ string) ([]*core.BudgetSpec, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	return m.specs, nil
}

type cbAgentExistence struct {
	exists bool
	err    error
}

func (m *cbAgentExistence) AgentExists(_ context.Context, _, _ string) (bool, error) {
	return m.exists, m.err
}

type cbIntentResolver struct {
	result *IntentResolveResult
	err    error
}

func (m *cbIntentResolver) Resolve(_ context.Context, _, _ string, _ core.Payload, _ []core.ContextRef, _ []string) (*IntentResolveResult, error) {
	return m.result, m.err
}

type cbRouterLookup struct {
	mailbox string
	err     error
}

func (m *cbRouterLookup) ListOnlineByCapability(_ context.Context, _, _ string) ([]routing.AgentCandidate, error) {
	return nil, nil
}
func (m *cbRouterLookup) GetAgentMailbox(_ context.Context, _, _ string) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	return m.mailbox, nil
}
func (m *cbRouterLookup) ValidateMailbox(_ context.Context, _, _ string) (bool, error) {
	return true, nil
}
func (m *cbRouterLookup) GetGroupMailboxes(_ context.Context, _, _ string) ([]string, error) {
	return nil, nil
}
func (m *cbRouterLookup) GetHumanMailboxes(_ context.Context, _, _ string) ([]string, error) {
	return nil, nil
}

type cbAPIKeyRepo struct {
	list      []core.APIKey
	createErr error
}

func (m *cbAPIKeyRepo) CreateAPIKey(_ context.Context, _, _, _, _ string, _ []string) (core.APIKey, error) {
	return core.APIKey{}, m.createErr
}
func (m *cbAPIKeyRepo) ListAPIKeys(_ context.Context, _ string) ([]core.APIKey, error) {
	return m.list, nil
}
func (m *cbAPIKeyRepo) RevokeAPIKey(_ context.Context, _, _ string) (*core.APIKey, error) {
	return nil, nil
}

type cbTenantRepo struct {
	ids     []string
	names   map[string]string
	nameErr map[string]bool
	listErr error
}

func (m *cbTenantRepo) Create(_ context.Context, _, _ string) error { return nil }
func (m *cbTenantRepo) GetName(_ context.Context, id string) (string, error) {
	if m.nameErr[id] {
		return "", errors.New("name lookup failed")
	}
	return m.names[id], nil
}
func (m *cbTenantRepo) ListIDs(_ context.Context) ([]string, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	return m.ids, nil
}

type cbAgentRepo struct {
	mockAgentRepo
	capErr    error
	statusErr error
}

func (m *cbAgentRepo) UpsertCapabilities(_ context.Context, _ core.Agent) error { return m.capErr }
func (m *cbAgentRepo) UpdateStatus(_ context.Context, _, _ string, _ core.AgentStatus) error {
	return m.statusErr
}

// --- apikey ---

func TestCov_APIKeyService_List(t *testing.T) {
	repo := &cbAPIKeyRepo{list: []core.APIKey{{TenantID: "acme", Name: "k1"}}}
	svc := NewAPIKeyService(repo)
	keys, err := svc.List(context.Background(), "acme")
	require.NoError(t, err)
	assert.Len(t, keys, 1)
}

func TestCov_APIKeyService_CreateRepoError(t *testing.T) {
	svc := NewAPIKeyService(&cbAPIKeyRepo{createErr: errors.New("db down")})
	_, _, err := svc.Create(context.Background(), "acme", "k", []string{"task:write"})
	require.Error(t, err)
}

// --- approval ---

func TestCov_ApprovalService_WithOutboxRepo(t *testing.T) {
	svc := NewApprovalService(&mockApprovalRepo{}, nil, nil)
	ret := svc.WithOutboxRepo(nil, nil)
	assert.Same(t, svc, ret)
}

func TestCov_ApprovalService_Expire_GetError(t *testing.T) {
	svc := NewApprovalService(&mockApprovalRepo{err: errors.New("db down")}, nil, nil)
	err := svc.Expire(context.Background(), "acme", "a1")
	require.Error(t, err)
}

func TestCov_ApprovalService_Expire_UpdateError(t *testing.T) {
	repo := &mockApprovalRepo{approvals: map[string]*core.Approval{
		"acme:a1": {ID: "a1", TenantID: "acme", Status: "pending", TaskID: "t1"},
	}, updateErr: errors.New("update fail")}
	svc := NewApprovalService(repo, NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil), nil)
	err := svc.Expire(context.Background(), "acme", "a1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expire approval")
}

func TestCov_ApprovalService_Expire_TransitionFailureLogged(t *testing.T) {
	repo := &mockApprovalRepo{approvals: map[string]*core.Approval{
		"acme:a1": {ID: "a1", TenantID: "acme", Status: "pending", TaskID: "missing-task"},
	}}
	taskSvc := NewTaskService(&mockTaskRepo{err: errors.New("db down")}, &mockQueueDriver{}, nil, nil)
	svc := NewApprovalService(repo, taskSvc, nil)
	err := svc.Expire(context.Background(), "acme", "a1")
	require.NoError(t, err, "transition failure on expire is logged, not returned")
}

func TestCov_ApprovalService_Approve_ExpiredRoutesToExpire(t *testing.T) {
	repo := &mockApprovalRepo{approvals: map[string]*core.Approval{
		"acme:a1": {ID: "a1", TenantID: "acme", Status: "pending", TaskID: "t1",
			ExpiresAt: time.Now().Add(-time.Hour)},
	}}
	taskSvc := NewTaskService(&mockTaskRepo{err: errors.New("db down")}, &mockQueueDriver{}, nil, nil)
	svc := NewApprovalService(repo, taskSvc, nil)
	err := svc.Approve(context.Background(), "acme", "a1", "boss", "ok")
	require.NoError(t, err)
	got, _ := repo.Get(context.Background(), "acme", "a1")
	assert.Equal(t, "expired", got.Status)
}

// --- budget ---

func TestCov_BudgetReserve_RateLimiterBranches(t *testing.T) {
	ctx := context.Background()

	agentSpec := &core.BudgetSpec{TenantID: "acme", ScopeType: core.BudgetScopeAgent, ScopeID: "a1", RPM: 10, TPM: 100}
	tenantSpec := &core.BudgetSpec{TenantID: "acme", ScopeType: core.BudgetScopeTenant, ScopeID: "acme", RPM: 10, TPM: 100}
	budget := &core.Budget{MaxTokens: 50}

	t.Run("agent rpm denied", func(t *testing.T) {
		repo := &mockBudgetRepo{budgets: map[string]*core.BudgetSpec{"acme:agent:a1": agentSpec}}
		svc := NewBudgetService(repo).WithRateLimiter(&cbRateLimiter{rpmErr: errors.New("rpm exceeded")})
		err := svc.Reserve(ctx, "acme", "a1", nil)
		var bp *core.BackpressureError
		require.ErrorAs(t, err, &bp)
		assert.Equal(t, core.ReasonModelRPMExceeded, bp.Reason)
	})

	t.Run("agent tpm denied", func(t *testing.T) {
		repo := &mockBudgetRepo{budgets: map[string]*core.BudgetSpec{"acme:agent:a1": agentSpec}}
		svc := NewBudgetService(repo).WithRateLimiter(&cbRateLimiter{tpmErr: errors.New("tpm exceeded")})
		err := svc.Reserve(ctx, "acme", "a1", budget)
		var bp *core.BackpressureError
		require.ErrorAs(t, err, &bp)
		assert.Equal(t, core.ReasonTenantTPMExceeded, bp.Reason)
	})

	t.Run("tenant rpm denied", func(t *testing.T) {
		repo := &mockBudgetRepo{budgets: map[string]*core.BudgetSpec{"acme:tenant:acme": tenantSpec}}
		svc := NewBudgetService(repo).WithRateLimiter(&cbRateLimiter{rpmErr: errors.New("rpm exceeded")})
		err := svc.Reserve(ctx, "acme", "a1", nil)
		var bp *core.BackpressureError
		require.ErrorAs(t, err, &bp)
		assert.Equal(t, core.ReasonModelRPMExceeded, bp.Reason)
	})

	t.Run("tenant tpm denied", func(t *testing.T) {
		repo := &mockBudgetRepo{budgets: map[string]*core.BudgetSpec{"acme:tenant:acme": tenantSpec}}
		svc := NewBudgetService(repo).WithRateLimiter(&cbRateLimiter{tpmErr: errors.New("tpm exceeded")})
		err := svc.Reserve(ctx, "acme", "a1", budget)
		var bp *core.BackpressureError
		require.ErrorAs(t, err, &bp)
		assert.Equal(t, core.ReasonTenantTPMExceeded, bp.Reason)
	})

	t.Run("no budgets with limiter passes", func(t *testing.T) {
		svc := NewBudgetService(&mockBudgetRepo{}).WithRateLimiter(&cbRateLimiter{})
		require.NoError(t, svc.Reserve(ctx, "acme", "a1", budget))
	})

	t.Run("limiter ok with usage repo", func(t *testing.T) {
		repo := &mockBudgetRepo{budgets: map[string]*core.BudgetSpec{"acme:agent:a1": agentSpec}}
		svc := NewBudgetServiceWithUsage(repo, &cbBudgetUsage{}).WithRateLimiter(&cbRateLimiter{})
		require.NoError(t, svc.Reserve(ctx, "acme", "a1", budget))
	})
}

func TestCov_BudgetReserve_DailyUsageLookupError(t *testing.T) {
	ctx := context.Background()

	t.Run("tenant usage error", func(t *testing.T) {
		repo := &mockBudgetRepo{budgets: map[string]*core.BudgetSpec{
			"acme:tenant:acme": {TenantID: "acme", ScopeType: core.BudgetScopeTenant, ScopeID: "acme", DailyCostUSD: 5},
		}}
		svc := NewBudgetServiceWithUsage(repo, &cbBudgetUsage{dailyErr: errors.New("usage db down")})
		err := svc.Reserve(ctx, "acme", "a1", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "usage db down")
	})

	t.Run("agent usage error", func(t *testing.T) {
		repo := &mockBudgetRepo{budgets: map[string]*core.BudgetSpec{
			"acme:agent:a1": {TenantID: "acme", ScopeType: core.BudgetScopeAgent, ScopeID: "a1", DailyCostUSD: 5},
		}}
		svc := NewBudgetServiceWithUsage(repo, &cbBudgetUsage{dailyErr: errors.New("usage db down")})
		err := svc.Reserve(ctx, "acme", "a1", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "usage db down")
	})
}

// --- budget spec service ---

func TestCov_BudgetSpecService_Get(t *testing.T) {
	ctx := context.Background()

	spec := &core.BudgetSpec{TenantID: "acme", ScopeType: core.BudgetScopeAgent, ScopeID: "a1", RPM: 10}

	_, err := NewBudgetSpecService(&cbBudgetSpecRepo{}).Get(ctx, "acme", "bogus_scope", "a1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown scope_type")

	got, err := NewBudgetSpecService(&cbBudgetSpecRepo{notFound: true}).Get(ctx, "acme", "agent", "a1")
	require.NoError(t, err)
	assert.Nil(t, got, "ErrNoRows must map to nil spec")

	_, err = NewBudgetSpecService(&cbBudgetSpecRepo{getErr: errors.New("db down")}).Get(ctx, "acme", "agent", "a1")
	require.Error(t, err)

	got, err = NewBudgetSpecService(&cbBudgetSpecRepo{specs: []*core.BudgetSpec{spec}}).Get(ctx, "acme", "agent", "a1")
	require.NoError(t, err)
	assert.Equal(t, spec, got)
}

func TestCov_BudgetSpecService_List(t *testing.T) {
	ctx := context.Background()
	spec := &core.BudgetSpec{TenantID: "acme", ScopeType: core.BudgetScopeTenant}

	got, err := NewBudgetSpecService(&cbBudgetSpecRepo{specs: []*core.BudgetSpec{spec}}).List(ctx, "acme")
	require.NoError(t, err)
	assert.Len(t, got, 1)

	_, err = NewBudgetSpecService(&cbBudgetSpecRepo{listErr: errors.New("db down")}).List(ctx, "acme")
	require.Error(t, err)
}

// --- context ref service ---

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

func newCovDispatchSvc() (*DispatchService, *cbQueueDriver, *cbTaskRepo, *mockDispatchAttemptRepo, *cbMailboxRepo) {
	qDrv := &cbQueueDriver{}
	tRepo := &cbTaskRepo{tasks: map[string]*core.Task{}}
	aRepo := &mockDispatchAttemptRepo{}
	mRepo := &cbMailboxRepo{mailboxes: map[string]*core.Mailbox{}}
	svc := NewDispatchService(tRepo, aRepo, mRepo, qDrv, NewPolicyService(&mockPolicyRuleRepo{}), NewBudgetService(&mockBudgetRepo{}))
	return svc, qDrv, tRepo, aRepo, mRepo
}

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

func TestCov_PullTask_FallbackUpdateStatusError(t *testing.T) {
	svc, qDrv, tRepo, _, _ := newCovDispatchSvc()
	tRepo.tasks["acme:task-1"] = makeDispatchTestTask("acme", "task-1", "mb-1", 0)
	tRepo.updateErr = errors.New("update fail")
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
	svc = svc.WithLifecycle(NewLifecycleService(nil), nil, nil)
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
	svc = svc.WithLifecycle(NewLifecycleService(nil), nil, nil)
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
		svc := NewTaskService(repo, qd, nil, nil).WithLifecycle(NewLifecycleService(nil))
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
		err := svc.ReportProgress(ctx, "acme", "missing", "a1", core.TaskProgress{Message: "working"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "task not found")
	})

	t.Run("wrong status", func(t *testing.T) {
		repo := &mockTaskRepo{tasks: map[string]*core.Task{
			"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusQueued},
		}}
		svc := NewTaskService(repo, &mockQueueDriver{}, nil, nil)
		err := svc.ReportProgress(ctx, "acme", "t1", "a1", core.TaskProgress{Message: "working"})
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
		err := svc.ReportProgress(ctx, "acme", "t1", "agent-impostor", core.TaskProgress{Message: "working"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not hold the latest attempt")
	})

	t.Run("attempt lookup error", func(t *testing.T) {
		repo := &mockTaskRepo{tasks: map[string]*core.Task{
			"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusRunning},
		}}
		svc := NewTaskService(repo, &mockQueueDriver{}, nil, nil).WithAttemptRepo(&mockDispatchAttemptRepo{})
		err := svc.ReportProgress(ctx, "acme", "t1", "a1", core.TaskProgress{Message: "working"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not hold the latest attempt")
	})

	t.Run("success without attempt repo and without outbox", func(t *testing.T) {
		repo := &mockTaskRepo{tasks: map[string]*core.Task{
			"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusClaimed},
		}}
		svc := NewTaskService(repo, &mockQueueDriver{}, nil, nil)
		err := svc.ReportProgress(ctx, "acme", "t1", "any-agent", core.TaskProgress{Message: "working"})
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

func TestCov_TaskService_TransitionInTx_RequiresPGRepo(t *testing.T) {
	svc := NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil)
	err := svc.TransitionInTx(context.Background(), nil, "acme", "t1",
		core.TaskStatusQueued, core.TaskStatusClaimed, core.EventTaskClaimed, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires postgres task repo")
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
