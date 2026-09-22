package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/service/routing"
)

// Shared in-memory fakes for the coverage suites, grouped by domain so
// each behavior file lists only the tests it needs.

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

func (m *cbAPIKeyRepo) CreateAPIKey(_ context.Context, _, _, _, _ string, _ []string, _ string) (core.APIKey, error) {
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

func newCovDispatchSvc() (*DispatchService, *cbQueueDriver, *cbTaskRepo, *mockDispatchAttemptRepo, *cbMailboxRepo) {
	qDrv := &cbQueueDriver{}
	tRepo := &cbTaskRepo{tasks: map[string]*core.Task{}}
	aRepo := &mockDispatchAttemptRepo{}
	mRepo := &cbMailboxRepo{mailboxes: map[string]*core.Mailbox{}}
	svc := NewDispatchService(tRepo, aRepo, mRepo, qDrv, NewPolicyService(&mockPolicyRuleRepo{}), NewBudgetService(&mockBudgetRepo{}))
	return svc, qDrv, tRepo, aRepo, mRepo
}

func extraTestDSN() string {
	host := os.Getenv("JANUS_PG_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("JANUS_PG_PORT")
	if port == "" {
		port = "5432"
	}
	user := os.Getenv("JANUS_PG_USER")
	if user == "" {
		user = "silv"
	}
	return fmt.Sprintf("host=%s port=%s user=%s dbname=janus_test sslmode=disable", host, port, user)
}

func openExtraTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), extraTestDSN())
	if err != nil {
		t.Skipf("postgres not reachable: %v", err)
	}
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		t.Skipf("postgres not reachable: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}
