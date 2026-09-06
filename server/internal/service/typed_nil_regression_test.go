package service

import (
	"context"
	"testing"

	"github.com/agentium-lab/Janus/core"
	redisdriver "github.com/agentium-lab/Janus/server/internal/driver/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The v1.5.0 review found that PG-only mode panicked on agent registration:
// main.go passed a typed nil (*redisdriver.Driver)(nil) into the
// HeartbeatDriver interface, so AgentService's nil guards never fired.
// These tests inject the typed nil DIRECTLY — if any constructor or method
// regresses, they panic instead of passing silently.

func TestAgentService_Register_TypedNilHeartbeatDriver(t *testing.T) {
	svc := NewAgentService(&stubAgentRepo{}, &stubMailboxRepo{}, typedNilHeartbeat(), nil)
	err := svc.Register(context.Background(), core.Agent{
		TenantID: "acme", ID: "agent-1", DisplayName: "Agent One", Status: core.AgentStatusOnline,
	})
	require.NoError(t, err, "typed-nil heartbeat driver must be normalized to nil at construction")

	err = svc.Heartbeat(context.Background(), "acme", "agent-1")
	require.NoError(t, err)
}

func TestAgentService_Register_NilHeartbeatDriver(t *testing.T) {
	svc := NewAgentService(&stubAgentRepo{}, &stubMailboxRepo{}, nil, nil)
	err := svc.Register(context.Background(), core.Agent{
		TenantID: "acme", ID: "agent-2", DisplayName: "Agent Two",
	})
	require.NoError(t, err)
}

func TestBudgetService_WithRateLimiter_TypedNil(t *testing.T) {
	svc := NewBudgetService(&stubBudgetRepo{}).WithRateLimiter(typedNilRateLimiter())
	err := svc.CheckConcurrency(context.Background(), "acme", "agent-1", 0, 0)
	assert.NoError(t, err, "typed-nil rate limiter must be normalized to nil")
}

func typedNilHeartbeat() HeartbeatDriver {
	var d *redisdriver.Driver
	return d
}

func typedNilRateLimiter() RateLimiter {
	var d *redisdriver.Driver
	return d
}

type stubAgentRepo struct{}

func (r *stubAgentRepo) Register(_ context.Context, _ core.Agent) error { return nil }
func (r *stubAgentRepo) UpsertCapabilities(_ context.Context, _ core.Agent) error {
	return nil
}
func (r *stubAgentRepo) Get(_ context.Context, _, _ string) (*core.Agent, error) {
	return nil, context.DeadlineExceeded
}
func (r *stubAgentRepo) List(_ context.Context, _ string) ([]*core.Agent, error) {
	return nil, nil
}
func (r *stubAgentRepo) ListByStatus(_ context.Context, _ string, _ core.AgentStatus) ([]*core.Agent, error) {
	return nil, nil
}
func (r *stubAgentRepo) ListAllByStatus(_ context.Context, _ core.AgentStatus) ([]*core.Agent, error) {
	return nil, nil
}
func (r *stubAgentRepo) UpdateHeartbeat(_ context.Context, _, _ string) error { return nil }
func (r *stubAgentRepo) UpdateStatus(_ context.Context, _, _ string, _ core.AgentStatus) error {
	return nil
}
func (r *stubAgentRepo) FindByCapability(_ context.Context, _, _ string) ([]*core.Agent, error) {
	return nil, nil
}

type stubMailboxRepo struct{}

func (r *stubMailboxRepo) Create(_ context.Context, _ core.Mailbox) error { return nil }
func (r *stubMailboxRepo) Get(_ context.Context, _, _ string) (*core.Mailbox, error) {
	return &core.Mailbox{TenantID: "acme", ID: "mb", Status: core.MailboxStatusActive}, nil
}
func (r *stubMailboxRepo) ListByAgent(_ context.Context, _, _ string) ([]*core.Mailbox, error) {
	return nil, nil
}
func (r *stubMailboxRepo) Backlog(_ context.Context, _, _ string) (int, error) { return 0, nil }
func (r *stubMailboxRepo) UpdateStatus(_ context.Context, _, _ string, _ core.MailboxStatus) error {
	return nil
}
func (r *stubMailboxRepo) UpdateConfig(_ context.Context, _, _ string, _, _, _, _ int) error {
	return nil
}

type stubBudgetRepo struct{}

func (r *stubBudgetRepo) Upsert(_ context.Context, _ core.BudgetSpec) error { return nil }
func (r *stubBudgetRepo) Get(_ context.Context, _ string, _ core.BudgetScopeType, _ string) (*core.BudgetSpec, error) {
	return nil, context.DeadlineExceeded
}
func (r *stubBudgetRepo) ListByTenant(_ context.Context, _ string) ([]*core.BudgetSpec, error) {
	return nil, nil
}

func TestAgentService_Heartbeat_NilDriver_StillWritesPG(t *testing.T) {
	// PG-only regression (sixth review, §5): Heartbeat used to return early
	// when the Redis driver was nil, skipping the durable PG record entirely.
	repo := &heartbeatRecordingRepo{}
	svc := NewAgentService(repo, &stubMailboxRepo{}, nil, nil)
	require.NoError(t, svc.Heartbeat(context.Background(), "acme", "agent-1"))
	assert.Equal(t, 1, repo.calls, "PG UpdateHeartbeat must run even without a heartbeat driver")
}

type heartbeatRecordingRepo struct {
	stubAgentRepo
	calls int
}

func (r *heartbeatRecordingRepo) UpdateHeartbeat(_ context.Context, _, _ string) error {
	r.calls++
	return nil
}
