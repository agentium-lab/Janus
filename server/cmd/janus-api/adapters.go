package main

import (
	"context"
	"errors"

	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/agentium-lab/Janus/core"
	pgdriver "github.com/agentium-lab/Janus/server/internal/driver/postgres"
	"github.com/agentium-lab/Janus/server/internal/handler"
	_ "github.com/agentium-lab/Janus/server/internal/metrics"

	"github.com/agentium-lab/Janus/server/internal/service"
	"github.com/agentium-lab/Janus/server/internal/service/intent"
)

// Thin adapters bridging concrete repos and service types into the
// interfaces the handler/service layers consume.

type intentAgentLookup struct {
	repo *pgdriver.AgentRepository
}

func (l *intentAgentLookup) ListOnlineAgents(ctx context.Context, tenantID string) ([]core.Agent, error) {
	ptrs, err := l.repo.ListOnlineWithCapabilities(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	out := make([]core.Agent, len(ptrs))
	for i, a := range ptrs {
		out[i] = *a
	}
	return out, nil
}

type intentAdapter struct {
	r *intent.IntentResolver
}

func (a *intentAdapter) Resolve(ctx context.Context, tenantID, intentValue string, payload core.Payload, contextRefs []core.ContextRef, policyHints []string) (*service.IntentResolveResult, error) {
	result, err := a.r.Resolve(ctx, tenantID, intentValue, payload, contextRefs, policyHints)
	if err != nil {
		return nil, err
	}
	return &service.IntentResolveResult{
		ResolvedCapability: result.ResolvedCapability,
		Confidence:         result.Confidence,
		Reason:             result.Reason,
	}, nil
}

type agentExistenceAdapter struct {
	repo *pgdriver.AgentRepository
}

func (a agentExistenceAdapter) AgentExists(ctx context.Context, tenantID, agentID string) (bool, error) {
	_, err := a.repo.Get(ctx, tenantID, agentID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

type policyCheckerAdapter struct {
	svc *service.PolicyService
}

// CheckRoute gates a routing candidate against tenant policy rules. Two
// rule families share this hook, so BOTH actions are evaluated: hand-
// written rules match action "execute", while data-classification
// template rules match action "task.route". Policy matching is exact
// equality on every condition key, so evaluating only one action would
// leave the other family inert. The candidate is allowed only when BOTH
// evaluations allow; deny or approval_required from either filters it.
// An Evaluate error is returned to the caller — the router keeps the
// candidate (fail-open) by doctrine.
func (a policyCheckerAdapter) CheckRoute(ctx context.Context, tenantID, agentID, dataClass string) (bool, error) {
	input := core.PolicyInput{
		TenantID: tenantID,
		Actor:    core.PolicyActor{Type: "agent", ID: agentID},
		Resource: core.PolicyResource{Type: "agent", Value: agentID},
		Context:  core.PolicyContextData{DataClassification: dataClass, TargetAgentID: agentID},
	}
	for _, action := range []string{"execute", "task.route"} {
		input.Action = action
		decision, err := a.svc.Evaluate(ctx, input)
		if err != nil {
			return false, err
		}
		if decision.Decision != core.PolicyDecisionAllow {
			return false, nil
		}
	}
	return true, nil
}

type budgetCheckerAdapter struct {
	svc *service.BudgetService
}

// CheckCapacity filters routing candidates at agent level only; tenant-level
// concurrency and rate limits stay enforced at PullTask where real counters
// are available.

// CheckCapacity filters routing candidates at agent level only; tenant-level
// concurrency and rate limits stay enforced at PullTask where real counters
// are available.
func (b budgetCheckerAdapter) CheckCapacity(ctx context.Context, tenantID, agentID string, running int, _ int) (bool, error) {
	return b.svc.CheckConcurrency(ctx, tenantID, agentID, running, 0) == nil, nil
}

type dispatchAdapter struct {
	svc *service.DispatchService
}

func (a *dispatchAdapter) PullTask(ctx context.Context, tenantID, mailboxID, agentID string) (*handler.ServicePullResult, error) {
	res, err := a.svc.PullTask(ctx, tenantID, mailboxID, agentID)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	return &handler.ServicePullResult{
		Task:      res.Task,
		LeaseID:   res.LeaseID,
		Attempt:   res.Attempt,
		ExpiresAt: res.ExpiresAt,
	}, nil
}

func (a *dispatchAdapter) StartTask(ctx context.Context, tenantID, taskID, leaseID string) error {
	return a.svc.StartTask(ctx, tenantID, taskID, leaseID)
}

func (a *dispatchAdapter) StartTaskAs(ctx context.Context, tenantID, taskID, leaseID, agentID string) error {
	return a.svc.StartTaskAs(ctx, tenantID, taskID, leaseID, agentID)
}

func (a *dispatchAdapter) TaskHeartbeatAs(ctx context.Context, tenantID, taskID, leaseID, agentID string) error {
	return a.svc.TaskHeartbeatAs(ctx, tenantID, taskID, leaseID, agentID)
}

func (a *dispatchAdapter) AckTaskAs(ctx context.Context, tenantID, taskID, leaseID, agentID, resultRef string, usage *core.TokenUsage) error {
	return a.svc.AckTaskAs(ctx, tenantID, taskID, leaseID, agentID, resultRef, usage)
}

func (a *dispatchAdapter) TaskHeartbeat(ctx context.Context, tenantID, taskID, leaseID string) error {
	return a.svc.TaskHeartbeat(ctx, tenantID, taskID, leaseID)
}

func (a *dispatchAdapter) AckTask(ctx context.Context, tenantID, taskID, leaseID string, resultRef string, usage *core.TokenUsage) error {
	return a.svc.AckTask(ctx, tenantID, taskID, leaseID, resultRef, usage)
}

func (a *dispatchAdapter) NackTask(ctx context.Context, tenantID, taskID, leaseID string, retriable bool, taskErr *core.TaskError) error {
	return a.svc.NackTask(ctx, tenantID, taskID, leaseID, retriable, taskErr)
}

func (a *dispatchAdapter) NackTaskAs(ctx context.Context, tenantID, taskID, leaseID, agentID string, retriable bool, taskErr *core.TaskError) error {
	return a.svc.NackTaskAs(ctx, tenantID, taskID, leaseID, agentID, retriable, taskErr)
}

type auditAdapter struct {
	svc *service.EventService
}

func (a *auditAdapter) QueryByTask(ctx context.Context, tenantID, taskID string, limit int) (interface{}, error) {
	return a.svc.QueryByTask(ctx, tenantID, taskID, limit)
}

func (a *auditAdapter) QueryByTrace(ctx context.Context, tenantID, traceID string, limit int) (interface{}, error) {
	return a.svc.QueryByTrace(ctx, tenantID, traceID, limit)
}

func (a *auditAdapter) QueryByTenant(ctx context.Context, tenantID string, limit int) (interface{}, error) {
	return a.svc.QueryByTenant(ctx, tenantID, limit)
}

type pgTaskLister struct {
	repo *pgdriver.TaskRepository
}

func (l pgTaskLister) ListPage(ctx context.Context, tenantID string, pageSize int, pageToken string) ([]*core.Task, string, error) {
	return l.repo.ListPage(ctx, tenantID, pageSize, pageToken)
}

func (l pgTaskLister) Total(ctx context.Context, tenantID string) int {
	total := 0
	for _, st := range []core.TaskStatus{core.TaskStatusCreated, core.TaskStatusQueued, core.TaskStatusApprovalPending,
		core.TaskStatusClaimed, core.TaskStatusRunning, core.TaskStatusBlocked, core.TaskStatusRetryScheduled,
		core.TaskStatusCompleted, core.TaskStatusFailed, core.TaskStatusDeadLettered, core.TaskStatusExpired, core.TaskStatusCancelled} {
		if n, err := l.repo.CountByStatus(ctx, tenantID, st); err == nil {
			total += n
		}
	}
	return total
}
