package service

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
	pgdriver "github.com/agentium-lab/Janus/server/internal/driver/postgres"
	"github.com/agentium-lab/Janus/server/internal/service/routing"
)

type ga02RouteChecker struct {
	svc *PolicyService
}

func (c ga02RouteChecker) CheckRoute(ctx context.Context, tenantID, agentID, dataClass, teamID string) (bool, error) {
	for _, action := range []string{"execute", "task.route"} {
		decision, err := c.svc.Evaluate(ctx, core.PolicyInput{
			TenantID: tenantID,
			Actor:    core.PolicyActor{Type: "agent", ID: agentID, TeamID: teamID},
			Action:   action,
			Resource: core.PolicyResource{Type: "agent", Value: agentID},
			Context:  core.PolicyContextData{DataClassification: dataClass, TargetAgentID: agentID, TargetTeamID: teamID},
		})
		if err != nil {
			return false, err
		}
		if decision.Decision != core.PolicyDecisionAllow {
			return false, nil
		}
	}
	return true, nil
}

// GA-02: pulling a queued task re-runs the target admission against the
// mailbox owner's CURRENT attributes. A rule added (or an attribute changed)
// AFTER the task was queued must stop the claim; lifting the deny must make
// it claimable again.
func TestGA02_PullFinalAdmission_CurrentOwnerRecheck_PG(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	policyRepo := pgdriver.NewPolicyRuleRepository(env.pool)
	policySvc := NewPolicyService(policyRepo)
	router := routing.NewRouter(pgdriver.NewAgentLookupRepo(env.pool), ga02RouteChecker{svc: policySvc}, nil)
	dispatch := NewDispatchService(env.taskRepo, env.attemptRepo, env.mailboxRepo, env.driver, policySvc, NewBudgetService(pgdriver.NewBudgetRepository(env.pool))).
		WithTxPath(env.lifecycle, env.outboxRepo, PGBudgetLedger(env.budgetUsage)).
		WithFinalAdmission(router)

	createTestTask(t, env, "ga02-1")
	env.driver.deliveries = []core.TaskDelivery{{TaskID: "ga02-1", DeliveryRef: "ref-ga02-1"}}
	res, err := dispatch.PullTask(ctx, "acme", "mb-1", "agent-1")
	require.NoError(t, err)
	require.NotNil(t, res)

	createTestTask(t, env, "ga02-2")
	env.driver.deliveries = []core.TaskDelivery{{TaskID: "ga02-2", DeliveryRef: "ref-ga02-2"}}
	require.NoError(t, policyRepo.Create(ctx, core.PolicyRule{
		TenantID: "acme", ID: "ga02-deny-exec", Name: "ga02 deny execute", Status: "active",
		Condition: json.RawMessage(`{"action":"execute"}`),
		Action:    json.RawMessage(`{"decision":"deny"}`),
	}))

	_, err = dispatch.PullTask(ctx, "acme", "mb-1", "agent-1")
	require.Error(t, err, "a queued task whose target is no longer admissible must not be claimable")
	var status string
	require.NoError(t, env.pool.QueryRow(ctx, `SELECT status FROM tasks WHERE tenant_id='acme' AND id='ga02-2'`).Scan(&status))
	require.Equal(t, "queued", status, "the denied task must stay queued")
	var attemptRows int
	require.NoError(t, env.pool.QueryRow(ctx, `SELECT count(*) FROM task_attempts WHERE tenant_id='acme' AND task_id='ga02-2'`).Scan(&attemptRows))
	require.Zero(t, attemptRows, "the denied pull must not mint an attempt")

	_, err = env.pool.Exec(ctx, `UPDATE policy_rules SET status='disabled' WHERE tenant_id='acme' AND id='ga02-deny-exec'`)
	require.NoError(t, err)
	env.driver.deliveries = []core.TaskDelivery{{TaskID: "ga02-2", DeliveryRef: "ref-ga02-2b"}}
	res, err = dispatch.PullTask(ctx, "acme", "mb-1", "agent-1")
	require.NoError(t, err, "lifting the deny must make the task claimable again")
	require.NotNil(t, res)
}
