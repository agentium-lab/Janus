package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/driver/postgres"
)

// mustDeadPool returns a pool pointed at a closed port so Begin fails fast.
func mustDeadPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "host=127.0.0.1 port=59999 user=janus dbname=janus_test sslmode=disable connect_timeout=1")
	require.NoError(t, err)
	return pool
}

func TestCovPG_ApproveAtomic_HappyPath(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	task := core.Task{
		ID: "task-appr-1", TenantID: "acme", MailboxID: "mb-1", SourceAgent: "agent-1",
		TargetType: core.TargetTypeMailbox, TargetValue: "mb-1",
		Status: core.TaskStatusApprovalPending, Priority: core.PriorityNormal,
		Envelope: core.TaskEnvelope{
			JanusVersion: "1", TaskID: "task-appr-1", TenantID: "acme", SourceAgent: "agent-1",
			Target:  core.Target{Type: "mailbox", Value: "mb-1"},
			Payload: core.Payload{Type: "json", Content: "{}"},
			Trace:   core.TraceContext{TraceID: "trace-appr-1"},
		},
	}
	require.NoError(t, env.taskRepo.Create(ctx, task))

	pgApprovalRepo := postgres.NewApprovalRepo(env.pool)
	approvalSvc := NewApprovalService(pgApprovalRepo, env.taskSvc, env.driver).
		WithOutboxRepo(env.outboxRepo, env.pool)

	approval, err := approvalSvc.RequestApproval(ctx, core.Approval{
		TenantID: "acme", TaskID: "task-appr-1", RequestedBy: "agent-1", Reason: "policy",
	})
	require.NoError(t, err)

	require.NoError(t, approvalSvc.Approve(ctx, "acme", approval.ID, "boss", "ok"))

	got, _ := pgApprovalRepo.Get(ctx, "acme", approval.ID)
	assert.Equal(t, "approved", got.Status)

	taskAfter, _ := env.taskRepo.Get(ctx, "acme", "task-appr-1")
	assert.Equal(t, core.TaskStatusQueued, taskAfter.Status)
}

func TestCovPG_ApproveAtomic_RequiresPGRepo(t *testing.T) {
	env := setupServiceTestEnv(t)

	repo := &mockApprovalRepo{approvals: map[string]*core.Approval{
		"acme:a1": {ID: "a1", TenantID: "acme", Status: "pending", TaskID: "t1"},
	}}
	svc := NewApprovalService(repo, nil, nil).WithOutboxRepo(env.outboxRepo, env.pool)
	err := svc.Approve(context.Background(), "acme", "a1", "boss", "ok")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "postgres approval repo required")
}

func TestCovPG_RejectAtomic_HappyPath(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	task := core.Task{
		ID: "task-rej-1", TenantID: "acme", MailboxID: "mb-1", SourceAgent: "agent-1",
		TargetType: core.TargetTypeMailbox, TargetValue: "mb-1",
		Status: core.TaskStatusApprovalPending, Priority: core.PriorityNormal,
		Envelope: core.TaskEnvelope{TaskID: "task-rej-1", TenantID: "acme", SourceAgent: "agent-1"},
	}
	require.NoError(t, env.taskRepo.Create(ctx, task))

	pgApprovalRepo := postgres.NewApprovalRepo(env.pool)
	approvalSvc := NewApprovalService(pgApprovalRepo, env.taskSvc, env.driver).
		WithOutboxRepo(env.outboxRepo, env.pool)

	approval, err := approvalSvc.RequestApproval(ctx, core.Approval{
		TenantID: "acme", TaskID: "task-rej-1", RequestedBy: "agent-1",
	})
	require.NoError(t, err)

	require.NoError(t, approvalSvc.Reject(ctx, "acme", approval.ID, "boss", "no"))

	got, _ := pgApprovalRepo.Get(ctx, "acme", approval.ID)
	assert.Equal(t, "rejected", got.Status)

	taskAfter, _ := env.taskRepo.Get(ctx, "acme", "task-rej-1")
	assert.Equal(t, core.TaskStatusCancelled, taskAfter.Status)
}

func TestCovPG_RejectAtomic_RequiresPGRepo(t *testing.T) {
	env := setupServiceTestEnv(t)

	repo := &mockApprovalRepo{approvals: map[string]*core.Approval{
		"acme:a1": {ID: "a1", TenantID: "acme", Status: "pending", TaskID: "t1"},
	}}
	svc := NewApprovalService(repo, nil, nil).WithOutboxRepo(env.outboxRepo, env.pool)
	err := svc.Reject(context.Background(), "acme", "a1", "boss", "no")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "postgres approval repo required")
}

func TestCovPG_Expire_AtomicPath(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	task := core.Task{
		ID: "task-exp-1", TenantID: "acme", SourceAgent: "agent-1",
		TargetType: core.TargetTypeCapability, TargetValue: "review",
		Status: core.TaskStatusApprovalPending, Priority: core.PriorityNormal,
		Envelope: core.TaskEnvelope{TaskID: "task-exp-1", TenantID: "acme", SourceAgent: "agent-1"},
	}
	require.NoError(t, env.taskRepo.Create(ctx, task))

	pgApprovalRepo := postgres.NewApprovalRepo(env.pool)
	approvalSvc := NewApprovalService(pgApprovalRepo, env.taskSvc, env.driver).
		WithOutboxRepo(env.outboxRepo, env.pool)

	approval, err := approvalSvc.RequestApproval(ctx, core.Approval{
		TenantID: "acme", TaskID: "task-exp-1", RequestedBy: "agent-1",
	})
	require.NoError(t, err)

	require.NoError(t, approvalSvc.Expire(ctx, "acme", approval.ID))

	got, _ := pgApprovalRepo.Get(ctx, "acme", approval.ID)
	assert.Equal(t, "expired", got.Status)

	taskAfter, _ := env.taskRepo.Get(ctx, "acme", "task-exp-1")
	assert.Equal(t, core.TaskStatusCancelled, taskAfter.Status)
}

func TestCovPG_AckTask_QueueAckErrorAfterCommit(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	task := createTestTask(t, env, "task-ackwarn-1")

	require.NoError(t, env.attemptRepo.Create(ctx, core.TaskAttempt{
		TenantID: "acme", TaskID: task.ID, Attempt: 1, AgentID: "agent-1",
		LeaseID: "lease-warn", Status: "claimed", StartedAt: time.Now(),
		DeliveryRef: "delivery-warn",
	}))
	_, err := env.taskRepo.UpdateStatusWithCheck(ctx, "acme", task.ID, core.TaskStatusQueued, core.TaskStatusClaimed, 1)
	require.NoError(t, err)

	env.driver.ackErr = errors.New("nats down after commit")

	// The ACK error after the DB commit must not fail the call: the task is
	// already completed durably and a warning event lands in the outbox.
	err = env.dispatch.AckTask(ctx, "acme", task.ID, "lease-warn", "result://x", nil)
	require.NoError(t, err)

	got, _ := env.taskRepo.Get(ctx, "acme", task.ID)
	assert.Equal(t, core.TaskStatusCompleted, got.Status)

	warnSeen := false
	outbox, _ := env.outboxRepo.FetchPending(ctx, 100)
	for _, e := range outbox {
		if e.Kind != "event_publish" {
			continue
		}
		var event core.JanusEvent
		if json.Unmarshal(e.Payload, &event) == nil && event.TaskID == task.ID {
			var payload map[string]string
			if json.Unmarshal(event.Payload, &payload) == nil && payload["ack_error"] != "" {
				warnSeen = true
			}
		}
	}
	assert.True(t, warnSeen, "completed event with ack_error warning must be in outbox")
}

func TestCovPG_NackTask_TaskMissingInLifecyclePath(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	// Attempt without a matching task row: the lifecycle NACK path must fail
	// at the in-tx task lookup.
	require.NoError(t, env.attemptRepo.Create(ctx, core.TaskAttempt{
		TenantID: "acme", TaskID: "ghost-task", Attempt: 1, AgentID: "agent-1",
		LeaseID: "lease-ghost", Status: "claimed", StartedAt: time.Now(),
		DeliveryRef: "delivery-ghost",
	}))

	err := env.dispatch.NackTask(ctx, "acme", "ghost-task", "lease-ghost", false, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get task")
}

func TestCovPG_StartTask_TaskDeletedMidFlight(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	task := createTestTask(t, env, "task-del-1")

	require.NoError(t, env.attemptRepo.Create(ctx, core.TaskAttempt{
		TenantID: "acme", TaskID: task.ID, Attempt: 1, AgentID: "agent-1",
		LeaseID: "lease-del", Status: "claimed", StartedAt: time.Now(),
	}))
	_, err := env.taskRepo.UpdateStatusWithCheck(ctx, "acme", task.ID, core.TaskStatusQueued, core.TaskStatusClaimed, 1)
	require.NoError(t, err)

	_, err = env.pool.Exec(ctx, "DELETE FROM tasks WHERE tenant_id = $1 AND id = $2", "acme", task.ID)
	require.NoError(t, err)

	err = env.dispatch.StartTask(ctx, "acme", task.ID, "lease-del")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get task")
}

func TestCovPG_PullTask_CreateAttemptConflict(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	task := createTestTask(t, env, "task-dupatt-1")

	// Occupy attempt #1 so the claim transaction hits the PK conflict.
	require.NoError(t, env.attemptRepo.Create(ctx, core.TaskAttempt{
		TenantID: "acme", TaskID: task.ID, Attempt: 1, AgentID: "agent-1",
		LeaseID: "lease-taken", Status: "claimed", StartedAt: time.Now(),
	}))

	env.driver.deliveries = []core.TaskDelivery{{TaskID: task.ID, DeliveryRef: "delivery-dupatt"}}

	_, err := env.dispatch.PullTask(ctx, "acme", "mb-1", "agent-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create attempt")
}

func TestCovPG_TransitionInTx_Branches(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	task := createTestTask(t, env, "task-tx-1")

	t.Run("invalid transition rejected before repo access", func(t *testing.T) {
		err := env.taskSvc.TransitionInTx(ctx, nil, "acme", task.ID,
			core.TaskStatusCompleted, core.TaskStatusQueued, core.EventTaskQueued, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid transition")
	})

	t.Run("conflict on stale expected status", func(t *testing.T) {
		tx, err := env.pool.Begin(ctx)
		require.NoError(t, err)
		defer tx.Rollback(ctx)
		err = env.taskSvc.TransitionInTx(ctx, tx, "acme", task.ID,
			core.TaskStatusCreated, core.TaskStatusQueued, core.EventTaskQueued, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "status changed concurrently")
	})

	t.Run("nil outbox repo still records status", func(t *testing.T) {
		svcNoOutbox := NewTaskService(env.taskRepo, env.driver, env.pool, nil)
		tx, err := env.pool.Begin(ctx)
		require.NoError(t, err)
		defer tx.Rollback(ctx)
		err = svcNoOutbox.TransitionInTx(ctx, tx, "acme", task.ID,
			core.TaskStatusQueued, core.TaskStatusClaimed, core.EventTaskClaimed, 1)
		require.NoError(t, err)
	})
}

func TestCovPG_ApproveAtomic_TaskLookupAndTransitionErrors(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	pgApprovalRepo := postgres.NewApprovalRepo(env.pool)
	approvalSvc := NewApprovalService(pgApprovalRepo, env.taskSvc, env.driver).
		WithOutboxRepo(env.outboxRepo, env.pool)

	ghost, err := approvalSvc.RequestApproval(ctx, core.Approval{
		TenantID: "acme", TaskID: "ghost-task", RequestedBy: "agent-1",
	})
	require.NoError(t, err)

	err = approvalSvc.Approve(ctx, "acme", ghost.ID, "boss", "ok")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get task")

	err = approvalSvc.Reject(ctx, "acme", ghost.ID, "boss", "no")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get task")

	// Task already queued: the guarded transition to queued fails and the
	// whole decision transaction rolls back.
	queuedTask := core.Task{
		ID: "task-queued-1", TenantID: "acme", MailboxID: "mb-1", SourceAgent: "agent-1",
		TargetType: core.TargetTypeMailbox, TargetValue: "mb-1",
		Status:   core.TaskStatusQueued,
		Priority: core.PriorityNormal,
		Envelope: core.TaskEnvelope{TaskID: "task-queued-1", TenantID: "acme", SourceAgent: "agent-1"},
	}
	require.NoError(t, env.taskRepo.Create(ctx, queuedTask))
	queuedApproval, err := approvalSvc.RequestApproval(ctx, core.Approval{
		TenantID: "acme", TaskID: "task-queued-1", RequestedBy: "agent-1",
	})
	require.NoError(t, err)

	err = approvalSvc.Approve(ctx, "acme", queuedApproval.ID, "boss", "ok")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "queue task in approval tx")

	stillPending, _ := pgApprovalRepo.Get(ctx, "acme", queuedApproval.ID)
	assert.Equal(t, "pending", stillPending.Status, "failed decision tx must roll back the approval")
}

func TestCovPG_CreateWithOutbox_BeginError(t *testing.T) {
	env := setupServiceTestEnv(t)

	deadPool := mustDeadPool(t)
	defer deadPool.Close()

	svc := NewTaskService(env.taskRepo, env.driver, deadPool, env.outboxRepo)
	_, err := svc.Create(context.Background(), core.Task{
		TenantID: "acme", ID: "t-dead", SourceAgent: "agent-1",
		TargetType: core.TargetTypeCapability, TargetValue: "review",
		Envelope: makeTestEnvelope("t-dead", "acme"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "begin tx")
}

func TestCovPG_CreateWithOutbox_NonPGRepoFallsBackToDirect(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	svc := NewTaskService(&mockTaskRepo{}, env.driver, env.pool, env.outboxRepo)
	created, err := svc.Create(ctx, core.Task{
		TenantID: "acme", ID: "t-mixed", SourceAgent: "agent-1",
		TargetType: core.TargetTypeCapability, TargetValue: "review",
		Envelope: makeTestEnvelope("t-mixed", "acme"),
	})
	require.NoError(t, err)
	assert.Equal(t, "t-mixed", created.ID)

	found := false
	for _, evt := range env.driver.events {
		if evt.EventType == core.EventTaskCreated && evt.TaskID == "t-mixed" {
			found = true
		}
	}
	assert.True(t, found, "created event must still publish via direct path")
}

func TestCovPG_ReportProgress_OutboxPersisted(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	task := createTestTask(t, env, "task-prog-1")
	_, err := env.taskRepo.UpdateStatusWithCheck(ctx, "acme", task.ID, core.TaskStatusQueued, core.TaskStatusClaimed, 1)
	require.NoError(t, err)

	err = env.taskSvc.ReportProgress(ctx, "acme", task.ID, "agent-1", core.TaskProgress{Message: "halfway"})
	require.NoError(t, err)

	outbox, _ := env.outboxRepo.FetchPending(ctx, 100)
	found := false
	for _, e := range outbox {
		if e.Kind != "event_publish" {
			continue
		}
		var event core.JanusEvent
		if json.Unmarshal(e.Payload, &event) == nil &&
			event.EventType == core.EventTaskProgress && event.TaskID == task.ID {
			found = true
		}
	}
	assert.True(t, found, "progress event must be persisted via outbox")
}
