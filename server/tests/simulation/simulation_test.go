package simulation

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/handler"
	"github.com/agentium-lab/Janus/server/internal/service"
)

// In-memory queue that behaves like a real message broker.
// PublishTask routes messages to mailbox queues.
// FetchTasks retrieves and removes messages from a mailbox.

type simQueueDriver struct {
	mu        sync.Mutex
	queues    map[string][]core.TaskDelivery
	published []core.TaskMessage
	events    []core.JanusEvent
	refSeq    int
}

func (d *simQueueDriver) PublishTask(_ context.Context, msg core.TaskMessage) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.queues == nil {
		d.queues = make(map[string][]core.TaskDelivery)
	}
	d.refSeq++
	ref := core.DeliveryRef(fmt.Sprintf("ref-%d", d.refSeq))
	d.queues[msg.MailboxID] = append(d.queues[msg.MailboxID], core.TaskDelivery{
		TaskID:      msg.TaskID,
		Payload:     msg.Payload,
		DeliveryRef: ref,
	})
	d.published = append(d.published, msg)
	return nil
}

func (d *simQueueDriver) FetchTasks(_ context.Context, mailbox string, opts core.FetchOptions) ([]core.TaskDelivery, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.queues == nil {
		return nil, nil
	}
	queue := d.queues[mailbox]
	if len(queue) == 0 {
		return nil, nil
	}
	limit := opts.MaxMessages
	if limit <= 0 {
		limit = 1
	}
	if limit > len(queue) {
		limit = len(queue)
	}
	taken := make([]core.TaskDelivery, limit)
	copy(taken, queue[:limit])
	d.queues[mailbox] = queue[limit:]
	return taken, nil
}

func (d *simQueueDriver) AckTask(_ context.Context, ref core.DeliveryRef) error        { return nil }
func (d *simQueueDriver) NackTask(_ context.Context, ref core.DeliveryRef, reason core.NackReason) error { return nil }

func (d *simQueueDriver) PublishEvent(_ context.Context, event core.JanusEvent) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.events = append(d.events, event)
	return nil
}

func (d *simQueueDriver) ReplayEvents(_ context.Context, filter core.EventReplayFilter) (core.EventIterator, error) {
	return nil, nil
}
func (d *simQueueDriver) EnsureTenant(_ context.Context, tenantID string) error { return nil }
func (d *simQueueDriver) EnsureMailbox(_ context.Context, spec core.MailboxSpec) error {
	return nil
}
func (d *simQueueDriver) EnsureConsumer(_ context.Context, spec core.ConsumerSpec) error {
	return nil
}
func (d *simQueueDriver) PublishDLQ(_ context.Context, msg core.TaskMessage, errPayload []byte) error {
	return nil
}
func (d *simQueueDriver) Close() error { return nil }

func (d *simQueueDriver) PublishedCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.published)
}

func (d *simQueueDriver) EventCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.events)
}

type simTaskRepo struct {
	mu    sync.RWMutex
	tasks map[string]*core.Task
}

func (r *simTaskRepo) key(tenantID, taskID string) string { return tenantID + ":" + taskID }

func (r *simTaskRepo) Create(_ context.Context, task core.Task) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.tasks == nil {
		r.tasks = make(map[string]*core.Task)
	}
	t := task
	r.tasks[r.key(task.TenantID, task.ID)] = &t
	return nil
}

func (r *simTaskRepo) Get(_ context.Context, tenantID, taskID string) (*core.Task, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.tasks[r.key(tenantID, taskID)]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return t, nil
}

func (r *simTaskRepo) GetByIdempotencyKey(_ context.Context, tenantID, key string) (*core.Task, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, t := range r.tasks {
		if t.TenantID == tenantID && t.IdempotencyKey == key {
			return t, nil
		}
	}
	return nil, fmt.Errorf("not found")
}

func (r *simTaskRepo) UpdateStatus(_ context.Context, tenantID, taskID string, status core.TaskStatus, attemptIncrement int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.tasks[r.key(tenantID, taskID)]
	if !ok {
		return fmt.Errorf("not found")
	}
	t.Status = status
	if attemptIncrement > 0 {
		t.AttemptCount += attemptIncrement
	}
	t.UpdatedAt = time.Now()
	if status == core.TaskStatusCompleted {
		now := time.Now()
		t.CompletedAt = &now
	}
	return nil
}

func (r *simTaskRepo) UpdateRetryAt(_ context.Context, tenantID, taskID string, retryAt time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.tasks[r.key(tenantID, taskID)]
	if !ok {
		return fmt.Errorf("not found")
	}
	t.Status = core.TaskStatusRetryScheduled
	t.UpdatedAt = time.Now()
	return nil
}

func (r *simTaskRepo) ListByStatus(_ context.Context, tenantID string, status core.TaskStatus, limit int) ([]*core.Task, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var result []*core.Task
	for _, t := range r.tasks {
		if t.TenantID == tenantID && t.Status == status {
			result = append(result, t)
			if len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}

func (r *simTaskRepo) SetResultRef(_ context.Context, tenantID, taskID, resultRef string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.tasks[r.key(tenantID, taskID)]
	if ok {
		t.ResultRef = resultRef
	}
	return nil
}

func (r *simTaskRepo) CountByStatus(_ context.Context, tenantID string, status core.TaskStatus) (int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	count := 0
	for _, t := range r.tasks {
		if t.TenantID == tenantID && t.Status == status {
			count++
		}
	}
	return count, nil
}

func (r *simTaskRepo) ResetForReplay(_ context.Context, tenantID, taskID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := r.key(tenantID, taskID)
	t, ok := r.tasks[key]
	if !ok {
		return fmt.Errorf("task not found")
	}
	t.Status = core.TaskStatusCreated
	t.AttemptCount = 0
	t.ResultRef = ""
	t.Error = nil
	r.tasks[key] = t
	return nil
}

type simAttemptRepo struct {
	mu       sync.Mutex
	attempts map[string]*core.TaskAttempt
}

func (r *simAttemptRepo) key(tenantID, taskID string) string { return tenantID + ":" + taskID }

func (r *simAttemptRepo) Create(_ context.Context, a core.TaskAttempt) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.attempts == nil {
		r.attempts = make(map[string]*core.TaskAttempt)
	}
	r.attempts[r.key(a.TenantID, a.TaskID)] = &a
	return nil
}

func (r *simAttemptRepo) GetLatest(_ context.Context, tenantID, taskID string) (*core.TaskAttempt, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	a, ok := r.attempts[r.key(tenantID, taskID)]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return a, nil
}

func (r *simAttemptRepo) UpdateHeartbeat(_ context.Context, tenantID, taskID string, attempt int) error {
	return nil
}

func (r *simAttemptRepo) UpdateFinished(_ context.Context, tenantID, taskID string, attempt int, status string, errJSON []byte, usageJSON []byte) error {
	return nil
}

type simMailboxRepo struct {
	mailboxes map[string]*core.Mailbox
}

func (r *simMailboxRepo) Create(_ context.Context, mb core.Mailbox) error {
	if r.mailboxes == nil {
		r.mailboxes = make(map[string]*core.Mailbox)
	}
	r.mailboxes[mb.TenantID+":"+mb.ID] = &mb
	return nil
}

func (r *simMailboxRepo) Get(_ context.Context, tenantID, mailboxID string) (*core.Mailbox, error) {
	mb, ok := r.mailboxes[tenantID+":"+mailboxID]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return mb, nil
}

func (r *simMailboxRepo) ListByAgent(_ context.Context, tenantID, agentID string) ([]*core.Mailbox, error) {
	return nil, nil
}

func (r *simMailboxRepo) Backlog(_ context.Context, tenantID, mailboxID string) (int, error) {
	return 0, nil
}

func (r *simMailboxRepo) UpdateStatus(_ context.Context, tenantID, mailboxID string, status core.MailboxStatus) error {
	return nil
}

func (r *simMailboxRepo) UpdateConfig(_ context.Context, tenantID, mailboxID string, maxConcurrency, ackWaitSeconds, maxDeliver, retentionSeconds int) error {
	return nil
}

type simApprovalRepo struct {
	mu        sync.Mutex
	approvals map[string]*core.Approval
}

func (r *simApprovalRepo) key(tenantID, approvalID string) string { return tenantID + ":" + approvalID }

func (r *simApprovalRepo) Create(_ context.Context, a core.Approval) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.approvals == nil {
		r.approvals = make(map[string]*core.Approval)
	}
	r.approvals[r.key(a.TenantID, a.ID)] = &a
	return nil
}

func (r *simApprovalRepo) Get(_ context.Context, tenantID, approvalID string) (*core.Approval, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	a, ok := r.approvals[r.key(tenantID, approvalID)]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return a, nil
}

func (r *simApprovalRepo) GetPendingByTask(_ context.Context, tenantID, taskID string) (*core.Approval, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, a := range r.approvals {
		if a.TenantID == tenantID && a.TaskID == taskID && a.Status == "pending" {
			return a, nil
		}
	}
	return nil, fmt.Errorf("not found")
}

func (r *simApprovalRepo) UpdateDecision(_ context.Context, tenantID, approvalID, decision, approver, reason string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	a, ok := r.approvals[r.key(tenantID, approvalID)]
	if !ok {
		return fmt.Errorf("not found")
	}
	a.Status = decision
	a.Approver = approver
	a.Reason = reason
	now := time.Now()
	a.DecidedAt = &now
	return nil
}

func (r *simApprovalRepo) ListPending(_ context.Context, tenantID string, limit int) ([]*core.Approval, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var result []*core.Approval
	for _, a := range r.approvals {
		if a.TenantID == tenantID && a.Status == "pending" {
			result = append(result, a)
		}
	}
	return result, nil
}

type simPolicyRuleRepo struct{}

func (r *simPolicyRuleRepo) Create(_ context.Context, rule core.PolicyRule) error { return nil }
func (r *simPolicyRuleRepo) ListActive(_ context.Context, tenantID string) ([]*core.PolicyRule, error) {
	return nil, nil
}

type simBudgetRepo struct{}

func (r *simBudgetRepo) Upsert(_ context.Context, spec core.BudgetSpec) error { return nil }
func (r *simBudgetRepo) Get(_ context.Context, tenantID string, scopeType core.BudgetScopeType, scopeID string) (*core.BudgetSpec, error) {
	return nil, fmt.Errorf("not found")
}
func (r *simBudgetRepo) ListByTenant(_ context.Context, tenantID string) ([]*core.BudgetSpec, error) {
	return nil, nil
}

type simBudgetUsageRepo struct{}

func (r *simBudgetUsageRepo) ReserveTask(_ context.Context, tenantID, scopeType, scopeID string) error { return nil }
func (r *simBudgetUsageRepo) SettleUsage(_ context.Context, tenantID, scopeType, scopeID string, tokens int, costUSD float64) error {
	return nil
}
func (r *simBudgetUsageRepo) ReleaseTask(_ context.Context, tenantID, scopeType, scopeID string) error { return nil }
func (r *simBudgetUsageRepo) GetDailyUsage(_ context.Context, tenantID, scopeType, scopeID string) (int, float64, int, error) {
	return 0, 0, 0, nil
}

// simAgent is a simulated agent actor in the pipeline.
// Each agent owns a mailbox, can pull tasks, process them,
// and on completion, send a new task to the next agent's mailbox.

type simAgent struct {
	id         string
	mailboxID  string
	dispatcher *service.DispatchService
	taskSvc    *service.TaskService
	taskRepo   *simTaskRepo
	attemptRepo *simAttemptRepo
	tenantID   string
	onComplete func(ctx context.Context, agent *simAgent, originalTask core.Task)
}

func (a *simAgent) pullAndProcess(ctx context.Context) (*core.Task, error) {
	result, err := a.dispatcher.PullTask(ctx, a.tenantID, a.mailboxID, a.id)
	if err != nil {
		return nil, fmt.Errorf("pull: %w", err)
	}
	if result == nil {
		return nil, nil
	}
	return result.Task, nil
}

func (a *simAgent) start(ctx context.Context, taskID string) error {
	task, err := a.taskRepo.Get(ctx, a.tenantID, taskID)
	if err != nil {
		return err
	}
	task.Status = core.TaskStatusClaimed
	a.taskRepo.UpdateStatus(ctx, a.tenantID, taskID, core.TaskStatusClaimed, 0)

	a.attemptRepo.Create(ctx, core.TaskAttempt{
		TenantID: a.tenantID, TaskID: taskID,
		Attempt: task.AttemptCount + 1, AgentID: a.id,
		LeaseID: fmt.Sprintf("lease-%s-%s", a.id, taskID),
	})

	return a.taskSvc.Start(ctx, a.tenantID, taskID)
}

func (a *simAgent) complete(ctx context.Context, taskID string) error {
	err := a.taskSvc.Complete(ctx, a.tenantID, taskID)
	if err != nil {
		return err
	}
	if a.onComplete != nil {
		task, _ := a.taskRepo.Get(ctx, a.tenantID, taskID)
		if task != nil {
			a.onComplete(ctx, a, *task)
		}
	}
	return nil
}

func (a *simAgent) fail(ctx context.Context, taskID string, code, message string) error {
	return a.taskSvc.Fail(ctx, a.tenantID, taskID, &core.TaskError{Code: code, Message: message})
}

func (a *simAgent) sendTaskTo(ctx context.Context, targetMailboxID, targetAgentID, taskID, payloadType string) error {
	task := core.Task{
		TenantID:    a.tenantID,
		ID:          taskID,
		SourceAgent: a.id,
		TargetType:  core.TargetTypeMailbox,
		TargetValue: targetMailboxID,
		MailboxID:   targetMailboxID,
		Priority:    core.PriorityNormal,
		Envelope: core.TaskEnvelope{
			JanusVersion: "0.8",
			TaskID:       taskID,
			TenantID:     a.tenantID,
			SourceAgent:  a.id,
			Target:       core.Target{Type: core.TargetTypeMailbox, Value: targetMailboxID},
			Payload:      core.Payload{Type: payloadType, Content: fmt.Sprintf("from %s", a.id)},
			Trace:        core.TraceContext{TraceID: fmt.Sprintf("trace-%s", taskID)},
		},
	}
	_, err := a.taskSvc.Create(ctx, task)
	return err
}

// Test: Real Agent-to-Agent Pipeline with Feedback Loop
//
// Pipeline: product → review → code → test (FAIL, send back to code)
//                                      → code (fix, send back to test)
//                                      → test (PASS, send to deploy)
//                                      → deploy (done)

func TestAgentToAgentPipeline(t *testing.T) {
	taskRepo := &simTaskRepo{}
	attemptRepo := &simAttemptRepo{}
	mailboxRepo := &simMailboxRepo{}
	queueDrv := &simQueueDriver{}
	approvalRepo := &simApprovalRepo{}
	policyRuleRepo := &simPolicyRuleRepo{}
	budgetRepo := &simBudgetRepo{}
	budgetUsageRepo := &simBudgetUsageRepo{}

	policySvc := service.NewPolicyService(policyRuleRepo)
	budgetSvc := service.NewBudgetServiceWithUsage(budgetRepo, budgetUsageRepo)
	taskSvc := service.NewTaskService(taskRepo, queueDrv, nil, nil).WithPolicy(policySvc)
	approvalSvc := service.NewApprovalService(approvalRepo, taskSvc, queueDrv)
	taskSvc.WithApproval(approvalSvc)
	dispatchSvc := service.NewDispatchService(taskRepo, attemptRepo, mailboxRepo, queueDrv, policySvc, budgetSvc)

	tenantID := "acme"
	ctx := context.Background()

	var pipelineCompleted atomic.Int64
	var totalTasksCreated atomic.Int64
	var testFailCount atomic.Int64
	var codeFixCount atomic.Int64

	makeAgent := func(id, mailboxID string, onComplete func(ctx context.Context, a *simAgent, orig core.Task)) *simAgent {
		return &simAgent{
			id:          id,
			mailboxID:   mailboxID,
			dispatcher:  dispatchSvc,
			taskSvc:     taskSvc,
			taskRepo:    taskRepo,
			attemptRepo: attemptRepo,
			tenantID:    tenantID,
			onComplete:  onComplete,
		}
	}

	deployAgent := makeAgent("deploy-agent", "deploy-mb", func(ctx context.Context, a *simAgent, orig core.Task) {
		pipelineCompleted.Add(1)
	})

	testAgent := makeAgent("test-agent", "test-mb", nil)

	codeAgent := makeAgent("code-agent", "code-mb", nil)

	reviewAgent := makeAgent("review-agent", "review-mb", nil)

	productAgent := makeAgent("product-agent", "product-mb", nil)

	_ = deployAgent

	t.Run("step1_product_sends_review_request", func(t *testing.T) {
		err := productAgent.sendTaskTo(ctx, "review-mb", "review-agent", "task-review-001", "code_review_request")
		require.NoError(t, err)
		totalTasksCreated.Add(1)

		task, err := taskRepo.Get(ctx, tenantID, "task-review-001")
		require.NoError(t, err)
		assert.Equal(t, core.TaskStatusQueued, task.Status)
		assert.Equal(t, "product-agent", task.SourceAgent)
		assert.Equal(t, "review-mb", task.MailboxID)
	})

	t.Run("step2_review_agent_pulls_and_completes", func(t *testing.T) {
		task, err := reviewAgent.pullAndProcess(ctx)
		require.NoError(t, err)
		require.NotNil(t, task)
		assert.Equal(t, "task-review-001", task.ID)

		reviewAgent.start(ctx, task.ID)
		err = reviewAgent.complete(ctx, task.ID)
		require.NoError(t, err)

		got, _ := taskRepo.Get(ctx, tenantID, "task-review-001")
		assert.Equal(t, core.TaskStatusCompleted, got.Status)
	})

	t.Run("step3_review_sends_code_task_and_code_agent_works", func(t *testing.T) {
		err := reviewAgent.sendTaskTo(ctx, "code-mb", "code-agent", "task-code-001", "code_change_request")
		require.NoError(t, err)
		totalTasksCreated.Add(1)

		task, err := codeAgent.pullAndProcess(ctx)
		require.NoError(t, err)
		require.NotNil(t, task)
		assert.Equal(t, "task-code-001", task.ID)

		codeAgent.start(ctx, task.ID)
		err = codeAgent.complete(ctx, task.ID)
		require.NoError(t, err)

		got, _ := taskRepo.Get(ctx, tenantID, "task-code-001")
		assert.Equal(t, core.TaskStatusCompleted, got.Status)
	})

	t.Run("step4_code_sends_test_task_and_test_agent_fails", func(t *testing.T) {
		err := codeAgent.sendTaskTo(ctx, "test-mb", "test-agent", "task-test-001", "run_tests")
		require.NoError(t, err)
		totalTasksCreated.Add(1)

		task, err := testAgent.pullAndProcess(ctx)
		require.NoError(t, err)
		require.NotNil(t, task)
		assert.Equal(t, "task-test-001", task.ID)

		testAgent.start(ctx, task.ID)

		err = testAgent.fail(ctx, task.ID, "test_failure", "assertion failed: expected 200 got 500")
		require.NoError(t, err)
		testFailCount.Add(1)

		got, _ := taskRepo.Get(ctx, tenantID, "task-test-001")
		assert.Equal(t, core.TaskStatusFailed, got.Status)
	})

	t.Run("step5_test_agent_sends_fix_request_back_to_code_agent", func(t *testing.T) {
		err := testAgent.sendTaskTo(ctx, "code-mb", "code-agent", "task-code-fix-001", "fix_request")
		require.NoError(t, err)
		totalTasksCreated.Add(1)

		task, err := taskRepo.Get(ctx, tenantID, "task-code-fix-001")
		require.NoError(t, err)
		assert.Equal(t, core.TaskStatusQueued, task.Status)
		assert.Equal(t, "test-agent", task.SourceAgent)
		assert.Equal(t, "code-mb", task.MailboxID)
	})

	t.Run("step6_code_agent_pulls_fix_request_and_fixes", func(t *testing.T) {
		task, err := codeAgent.pullAndProcess(ctx)
		require.NoError(t, err)
		require.NotNil(t, task)
		assert.Equal(t, "task-code-fix-001", task.ID)
		assert.Equal(t, "test-agent", task.SourceAgent)

		codeAgent.start(ctx, task.ID)
		err = codeAgent.complete(ctx, task.ID)
		require.NoError(t, err)
		codeFixCount.Add(1)

		got, _ := taskRepo.Get(ctx, tenantID, "task-code-fix-001")
		assert.Equal(t, core.TaskStatusCompleted, got.Status)
	})

	t.Run("step7_code_agent_sends_retest_and_test_agent_passes", func(t *testing.T) {
		err := codeAgent.sendTaskTo(ctx, "test-mb", "test-agent", "task-test-002", "run_tests")
		require.NoError(t, err)
		totalTasksCreated.Add(1)

		task, err := testAgent.pullAndProcess(ctx)
		require.NoError(t, err)
		require.NotNil(t, task)
		assert.Equal(t, "task-test-002", task.ID)
		assert.Equal(t, "code-agent", task.SourceAgent)

		testAgent.start(ctx, task.ID)
		err = testAgent.complete(ctx, task.ID)
		require.NoError(t, err)

		got, _ := taskRepo.Get(ctx, tenantID, "task-test-002")
		assert.Equal(t, core.TaskStatusCompleted, got.Status)
	})

	t.Run("step8_test_agent_sends_deploy_and_deploy_agent_finishes", func(t *testing.T) {
		err := testAgent.sendTaskTo(ctx, "deploy-mb", "deploy-agent", "task-deploy-001", "deploy_request")
		require.NoError(t, err)
		totalTasksCreated.Add(1)

		task, err := deployAgent.pullAndProcess(ctx)
		require.NoError(t, err)
		require.NotNil(t, task)
		assert.Equal(t, "task-deploy-001", task.ID)
		assert.Equal(t, "test-agent", task.SourceAgent)

		deployAgent.start(ctx, task.ID)
		err = deployAgent.complete(ctx, task.ID)
		require.NoError(t, err)

		got, _ := taskRepo.Get(ctx, tenantID, "task-deploy-001")
		assert.Equal(t, core.TaskStatusCompleted, got.Status)
	})

	t.Run("pipeline_summary", func(t *testing.T) {
		assert.Equal(t, int64(1), pipelineCompleted.Load(), "pipeline completed once")
		assert.Equal(t, int64(1), testFailCount.Load(), "test failed once")
		assert.Equal(t, int64(1), codeFixCount.Load(), "code fixed once")
		assert.Equal(t, int64(6), totalTasksCreated.Load(), "6 tasks total")

		var completedCount int
		var failedCount int
		taskRepo.mu.RLock()
		for _, t := range taskRepo.tasks {
			if t.Status == core.TaskStatusCompleted {
				completedCount++
			}
			if t.Status == core.TaskStatusFailed {
				failedCount++
			}
		}
		taskRepo.mu.RUnlock()
		assert.Equal(t, 5, completedCount, "5 tasks completed (review + code + code-fix + retest + deploy)")
		assert.Equal(t, 1, failedCount, "1 task failed (first test run)")

		pubCount := queueDrv.PublishedCount()
		eventCount := queueDrv.EventCount()

		t.Logf("Pipeline: %d created, %d completed, %d failed, %d queue publishes, %d events",
			totalTasksCreated.Load(), completedCount, failedCount, pubCount, eventCount)

		assert.GreaterOrEqual(t, pubCount, 6, "at least 6 task publishes")
		assert.GreaterOrEqual(t, eventCount, 14, "events for full lifecycle")

		trace := []string{
			"product-agent → review-mb",
			"review-agent → code-mb",
			"code-agent → test-mb",
			"test-agent FAILS → code-mb",
			"code-agent FIXES → test-mb",
			"test-agent PASSES → deploy-mb",
			"deploy-agent DONE",
		}
		for _, step := range trace {
			t.Logf("  %s", step)
		}
	})
}

// Test: Agent-to-Agent with Approval Gate
//
// product-agent → review-agent (blocks for approval)
// human approves → review-agent resumes → sends to code-agent

func TestAgentToAgentWithApprovalGate(t *testing.T) {
	taskRepo := &simTaskRepo{}
	attemptRepo := &simAttemptRepo{}
	mailboxRepo := &simMailboxRepo{}
	queueDrv := &simQueueDriver{}
	approvalRepo := &simApprovalRepo{}
	policyRuleRepo := &simPolicyRuleRepo{}
	budgetRepo := &simBudgetRepo{}
	budgetUsageRepo := &simBudgetUsageRepo{}

	policySvc := service.NewPolicyService(policyRuleRepo)
	budgetSvc := service.NewBudgetServiceWithUsage(budgetRepo, budgetUsageRepo)
	taskSvc := service.NewTaskService(taskRepo, queueDrv, nil, nil).WithPolicy(policySvc)
	approvalSvc := service.NewApprovalService(approvalRepo, taskSvc, queueDrv)
	taskSvc.WithApproval(approvalSvc)
	dispatchSvc := service.NewDispatchService(taskRepo, attemptRepo, mailboxRepo, queueDrv, policySvc, budgetSvc)

	tenantID := "acme"
	ctx := context.Background()

	codeAgent := &simAgent{
		id: "code-agent", mailboxID: "code-mb",
		dispatcher: dispatchSvc, taskSvc: taskSvc, taskRepo: taskRepo, attemptRepo: attemptRepo, tenantID: tenantID,
	}

	t.Run("step1_send_task_with_manual_approval", func(t *testing.T) {
		task := core.Task{
			TenantID: tenantID, ID: "task-review-002",
			SourceAgent: "product-agent", TargetType: core.TargetTypeMailbox,
			TargetValue: "review-mb", MailboxID: "review-mb",
			Priority: core.PriorityHigh,
			Envelope: core.TaskEnvelope{
				JanusVersion: "0.8", TaskID: "task-review-002", TenantID: tenantID,
				SourceAgent: "product-agent",
				Target:      core.Target{Type: core.TargetTypeMailbox, Value: "review-mb"},
				Payload:     core.Payload{Type: "sensitive_review"},
				Trace:       core.TraceContext{TraceID: "trace-review-002"},
			},
		}
		_, err := taskSvc.Create(ctx, task)
		require.NoError(t, err)

		got, _ := taskRepo.Get(ctx, tenantID, "task-review-002")
		assert.Equal(t, core.TaskStatusQueued, got.Status)
	})

	t.Run("step2_review_agent_pulls_and_blocks", func(t *testing.T) {
		reviewAgent := &simAgent{
			id: "review-agent", mailboxID: "review-mb",
			dispatcher: dispatchSvc, taskSvc: taskSvc, taskRepo: taskRepo, attemptRepo: attemptRepo, tenantID: tenantID,
		}
		task, err := reviewAgent.pullAndProcess(ctx)
		require.NoError(t, err)
		require.NotNil(t, task)
		assert.Equal(t, "task-review-002", task.ID)

		reviewAgent.start(ctx, task.ID)

		err = taskSvc.Block(ctx, tenantID, "task-review-002", "awaiting human approval")
		require.NoError(t, err)

		got, _ := taskRepo.Get(ctx, tenantID, "task-review-002")
		assert.Equal(t, core.TaskStatusBlocked, got.Status)
	})

	t.Run("step3_human_approves_and_unblocks", func(t *testing.T) {
		err := taskSvc.Unblock(ctx, tenantID, "task-review-002")
		require.NoError(t, err)

		got, _ := taskRepo.Get(ctx, tenantID, "task-review-002")
		assert.Equal(t, core.TaskStatusRunning, got.Status)
	})

	t.Run("step4_review_agent_completes_and_sends_to_code_agent", func(t *testing.T) {
		reviewAgent := &simAgent{
			id: "review-agent", mailboxID: "review-mb",
			dispatcher: dispatchSvc, taskSvc: taskSvc, taskRepo: taskRepo, attemptRepo: attemptRepo, tenantID: tenantID,
			onComplete: func(ctx context.Context, a *simAgent, orig core.Task) {
				a.sendTaskTo(ctx, "code-mb", "code-agent", "task-code-002", "code_change")
			},
		}

		reviewAgent.complete(ctx, "task-review-002")

		got, _ := taskRepo.Get(ctx, tenantID, "task-review-002")
		assert.Equal(t, core.TaskStatusCompleted, got.Status)

		codeTask, err := taskRepo.Get(ctx, tenantID, "task-code-002")
		require.NoError(t, err)
		assert.Equal(t, core.TaskStatusQueued, codeTask.Status)
		assert.Equal(t, "review-agent", codeTask.SourceAgent)
		assert.Equal(t, "code-mb", codeTask.MailboxID)

		task, err := codeAgent.pullAndProcess(ctx)
		require.NoError(t, err)
		require.NotNil(t, task)
		assert.Equal(t, "task-code-002", task.ID)

		codeAgent.start(ctx, task.ID)
		codeAgent.complete(ctx, task.ID)

		got, _ = taskRepo.Get(ctx, tenantID, "task-code-002")
		assert.Equal(t, core.TaskStatusCompleted, got.Status)
	})
}

func TestAckResultRefPersistence(t *testing.T) {
	taskRepo := &simTaskRepo{}
	queueDrv := &simQueueDriver{}
	attemptRepo := &simAttemptRepo{}
	mailboxRepo := &simMailboxRepo{}
	policyRuleRepo := &simPolicyRuleRepo{}
	budgetRepo := &simBudgetRepo{}

	policySvc := service.NewPolicyService(policyRuleRepo)
	budgetSvc := service.NewBudgetService(budgetRepo)
	dispatchSvc := service.NewDispatchService(taskRepo, attemptRepo, mailboxRepo, queueDrv, policySvc, budgetSvc)

	ctx := context.Background()

	task := core.Task{
		TenantID: "acme", ID: "task-resultref-001",
		SourceAgent: "agent-1", TargetType: core.TargetTypeCapability,
		TargetValue: "coding", MailboxID: "coding-mb",
		Priority: core.PriorityNormal, Status: core.TaskStatusClaimed,
		Envelope: core.TaskEnvelope{JanusVersion: "0.8", TaskID: "task-resultref-001", TenantID: "acme", SourceAgent: "agent-1", Target: core.Target{Type: core.TargetTypeCapability, Value: "coding"}, Payload: core.Payload{Type: "code_change"}, Trace: core.TraceContext{TraceID: "trace-resultref-001"}},
	}
	require.NoError(t, taskRepo.Create(ctx, task))

	require.NoError(t, attemptRepo.Create(ctx, core.TaskAttempt{
		TenantID: "acme", TaskID: "task-resultref-001",
		Attempt: 1, AgentID: "agent-1", LeaseID: "lease-001",
	}))

	err := dispatchSvc.AckTask(ctx, "acme", "task-resultref-001", "lease-001", "s3://results/task-resultref-001.json", nil)
	require.NoError(t, err)

	got, err := taskRepo.Get(ctx, "acme", "task-resultref-001")
	require.NoError(t, err)
	assert.Equal(t, "s3://results/task-resultref-001.json", got.ResultRef)
	assert.Equal(t, core.TaskStatusCompleted, got.Status)
}

func TestMultiAgentConcurrentPublish(t *testing.T) {
	taskRepo := &simTaskRepo{}
	queueDrv := &simQueueDriver{}
	taskSvc := service.NewTaskService(taskRepo, queueDrv, nil, nil)
	taskH := handler.NewTaskHandler(taskSvc)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/tenants/acme/tasks", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			taskH.Create(w, r)
		}
	})
	mux.HandleFunc("/v1/tenants/acme/tasks/", func(w http.ResponseWriter, r *http.Request) {
		taskH.Get(w, r)
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	var wg sync.WaitGroup
	var successCount atomic.Int64

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			taskID := fmt.Sprintf("concurrent-task-%d", idx)
			body, _ := json.Marshal(map[string]interface{}{
				"id": taskID, "source_agent": fmt.Sprintf("agent-%d", idx),
				"target_type": "agent", "target_value": "target-agent",
				"envelope": map[string]interface{}{
					"janus_version": "0.8", "task_id": taskID, "tenant_id": "acme",
					"source_agent": fmt.Sprintf("agent-%d", idx),
					"target":       map[string]string{"type": "agent", "value": "target-agent"},
					"payload":      map[string]string{"type": "test", "content": "concurrent"},
					"trace":        map[string]string{"trace_id": fmt.Sprintf("trace-%d", idx)},
				},
			})
			resp, err := http.Post(server.URL+"/v1/tenants/acme/tasks", "application/json", bytes.NewReader(body))
			if err == nil && resp.StatusCode == http.StatusCreated {
				successCount.Add(1)
			}
			if resp != nil {
				resp.Body.Close()
			}
		}(i)
	}
	wg.Wait()

	assert.Equal(t, int64(10), successCount.Load(), "all 10 concurrent tasks should be created")
}

func TestEventPublishingOnLifecycle(t *testing.T) {
	taskRepo := &simTaskRepo{}
	queueDrv := &simQueueDriver{}
	taskSvc := service.NewTaskService(taskRepo, queueDrv, nil, nil)

	ctx := context.Background()

	task := core.Task{
		TenantID: "acme", ID: "task-events-001",
		SourceAgent: "agent-1", TargetType: core.TargetTypeCapability,
		TargetValue: "testing", MailboxID: "testing-mb",
		Priority: core.PriorityNormal, Status: core.TaskStatusCreated,
		Envelope: core.TaskEnvelope{JanusVersion: "0.8", TaskID: "task-events-001", TenantID: "acme", SourceAgent: "agent-1", Target: core.Target{Type: core.TargetTypeCapability, Value: "testing"}, Payload: core.Payload{Type: "test_run"}, Trace: core.TraceContext{TraceID: "trace-events-001"}},
	}
	_, err := taskSvc.Create(ctx, task)
	require.NoError(t, err)

	eventsAfterCreate := queueDrv.EventCount()
	assert.GreaterOrEqual(t, eventsAfterCreate, 1, "at least 1 event after create")

	taskRepo.UpdateStatus(ctx, "acme", "task-events-001", core.TaskStatusClaimed, 0)
	require.NoError(t, taskSvc.Start(ctx, "acme", "task-events-001"))

	eventsAfterStart := queueDrv.EventCount()
	assert.Greater(t, eventsAfterStart, eventsAfterCreate, "more events after start")

	require.NoError(t, taskSvc.Complete(ctx, "acme", "task-events-001"))

	eventsAfterComplete := queueDrv.EventCount()
	assert.Greater(t, eventsAfterComplete, eventsAfterStart, "more events after complete")

	queueDrv.mu.Lock()
	eventTypes := make(map[string]int)
	for _, e := range queueDrv.events {
		eventTypes[string(e.EventType)]++
	}
	queueDrv.mu.Unlock()

	assert.GreaterOrEqual(t, eventTypes["task.created"], 1)
	assert.GreaterOrEqual(t, eventTypes["task.started"], 1)
	assert.GreaterOrEqual(t, eventTypes["task.completed"], 1)

	t.Logf("Event types: %v", eventTypes)
}

func TestStateMachineValidation(t *testing.T) {
	taskRepo := &simTaskRepo{}
	queueDrv := &simQueueDriver{}
	taskSvc := service.NewTaskService(taskRepo, queueDrv, nil, nil)

	ctx := context.Background()

	task := core.Task{
		TenantID: "acme", ID: "task-terminal-001",
		SourceAgent: "agent-1", TargetType: core.TargetTypeAgent,
		TargetValue: "target", Priority: core.PriorityNormal, Status: core.TaskStatusCompleted,
		Envelope: core.TaskEnvelope{JanusVersion: "0.8", TaskID: "task-terminal-001", TenantID: "acme", SourceAgent: "agent-1", Target: core.Target{Type: core.TargetTypeAgent, Value: "target"}, Payload: core.Payload{Type: "test_terminal"}, Trace: core.TraceContext{TraceID: "trace-terminal-001"}},
	}
	_, err := taskSvc.Create(ctx, task)
	require.NoError(t, err)

	err = taskSvc.Start(ctx, "acme", "task-terminal-001")
	assert.Error(t, err, "should not allow transition from completed to running")
	assert.Contains(t, err.Error(), "terminal state")
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
		Task:     res.Task,
		LeaseID:  res.LeaseID,
		ExpiresAt: res.ExpiresAt,
	}, nil
}

func (a *dispatchAdapter) StartTask(ctx context.Context, tenantID, taskID, leaseID string) error {
	return a.svc.StartTask(ctx, tenantID, taskID, leaseID)
}

func (a *dispatchAdapter) TaskHeartbeat(ctx context.Context, tenantID, taskID, leaseID string) error {
	return a.svc.TaskHeartbeat(ctx, tenantID, taskID, leaseID)
}

func (a *dispatchAdapter) AckTask(ctx context.Context, tenantID, taskID, leaseID, resultRef string, usage *core.TokenUsage) error {
	return a.svc.AckTask(ctx, tenantID, taskID, leaseID, resultRef, usage)
}

func (a *dispatchAdapter) NackTask(ctx context.Context, tenantID, taskID, leaseID string, retriable bool, taskErr *core.TaskError) error {
	return a.svc.NackTask(ctx, tenantID, taskID, leaseID, retriable, taskErr)
}

func (a *dispatchAdapter) Heartbeat(ctx context.Context, tenantID, agentID string) error {
	return nil
}

func (a *dispatchAdapter) Ack(ctx context.Context, tenantID, taskID, leaseID, resultRef string, usage *core.TokenUsage) error {
	return a.svc.AckTask(ctx, tenantID, taskID, leaseID, resultRef, usage)
}

func (a *dispatchAdapter) Nack(ctx context.Context, tenantID, taskID, leaseID string, retriable bool, taskErr *core.TaskError) error {
	return a.svc.NackTask(ctx, tenantID, taskID, leaseID, retriable, taskErr)
}
