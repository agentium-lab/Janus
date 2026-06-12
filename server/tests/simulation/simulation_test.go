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

// Mock Repos

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
	mu         sync.Mutex
	approvals  map[string]*core.Approval
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

func (r *simBudgetRepo) Upsert(_ context.Context, spec core.BudgetSpec) error      { return nil }
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

type simQueueDriver struct {
	published []core.TaskMessage
	events    []core.JanusEvent
	mu        sync.Mutex
}

func (d *simQueueDriver) PublishTask(_ context.Context, msg core.TaskMessage) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.published = append(d.published, msg)
	return nil
}

func (d *simQueueDriver) FetchTasks(_ context.Context, mailbox string, opts core.FetchOptions) ([]core.TaskDelivery, error) {
	return nil, nil
}

func (d *simQueueDriver) AckTask(_ context.Context, ref core.DeliveryRef) error  { return nil }
func (d *simQueueDriver) NackTask(_ context.Context, ref core.DeliveryRef, reason core.NackReason) error {
	return nil
}

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
func (d *simQueueDriver) SubscribeEvents(_ context.Context, ch chan<- core.JanusEvent) (context.CancelFunc, error) {
	return func() {}, nil
}
func (d *simQueueDriver) PublishDLQ(_ context.Context, msg core.TaskMessage, errPayload []byte) error {
	return nil
}
func (d *simQueueDriver) Close() error { return nil }

// Test: 7-Agent Full Pipeline Simulation

func TestSevenAgentSimulation(t *testing.T) {
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

	taskH := handler.NewTaskHandler(taskSvc)
	approvalH := handler.NewApprovalHandler(approvalSvc)
	_ = handler.NewDispatchHandler(&dispatchAdapter{svc: dispatchSvc})

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/tenants/acme/tasks", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			taskH.Create(w, r)
		}
	})
	mux.HandleFunc("/v1/tenants/acme/tasks/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case bytes.HasSuffix([]byte(path), []byte("/start")):
			taskH.Start(w, r)
		case bytes.HasSuffix([]byte(path), []byte("/complete")):
			taskH.Complete(w, r)
		case bytes.HasSuffix([]byte(path), []byte("/fail")):
			taskH.Fail(w, r)
		case bytes.HasSuffix([]byte(path), []byte("/cancel")):
			taskH.Cancel(w, r)
		case bytes.HasSuffix([]byte(path), []byte("/block")):
			taskH.Block(w, r)
		case bytes.HasSuffix([]byte(path), []byte("/unblock")):
			taskH.Unblock(w, r)
		case bytes.HasSuffix([]byte(path), []byte("/replay")):
			taskH.Replay(w, r)
		default:
			taskH.Get(w, r)
		}
	})
	mux.HandleFunc("/v1/tenants/acme/approvals/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case bytes.HasSuffix([]byte(path), []byte("/approve")):
			approvalH.Approve(w, r)
		case bytes.HasSuffix([]byte(path), []byte("/reject")):
			approvalH.Reject(w, r)
		default:
			approvalH.Get(w, r)
		}
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	tenantID := "acme"

	var tasksCreated atomic.Int64
	var tasksCompleted atomic.Int64
	var tasksFailed atomic.Int64

	createTask := func(taskID, sourceAgent, targetValue, payloadType string) {
		body, _ := json.Marshal(map[string]interface{}{
			"id": taskID, "source_agent": sourceAgent,
			"target_type": "capability", "target_value": targetValue,
			"mailbox_id": targetValue + "-mb",
			"envelope": map[string]interface{}{
				"janus_version": "0.8", "task_id": taskID, "tenant_id": tenantID,
				"source_agent": sourceAgent,
				"target":       map[string]string{"type": "capability", "value": targetValue},
				"payload":      map[string]string{"type": payloadType, "content": "auto-generated"},
				"trace":        map[string]string{"trace_id": "trace-sim-001"},
			},
		})
		resp, err := http.Post(server.URL+"/v1/tenants/acme/tasks", "application/json", bytes.NewReader(body))
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, resp.StatusCode)
		tasksCreated.Add(1)
		resp.Body.Close()
	}

	transitionTask := func(taskID, action string) int {
		resp, err := http.Post(server.URL+"/v1/tenants/acme/tasks/"+taskID+"/"+action, "application/json", bytes.NewReader([]byte("{}")))
		require.NoError(t, err)
		resp.Body.Close()
		return resp.StatusCode
	}

	getTaskStatus := func(taskID string) core.TaskStatus {
		resp, err := http.Get(server.URL + "/v1/tenants/acme/tasks/" + taskID)
		require.NoError(t, err)
		defer resp.Body.Close()
		var result map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&result)
		status, _ := result["status"].(string)
		return core.TaskStatus(status)
	}

	// Agent 1: product-agent publishes review request
	t.Run("agent1_product_publishes_review", func(t *testing.T) {
		createTask("task-review-001", "product-agent", "code_review", "code_review_request")
		assert.Equal(t, core.TaskStatusQueued, getTaskStatus("task-review-001"))
	})

	// ---- Agent 2: review-agent pulls and starts review ----
	t.Run("agent2_review_agent_starts", func(t *testing.T) {
		// Simulate claim via state machine: queued -> claimed -> running
		task, err := taskSvc.Get(context.Background(), tenantID, "task-review-001")
		require.NoError(t, err)
		task.Status = core.TaskStatusClaimed
		taskRepo.UpdateStatus(context.Background(), tenantID, "task-review-001", core.TaskStatusClaimed, 0)

		code := transitionTask("task-review-001", "start")
		assert.Equal(t, http.StatusOK, code)
		assert.Equal(t, core.TaskStatusRunning, getTaskStatus("task-review-001"))
	})

	// ---- Agent 3: review blocks for human approval ----
	t.Run("agent3_review_blocks_for_approval", func(t *testing.T) {
		code := transitionTask("task-review-001", "block")
		assert.Equal(t, http.StatusOK, code)
		assert.Equal(t, core.TaskStatusBlocked, getTaskStatus("task-review-001"))
	})

	// ---- Agent 4: human-approver unblocks ----
	t.Run("agent4_human_approver_unblocks", func(t *testing.T) {
		code := transitionTask("task-review-001", "unblock")
		assert.Equal(t, http.StatusOK, code)
		assert.Equal(t, core.TaskStatusRunning, getTaskStatus("task-review-001"))
	})

	// ---- Agent 2: review-agent completes ----
	t.Run("agent2_review_completes", func(t *testing.T) {
		code := transitionTask("task-review-001", "complete")
		assert.Equal(t, http.StatusOK, code)
		assert.Equal(t, core.TaskStatusCompleted, getTaskStatus("task-review-001"))
		tasksCompleted.Add(1)
	})

	// ---- Agent 5: code-agent picks up coding task ----
	t.Run("agent5_code_agent_writes_code", func(t *testing.T) {
		createTask("task-code-001", "review-agent", "coding", "code_change_request")

		// Simulate claim -> start
		taskRepo.UpdateStatus(context.Background(), tenantID, "task-code-001", core.TaskStatusClaimed, 0)
		transitionTask("task-code-001", "start")
		assert.Equal(t, core.TaskStatusRunning, getTaskStatus("task-code-001"))

		code := transitionTask("task-code-001", "complete")
		assert.Equal(t, http.StatusOK, code)
		tasksCompleted.Add(1)
	})

	// ---- Agent 6: test-agent runs tests (first fails, retry succeeds) ----
	t.Run("agent6_test_agent_fail_then_succeed", func(t *testing.T) {
		createTask("task-test-001", "code-agent", "testing", "run_tests")

		// Claim -> start
		taskRepo.UpdateStatus(context.Background(), tenantID, "task-test-001", core.TaskStatusClaimed, 0)
		transitionTask("task-test-001", "start")
		assert.Equal(t, core.TaskStatusRunning, getTaskStatus("task-test-001"))

		// First attempt fails
		failBody, _ := json.Marshal(map[string]string{"code": "test_failure", "message": "flaky test"})
		resp, err := http.Post(server.URL+"/v1/tenants/acme/tasks/task-test-001/fail", "application/json", bytes.NewReader(failBody))
		require.NoError(t, err)
		resp.Body.Close()
		assert.Equal(t, core.TaskStatusFailed, getTaskStatus("task-test-001"))
		tasksFailed.Add(1)

		taskRepo.UpdateStatus(context.Background(), tenantID, "task-test-001", core.TaskStatusDeadLettered, 0)

		code := transitionTask("task-test-001", "replay")
		assert.Equal(t, http.StatusOK, code)
		assert.Equal(t, core.TaskStatusQueued, getTaskStatus("task-test-001"))

		// Second attempt: claim -> start -> complete
		taskRepo.UpdateStatus(context.Background(), tenantID, "task-test-001", core.TaskStatusClaimed, 0)
		transitionTask("task-test-001", "start")
		code = transitionTask("task-test-001", "complete")
		assert.Equal(t, http.StatusOK, code)
		tasksCompleted.Add(1)
	})

	// ---- Agent 7: deploy-agent deploys ----
	t.Run("agent7_deploy_agent_deploys", func(t *testing.T) {
		createTask("task-deploy-001", "test-agent", "deployment", "deploy_request")

		taskRepo.UpdateStatus(context.Background(), tenantID, "task-deploy-001", core.TaskStatusClaimed, 0)
		transitionTask("task-deploy-001", "start")
		code := transitionTask("task-deploy-001", "complete")
		assert.Equal(t, http.StatusOK, code)
		tasksCompleted.Add(1)
	})

	// ---- Simulation summary ----
	t.Run("simulation_summary", func(t *testing.T) {
		assert.Equal(t, int64(4), tasksCreated.Load(), "4 tasks created total")
		assert.Equal(t, int64(4), tasksCompleted.Load(), "4 tasks completed")
		assert.Equal(t, int64(1), tasksFailed.Load(), "1 task failed (test retry)")

		queueDrv.mu.Lock()
		pubCount := len(queueDrv.published)
		eventCount := len(queueDrv.events)
		queueDrv.mu.Unlock()
		assert.GreaterOrEqual(t, pubCount, 4, "at least 4 task publishes to queue")
		assert.GreaterOrEqual(t, eventCount, 4, "at least 4 events published")

		t.Logf("Simulation: %d created, %d completed, %d failed, %d queue msgs, %d events",
			tasksCreated.Load(), tasksCompleted.Load(), tasksFailed.Load(), pubCount, eventCount)
	})
}

// Test: Approval Lifecycle End-to-End

func TestApprovalLifecycleE2E(t *testing.T) {
	taskRepo := &simTaskRepo{}
	queueDrv := &simQueueDriver{}
	approvalRepo := &simApprovalRepo{}
	policyRuleRepo := &simPolicyRuleRepo{}
	budgetRepo := &simBudgetRepo{}

	policySvc := service.NewPolicyService(policyRuleRepo)
	_ = service.NewBudgetService(budgetRepo)
	taskSvc := service.NewTaskService(taskRepo, queueDrv, nil, nil).WithPolicy(policySvc)
	approvalSvc := service.NewApprovalService(approvalRepo, taskSvc, queueDrv)
	taskSvc.WithApproval(approvalSvc)

	taskH := handler.NewTaskHandler(taskSvc)
	approvalH := handler.NewApprovalHandler(approvalSvc)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/tenants/acme/tasks", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			taskH.Create(w, r)
		}
	})
	mux.HandleFunc("/v1/tenants/acme/tasks/", func(w http.ResponseWriter, r *http.Request) {
		taskH.Get(w, r)
	})
	mux.HandleFunc("/v1/tenants/acme/approvals", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			approvalH.Request(w, r)
		}
	})
	mux.HandleFunc("/v1/tenants/acme/approvals/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case bytes.HasSuffix([]byte(path), []byte("/approve")):
			approvalH.Approve(w, r)
		case bytes.HasSuffix([]byte(path), []byte("/reject")):
			approvalH.Reject(w, r)
		default:
			approvalH.Get(w, r)
		}
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	ctx := context.Background()

	// Step 1: Create approval request manually
	t.Run("step1_create_approval_request", func(t *testing.T) {
		// First create a task to approve
		task := core.Task{
			TenantID: "acme", ID: "task-approval-001",
			SourceAgent: "product-agent", TargetType: core.TargetTypeCapability,
			TargetValue: "sensitive-op", MailboxID: "sensitive-op-mb",
			Priority: core.PriorityHigh, Status: core.TaskStatusApprovalPending,
			Envelope: core.TaskEnvelope{JanusVersion: "0.8", TaskID: "task-approval-001", TenantID: "acme", SourceAgent: "product-agent", Target: core.Target{Type: core.TargetTypeCapability, Value: "sensitive-op"}, Payload: core.Payload{Type: "approval_request"}, Trace: core.TraceContext{TraceID: "trace-approval-001"}},
		}
		_, err := taskSvc.Create(ctx, task)
		require.NoError(t, err)

		// Request approval
		body, _ := json.Marshal(map[string]string{
			"tenant_id": "acme", "task_id": "task-approval-001", "requested_by": "product-agent",
		})
		resp, err := http.Post(server.URL+"/v1/tenants/acme/approvals", "application/json", bytes.NewReader(body))
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusCreated, resp.StatusCode)

		var result map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&result)
		assert.Equal(t, "pending", result["status"])
		assert.NotEmpty(t, result["id"])
	})

	// Step 2: Verify task is in approval_pending state
	t.Run("step2_task_is_approval_pending", func(t *testing.T) {
		resp, err := http.Get(server.URL + "/v1/tenants/acme/tasks/task-approval-001")
		require.NoError(t, err)
		defer resp.Body.Close()
		var result map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&result)
		assert.Equal(t, "approval_pending", result["status"])
	})

	// Step 3: Approve the task
	t.Run("step3_approve_task", func(t *testing.T) {
		// Get the approval ID first
		approval, _ := approvalRepo.GetPendingByTask(ctx, "acme", "task-approval-001")
		require.NotNil(t, approval)
		approvalID := approval.ID

		body, _ := json.Marshal(map[string]string{
			"approver": "admin", "reason": "looks good",
		})
		resp, err := http.Post(server.URL+"/v1/tenants/acme/approvals/"+approvalID+"/approve", "application/json", bytes.NewReader(body))
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})

	// Step 4: Verify task is queued and was published to queue
	t.Run("step4_task_queued_and_published", func(t *testing.T) {
		resp, err := http.Get(server.URL + "/v1/tenants/acme/tasks/task-approval-001")
		require.NoError(t, err)
		defer resp.Body.Close()
		var result map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&result)
		assert.Equal(t, "queued", result["status"])

		queueDrv.mu.Lock()
		pubCount := len(queueDrv.published)
		queueDrv.mu.Unlock()
		assert.GreaterOrEqual(t, pubCount, 1, "task should be published to queue after approval")
	})
}

// Test: ACK result_ref persistence

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

	// Create a task and set it to claimed
	task := core.Task{
		TenantID: "acme", ID: "task-resultref-001",
		SourceAgent: "agent-1", TargetType: core.TargetTypeCapability,
		TargetValue: "coding", MailboxID: "coding-mb",
		Priority: core.PriorityNormal, Status: core.TaskStatusClaimed,
		Envelope: core.TaskEnvelope{JanusVersion: "0.8", TaskID: "task-resultref-001", TenantID: "acme", SourceAgent: "agent-1", Target: core.Target{Type: core.TargetTypeCapability, Value: "coding"}, Payload: core.Payload{Type: "code_change"}, Trace: core.TraceContext{TraceID: "trace-resultref-001"}},
	}
	require.NoError(t, taskRepo.Create(ctx, task))

	// Create an attempt
	require.NoError(t, attemptRepo.Create(ctx, core.TaskAttempt{
		TenantID: "acme", TaskID: "task-resultref-001",
		Attempt: 1, AgentID: "agent-1", LeaseID: "lease-001",
	}))

	// ACK with result_ref
	err := dispatchSvc.AckTask(ctx, "acme", "task-resultref-001", "lease-001", "s3://results/task-resultref-001.json", nil)
	require.NoError(t, err)

	// Verify result_ref persisted
	got, err := taskRepo.Get(ctx, "acme", "task-resultref-001")
	require.NoError(t, err)
	assert.Equal(t, "s3://results/task-resultref-001.json", got.ResultRef)
	assert.Equal(t, core.TaskStatusCompleted, got.Status)
}

// Test: Multi-Agent Concurrent Publish

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

// Test: Event Publishing Verification

func TestEventPublishingOnLifecycle(t *testing.T) {
	taskRepo := &simTaskRepo{}
	queueDrv := &simQueueDriver{}
	taskSvc := service.NewTaskService(taskRepo, queueDrv, nil, nil)

	ctx := context.Background()

	// Create and transition a task through its lifecycle
	task := core.Task{
		TenantID: "acme", ID: "task-events-001",
		SourceAgent: "agent-1", TargetType: core.TargetTypeCapability,
		TargetValue: "testing", MailboxID: "testing-mb",
		Priority: core.PriorityNormal, Status: core.TaskStatusCreated,
		Envelope: core.TaskEnvelope{JanusVersion: "0.8", TaskID: "task-events-001", TenantID: "acme", SourceAgent: "agent-1", Target: core.Target{Type: core.TargetTypeCapability, Value: "testing"}, Payload: core.Payload{Type: "test_run"}, Trace: core.TraceContext{TraceID: "trace-events-001"}},
	}
	_, err := taskSvc.Create(ctx, task)
	require.NoError(t, err)

	queueDrv.mu.Lock()
	eventsAfterCreate := len(queueDrv.events)
	queueDrv.mu.Unlock()
	assert.GreaterOrEqual(t, eventsAfterCreate, 1, "at least 1 event after create")

	taskRepo.UpdateStatus(ctx, "acme", "task-events-001", core.TaskStatusClaimed, 0)
	require.NoError(t, taskSvc.Start(ctx, "acme", "task-events-001"))

	queueDrv.mu.Lock()
	eventsAfterStart := len(queueDrv.events)
	queueDrv.mu.Unlock()
	assert.Greater(t, eventsAfterStart, eventsAfterCreate, "more events after start")

	// Complete
	require.NoError(t, taskSvc.Complete(ctx, "acme", "task-events-001"))

	queueDrv.mu.Lock()
	eventsAfterComplete := len(queueDrv.events)
	queueDrv.mu.Unlock()
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

// Test: State Machine Validation

func TestStateMachineValidation(t *testing.T) {
	taskRepo := &simTaskRepo{}
	queueDrv := &simQueueDriver{}
	taskSvc := service.NewTaskService(taskRepo, queueDrv, nil, nil)

	ctx := context.Background()

	// Create a completed task
	task := core.Task{
		TenantID: "acme", ID: "task-terminal-001",
		SourceAgent: "agent-1", TargetType: core.TargetTypeAgent,
		TargetValue: "target", Priority: core.PriorityNormal, Status: core.TaskStatusCompleted,
		Envelope: core.TaskEnvelope{JanusVersion: "0.8", TaskID: "task-terminal-001", TenantID: "acme", SourceAgent: "agent-1", Target: core.Target{Type: core.TargetTypeAgent, Value: "target"}, Payload: core.Payload{Type: "test_terminal"}, Trace: core.TraceContext{TraceID: "trace-terminal-001"}},
	}
	_, err := taskSvc.Create(ctx, task)
	require.NoError(t, err)

	// Try to start a completed task — should fail
	err = taskSvc.Start(ctx, "acme", "task-terminal-001")
	assert.Error(t, err, "should not allow transition from completed to running")
	assert.Contains(t, err.Error(), "terminal state")
}

// dispatchAdapter for handler wiring

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
		Task:    res.Task,
		LeaseID: res.LeaseID,
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
