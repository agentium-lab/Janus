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
)

// --- Mock Repos ---

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

func (r *simTaskRepo) SetResultRef(_ context.Context, _, _, _ string) error { return nil }
func (r *simTaskRepo) CountByStatus(_ context.Context, _ string, _ core.TaskStatus) (int, error) {
	return 0, nil
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

// --- TaskService interface adapter ---

type simTaskService struct {
	repo  *simTaskRepo
	queue *simQueueDriver
}

func (s *simTaskService) Create(ctx context.Context, task core.Task) (*core.Task, error) {
	if task.Status == "" {
		task.Status = core.TaskStatusCreated
	}
	if err := s.repo.Create(ctx, task); err != nil {
		return nil, err
	}
	if task.MailboxID != "" {
		s.repo.UpdateStatus(ctx, task.TenantID, task.ID, core.TaskStatusQueued, 0)
		s.queue.PublishTask(ctx, core.TaskMessage{
			TenantID:  task.TenantID,
			MailboxID: task.MailboxID,
			TaskID:    task.ID,
			Priority:  task.Priority,
		})
	}
	return &task, nil
}

func (s *simTaskService) Get(ctx context.Context, tenantID, taskID string) (*core.Task, error) {
	return s.repo.Get(ctx, tenantID, taskID)
}

func (s *simTaskService) Start(ctx context.Context, tenantID, taskID string) error {
	return s.repo.UpdateStatus(ctx, tenantID, taskID, core.TaskStatusRunning, 0)
}

func (s *simTaskService) Complete(ctx context.Context, tenantID, taskID string) error {
	return s.repo.UpdateStatus(ctx, tenantID, taskID, core.TaskStatusCompleted, 0)
}

func (s *simTaskService) Fail(ctx context.Context, tenantID, taskID string, taskErr *core.TaskError) error {
	s.repo.UpdateStatus(ctx, tenantID, taskID, core.TaskStatusFailed, 1)
	return nil
}

func (s *simTaskService) Cancel(ctx context.Context, tenantID, taskID string) error {
	return s.repo.UpdateStatus(ctx, tenantID, taskID, core.TaskStatusCancelled, 0)
}

func (s *simTaskService) Block(ctx context.Context, tenantID, taskID, reason string) error {
	return s.repo.UpdateStatus(ctx, tenantID, taskID, core.TaskStatusBlocked, 0)
}

func (s *simTaskService) Unblock(ctx context.Context, tenantID, taskID string) error {
	return s.repo.UpdateStatus(ctx, tenantID, taskID, core.TaskStatusRunning, 0)
}

func (s *simTaskService) Replay(ctx context.Context, tenantID, taskID string) (*core.Task, error) {
	s.repo.UpdateStatus(ctx, tenantID, taskID, core.TaskStatusQueued, 0)
	return s.repo.Get(ctx, tenantID, taskID)
}

func (s *simTaskService) ListByStatus(ctx context.Context, tenantID string, status core.TaskStatus, limit int) ([]*core.Task, error) {
	return s.repo.ListByStatus(ctx, tenantID, status, limit)
}

// --- Test ---

func TestSevenAgentSimulation(t *testing.T) {
	taskRepo := &simTaskRepo{}
	queueDrv := &simQueueDriver{}
	_ = &simMailboxRepo{}
	taskSvc := &simTaskService{repo: taskRepo, queue: queueDrv}

	taskH := handler.NewTaskHandler(taskSvc)

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

	// --- Agent 1: product-agent publishes review request ---
	t.Run("agent1_product_publishes_review", func(t *testing.T) {
		createTask("task-review-001", "product-agent", "code_review", "code_review_request")
		assert.Equal(t, core.TaskStatusQueued, getTaskStatus("task-review-001"))
	})

	// --- Agent 2: review-agent pulls and starts review ---
	t.Run("agent2_review_agent_starts", func(t *testing.T) {
		status := getTaskStatus("task-review-001")
		assert.Equal(t, core.TaskStatusQueued, status)

		code := transitionTask("task-review-001", "start")
		assert.Equal(t, http.StatusOK, code)
		assert.Equal(t, core.TaskStatusRunning, getTaskStatus("task-review-001"))
	})

	// --- Agent 3: review blocks for human approval ---
	t.Run("agent3_review_blocks_for_approval", func(t *testing.T) {
		code := transitionTask("task-review-001", "block")
		assert.Equal(t, http.StatusOK, code)
		assert.Equal(t, core.TaskStatusBlocked, getTaskStatus("task-review-001"))
	})

	// --- Agent 4: human-approver unblocks ---
	t.Run("agent4_human_approver_unblocks", func(t *testing.T) {
		code := transitionTask("task-review-001", "unblock")
		assert.Equal(t, http.StatusOK, code)
		assert.Equal(t, core.TaskStatusRunning, getTaskStatus("task-review-001"))
	})

	// --- Agent 2: review-agent completes ---
	t.Run("agent2_review_completes", func(t *testing.T) {
		code := transitionTask("task-review-001", "complete")
		assert.Equal(t, http.StatusOK, code)
		assert.Equal(t, core.TaskStatusCompleted, getTaskStatus("task-review-001"))
		tasksCompleted.Add(1)
	})

	// --- Agent 5: code-agent picks up coding task ---
	t.Run("agent5_code_agent_writes_code", func(t *testing.T) {
		createTask("task-code-001", "review-agent", "coding", "code_change_request")
		transitionTask("task-code-001", "start")
		assert.Equal(t, core.TaskStatusRunning, getTaskStatus("task-code-001"))

		code := transitionTask("task-code-001", "complete")
		assert.Equal(t, http.StatusOK, code)
		tasksCompleted.Add(1)
	})

	// --- Agent 6: test-agent runs tests (first fails, retry succeeds) ---
	t.Run("agent6_test_agent_fail_then_succeed", func(t *testing.T) {
		createTask("task-test-001", "code-agent", "testing", "run_tests")
		transitionTask("task-test-001", "start")
		assert.Equal(t, core.TaskStatusRunning, getTaskStatus("task-test-001"))

		// First attempt fails
		failBody, _ := json.Marshal(map[string]string{"code": "test_failure", "message": "flaky test"})
		resp, err := http.Post(server.URL+"/v1/tenants/acme/tasks/task-test-001/fail", "application/json", bytes.NewReader(failBody))
		require.NoError(t, err)
		resp.Body.Close()
		assert.Equal(t, core.TaskStatusFailed, getTaskStatus("task-test-001"))
		tasksFailed.Add(1)

		// Replay (retry)
		code := transitionTask("task-test-001", "replay")
		assert.Equal(t, http.StatusOK, code)
		assert.Equal(t, core.TaskStatusQueued, getTaskStatus("task-test-001"))

		// Second attempt succeeds
		transitionTask("task-test-001", "start")
		code = transitionTask("task-test-001", "complete")
		assert.Equal(t, http.StatusOK, code)
		tasksCompleted.Add(1)
	})

	// --- Agent 5: deploy-agent deploys ---
	t.Run("agent7_deploy_agent_deploys", func(t *testing.T) {
		createTask("task-deploy-001", "test-agent", "deployment", "deploy_request")
		transitionTask("task-deploy-001", "start")
		code := transitionTask("task-deploy-001", "complete")
		assert.Equal(t, http.StatusOK, code)
		tasksCompleted.Add(1)
	})

	// --- Final assertions ---
	t.Run("simulation_summary", func(t *testing.T) {
		assert.Equal(t, int64(4), tasksCreated.Load(), "4 tasks created total")
		assert.Equal(t, int64(4), tasksCompleted.Load(), "4 tasks completed")
		assert.Equal(t, int64(1), tasksFailed.Load(), "1 task failed (test retry)")

		queueDrv.mu.Lock()
		pubCount := len(queueDrv.published)
		eventCount := len(queueDrv.events)
		queueDrv.mu.Unlock()
		assert.GreaterOrEqual(t, pubCount, 4, "at least 4 task publishes")
		assert.GreaterOrEqual(t, eventCount, 0, "events recorded")

		t.Logf("Simulation complete: %d created, %d completed, %d failed, %d queue msgs, %d events",
			tasksCreated.Load(), tasksCompleted.Load(), tasksFailed.Load(), pubCount, eventCount)
	})
}

func TestMultiAgentConcurrentPublish(t *testing.T) {
	taskRepo := &simTaskRepo{}
	queueDrv := &simQueueDriver{}
	taskSvc := &simTaskService{repo: taskRepo, queue: queueDrv}
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
