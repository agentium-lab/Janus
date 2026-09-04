package scenario

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/auth"
	"github.com/agentium-lab/Janus/server/internal/gateway/a2a"
	"github.com/agentium-lab/Janus/server/internal/handler"
	"github.com/agentium-lab/Janus/server/internal/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type memTaskRepo struct {
	mu    sync.Mutex
	tasks map[string]*core.Task
}

func newMemTaskRepo() *memTaskRepo { return &memTaskRepo{tasks: map[string]*core.Task{}} }

func (r *memTaskRepo) Create(_ context.Context, task core.Task) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := task
	r.tasks[task.TenantID+"/"+task.ID] = &cp
	return nil
}

func (r *memTaskRepo) Get(_ context.Context, tenantID, taskID string) (*core.Task, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.tasks[tenantID+"/"+taskID]
	if !ok {
		return nil, fmt.Errorf("task not found: %s", taskID)
	}
	cp := *t
	return &cp, nil
}

func (r *memTaskRepo) GetByIdempotencyKey(_ context.Context, _, _ string) (*core.Task, error) {
	return nil, fmt.Errorf("not found")
}

func (r *memTaskRepo) UpdateStatus(_ context.Context, tenantID, taskID string, status core.TaskStatus, attempt int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.tasks[tenantID+"/"+taskID]
	if !ok {
		return fmt.Errorf("task not found")
	}
	t.Status = status
	t.AttemptCount += attempt
	now := time.Now().UTC()
	t.UpdatedAt = now
	return nil
}

func (r *memTaskRepo) UpdateStatusWithCheck(_ context.Context, tenantID, taskID string, expected, next core.TaskStatus, attempt int) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.tasks[tenantID+"/"+taskID]
	if !ok {
		return false, fmt.Errorf("task not found")
	}
	if t.Status != expected {
		return false, nil
	}
	t.Status = next
	t.AttemptCount += attempt
	t.UpdatedAt = time.Now().UTC()
	return true, nil
}

func (r *memTaskRepo) UpdateRetryAt(_ context.Context, tenantID, taskID string, _ time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.tasks[tenantID+"/"+taskID]
	if !ok {
		return fmt.Errorf("task not found")
	}
	t.Status = core.TaskStatusRetryScheduled
	t.UpdatedAt = time.Now().UTC()
	return nil
}

func (r *memTaskRepo) ListByStatus(_ context.Context, tenantID string, status core.TaskStatus, limit int) ([]*core.Task, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*core.Task
	for _, t := range r.tasks {
		if t.TenantID == tenantID && t.Status == status && len(out) < limit {
			cp := *t
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (r *memTaskRepo) SetResultRef(_ context.Context, tenantID, taskID, ref string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.tasks[tenantID+"/"+taskID]
	if !ok {
		return fmt.Errorf("task not found")
	}
	t.ResultRef = ref
	return nil
}

func (r *memTaskRepo) CountByStatus(_ context.Context, tenantID string, status core.TaskStatus) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, t := range r.tasks {
		if t.TenantID == tenantID && t.Status == status {
			n++
		}
	}
	return n, nil
}

func (r *memTaskRepo) CountRunningByAgent(_ context.Context, _, _ string) (int, error) { return 0, nil }
func (r *memTaskRepo) ResetForReplay(_ context.Context, _, _ string) error             { return nil }

type memQueue struct {
	mu      sync.Mutex
	queues  map[string][]core.TaskDelivery
	dlq     []core.TaskMessage
	events  []core.JanusEvent
	onEvent func(core.JanusEvent)
	seq     int
}

func newMemQueue() *memQueue { return &memQueue{queues: map[string][]core.TaskDelivery{}} }

func (q *memQueue) PublishTask(_ context.Context, msg core.TaskMessage) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.seq++
	q.queues[msg.MailboxID] = append(q.queues[msg.MailboxID], core.TaskDelivery{
		TaskID:      msg.TaskID,
		Payload:     msg.Payload,
		DeliveryRef: core.DeliveryRef(fmt.Sprintf("ref-%d", q.seq)),
	})
	return nil
}

func (q *memQueue) FetchTasks(_ context.Context, _, mailbox string, opts core.FetchOptions) ([]core.TaskDelivery, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	limit := opts.MaxMessages
	if limit <= 0 {
		limit = 1
	}
	if limit > len(q.queues[mailbox]) {
		limit = len(q.queues[mailbox])
	}
	taken := make([]core.TaskDelivery, limit)
	copy(taken, q.queues[mailbox][:limit])
	q.queues[mailbox] = q.queues[mailbox][limit:]
	return taken, nil
}

func (q *memQueue) AckTask(_ context.Context, _ string, _ core.DeliveryRef) error { return nil }

func (q *memQueue) NackTask(_ context.Context, _ string, _ core.DeliveryRef, _ core.NackReason) error {
	return nil
}

func (q *memQueue) PublishDLQ(_ context.Context, msg core.TaskMessage, _ []byte) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.dlq = append(q.dlq, msg)
	return nil
}

func (q *memQueue) PublishEvent(_ context.Context, event core.JanusEvent) error {
	q.mu.Lock()
	q.events = append(q.events, event)
	hook := q.onEvent
	q.mu.Unlock()
	if hook != nil {
		hook(event)
	}
	return nil
}

func (q *memQueue) ReplayEvents(_ context.Context, _ core.EventReplayFilter) (core.EventIterator, error) {
	return nil, fmt.Errorf("replay not supported in e2e fake")
}

func (q *memQueue) EnsureTenant(_ context.Context, _ string) error              { return nil }
func (q *memQueue) EnsureMailbox(_ context.Context, _ core.MailboxSpec) error   { return nil }
func (q *memQueue) EnsureConsumer(_ context.Context, _ core.ConsumerSpec) error { return nil }
func (q *memQueue) Close() error                                                { return nil }

func (q *memQueue) requeue(mailbox string, d core.TaskDelivery) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queues[mailbox] = append([]core.TaskDelivery{d}, q.queues[mailbox]...)
}

func (q *memQueue) eventLog() []core.JanusEvent {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]core.JanusEvent, len(q.events))
	copy(out, q.events)
	return out
}

type memApprovalRepo struct {
	mu    sync.Mutex
	items map[string]*core.Approval
	seq   int
}

func newMemApprovalRepo() *memApprovalRepo {
	return &memApprovalRepo{items: map[string]*core.Approval{}}
}

func (r *memApprovalRepo) Create(_ context.Context, a core.Approval) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if a.ID == "" {
		r.seq++
		a.ID = fmt.Sprintf("apr-%d", r.seq)
	}
	cp := a
	r.items[a.TenantID+"/"+a.ID] = &cp
	return nil
}

func (r *memApprovalRepo) Get(_ context.Context, tenantID, approvalID string) (*core.Approval, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	a, ok := r.items[tenantID+"/"+approvalID]
	if !ok {
		return nil, fmt.Errorf("approval not found")
	}
	cp := *a
	return &cp, nil
}

func (r *memApprovalRepo) GetPendingByTask(_ context.Context, tenantID, taskID string) (*core.Approval, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, a := range r.items {
		if a.TenantID == tenantID && a.TaskID == taskID && a.Status == "pending" {
			cp := *a
			return &cp, nil
		}
	}
	return nil, fmt.Errorf("no pending approval")
}

func (r *memApprovalRepo) UpdateDecision(_ context.Context, tenantID, approvalID, decision, _, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	a, ok := r.items[tenantID+"/"+approvalID]
	if !ok {
		return fmt.Errorf("approval not found")
	}
	a.Status = decision
	return nil
}

func (r *memApprovalRepo) ListPending(_ context.Context, tenantID string, limit int) ([]*core.Approval, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*core.Approval
	for _, a := range r.items {
		if a.TenantID == tenantID && a.Status == "pending" && len(out) < limit {
			cp := *a
			out = append(out, &cp)
		}
	}
	return out, nil
}

type memPolicyRuleRepo struct {
	rules []*core.PolicyRule
}

func (r *memPolicyRuleRepo) Create(_ context.Context, rule core.PolicyRule) error {
	cp := rule
	r.rules = append(r.rules, &cp)
	return nil
}

func (r *memPolicyRuleRepo) ListActive(_ context.Context, _ string) ([]*core.PolicyRule, error) {
	return r.rules, nil
}

type memMailboxRepo struct {
	mu        sync.Mutex
	mailboxes map[string]*core.Mailbox
}

func newMemMailboxRepo() *memMailboxRepo {
	return &memMailboxRepo{mailboxes: map[string]*core.Mailbox{}}
}

func (r *memMailboxRepo) Create(_ context.Context, mb core.Mailbox) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := mb
	r.mailboxes[mb.TenantID+"/"+mb.ID] = &cp
	return nil
}

func (r *memMailboxRepo) Get(_ context.Context, tenantID, mailboxID string) (*core.Mailbox, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	mb, ok := r.mailboxes[tenantID+"/"+mailboxID]
	if !ok {
		return &core.Mailbox{TenantID: tenantID, ID: mailboxID, Status: core.MailboxStatusActive, RetryPolicy: core.DefaultRetryPolicy()}, nil
	}
	cp := *mb
	return &cp, nil
}

func (r *memMailboxRepo) ListByAgent(_ context.Context, _, _ string) ([]*core.Mailbox, error) {
	return nil, nil
}

func (r *memMailboxRepo) Backlog(_ context.Context, _, _ string) (int, error) { return 0, nil }

func (r *memMailboxRepo) UpdateStatus(_ context.Context, _, _ string, _ core.MailboxStatus) error {
	return nil
}

func (r *memMailboxRepo) UpdateConfig(_ context.Context, _, _ string, _, _, _, _ int) error {
	return nil
}

type memBudgetRepo struct{}

func (r *memBudgetRepo) Upsert(_ context.Context, _ core.BudgetSpec) error { return nil }
func (r *memBudgetRepo) Get(_ context.Context, _ string, _ core.BudgetScopeType, _ string) (*core.BudgetSpec, error) {
	return nil, fmt.Errorf("not found")
}
func (r *memBudgetRepo) ListByTenant(_ context.Context, _ string) ([]*core.BudgetSpec, error) {
	return nil, nil
}

type memBudgetUsageRepo struct{}

func (r *memBudgetUsageRepo) ReserveTask(_ context.Context, _, _, _ string) error { return nil }
func (r *memBudgetUsageRepo) SettleUsage(_ context.Context, _, _, _ string, _ int, _ float64) error {
	return nil
}
func (r *memBudgetUsageRepo) ReleaseTask(_ context.Context, _, _, _ string) error { return nil }
func (r *memBudgetUsageRepo) GetDailyUsage(_ context.Context, _, _, _ string) (int, float64, int, error) {
	return 0, 0, 0, nil
}

type memAttemptRepo struct {
	mu     sync.Mutex
	latest map[string]*core.TaskAttempt
}

func newMemAttemptRepo() *memAttemptRepo {
	return &memAttemptRepo{latest: map[string]*core.TaskAttempt{}}
}

func (r *memAttemptRepo) Create(_ context.Context, a core.TaskAttempt) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := a
	r.latest[a.TenantID+"/"+a.TaskID] = &cp
	return nil
}

func (r *memAttemptRepo) GetLatest(_ context.Context, tenantID, taskID string) (*core.TaskAttempt, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	a, ok := r.latest[tenantID+"/"+taskID]
	if !ok {
		return nil, fmt.Errorf("no attempt")
	}
	cp := *a
	return &cp, nil
}
func (r *memAttemptRepo) UpdateHeartbeat(_ context.Context, _, _ string, _ int) error { return nil }
func (r *memAttemptRepo) UpdateFinished(_ context.Context, _, _ string, _ int, _ string, _, _ []byte) error {
	return nil
}
func (r *memAttemptRepo) UpdateFinishedWithCheck(_ context.Context, _, _ string, _ int, _ string, _, _ []byte) (bool, error) {
	return true, nil
}

type harness struct {
	ts           *httptest.Server
	taskRepo     *memTaskRepo
	queue        *memQueue
	approvalRepo *memApprovalRepo
}

func newHarness(t *testing.T) *harness {
	t.Helper()

	taskRepo := newMemTaskRepo()
	queue := newMemQueue()
	approvalRepo := newMemApprovalRepo()
	policyRuleRepo := &memPolicyRuleRepo{}
	mailboxRepo := newMemMailboxRepo()

	policyRuleRepo.rules = []*core.PolicyRule{{
		TenantID: "acme", ID: "rule-refund-gate", Name: "客服邮箱任务需人工审批", Status: "active", Priority: 100,
		Condition: json.RawMessage(`{"action":"task.publish","resource.value":"cs-mb"}`),
		Action:    json.RawMessage(`{"decision":"approval_required"}`),
	}}
	policySvc := service.NewPolicyService(policyRuleRepo)
	taskSvc := service.NewTaskService(taskRepo, queue, nil, nil).WithPolicy(policySvc)
	approvalSvc := service.NewApprovalService(approvalRepo, taskSvc, queue)
	taskSvc.WithApproval(approvalSvc)
	budgetSvc := service.NewBudgetServiceWithUsage(&memBudgetRepo{}, &memBudgetUsageRepo{})
	attemptRepo := newMemAttemptRepo()
	dispatchSvc := service.NewDispatchService(taskRepo, attemptRepo, mailboxRepo, queue, policySvc, budgetSvc)
	dispatchH := handler.NewDispatchHandler(&dispatchAdapter{svc: dispatchSvc})

	broadcastCh := make(chan core.JanusEvent, 256)
	queue.onEvent = func(evt core.JanusEvent) {
		select {
		case broadcastCh <- evt:
		default:
		}
	}
	broadcaster := handler.NewFanoutBroadcaster(broadcastCh)
	sseH := handler.NewSSEHandler(broadcaster).WithStatusChecker(taskSvc)
	progressH := handler.NewProgressHandler(taskSvc, broadcaster)
	approvalH := handler.NewApprovalHandler(approvalSvc)
	taskH := handler.NewTaskHandler(taskSvc)
	a2aGw := a2a.NewGatewayWithStatus(noopRegistrar{}, taskSvc, taskSvc).WithEventSubscriber(broadcaster)

	auditQuery := func(w http.ResponseWriter, r *http.Request) {
		taskID := lastPathSegmentBefore(r.URL.Path, "/events")
		var out []core.JanusEvent
		for _, evt := range queue.eventLog() {
			if evt.TaskID == taskID {
				out = append(out, evt)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"events": out, "count": len(out)})
	}

	mux := http.NewServeMux()
	mux.Handle("/a2a/", a2aGw)
	mux.HandleFunc("/v1/tenants/acme/tasks/", func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		switch {
		case strings.HasSuffix(p, "/progress") && r.Method == http.MethodPost:
			progressH.Report(w, r)
		case strings.HasSuffix(p, "/stream"):
			sseH.ServeHTTP(w, r)
		case strings.HasSuffix(p, "/events"):
			auditQuery(w, r)
		case strings.HasSuffix(p, "/start") && r.Method == http.MethodPost:
			dispatchH.Start(w, r)
		case strings.HasSuffix(p, "/ack") && r.Method == http.MethodPost:
			dispatchH.Ack(w, r)
		case strings.HasSuffix(p, "/complete") && r.Method == http.MethodPost:
			taskH.Complete(w, r)
		case strings.HasSuffix(p, "/fail") && r.Method == http.MethodPost:
			taskH.Fail(w, r)
		case strings.HasSuffix(p, "/nack") && r.Method == http.MethodPost:
			dispatchH.Nack(w, r)
		default:
			http.NotFound(w, r)
		}
	})
	mux.HandleFunc("/v1/tenants/acme/mailboxes/", func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/pull") || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		dispatchH.Pull(w, r)
	})
	mux.HandleFunc("/v1/tenants/acme/approvals/", func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		switch {
		case strings.HasSuffix(p, "/approve") && r.Method == http.MethodPost:
			approvalH.Approve(w, r)
		case strings.HasSuffix(p, "/reject") && r.Method == http.MethodPost:
			approvalH.Reject(w, r)
		default:
			approvalH.ListPending(w, r)
		}
	})

	authMw := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), auth.TenantCtxKey, "acme")))
		})
	}

	ts := httptest.NewServer(authMw(mux))
	t.Cleanup(ts.Close)

	return &harness{ts: ts, taskRepo: taskRepo, queue: queue, approvalRepo: approvalRepo}
}

type noopRegistrar struct{}

func (noopRegistrar) Register(_ context.Context, _ core.Agent) error { return nil }

type dispatchAdapter struct {
	svc *service.DispatchService
}

func (a *dispatchAdapter) PullTask(ctx context.Context, tenantID, mailboxID, agentID string) (*handler.ServicePullResult, error) {
	res, err := a.svc.PullTask(ctx, tenantID, mailboxID, agentID)
	if err != nil || res == nil {
		return nil, err
	}
	return &handler.ServicePullResult{Task: res.Task, LeaseID: res.LeaseID, ExpiresAt: res.ExpiresAt}, nil
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

func lastPathSegmentBefore(path, suffix string) string {
	trimmed := strings.TrimSuffix(path, suffix)
	segs := strings.Split(trimmed, "/")
	if len(segs) == 0 {
		return ""
	}
	return segs[len(segs)-1]
}

func (h *harness) do(t *testing.T, method, path, body string) (*http.Response, string) {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = bytes.NewReader([]byte(body))
	}
	req, err := http.NewRequest(method, h.ts.URL+path, rdr)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, string(b)
}

// TestSmartCustomerService_Journey drives the full feature set through one
// narrative: customer refund request → approval gate → agent works with live
// progress → completion, plus fanout and crash-recovery side quests.
func TestSmartCustomerService_Journey(t *testing.T) {
	h := newHarness(t)

	t.Run("refund_request_streams_and_hits_approval_gate", func(t *testing.T) {
		body := `{"message":{"role":"ROLE_USER","parts":[{"text":"订单123退款，金额250元"}],"messageId":"m-1"},"metadata":{"mailbox_id":"cs-mb"}}`
		resp, respBody := h.do(t, http.MethodPost, "/a2a/message:send", body)
		require.Equal(t, http.StatusOK, resp.StatusCode, respBody)

		var sendResp map[string]interface{}
		require.NoError(t, json.Unmarshal([]byte(respBody), &sendResp))
		taskObj := sendResp["task"].(map[string]interface{})
		taskID := taskObj["id"].(string)
		t.Logf("task created: %s state=%v", taskID, taskObj["status"])

		req, _ := http.NewRequest(http.MethodGet, h.ts.URL+"/a2a/tasks/"+taskID+":subscribe", nil)
		req = req.WithContext(context.WithValue(req.Context(), auth.TenantCtxKey, "acme"))
		resp2, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp2.Body.Close()
		assert.Contains(t, resp2.Header.Get("Content-Type"), "text/event-stream")

		eventCh := make(chan string, 64)
		readDone := make(chan struct{})
		go func() {
			defer close(readDone)
			sc := bufio.NewScanner(resp2.Body)
			sc.Buffer(make([]byte, 64*1024), 1024*1024)
			for sc.Scan() {
				line := sc.Text()
				if strings.HasPrefix(line, "data: ") {
					eventCh <- strings.TrimPrefix(line, "data: ")
				}
			}
		}()

		var snapshotSeen, approvalSeen bool
		waitDeadline := time.After(5 * time.Second)
		for !(snapshotSeen && approvalSeen) {
			select {
			case payload := <-eventCh:
				if strings.Contains(payload, `"task":`) {
					snapshotSeen = true
				}
				if strings.Contains(payload, a2a.V1StateInputRequired) {
					approvalSeen = true
				}
			case <-waitDeadline:
				resp2.Body.Close()
				t.Fatalf("timed out: snapshot=%v approval=%v", snapshotSeen, approvalSeen)
			}
		}
		resp2.Body.Close()
		<-readDone
		assert.True(t, snapshotSeen, "stream must start with task snapshot")
		assert.True(t, approvalSeen, "refund must land in INPUT_REQUIRED (approval gate)")

		pendingResp, pendingBody := h.do(t, http.MethodGet, "/v1/tenants/acme/approvals", "")
		require.Equal(t, http.StatusOK, pendingResp.StatusCode)
		assert.Contains(t, pendingBody, "pending", "approval must be listed pending: "+pendingBody)
	})

	t.Run("supervisor_approves_and_agent_pulls_starts_progresses_completes", func(t *testing.T) {
		_, pendingBody := h.do(t, http.MethodGet, "/v1/tenants/acme/approvals", "")
		approvalID := firstApprovalID(pendingBody)
		require.NotEmpty(t, approvalID, "pending approval id from: "+pendingBody)

		resp, body := h.do(t, http.MethodPost, "/v1/tenants/acme/approvals/"+approvalID+"/approve", `{"approver":"supervisor-wang","reason":"金额可退"}`)
		require.Equal(t, http.StatusOK, resp.StatusCode, body)

		pullResp, pullBody := h.do(t, http.MethodPost, "/v1/tenants/acme/mailboxes/cs-mb/pull", `{"agent_id":"cs-agent-1"}`)
		require.Equal(t, http.StatusOK, pullResp.StatusCode, pullBody)
		taskID := pulledTaskID(pullBody)
		require.NotEmpty(t, taskID, "pulled task from: "+pullBody)

		leaseID := pulledLeaseID(pullBody)
		require.NotEmpty(t, leaseID, "lease from pull: "+pullBody)

		startResp, startBody := h.do(t, http.MethodPost, "/v1/tenants/acme/tasks/"+taskID+"/start", fmt.Sprintf(`{"lease_id":%q}`, leaseID))
		require.Equal(t, http.StatusOK, startResp.StatusCode, startBody)

		progResp, progBody := h.do(t, http.MethodPost, "/v1/tenants/acme/tasks/"+taskID+"/progress", `{"message":"正在核对订单支付记录…","percent":40,"agent_id":"cs-agent-1"}`)
		require.Equal(t, http.StatusAccepted, progResp.StatusCode, progBody)
		_, _ = h.do(t, http.MethodPost, "/v1/tenants/acme/tasks/"+taskID+"/progress", `{"message":"退款已发起，等待渠道确认","percent":90,"agent_id":"cs-agent-1"}`)

		ackResp, ackBody := h.do(t, http.MethodPost, "/v1/tenants/acme/tasks/"+taskID+"/ack", fmt.Sprintf(`{"lease_id":%q,"result_ref":"refund://123/ok"}`, leaseID))
		require.Equal(t, http.StatusOK, ackResp.StatusCode, ackBody)

		task, _ := h.taskRepo.Get(context.Background(), "acme", taskID)
		assert.Equal(t, core.TaskStatusCompleted, task.Status)
		assert.Equal(t, "refund://123/ok", task.ResultRef)

		eventsResp, eventsBody := h.do(t, http.MethodGet, "/v1/tenants/acme/tasks/"+taskID+"/events", "")
		require.Equal(t, http.StatusOK, eventsResp.StatusCode)
		for _, want := range []string{"task.created", "task.queued", "task.claimed", "task.started", "task.completed"} {
			assert.Contains(t, eventsBody, want, "audit trail must contain "+want)
		}
	})

	t.Run("multi_agent_fanout_billing_and_shipping", func(t *testing.T) {
		for _, mb := range []string{"billing-mb", "shipping-mb"} {
			body := fmt.Sprintf(`{"message":{"role":"ROLE_USER","parts":[{"text":"订单123后续处理"}],"messageId":"m-%s"},"metadata":{"mailbox_id":"%s"}}`, mb, mb)
			resp, _ := h.do(t, http.MethodPost, "/a2a/message:send", body)
			require.Equal(t, http.StatusOK, resp.StatusCode)
		}

		bResp, bBody := h.do(t, http.MethodPost, "/v1/tenants/acme/mailboxes/billing-mb/pull", `{"agent_id":"billing-agent"}`)
		require.Equal(t, http.StatusOK, bResp.StatusCode, bBody)
		assert.NotEmpty(t, pulledTaskID(bBody))

		sResp, sBody := h.do(t, http.MethodPost, "/v1/tenants/acme/mailboxes/shipping-mb/pull", `{"agent_id":"shipping-agent"}`)
		require.Equal(t, http.StatusOK, sResp.StatusCode, sBody)
		assert.NotEmpty(t, pulledTaskID(sBody))
	})

	t.Run("crash_recovery_nack_requeues_for_next_worker", func(t *testing.T) {
		body := `{"message":{"role":"ROLE_USER","parts":[{"text":"物流地址修改"}],"messageId":"m-crash"},"metadata":{"mailbox_id":"recovery-mb"}}`
		resp, _ := h.do(t, http.MethodPost, "/a2a/message:send", body)
		require.Equal(t, http.StatusOK, resp.StatusCode)

		first, firstBody := h.do(t, http.MethodPost, "/v1/tenants/acme/mailboxes/recovery-mb/pull", `{"agent_id":"worker-1"}`)
		require.Equal(t, http.StatusOK, first.StatusCode, firstBody)
		taskID := pulledTaskID(firstBody)
		require.NotEmpty(t, taskID)

		leaseID := pulledLeaseID(firstBody)
		nackResp, nackBody := h.do(t, http.MethodPost, "/v1/tenants/acme/tasks/"+taskID+"/nack", fmt.Sprintf(`{"lease_id":%q,"retriable":true,"error":{"code":"worker_crashed","message":"agent process died"}}`, leaseID))
		require.Contains(t, []int{http.StatusOK, http.StatusNoContent}, nackResp.StatusCode, nackBody)

		nacked, err := h.taskRepo.Get(context.Background(), "acme", taskID)
		require.NoError(t, err)
		require.Equal(t, core.TaskStatusRetryScheduled, nacked.Status, "nack must schedule a retry")

		// The retry scheduler is PG-only in production; simulate its tick.
		require.NoError(t, h.taskRepo.UpdateStatus(context.Background(), "acme", taskID, core.TaskStatusQueued, 0))
		require.NoError(t, h.queue.PublishTask(context.Background(), core.TaskMessage{
			TenantID: "acme", MailboxID: "recovery-mb", TaskID: taskID,
			Payload: []byte(nacked.Envelope.Payload.Content),
		}))

		second, secondBody := h.do(t, http.MethodPost, "/v1/tenants/acme/mailboxes/recovery-mb/pull", `{"agent_id":"worker-2"}`)
		require.Equal(t, http.StatusOK, second.StatusCode, secondBody)
		assert.Equal(t, taskID, pulledTaskID(secondBody), "same task must be redelivered to the next worker")
	})
}

func pulledTaskID(raw string) string {
	var resp struct {
		Task struct {
			ID string `json:"id"`
		} `json:"task"`
	}
	if err := json.Unmarshal([]byte(raw), &resp); err != nil {
		return ""
	}
	return resp.Task.ID
}

func pulledLeaseID(raw string) string {
	var resp struct {
		Lease struct {
			LeaseID string `json:"lease_id"`
		} `json:"lease"`
	}
	if err := json.Unmarshal([]byte(raw), &resp); err != nil {
		return ""
	}
	return resp.Lease.LeaseID
}

func firstApprovalID(raw string) string {
	var arr []map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &arr); err != nil || len(arr) == 0 {
		return ""
	}
	if id, ok := arr[0]["id"].(string); ok {
		return id
	}
	return ""
}

func extractJSONString(raw, key string) string {
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return ""
	}
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}
