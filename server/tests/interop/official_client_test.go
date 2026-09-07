package interop

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	a2acore "github.com/a2aproject/a2a-go/v2/a2a"
	a2aclient "github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2aclient/agentcard"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/auth"
	"github.com/agentium-lab/Janus/server/internal/gateway/a2a"
	"github.com/agentium-lab/Janus/server/internal/handler"
	"github.com/agentium-lab/Janus/server/internal/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Black-box interop: the OFFICIAL a2a-go client drives the Janus A2A gateway
// over real HTTP. This is the conformance evidence behind the "A2A v1 subset"
// claim — our own unit tests cannot prove wire compatibility.

type memTaskRepo struct {
	mu    sync.Mutex
	tasks map[string]*core.Task
	seq   int
}

func (r *memTaskRepo) Create(_ context.Context, task core.Task) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if task.ID == "" {
		r.seq++
		task.ID = fmt.Sprintf("t-%d", r.seq)
	}
	cp := task
	if cp.Status == core.TaskStatusCreated {
		cp.Status = core.TaskStatusQueued
	}
	now := time.Now().UTC()
	cp.CreatedAt, cp.UpdatedAt = now, now
	r.tasks[task.TenantID+"/"+cp.ID] = &cp
	return nil
}

func (r *memTaskRepo) Get(_ context.Context, tenantID, taskID string) (*core.Task, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if t, ok := r.tasks[tenantID+"/"+taskID]; ok {
		cp := *t
		return &cp, nil
	}
	return nil, fmt.Errorf("task not found")
}

func (r *memTaskRepo) GetByIdempotencyKey(_ context.Context, _, _ string) (*core.Task, error) {
	return nil, fmt.Errorf("not found")
}
func (r *memTaskRepo) UpdateStatus(_ context.Context, tenantID, taskID string, status core.TaskStatus, attempt int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if t, ok := r.tasks[tenantID+"/"+taskID]; ok {
		t.Status = status
		t.AttemptCount += attempt
		t.UpdatedAt = time.Now().UTC()
	}
	return nil
}
func (r *memTaskRepo) UpdateStatusWithCheck(_ context.Context, tenantID, taskID string, expected, next core.TaskStatus, attempt int) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if t, ok := r.tasks[tenantID+"/"+taskID]; ok && t.Status == expected {
		t.Status = next
		t.AttemptCount += attempt
		t.UpdatedAt = time.Now().UTC()
		return true, nil
	}
	return false, nil
}
func (r *memTaskRepo) UpdateRetryAt(_ context.Context, tenantID, taskID string, _ time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if t, ok := r.tasks[tenantID+"/"+taskID]; ok {
		t.Status = core.TaskStatusRetryScheduled
	}
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
func (r *memTaskRepo) ListPage(_ context.Context, tenantID string, pageSize int, pageToken string) ([]*core.Task, string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var all []*core.Task
	for _, t := range r.tasks {
		if t.TenantID == tenantID {
			cp := *t
			all = append(all, &cp)
		}
	}
	sort.Slice(all, func(i, j int) bool {
		if !all[i].UpdatedAt.Equal(all[j].UpdatedAt) {
			return all[i].UpdatedAt.After(all[j].UpdatedAt)
		}
		return all[i].ID > all[j].ID
	})
	start := 0
	if pageToken != "" {
		for i, t := range all {
			if t.ID == pageToken {
				start = i + 1
				break
			}
		}
	}
	end := start + pageSize
	next := ""
	if end < len(all) {
		next = all[end-1].ID
	} else {
		end = len(all)
	}
	return all[start:end], next, nil
}
func (r *memTaskRepo) SetResultRef(_ context.Context, tenantID, taskID, ref string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if t, ok := r.tasks[tenantID+"/"+taskID]; ok {
		t.ResultRef = ref
	}
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

func (r *memTaskRepo) snapshot() []core.Task {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]core.Task, 0, len(r.tasks))
	for _, t := range r.tasks {
		out = append(out, *t)
	}
	return out
}

type memQueue struct {
	mu      sync.Mutex
	events  []core.JanusEvent
	onEvent func(core.JanusEvent)
}

func (q *memQueue) PublishTask(_ context.Context, _ core.TaskMessage) error { return nil }
func (q *memQueue) FetchTasks(_ context.Context, _, _ string, _ core.FetchOptions) ([]core.TaskDelivery, error) {
	return nil, nil
}
func (q *memQueue) AckTask(_ context.Context, _ string, _ core.DeliveryRef) error { return nil }
func (q *memQueue) NackTask(_ context.Context, _ string, _ core.DeliveryRef, _ core.NackReason) error {
	return nil
}
func (q *memQueue) PublishDLQ(_ context.Context, _ core.TaskMessage, _ []byte) error { return nil }
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
	return nil, fmt.Errorf("replay not supported")
}
func (q *memQueue) EnsureTenant(_ context.Context, _ string) error            { return nil }
func (q *memQueue) EnsureMailbox(_ context.Context, _ core.MailboxSpec) error { return nil }
func (q *memQueue) EnsureConsumer(_ context.Context, _ core.ConsumerSpec) error {
	return nil
}
func (q *memQueue) Close() error { return nil }

type harness struct {
	ts      *httptest.Server
	repo    *memTaskRepo
	queue   *memQueue
	broad   *handler.FanoutBroadcaster
	taskSvc *service.TaskService
}

func newHarness(t *testing.T) *harness {
	t.Helper()

	repo := &memTaskRepo{tasks: map[string]*core.Task{}}
	queue := &memQueue{}
	taskSvc := service.NewTaskService(repo, queue, nil, nil)

	broadcastCh := make(chan core.JanusEvent, 256)
	queue.onEvent = func(evt core.JanusEvent) {
		select {
		case broadcastCh <- evt:
		default:
		}
	}
	broad := handler.NewFanoutBroadcaster(broadcastCh)
	a2aGw := a2a.NewGatewayWithStatus(noopRegistrar{}, taskSvc, taskSvc).
		WithEventSubscriber(broad).WithTaskLister(repo)

	mux := http.NewServeMux()
	mux.Handle("/a2a/", a2aGw)
	mux.Handle("/.well-known/agent-card.json", a2a.AgentCardV1Handler())

	authMw := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), auth.TenantCtxKey, "interop")))
		})
	}
	ts := httptest.NewServer(authMw(mux))
	t.Cleanup(ts.Close)
	return &harness{ts: ts, repo: repo, queue: queue, broad: broad, taskSvc: taskSvc}
}

type noopRegistrar struct{}

func (noopRegistrar) Register(_ context.Context, _ core.Agent) error { return nil }

func (h *harness) client(t *testing.T) *a2aclient.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	card, err := agentcard.DefaultResolver.Resolve(ctx, h.ts.URL)
	require.NoError(t, err, "official resolver must fetch and parse our Agent Card")
	client, err := a2aclient.NewFromCard(ctx, card)
	require.NoError(t, err, "official client must accept our card's HTTP+JSON interface")
	return client
}

func TestInterop_AgentCardResolves(t *testing.T) {
	h := newHarness(t)
	resp, err := http.Get(h.ts.URL + "/.well-known/agent-card.json")
	require.NoError(t, err)
	defer resp.Body.Close()
	var card map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&card))
	ifaces, _ := card["supportedInterfaces"].([]interface{})
	require.Len(t, ifaces, 1)
	iface := ifaces[0].(map[string]interface{})
	assert.Equal(t, "HTTP+JSON", iface["protocolBinding"])
	url := iface["url"].(string)
	assert.True(t, strings.HasSuffix(url, "/a2a/"), "card url must point at the gateway: %s", url)
}

func TestInterop_SendMessage_ReturnsTask(t *testing.T) {
	h := newHarness(t)
	client := h.client(t)

	msg := a2acore.NewMessage(a2acore.MessageRoleUser, a2acore.NewTextPart("interop hello"))
	result, err := client.SendMessage(context.Background(), &a2acore.SendMessageRequest{Message: msg})
	require.NoError(t, err, "official SendMessage against Janus")
	task, ok := result.(*a2acore.Task)
	require.True(t, ok, "non-streaming send must return a Task, got %T", result)
	assert.NotEmpty(t, task.ID)
	assert.Equal(t, a2acore.TaskStateSubmitted, task.Status.State)
}

func TestInterop_SendStreamingMessage_ReceivesLifecycle(t *testing.T) {
	h := newHarness(t)
	client := h.client(t)

	msg := a2acore.NewMessage(a2acore.MessageRoleUser, a2acore.NewTextPart("interop stream"))
	var sawWorking, sawCompleted bool
	var taskID string
	for evt, err := range client.SendStreamingMessage(context.Background(), &a2acore.SendMessageRequest{Message: msg}) {
		require.NoError(t, err)
		switch e := evt.(type) {
		case *a2acore.Task:
			taskID = string(e.ID)
			h.broad.Publish(core.JanusEvent{
				EventType: core.EventTaskStarted, TenantID: "interop", TaskID: string(e.ID),
				EventID: fmt.Sprintf("start-%d", time.Now().UnixNano()), Timestamp: time.Now(),
			})
			h.broad.Publish(core.JanusEvent{
				EventType: core.EventTaskCompleted, TenantID: "interop", TaskID: string(e.ID),
				EventID: fmt.Sprintf("done-%d", time.Now().UnixNano()), Timestamp: time.Now(),
			})
		case *a2acore.TaskStatusUpdateEvent:
			if e.Status.State == a2acore.TaskStateWorking {
				sawWorking = true
			}
			if e.Status.State == a2acore.TaskStateCompleted {
				sawCompleted = true
			}
		}
	}
	assert.NotEmpty(t, taskID)
	assert.True(t, sawWorking, "stream must deliver WORKING statusUpdate")
	assert.True(t, sawCompleted, "stream must deliver COMPLETED and close")
}

func TestInterop_TaskLifecycle_GetListCancel(t *testing.T) {
	h := newHarness(t)
	client := h.client(t)

	msg := a2acore.NewMessage(a2acore.MessageRoleUser, a2acore.NewTextPart("lifecycle"))
	result, err := client.SendMessage(context.Background(), &a2acore.SendMessageRequest{Message: msg})
	require.NoError(t, err)
	created := result.(*a2acore.Task)

	got, err := client.GetTask(context.Background(), &a2acore.GetTaskRequest{ID: created.ID})
	require.NoError(t, err, "official GetTask against Janus")
	assert.Equal(t, created.ID, got.ID)

	msg2 := a2acore.NewMessage(a2acore.MessageRoleUser, a2acore.NewTextPart("second"))
	_, err = client.SendMessage(context.Background(), &a2acore.SendMessageRequest{Message: msg2})
	require.NoError(t, err)

	pageSize := 1
	list, err := client.ListTasks(context.Background(), &a2acore.ListTasksRequest{PageSize: pageSize})
	require.NoError(t, err, "official ListTasks against Janus")
	require.Len(t, list.Tasks, 1, "pageSize must be honored")

	list2, err := client.ListTasks(context.Background(), &a2acore.ListTasksRequest{PageSize: pageSize, PageToken: list.NextPageToken})
	require.NoError(t, err, "second page via official pagination cursor")
	assert.Len(t, list2.Tasks, 1, "cursor pagination must return the next page")

	cancelled, err := client.CancelTask(context.Background(), &a2acore.CancelTaskRequest{ID: created.ID})
	require.NoError(t, err, "official CancelTask against Janus")
	assert.Equal(t, created.ID, cancelled.ID, "CancelTask must return the Task object")
	assert.Equal(t, a2acore.TaskStateCanceled, cancelled.Status.State)
}

func TestInterop_SubscribeTerminalTask_Rejected(t *testing.T) {
	h := newHarness(t)
	client := h.client(t)

	msg := a2acore.NewMessage(a2acore.MessageRoleUser, a2acore.NewTextPart("terminal"))
	result, err := client.SendMessage(context.Background(), &a2acore.SendMessageRequest{Message: msg})
	require.NoError(t, err)
	created := result.(*a2acore.Task)
	require.NoError(t, h.repo.UpdateStatus(context.Background(), "interop", string(created.ID), core.TaskStatusCompleted, 0))

	var subErr error
	for _, err := range client.SubscribeToTask(context.Background(), &a2acore.SubscribeToTaskRequest{ID: created.ID}) {
		if err != nil {
			subErr = err
			break
		}
	}
	require.Error(t, subErr, "subscribing to a terminal task must fail per spec")
	assert.Contains(t, subErr.Error(), "terminal", "error should mention terminal state, got: %v", subErr)
}
