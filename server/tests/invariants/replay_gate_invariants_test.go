package invariants

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/service"
)

// A2 invariant: Replay must not bypass the approval gate — and must not
// silently pass it when the approval system itself is failing. The gate
// runs through TaskService.Replay on the in-memory transaction path (the
// same logical path as production PG, see TaskService.initTxPath).

// replayGateTaskRepo serves exactly one terminal task; every method the
// replay gate never needs fails loudly if reached unexpectedly.
type replayGateTaskRepo struct {
	task *core.Task
}

func (r *replayGateTaskRepo) Create(context.Context, core.Task) error {
	return errors.New("unexpected Create call")
}

func (r *replayGateTaskRepo) Get(context.Context, string, string) (*core.Task, error) {
	return r.task, nil
}

func (r *replayGateTaskRepo) GetByIdempotencyKey(context.Context, string, string) (*core.Task, error) {
	return nil, errors.New("unexpected GetByIdempotencyKey call")
}

func (r *replayGateTaskRepo) UpdateStatus(context.Context, string, string, core.TaskStatus, int) error {
	return errors.New("unexpected UpdateStatus call")
}

func (r *replayGateTaskRepo) UpdateStatusWithCheck(context.Context, string, string, core.TaskStatus, core.TaskStatus, int) (bool, error) {
	return false, errors.New("unexpected UpdateStatusWithCheck call")
}

func (r *replayGateTaskRepo) UpdateRetryAt(context.Context, string, string, time.Time) error {
	return errors.New("unexpected UpdateRetryAt call")
}

func (r *replayGateTaskRepo) ListByStatus(context.Context, string, core.TaskStatus, int) ([]*core.Task, error) {
	return nil, errors.New("unexpected ListByStatus call")
}

func (r *replayGateTaskRepo) SetResultRef(context.Context, string, string, string) error {
	return errors.New("unexpected SetResultRef call")
}

func (r *replayGateTaskRepo) CountByStatus(context.Context, string, core.TaskStatus) (int, error) {
	return 0, errors.New("unexpected CountByStatus call")
}

func (r *replayGateTaskRepo) CountRunningByAgent(context.Context, string, string) (int, error) {
	return 0, errors.New("unexpected CountRunningByAgent call")
}

func (r *replayGateTaskRepo) ResetForReplay(context.Context, string, string) error {
	return nil // the gate has passed; the reset itself is B2's territory
}

// replayGateApprovalRepo answers GetLatestByTask with a canned verdict or
// an injected lookup failure.
type replayGateApprovalRepo struct {
	latest *core.Approval
	err    error
}

func (r *replayGateApprovalRepo) Create(context.Context, core.Approval) error { return nil }

func (r *replayGateApprovalRepo) Get(context.Context, string, string) (*core.Approval, error) {
	return nil, errors.New("unexpected Get call")
}

func (r *replayGateApprovalRepo) GetPendingByTask(context.Context, string, string) (*core.Approval, error) {
	return nil, nil
}

func (r *replayGateApprovalRepo) GetLatestByTask(context.Context, string, string) (*core.Approval, error) {
	return r.latest, r.err
}

func (r *replayGateApprovalRepo) UpdateDecision(context.Context, string, string, string, string, string) error {
	return nil
}

func (r *replayGateApprovalRepo) ListPending(context.Context, string, int) ([]*core.Approval, error) {
	return nil, nil
}

// replayGateQueueDriver counts event publications; the nil-embedded
// interface fails loudly if any other transport method is touched.
type replayGateQueueDriver struct {
	core.QueueEventDriver
	events int
}

func (d *replayGateQueueDriver) PublishEvent(context.Context, core.JanusEvent) error {
	d.events++
	return nil
}

func (d *replayGateQueueDriver) PublishTask(context.Context, core.TaskMessage) error {
	d.events++
	return nil
}

func TestReplay_ApprovalGate_FailClosedAndExpired(t *testing.T) {
	newSvc := func(repo *replayGateApprovalRepo, qd *replayGateQueueDriver) *service.TaskService {
		task := &core.Task{
			TenantID: "acme", ID: "gate-task", Status: core.TaskStatusCompleted,
			SourceAgent: "agent-1", Envelope: core.TaskEnvelope{TaskID: "gate-task"},
		}
		svc := service.NewTaskService(&replayGateTaskRepo{task: task}, qd, nil, nil)
		return svc.WithApproval(service.NewApprovalService(repo, svc, nil))
	}

	// 1. Approval lookup ERROR must fail closed: when the verdict cannot
	// be read, the gate denies — it is never silently skipped.
	qd := &replayGateQueueDriver{}
	_, err := newSvc(&replayGateApprovalRepo{err: errors.New("approval store unavailable")}, qd).
		Replay(context.Background(), "acme", "gate-task")
	require.Error(t, err, "approval-lookup failure must deny replay (fail-closed)")
	assert.Contains(t, err.Error(), "replay approval check")
	assert.Equal(t, 0, qd.events, "nothing may be re-queued when the gate cannot be evaluated")

	// 2. Latest decision "expired": replay is denied — a stale gate must
	// not resurrect the task.
	qd2 := &replayGateQueueDriver{}
	_, err = newSvc(&replayGateApprovalRepo{latest: &core.Approval{
		TenantID: "acme", TaskID: "gate-task", Decision: "expired",
	}}, qd2).Replay(context.Background(), "acme", "gate-task")
	require.Error(t, err, "expired approval verdict must deny replay")
	assert.Contains(t, err.Error(), "expired")
	assert.Equal(t, 0, qd2.events, "an expired verdict must not re-queue the task")

	// Control: an approved verdict does not block replay — the gate only
	// rejects bad verdicts, it does not reject everything.
	qd3 := &replayGateQueueDriver{}
	_, err = newSvc(&replayGateApprovalRepo{latest: &core.Approval{
		TenantID: "acme", TaskID: "gate-task", Decision: "approved",
	}}, qd3).Replay(context.Background(), "acme", "gate-task")
	require.NoError(t, err, "approved verdict must not block replay")
	assert.Equal(t, 1, qd3.events, "the replayed task must publish its task.created event")
}
