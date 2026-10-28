package service

// GA-04 lane A: barriers moved to where the races actually are.
// (a) claim-race window: between PullTask's task read and its claim CAS;
// (b) stale-pull-replay: a stale lease holder acting AFTER Cancel ->
//     Replay -> new Pull swapped the round (deterministic channel gates,
//     no sleeps).

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/driver/postgres"
)

// claimGate blocks the pull between its task read and its claim CAS: the
// attempt insert (CreateTx) has executed on the claim transaction and the
// gate holds the pull there, so the in-window writer commits before
// UpdateStatusWithCheckTx runs.
type claimGate struct {
	*postgres.TaskAttemptRepository
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *claimGate) CreateTx(ctx context.Context, tx pgx.Tx, a core.TaskAttempt) error {
	if err := g.TaskAttemptRepository.CreateTx(ctx, tx, a); err != nil {
		return err
	}
	g.once.Do(func() { close(g.entered) })
	<-g.release
	return nil
}

// preCheckGate blocks a stale Start/Ack/Heartbeat between its pool-side
// GetLatest pre-check (which already read the OLD attempt and passed) and
// the authoritative in-transaction revalidation that follows.
type preCheckGate struct {
	*postgres.TaskAttemptRepository
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *preCheckGate) GetLatest(ctx context.Context, tenantID, taskID string) (*core.TaskAttempt, error) {
	a, err := g.TaskAttemptRepository.GetLatest(ctx, tenantID, taskID)
	if err != nil {
		return nil, err
	}
	g.once.Do(func() { close(g.entered) })
	<-g.release
	return a, err
}

// staleTaskReadGate blocks NACK between its pool-side task read (the status
// its CAS will expect) and the nack transaction, so the in-window writer can
// commit a different non-terminal status first.
type staleTaskReadGate struct {
	*postgres.TaskRepository
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *staleTaskReadGate) Get(ctx context.Context, tenantID, taskID string) (*core.Task, error) {
	t, err := g.TaskRepository.Get(ctx, tenantID, taskID)
	if err != nil {
		return nil, err
	}
	g.once.Do(func() { close(g.entered) })
	<-g.release
	return t, err
}

// swapAfterLockTx reproduces on the task row the state the ACK task CAS
// guards against: the locked read returns the pre-swap status, then the row
// moves to a mismatching non-terminal status on the same transaction before
// the CAS executes. SELECT ... FOR UPDATE makes this interleaving impossible
// against a real concurrent writer; the wrapper pins the defense-in-depth
// branch (the swap rolls back with the transaction).
type swapAfterLockTx struct {
	TaskTxRepo
}

func (s swapAfterLockTx) GetTxForUpdate(ctx context.Context, tx pgx.Tx, tenantID, taskID string) (*core.Task, error) {
	t, err := s.TaskTxRepo.GetTxForUpdate(ctx, tx, tenantID, taskID)
	if err != nil {
		return nil, err
	}
	if t.Status == core.TaskStatusClaimed || t.Status == core.TaskStatusRunning {
		if _, uerr := tx.Exec(ctx,
			`UPDATE tasks SET status='blocked', updated_at=now() WHERE tenant_id=$1 AND id=$2`,
			tenantID, taskID); uerr != nil {
			return nil, uerr
		}
	}
	return t, err
}

func countOutboxEvents(t *testing.T, env *serviceTestEnv, taskID string, et core.EventType) int {
	t.Helper()
	rows, err := env.pool.Query(context.Background(),
		`SELECT payload FROM outbox_events WHERE tenant_id='acme' AND kind='event_publish'`)
	require.NoError(t, err)
	defer rows.Close()
	n := 0
	for rows.Next() {
		var payload []byte
		require.NoError(t, rows.Scan(&payload))
		var evt core.JanusEvent
		if json.Unmarshal(payload, &evt) == nil && evt.TaskID == taskID && evt.EventType == et {
			n++
		}
	}
	return n
}

func countOutboxPublishes(t *testing.T, env *serviceTestEnv, taskID, kind string) int {
	t.Helper()
	rows, err := env.pool.Query(context.Background(),
		`SELECT payload FROM outbox_events WHERE tenant_id='acme' AND kind=$1`, kind)
	require.NoError(t, err)
	defer rows.Close()
	n := 0
	for rows.Next() {
		var payload []byte
		require.NoError(t, rows.Scan(&payload))
		var msg core.TaskMessage
		if json.Unmarshal(payload, &msg) == nil && msg.TaskID == taskID {
			n++
		}
	}
	return n
}

// setupStaleHolderRound creates a claimed round via a real Pull (attempt 1,
// lease L1) and arms a two-delivery driver so the post-replay re-claim can
// also run through the real Pull path.
func setupStaleHolderRound(t *testing.T) (*serviceTestEnv, string, string) {
	t.Helper()
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	taskID := fmt.Sprintf("ga04-stale-%d", time.Now().UnixNano())
	createTestTask(t, env, taskID)

	drv := &perCallDriver{}
	drv.queue = []core.TaskDelivery{
		{TaskID: taskID, DeliveryRef: core.DeliveryRef("ga04-d1"), Payload: []byte(`{"task_id":"ga04-d1"}`)},
		{TaskID: taskID, DeliveryRef: core.DeliveryRef("ga04-d2"), Payload: []byte(`{"task_id":"ga04-d2"}`)},
	}
	env.dispatch = env.dispatch.WithQueueDriver(drv)

	res, err := env.dispatch.PullTask(ctx, "acme", "mb-1", "agent-1")
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotEmpty(t, res.LeaseID)
	return env, taskID, res.LeaseID
}

// swapRoundViaCancelReplayPull runs Cancel -> Replay -> new Pull while the
// stale holder is held between its pre-check and its transaction, and
// verifies the new round took shape: attempt numbering restarted at 1 under
// a fresh lease.
func swapRoundViaCancelReplayPull(t *testing.T, env *serviceTestEnv, taskID, staleLease string) string {
	t.Helper()
	ctx := context.Background()

	require.NoError(t, env.taskSvc.Cancel(ctx, "acme", taskID), "cancel the stale round")
	_, rerr := env.taskSvc.Replay(ctx, "acme", taskID)
	require.NoError(t, rerr, "replay resets the round")

	res, perr := env.dispatch.PullTask(ctx, "acme", "mb-1", "agent-1")
	require.NoError(t, perr, "new round claim")
	require.NotNil(t, res)

	var status string
	var lease string
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT status, lease_id FROM task_attempts WHERE tenant_id='acme' AND task_id=$1`,
		taskID).Scan(&status, &lease))
	require.Equal(t, "claimed", status)
	require.NotEmpty(t, lease)
	require.NotEqual(t, staleLease, lease, "the new round must be under a fresh lease")
	require.Equal(t, lease, res.LeaseID)
	return lease
}

// TestGA04_ClaimRace_CancelThenReplayInWindow_ClaimLandsOnNewRound_PG is the
// Replay variant of the claim race: Cancel -> Replay both commit inside the
// read-to-CAS window, so the task is back to 'queued' when the claim CAS
// runs. The status-only CAS matches and the claim legitimately lands on the
// replayed round (attempt numbering restarted); the replayed delivery is
// later absorbed by redelivery reconciliation, so this is the accepted ABA
// outcome, pinned here.
func TestGA04_ClaimRace_CancelThenReplayInWindow_ClaimLandsOnNewRound_PG(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	taskID := "ga04-claim-replay-window"
	createTestTask(t, env, taskID)

	drv := &mockDispatchQueueDriver{}
	drv.deliveries = []core.TaskDelivery{{
		TaskID: taskID, DeliveryRef: core.DeliveryRef("ga04-rr-ref"),
		Payload: []byte(fmt.Sprintf(`{"task_id":%q}`, taskID)),
	}}
	env.dispatch = env.dispatch.WithQueueDriver(drv)

	gate := &claimGate{entered: make(chan struct{}), release: make(chan struct{})}
	gate.TaskAttemptRepository = env.attemptRepo
	env.dispatch.attemptTx = gate

	type pullOutcome struct {
		res *PullResult
		err error
	}
	done := make(chan pullOutcome, 1)
	go func() {
		r, perr := env.dispatch.PullTask(ctx, "acme", "mb-1", "agent-1")
		done <- pullOutcome{res: r, err: perr}
	}()

	<-gate.entered // pull is past its 'queued' read, attempt inserted, pre-CAS
	require.NoError(t, env.taskSvc.Cancel(ctx, "acme", taskID))
	_, rerr := env.taskSvc.Replay(ctx, "acme", taskID)
	require.NoError(t, rerr, "cancel then replay inside the window")
	close(gate.release)

	out := <-done
	require.NoError(t, out.err)
	require.NotNil(t, out.res, "claim of the replayed round succeeds: status-only CAS matches queued")

	task, gerr := env.taskRepo.Get(ctx, "acme", taskID)
	require.NoError(t, gerr)
	require.Equal(t, core.TaskStatusClaimed, task.Status)
	require.Equal(t, 1, task.ReplayCount, "the in-window replay committed")
	require.Equal(t, 1, task.AttemptCount, "claim incremented the reset counter once")

	var attempts int
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_attempts WHERE tenant_id='acme' AND task_id=$1`, taskID).Scan(&attempts))
	require.Equal(t, 1, attempts, "single attempt: replay cleared the old round, the claim added one")

	var attemptNum int
	var lease string
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT attempt, lease_id FROM task_attempts WHERE tenant_id='acme' AND task_id=$1`, taskID).
		Scan(&attemptNum, &lease))
	require.Equal(t, 1, attemptNum, "attempt numbering restarted with the replayed round")
	require.Equal(t, out.res.LeaseID, lease)
}

// TestGA04_StalePullReplay_StaleStartRejectedByTxRevalidation_PG: the stale
// holder (lease L1, attempt 1) enters StartTaskAs, passes the pool-side
// pre-check on the OLD attempt, and is held while Cancel -> Replay -> new
// Pull swaps the round. Its transaction must reject via the in-tx
// revalidation and must not resurrect the new round to running.
func TestGA04_StalePullReplay_StaleStartRejectedByTxRevalidation_PG(t *testing.T) {
	env, taskID, staleLease := setupStaleHolderRound(t)
	ctx := context.Background()

	gate := &preCheckGate{entered: make(chan struct{}), release: make(chan struct{})}
	gate.TaskAttemptRepository = env.attemptRepo
	env.dispatch.attemptRepo = gate

	startErr := make(chan error, 1)
	go func() {
		startErr <- env.dispatch.StartTaskAs(ctx, "acme", taskID, staleLease, "agent-1")
	}()

	<-gate.entered // stale start is between its pre-check read and its tx
	newLease := swapRoundViaCancelReplayPull(t, env, taskID, staleLease)
	close(gate.release)

	err := <-startErr
	require.Error(t, err)
	require.EqualError(t, err,
		fmt.Sprintf("lease mismatch: task %s is held by a newer attempt", taskID))

	task, gerr := env.taskRepo.Get(ctx, "acme", taskID)
	require.NoError(t, gerr)
	require.Equal(t, core.TaskStatusClaimed, task.Status, "stale start must not resurrect the round to running")

	var status string
	var heartbeat *time.Time
	var lease string
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT status, heartbeat_at, lease_id FROM task_attempts WHERE tenant_id='acme' AND task_id=$1`,
		taskID).Scan(&status, &heartbeat, &lease))
	require.Equal(t, "claimed", status, "the new round's attempt stays claimed")
	require.Nil(t, heartbeat)
	require.Equal(t, newLease, lease)
	require.Zero(t, countOutboxEvents(t, env, taskID, core.EventTaskStarted),
		"no task.started event may be published by the stale holder")
}

// TestGA04_StalePullReplay_StaleAckDoesNotSettleNewRound_PG: a stale ACK
// (lease L1, attempt 1 read pre-swap) whose finish CAS runs after the round
// swap. UpdateFinishedWithCheckTx requires the lease to match, so the finish
// misses and the ACK degrades to the duplicate-ACK no-op: the new round is
// neither completed nor settled.
func TestGA04_StalePullReplay_StaleAckDoesNotSettleNewRound_PG(t *testing.T) {
	env, taskID, staleLease := setupStaleHolderRound(t)
	ctx := context.Background()

	gate := &preCheckGate{entered: make(chan struct{}), release: make(chan struct{})}
	gate.TaskAttemptRepository = env.attemptRepo
	env.dispatch.attemptRepo = gate

	ackErr := make(chan error, 1)
	go func() {
		ackErr <- env.dispatch.AckTaskAs(ctx, "acme", taskID, staleLease, "agent-1",
			"result://stale", &core.TokenUsage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2})
	}()

	<-gate.entered
	swapRoundViaCancelReplayPull(t, env, taskID, staleLease)
	close(gate.release)

	err := <-ackErr
	require.NoError(t, err, "stale ACK is absorbed as the duplicate-ACK no-op")

	task, gerr := env.taskRepo.Get(ctx, "acme", taskID)
	require.NoError(t, gerr)
	require.Equal(t, core.TaskStatusClaimed, task.Status, "stale ACK must not settle the new round")
	require.Empty(t, task.ResultRef, "stale ACK must not set a result ref")

	var status string
	var finishedAt *time.Time
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT status, finished_at FROM task_attempts WHERE tenant_id='acme' AND task_id=$1`,
		taskID).Scan(&status, &finishedAt))
	require.Equal(t, "claimed", status, "the new round's attempt stays open")
	require.Nil(t, finishedAt)

	var ledgerRows int
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT count(*) FROM budget_usage_ledger WHERE tenant_id='acme' AND task_id=$1`,
		taskID).Scan(&ledgerRows))
	require.Zero(t, ledgerRows, "stale ACK must not settle the ledger")
	require.Zero(t, countOutboxEvents(t, env, taskID, core.EventTaskCompleted),
		"no task.completed event may be published by the stale holder")
}

// TestGA04_StalePullReplay_StaleHeartbeatDoesNotRefreshNewRound_PG: a stale
// heartbeat whose CAS runs after the round swap. lease_id is part of the
// heartbeat CAS, so it must miss: the new round's attempt is not refreshed
// and the stale holder is told its lease is lost.
func TestGA04_StalePullReplay_StaleHeartbeatDoesNotRefreshNewRound_PG(t *testing.T) {
	env, taskID, staleLease := setupStaleHolderRound(t)
	ctx := context.Background()

	gate := &preCheckGate{entered: make(chan struct{}), release: make(chan struct{})}
	gate.TaskAttemptRepository = env.attemptRepo
	env.dispatch.attemptRepo = gate

	hbErr := make(chan error, 1)
	go func() {
		hbErr <- env.dispatch.TaskHeartbeatAs(ctx, "acme", taskID, staleLease, "agent-1")
	}()

	<-gate.entered
	newLease := swapRoundViaCancelReplayPull(t, env, taskID, staleLease)
	close(gate.release)

	err := <-hbErr
	require.Error(t, err)
	require.EqualError(t, err, "attempt is no longer active; lease presumed lost")

	var status string
	var heartbeat *time.Time
	var lease string
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT status, heartbeat_at, lease_id FROM task_attempts WHERE tenant_id='acme' AND task_id=$1`,
		taskID).Scan(&status, &heartbeat, &lease))
	require.Equal(t, "claimed", status)
	require.Nil(t, heartbeat, "the new round's heartbeat clock must not be refreshed by the stale lease")
	require.Equal(t, newLease, lease)
}

// TestGA04_StalePullAcrossReplay_RejectedByAttemptCountCAS_PG pins the
// stale-pull corruption the report's counterexample names: a pull that read
// queued/count=1, held across Cancel -> Replay (count reset to 0, attempts
// cleared), must NOT commit on the replayed round — the pre-fix status-only
// CAS matched, inserted attempt=2 while the counter restarted at 1, and the
// NEXT pull recomputed attempt=2 into a duplicate PK that blocked every
// later claim. The claim CAS must also predicate on attempt_count.
func TestGA04_StalePullAcrossReplay_RejectedByAttemptCountCAS_PG(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	taskID := fmt.Sprintf("ga04-stale-pull-replay-%d", time.Now().UnixNano())
	createTestTask(t, env, taskID)

	drv := &perCallDriver{}
	drv.queue = []core.TaskDelivery{
		{TaskID: taskID, DeliveryRef: core.DeliveryRef(taskID + ":1"), Payload: []byte(fmt.Sprintf(`{"task_id":%q}`, taskID))},
		{TaskID: taskID, DeliveryRef: core.DeliveryRef(taskID + ":2"), Payload: []byte(fmt.Sprintf(`{"task_id":%q}`, taskID))},
		{TaskID: taskID, DeliveryRef: core.DeliveryRef(taskID + ":3"), Payload: []byte(fmt.Sprintf(`{"task_id":%q}`, taskID))},
	}
	env.dispatch = env.dispatch.WithQueueDriver(drv)

	first, err := env.dispatch.PullTask(ctx, "acme", "mb-1", "agent-1")
	require.NoError(t, err)
	require.Equal(t, 1, first.Attempt)
	require.NoError(t, env.dispatch.NackTaskAs(ctx, "acme", taskID, first.LeaseID, "agent-1", true,
		&core.TaskError{Code: "seed", Message: "seed first retry"}))
	_, uerr := env.pool.Exec(ctx,
		`UPDATE tasks SET status='queued', retry_at=NULL WHERE tenant_id='acme' AND id=$1`, taskID)
	require.NoError(t, uerr)

	gate := &staleTaskReadGate{entered: make(chan struct{}), release: make(chan struct{})}
	gate.TaskRepository = env.taskRepo
	env.dispatch.taskRepo = gate

	type pullOutcome struct {
		res *PullResult
		err error
	}
	done := make(chan pullOutcome, 1)
	go func() {
		r, perr := env.dispatch.PullTask(ctx, "acme", "mb-1", "agent-1")
		done <- pullOutcome{res: r, err: perr}
	}()

	<-gate.entered
	require.NoError(t, env.taskSvc.Cancel(ctx, "acme", taskID))
	_, rerr := env.taskSvc.Replay(ctx, "acme", taskID)
	require.NoError(t, rerr)
	close(gate.release)

	out := <-done
	require.Error(t, out.err, "stale pull snapshot (attempt_count=1) must be rejected on the replayed round (count=0)")
	require.Contains(t, out.err.Error(), "claim race")

	task, gerr := env.taskRepo.Get(ctx, "acme", taskID)
	require.NoError(t, gerr)
	require.Equal(t, core.TaskStatusQueued, task.Status, "the replayed round stays queued")
	require.Equal(t, 0, task.AttemptCount)

	var attempts int
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_attempts WHERE tenant_id='acme' AND task_id=$1`, taskID).Scan(&attempts))
	require.Equal(t, 0, attempts, "the rejected claim's attempt insert rolled back")

	next, nerr := env.dispatch.PullTask(ctx, "acme", "mb-1", "agent-1")
	require.NoError(t, nerr, "the next pull must not hit a duplicate attempt PK")
	require.Equal(t, 1, next.Attempt)
}
