package service

// GA-05 (lane B): real two-round delivery on real PG + real NATS JetStream.
//
// Every delivery round crosses the real broker: a task_publish outbox row is
// drained to a live JetStream stream, PullTask fetches it through the durable
// consumer, and settlement (Ack/Nack) writes the terminal state through the
// PG lifecycle tx. Scenarios:
//
//	T1 normal replay: create + 2 replays = 3 delivery generations, one
//	   NACK -> retry-scheduler cycle inside round 2.
//	T2 DLQ replay: dead-letter twice, re-enter via Replay from dead_lettered
//	   twice, final round settles via real Pull -> Start -> Ack.
//	T3 Unblock resume: Block -> Unblock -> settle twice (replay only bridges
//	   back to a non-terminal state), keeping every old outbox row, then
//	   reconcile the audit projection against the full lifecycle.
//
// Harness: setupServiceTestEnv (throwaway PG DB per test) plus per-test
// tenant/mailbox names so JetStream streams (per-tenant) never collide with
// sibling lanes or already-running brokers. One NATS driver per test.

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"regexp"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
	natsdriver "github.com/agentium-lab/Janus/server/internal/driver/nats"
	outboxpkg "github.com/agentium-lab/Janus/server/internal/outbox"
	retrysched "github.com/agentium-lab/Janus/server/internal/retry"
)

// ga05NATSURL returns a reachable NATS URL. Preference order: JANUS_NATS_URL,
// an already-running server on localhost:4222, then a privately spawned
// nats-server on an OS-reserved port (the driver_test.go pattern — nats-server
// 2.14.x treats -p 0 as "the default port", so the port is reserved via a
// throwaway listener instead). Skips the test when no server can be obtained.
func ga05NATSURL(t *testing.T) string {
	t.Helper()
	if u := os.Getenv("JANUS_NATS_URL"); u != "" {
		return u
	}
	if c, err := net.DialTimeout("tcp", "127.0.0.1:4222", 300*time.Millisecond); err == nil {
		_ = c.Close()
		return "nats://127.0.0.1:4222"
	}
	bin := os.ExpandEnv("$HOME/go/bin/nats-server")
	if _, err := exec.LookPath(bin); err != nil {
		if p, lerr := exec.LookPath("nats-server"); lerr == nil {
			bin = p
		} else {
			t.Skipf("no nats-server available and JANUS_NATS_URL unset: %v / %v", err, lerr)
		}
	}
	ln, lerr := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, lerr)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())

	cmd := exec.Command(bin, "-a", "127.0.0.1", "-p", fmt.Sprintf("%d", port), "-js", "--store_dir", t.TempDir())
	stderr, err := cmd.StderrPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() { _ = cmd.Process.Signal(os.Interrupt); _ = cmd.Wait() })

	addrCh := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(stderr)
		for sc.Scan() {
			if m := regexp.MustCompile(`Listening for client connections on (.+)`).FindStringSubmatch(sc.Text()); len(m) > 1 {
				addrCh <- m[1]
				return
			}
		}
	}()
	select {
	case addr := <-addrCh:
		return "nats://" + addr
	case <-time.After(5 * time.Second):
		t.Fatal("nats-server did not start")
		return ""
	}
}

// ga05Rig bundles the PG env with a real NATS driver wired into dispatch and
// taskSvc (the TestB4 full-chain pattern), scoped to one throwaway tenant.
type ga05Rig struct {
	env     *serviceTestEnv
	tenant  string
	agent   string
	mailbox string
	taskID  string
	drv     *natsdriver.Driver
	nc      *natsgo.Conn
	js      natsgo.JetStream
	dlqSub  *natsgo.Subscription
}

func ga05RigFor(t *testing.T, tenant, taskID string) *ga05Rig {
	t.Helper()
	natsURL := ga05NATSURL(t)
	env := setupServiceTestEnv(t)
	ctx := context.Background()

	agent := tenant + "-agent"
	mailbox := tenant + "-mb"
	require.NoError(t, env.tenantRepo.Create(ctx, tenant, "GA05 "+tenant))
	require.NoError(t, env.agentRepo.Register(ctx, core.Agent{
		ID: agent, TenantID: tenant, DisplayName: "GA05",
		Status: core.AgentStatusOnline, Protocol: core.ProtocolA2A,
	}))
	require.NoError(t, env.mailboxRepo.Create(ctx, core.Mailbox{
		ID: mailbox, TenantID: tenant, AgentID: agent, Status: "active",
		ACKWaitSeconds: 30, MaxDeliver: 5,
		RetryPolicy: core.RetryPolicy{MaxAttempts: 5, BackoffType: "exponential", InitialSeconds: 1, MaxSeconds: 60},
	}))

	nc, nerr := natsgo.Connect(natsURL)
	require.NoError(t, nerr)
	js, jerr := nc.JetStream()
	require.NoError(t, jerr)

	drv, derr := natsdriver.NewDriver(natsdriver.Config{URL: natsURL})
	require.NoError(t, derr)
	require.NoError(t, drv.EnsureTenant(ctx, tenant))
	require.NoError(t, drv.EnsureMailbox(ctx, core.MailboxSpec{
		TenantID: tenant, MailboxID: mailbox, AgentID: agent, ACKWaitSeconds: 30,
	}))
	require.NoError(t, drv.EnsureConsumer(ctx, core.ConsumerSpec{
		TenantID: tenant, MailboxID: mailbox, DurableName: mailbox, ACKWaitSeconds: 30, MaxDeliver: 5,
	}))

	g := &ga05Rig{
		env: env, tenant: tenant, agent: agent, mailbox: mailbox, taskID: taskID,
		drv: drv, nc: nc, js: js,
	}
	env.dispatch = env.dispatch.WithQueueDriver(&driverCloser{QueueDriver: drv})
	env.taskSvc.WithQueueDriver(drv)
	t.Cleanup(func() {
		if g.dlqSub != nil {
			_ = g.dlqSub.Unsubscribe()
		}
		_ = drv.Close()
		nc.Close()
	})
	return g
}

// drain publishes pending task_publish / dlq_publish outbox rows to the real
// broker (the harness runs no publisher goroutine — TestB4's drain pattern).
func (g *ga05Rig) drain(ctx context.Context) {
	entries, _ := g.env.outboxRepo.FetchPending(ctx, 50)
	for _, e := range entries {
		switch e.Kind {
		case "task_publish":
			var msg core.TaskMessage
			if jsonUnmarshal(e.Payload, &msg) {
				_ = g.drv.PublishTask(ctx, msg)
			}
		case "dlq_publish":
			var msg core.TaskMessage
			if jsonUnmarshal(e.Payload, &msg) {
				_ = g.drv.PublishDLQ(ctx, msg, []byte(msg.Headers["error"]))
			}
		}
	}
}

func jsonUnmarshal(b []byte, v interface{}) bool {
	return json.Unmarshal(b, v) == nil
}

// deliver drains the outbox and pulls through the real broker until the task
// is handed out with a fresh lease.
func (g *ga05Rig) deliver(t *testing.T, ctx context.Context, what string) (*PullResult, error) {
	t.Helper()
	var res *PullResult
	err := withDeadline(t, 10*time.Second, func() error {
		g.drain(ctx)
		r, perr := g.env.dispatch.PullTask(ctx, g.tenant, g.mailbox, g.agent)
		if perr != nil {
			return perr
		}
		if r == nil {
			return fmt.Errorf("%s: not delivered yet", what)
		}
		res = r
		return nil
	})
	return res, err
}

// fetchDLQ pulls one DLQ message through a per-test durable consumer (data is
// the task envelope; the task id rides the JANUS-Task-ID header).
func (g *ga05Rig) fetchDLQ(t *testing.T, ctx context.Context, what string) {
	t.Helper()
	require.NoError(t, withDeadline(t, 10*time.Second, func() error {
		g.drain(ctx)
		if g.dlqSub == nil {
			sub, serr := g.js.PullSubscribe(
				fmt.Sprintf("janus.%s.tasks_dlq.%s", g.tenant, g.mailbox),
				"ga05dlq-"+g.mailbox)
			if serr != nil {
				return serr
			}
			g.dlqSub = sub
		}
		msgs, ferr := g.dlqSub.Fetch(1, natsgo.MaxWait(2*time.Second))
		if ferr != nil || len(msgs) == 0 {
			return fmt.Errorf("%s: no DLQ message: %v", what, ferr)
		}
		require.NoError(t, msgs[0].Ack())
		require.Equal(t, g.taskID, msgs[0].Header.Get("JANUS-Task-ID"), "%s: DLQ message task id", what)
		return nil
	}), what)
}

// taskPublishKeys lists the dedupe keys of every historical task_publish row
// for the task, oldest first ("" = no dedupe key). Rows are never deleted.
func (g *ga05Rig) taskPublishKeys(t *testing.T, ctx context.Context) []string {
	t.Helper()
	rows, err := g.env.pool.Query(ctx,
		`SELECT COALESCE(dedupe_key,'') FROM outbox_events
		 WHERE tenant_id=$1 AND kind='task_publish' AND payload->>'TaskID'=$2
		 ORDER BY created_at ASC`, g.tenant, g.taskID)
	require.NoError(t, err)
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var k string
		require.NoError(t, rows.Scan(&k))
		keys = append(keys, k)
	}
	require.NoError(t, rows.Err())
	return keys
}

func (g *ga05Rig) taskStatus(t *testing.T, ctx context.Context) (core.TaskStatus, int) {
	t.Helper()
	got, err := g.env.taskRepo.Get(ctx, g.tenant, g.taskID)
	require.NoError(t, err)
	return got.Status, got.ReplayCount
}

func (g *ga05Rig) createTask(t *testing.T, ctx context.Context) {
	t.Helper()
	_, err := g.env.taskSvc.Create(ctx, core.Task{
		ID: g.taskID, TenantID: g.tenant, SourceAgent: g.agent,
		TargetType: core.TargetTypeMailbox, TargetValue: g.mailbox, MailboxID: g.mailbox,
		Status: core.TaskStatusQueued, Priority: core.PriorityNormal,
		Envelope: core.TaskEnvelope{JanusVersion: "1", TaskID: g.taskID, TenantID: g.tenant,
			SourceAgent: g.agent, Target: core.Target{Type: "mailbox", Value: g.mailbox},
			Payload: core.Payload{Type: "text", Content: "ga05"},
			Trace: core.TraceContext{TraceID: g.taskID + "-trace"}},
	})
	require.NoError(t, err, "create must succeed")
}

// T1 — normal replay entrance, three delivery generations on real PG+NATS,
// with a NACK -> retry-scheduler cycle inside round 2.
func TestGA05_T1_ReplayTwoRounds_FullDelivery_PG(t *testing.T) {
	g := ga05RigFor(t, "ga05t1", "ga05-t1-task")
	ctx := context.Background()
	g.createTask(t, ctx)

	// Round 1 (generation 0): create publish -> real Pull -> Start -> Ack.
	r1, err := g.deliver(t, ctx, "round 1")
	require.NoError(t, err, "round 1 must deliver through the broker")
	require.Equal(t, 1, r1.Attempt, "round 1 attempt number")
	require.NoError(t, g.env.dispatch.StartTask(ctx, g.tenant, g.taskID, r1.LeaseID))
	require.NoError(t, g.env.dispatch.AckTask(ctx, g.tenant, g.taskID, r1.LeaseID, "s3://ga05/r1", nil))
	st, rc := g.taskStatus(t, ctx)
	require.Equal(t, core.TaskStatusCompleted, st, "round 1 must settle completed")
	require.Equal(t, 0, rc)

	// Replay 1 -> generation 1.
	_, err = g.env.taskSvc.Replay(ctx, g.tenant, g.taskID)
	require.NoError(t, err, "replay 1 must succeed")
	st, rc = g.taskStatus(t, ctx)
	require.Equal(t, core.TaskStatusQueued, st, "replay 1 re-queues")
	require.Equal(t, 1, rc, "replay 1 bumps replay_count")

	// Round 2a: new generation delivers; NACK retriable -> retry_scheduled.
	r2a, err := g.deliver(t, ctx, "round 2 first attempt")
	require.NoError(t, err, "replayed task must redeliver (dedupe key must not swallow round 2)")
	require.Equal(t, 1, r2a.Attempt, "attempt numbering restarts after replay reset")
	require.NoError(t, g.env.dispatch.StartTask(ctx, g.tenant, g.taskID, r2a.LeaseID))
	require.NoError(t, g.env.dispatch.NackTask(ctx, g.tenant, g.taskID, r2a.LeaseID, true,
		&core.TaskError{Code: "TRANSIENT", Message: "ga05 round2 retry"}))
	st, _ = g.taskStatus(t, ctx)
	require.Equal(t, core.TaskStatusRetryScheduled, st, "retriable nack schedules a retry")

	// Drive the REAL retry scheduler loop (outbox mode) until it promotes
	// the task back to queued with a republish outbox row.
	sctx, scancel := context.WithCancel(ctx)
	sched := retrysched.NewScheduler(g.env.pool, nil).WithOutbox()
	go sched.Start(sctx, 50*time.Millisecond)
	t.Cleanup(scancel)
	require.NoError(t, withDeadline(t, 15*time.Second, func() error {
		s, _ := g.taskStatus(t, ctx)
		if s != core.TaskStatusQueued {
			return fmt.Errorf("retry scheduler has not promoted yet: %s", s)
		}
		return nil
	}), "retry scheduler must promote the task to queued")

	// Round 2b: scheduler republish delivers as a NEW attempt; settle.
	r2b, err := g.deliver(t, ctx, "round 2 retry attempt")
	require.NoError(t, err, "scheduler republish must deliver")
	require.Equal(t, r2a.Attempt+1, r2b.Attempt, "retry delivery must be a new attempt")
	require.NoError(t, g.env.dispatch.StartTask(ctx, g.tenant, g.taskID, r2b.LeaseID))
	require.NoError(t, g.env.dispatch.AckTask(ctx, g.tenant, g.taskID, r2b.LeaseID, "s3://ga05/r2", nil))
	st, rc = g.taskStatus(t, ctx)
	require.Equal(t, core.TaskStatusCompleted, st, "round 2 must settle completed (second completion across generations)")
	require.Equal(t, 1, rc)

	// One task_publish per round so far: create + replay:1:1 + retry replay:1:2.
	require.Equal(t, []string{
		"",
		core.BuildReplayDedupeKey(g.tenant, g.taskID, "replay", 1, 1),
		core.BuildReplayDedupeKey(g.tenant, g.taskID, "replay", 1, 2),
	}, g.taskPublishKeys(t, ctx), "round-2 publishes must not be deduped away")

	// Replay 2 -> generation 2 (round 3): the regression-class check — a
	// fixed replay dedupe key would swallow this publish entirely.
	_, err = g.env.taskSvc.Replay(ctx, g.tenant, g.taskID)
	require.NoError(t, err, "replay 2 must succeed")
	st, rc = g.taskStatus(t, ctx)
	require.Equal(t, core.TaskStatusQueued, st)
	require.Equal(t, 2, rc, "generation must advance on the second replay")

	r3, err := g.deliver(t, ctx, "round 3")
	require.NoError(t, err, "round 3 must deliver — generation-2 dedupe key must not collide with round 2's")
	require.Equal(t, 1, r3.Attempt, "attempt numbering restarts after replay 2 reset")
	require.NoError(t, g.env.dispatch.StartTask(ctx, g.tenant, g.taskID, r3.LeaseID))
	require.NoError(t, g.env.dispatch.AckTask(ctx, g.tenant, g.taskID, r3.LeaseID, "s3://ga05/r3", nil))
	st, _ = g.taskStatus(t, ctx)
	require.Equal(t, core.TaskStatusCompleted, st, "round 3 must settle completed")

	// Final outbox reconciliation: every historical task_publish row kept,
	// one per delivery round, all dedupe keys distinct.
	require.Equal(t, []string{
		"",
		core.BuildReplayDedupeKey(g.tenant, g.taskID, "replay", 1, 1),
		core.BuildReplayDedupeKey(g.tenant, g.taskID, "replay", 1, 2),
		core.BuildReplayDedupeKey(g.tenant, g.taskID, "replay", 2, 1),
	}, g.taskPublishKeys(t, ctx), "outbox must retain one task_publish per round with per-generation keys")
}

// T2 — DLQ replay entrance, two full rounds: dead-letter, replay from
// dead_lettered, dead-letter again, replay again, settle via real Ack.
func TestGA05_T2_DLQReplayTwoRounds_PG(t *testing.T) {
	g := ga05RigFor(t, "ga05t2", "ga05-t2-task")
	ctx := context.Background()
	g.createTask(t, ctx)

	// Round 1: deliver, then non-retriable NACK -> DLQ.
	r1, err := g.deliver(t, ctx, "round 1")
	require.NoError(t, err)
	require.NoError(t, g.env.dispatch.StartTask(ctx, g.tenant, g.taskID, r1.LeaseID))
	require.NoError(t, g.env.dispatch.NackTask(ctx, g.tenant, g.taskID, r1.LeaseID, false,
		&core.TaskError{Code: "PERMANENT", Message: "ga05 dlq 1"}))
	st, rc := g.taskStatus(t, ctx)
	require.Equal(t, core.TaskStatusDeadLettered, st, "round 1 must settle dead_lettered")
	require.Equal(t, 0, rc)
	g.fetchDLQ(t, ctx, "round 1")

	// DLQ replay 1: re-queue + redeliver.
	_, err = g.env.taskSvc.Replay(ctx, g.tenant, g.taskID)
	require.NoError(t, err, "dlq replay 1 must succeed")
	st, rc = g.taskStatus(t, ctx)
	require.Equal(t, core.TaskStatusQueued, st)
	require.Equal(t, 1, rc)

	// Round 2: deliver, dead-letter AGAIN.
	r2, err := g.deliver(t, ctx, "round 2")
	require.NoError(t, err, "dlq replay 1 must redeliver")
	require.Equal(t, 1, r2.Attempt)
	require.NoError(t, g.env.dispatch.StartTask(ctx, g.tenant, g.taskID, r2.LeaseID))
	require.NoError(t, g.env.dispatch.NackTask(ctx, g.tenant, g.taskID, r2.LeaseID, false,
		&core.TaskError{Code: "PERMANENT", Message: "ga05 dlq 2"}))
	st, rc = g.taskStatus(t, ctx)
	require.Equal(t, core.TaskStatusDeadLettered, st, "round 2 must settle dead_lettered again")
	require.Equal(t, 1, rc)
	g.fetchDLQ(t, ctx, "round 2")

	// DLQ replay 2: generation advances; round 3 settles via real Ack.
	_, err = g.env.taskSvc.Replay(ctx, g.tenant, g.taskID)
	require.NoError(t, err, "dlq replay 2 must succeed")
	st, rc = g.taskStatus(t, ctx)
	require.Equal(t, core.TaskStatusQueued, st)
	require.Equal(t, 2, rc, "generation must advance on the second dlq replay")

	r3, err := g.deliver(t, ctx, "round 3")
	require.NoError(t, err, "dlq replay 2 must redeliver — generation-2 key must not collide")
	require.NoError(t, g.env.dispatch.StartTask(ctx, g.tenant, g.taskID, r3.LeaseID))
	require.NoError(t, g.env.dispatch.AckTask(ctx, g.tenant, g.taskID, r3.LeaseID, "s3://ga05/t2r3", nil))
	st, _ = g.taskStatus(t, ctx)
	require.Equal(t, core.TaskStatusCompleted, st, "dlq replay round 2 must settle completed")

	require.Equal(t, []string{
		"",
		core.BuildReplayDedupeKey(g.tenant, g.taskID, "replay", 1, 1),
		core.BuildReplayDedupeKey(g.tenant, g.taskID, "replay", 2, 1),
	}, g.taskPublishKeys(t, ctx), "one task_publish per delivery round, keys per generation")
}

// T3 — Unblock resume, two rounds, keeping every old outbox row; then the
// audit projection must contain the complete ordered lifecycle with no
// swallowed events and no duplicate event_ids.
func TestGA05_T3_UnblockTwoRounds_AuditReconcile_PG(t *testing.T) {
	g := ga05RigFor(t, "ga05t3", "ga05-t3-task")
	ctx := context.Background()
	g.createTask(t, ctx)

	settleRound := func(round string) {
		r, err := g.deliver(t, ctx, round+" post-unblock delivery")
		require.NoError(t, err, "%s: unblocked task must redeliver", round)
		require.NoError(t, g.env.dispatch.StartTask(ctx, g.tenant, g.taskID, r.LeaseID))
		require.NoError(t, g.env.dispatch.AckTask(ctx, g.tenant, g.taskID, r.LeaseID, "s3://ga05/"+round, nil))
		st, _ := g.taskStatus(t, ctx)
		require.Equal(t, core.TaskStatusCompleted, st, "%s: must settle completed", round)
	}

	// Round 1: claim -> start -> Block -> Unblock -> settle.
	r1, err := g.deliver(t, ctx, "round 1 initial delivery")
	require.NoError(t, err)
	require.NoError(t, g.env.dispatch.StartTask(ctx, g.tenant, g.taskID, r1.LeaseID))
	require.NoError(t, g.env.taskSvc.Block(ctx, g.tenant, g.taskID, "ga05 pause 1"))
	st, _ := g.taskStatus(t, ctx)
	require.Equal(t, core.TaskStatusBlocked, st, "round 1 block")
	require.NoError(t, g.env.taskSvc.Unblock(ctx, g.tenant, g.taskID))
	st, _ = g.taskStatus(t, ctx)
	require.Equal(t, core.TaskStatusQueued, st, "round 1 unblock re-queues")
	settleRound("round1")

	// Round 2: same Block -> Unblock -> settle sequence.
	_, err = g.env.taskSvc.Replay(ctx, g.tenant, g.taskID)
	require.NoError(t, err, "bridge replay must succeed")
	st, rc := g.taskStatus(t, ctx)
	require.Equal(t, core.TaskStatusQueued, st)
	require.Equal(t, 1, rc)

	r2, err := g.deliver(t, ctx, "round 2 initial delivery")
	require.NoError(t, err)
	require.NoError(t, g.env.dispatch.StartTask(ctx, g.tenant, g.taskID, r2.LeaseID))
	require.NoError(t, g.env.taskSvc.Block(ctx, g.tenant, g.taskID, "ga05 pause 2"))
	st, _ = g.taskStatus(t, ctx)
	require.Equal(t, core.TaskStatusBlocked, st, "round 2 block")
	require.NoError(t, g.env.taskSvc.Unblock(ctx, g.tenant, g.taskID))
	st, _ = g.taskStatus(t, ctx)
	require.Equal(t, core.TaskStatusQueued, st, "round 2 unblock re-queues")
	settleRound("round2")

	// Keep-old-outbox: every historical task_publish row survives — one per
	// delivery round (create, unblock 1, bridge replay, unblock 2).
	require.Equal(t, []string{
		"",
		"",
		core.BuildReplayDedupeKey(g.tenant, g.taskID, "replay", 1, 1),
		"",
	}, g.taskPublishKeys(t, ctx), "outbox must retain every historical task_publish row (never deleted)")

	// Audit reconciliation via the outbox drain pattern: project every
	// event_publish row through the real AuditProjector.
	projector := outboxpkg.NewAuditProjector(g.env.outboxRepo, NewEventService(g.env.eventRepo))
	pres, perr := projector.Replay(ctx, g.tenant,
		time.Now().Add(-time.Hour), time.Now().Add(time.Hour), 1000)
	require.NoError(t, perr, "audit projection must run")
	require.Zero(t, pres.Failed, "audit projection failures")
	require.Equal(t, 17, pres.Projected, "every lifecycle event must project")

	var outboxEvents int
	require.NoError(t, g.env.pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox_events WHERE tenant_id=$1 AND kind='event_publish'`, g.tenant).Scan(&outboxEvents))
	require.Equal(t, 17, outboxEvents, "outbox must hold every event_publish row (none swallowed at source)")

	events, lerr := g.env.eventRepo.ListByTask(ctx, g.tenant, g.taskID, 100)
	require.NoError(t, lerr)
	require.Len(t, events, 17, "audit trail must contain the complete lifecycle")

	var got []core.EventType
	for _, e := range events {
		got = append(got, e.EventType)
	}
	want := []core.EventType{
		core.EventTaskCreated, core.EventTaskQueued, // create
		core.EventTaskClaimed, core.EventTaskStarted, core.EventTaskBlocked, core.EventTaskQueued, // round 1: pull/start/block/unblock
		core.EventTaskClaimed, core.EventTaskStarted, core.EventTaskCompleted, // round 1 settle
		core.EventTaskCreated, // bridge replay
		core.EventTaskClaimed, core.EventTaskStarted, core.EventTaskBlocked, core.EventTaskQueued, // round 2: pull/start/block/unblock
		core.EventTaskClaimed, core.EventTaskStarted, core.EventTaskCompleted, // round 2 settle
	}
	require.Equal(t, want, got, "audit trail must be the complete ordered lifecycle")

	// Unblock round-trips are visible in the queued events' payloads.
	for _, idx := range []int{5, 13} {
		require.Contains(t, string(events[idx].Payload), "unblocked", "unblock round must be distinguishable in the audit trail")
	}

	// No duplicate event_ids (a colliding identity would have swallowed rows).
	var dupIDs int
	require.NoError(t, g.env.pool.QueryRow(ctx,
		`SELECT count(*) FROM (
			SELECT event_id FROM audit_event_projection WHERE tenant_id=$1
			GROUP BY event_id HAVING count(*) > 1) d`, g.tenant).Scan(&dupIDs))
	require.Zero(t, dupIDs, "no duplicate event_ids in the audit projection")
}
