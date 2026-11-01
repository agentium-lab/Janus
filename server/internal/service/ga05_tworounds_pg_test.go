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
	"fmt"
	"net"
	"os"
	"os/exec"
	"regexp"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
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
	pub     *outboxpkg.Publisher
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

	// The production Publisher drains the outbox: dedupe key = row id (the
	// JetStream Nats-Msg-Id) and the row transitions to 'published'. Start it
	// on a background context so Stop() alone ends it — cancelling the context
	// would abort an in-flight batch and strand that row unpublished.
	g.pub = outboxpkg.NewPublisher(env.outboxRepo, drv)
	go g.pub.Start(context.Background(), 20*time.Millisecond)

	t.Cleanup(func() {
		g.pub.Stop()
		if g.dlqSub != nil {
			_ = g.dlqSub.Unsubscribe()
		}
		_ = drv.Close()
		nc.Close()
	})
	return g
}

// drain waits until the production Publisher has pushed every outbox row of
// this rig's tenant to the broker.
func (g *ga05Rig) drain(t *testing.T, ctx context.Context) {
	t.Helper()
	require.NoError(t, withDeadline(t, 15*time.Second, func() error {
		var pending int
		if err := g.env.pool.QueryRow(ctx,
			`SELECT count(*) FROM outbox_events
			  WHERE tenant_id=$1 AND status IN ('pending','retry','publishing')`,
			g.tenant).Scan(&pending); err != nil {
			return err
		}
		if pending > 0 {
			return fmt.Errorf("%d outbox rows not published yet", pending)
		}
		return nil
	}), "the real publisher must drain the outbox")
}

// requireAllPublished waits for every outbox row of the kind to reach the
// publisher's terminal state, and returns how many rows there are.
func (g *ga05Rig) requireAllPublished(t *testing.T, ctx context.Context, kind string) int {
	t.Helper()
	var total, published int
	require.NoError(t, withDeadline(t, 15*time.Second, func() error {
		if err := g.env.pool.QueryRow(ctx,
			`SELECT count(*),
			        count(*) FILTER (WHERE status='published' AND published_at IS NOT NULL)
			   FROM outbox_events WHERE tenant_id=$1 AND kind=$2`, g.tenant, kind).Scan(&total, &published); err != nil {
			return err
		}
		if total == 0 {
			return fmt.Errorf("no %s rows yet", kind)
		}
		if total != published {
			return fmt.Errorf("%d of %d %s rows still unpublished", total-published, total, kind)
		}
		return nil
	}), "every %s row must reach published with a published_at stamp", kind)
	return total
}

func (g *ga05Rig) latestOutboxID(t *testing.T, ctx context.Context, kind string) string {
	t.Helper()
	var id string
	require.NoError(t, g.env.pool.QueryRow(ctx,
		`SELECT id FROM outbox_events WHERE tenant_id=$1 AND kind=$2
		  ORDER BY created_at DESC, id DESC LIMIT 1`, g.tenant, kind).Scan(&id))
	return id
}

// streamByPrefix resolves a JetStream stream at runtime: the DLQ stream name
// embeds sanitize(mailbox), which the test must not reimplement.
func (g *ga05Rig) streamByPrefix(t *testing.T, ctx context.Context, prefix string) string {
	t.Helper()
	jsm, err := jetstream.New(g.nc)
	require.NoError(t, err)
	lister := jsm.StreamNames(ctx)
	var names []string
	for name := range lister.Name() {
		names = append(names, name)
	}
	require.NoError(t, lister.Err())
	for _, name := range names {
		if len(name) >= len(prefix) && name[:len(prefix)] == prefix {
			return name
		}
	}
	t.Fatalf("no JetStream stream with prefix %q (have %v)", prefix, names)
	return ""
}

// requireBrokerDedupeID proves the publisher stamped the outbox row id as the
// JetStream dedupe id on the broker's newest message for the subject.
func (g *ga05Rig) requireBrokerDedupeID(t *testing.T, ctx context.Context, streamPrefix, subject, wantID string) {
	t.Helper()
	stream := g.streamByPrefix(t, ctx, streamPrefix)
	require.NoError(t, withDeadline(t, 15*time.Second, func() error {
		jsm, err := jetstream.New(g.nc)
		if err != nil {
			return err
		}
		st, err := jsm.Stream(ctx, stream)
		if err != nil {
			return err
		}
		msg, err := st.GetLastMsgForSubject(ctx, subject)
		if err != nil {
			return err
		}
		if got := msg.Header.Get("Nats-Msg-Id"); got != wantID {
			return fmt.Errorf("broker dedupe id = %q, want outbox row %q", got, wantID)
		}
		return nil
	}), "the publisher must stamp the outbox row id as the JetStream dedupe id")
}

type ga05LedgerRow struct {
	scopeType string
	scopeID   string
	attempt   int
	total     int64
	cost      float64
}

func (g *ga05Rig) ledgerRows(t *testing.T, ctx context.Context) []ga05LedgerRow {
	t.Helper()
	rows, err := g.env.pool.Query(ctx,
		`SELECT scope_type, scope_id, attempt, total_tokens, cost_usd
		   FROM budget_usage_ledger WHERE tenant_id=$1 AND task_id=$2
		  ORDER BY attempt, scope_type`, g.tenant, g.taskID)
	require.NoError(t, err)
	defer rows.Close()
	var out []ga05LedgerRow
	for rows.Next() {
		var row ga05LedgerRow
		require.NoError(t, rows.Scan(&row.scopeType, &row.scopeID, &row.attempt, &row.total, &row.cost))
		out = append(out, row)
	}
	require.NoError(t, rows.Err())
	return out
}

// deliver drains the outbox and pulls through the real broker until the task
// is handed out with a fresh lease.
func (g *ga05Rig) deliver(t *testing.T, ctx context.Context, what string) (*PullResult, error) {
	t.Helper()
	var res *PullResult
	err := withDeadline(t, 10*time.Second, func() error {
		g.drain(t, ctx)
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
		g.drain(t, ctx)
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

	// The TASKS stream is a work queue (WorkQueuePolicy): a message is gone
	// once the pull consumer acks it, so the dedupe id is asserted on the
	// unconsumed publish, before round 1 pulls it.
	g.drain(t, ctx)
	g.requireBrokerDedupeID(t, ctx, "JANUS_"+g.tenant+"_TASKS",
		"janus."+g.tenant+".tasks."+g.mailbox, g.latestOutboxID(t, ctx, "task_publish"))

	// Round 1 (generation 0): create publish -> real Pull -> Start -> Ack.
	r1, err := g.deliver(t, ctx, "round 1")
	require.NoError(t, err, "round 1 must deliver through the broker")
	require.Equal(t, 1, r1.Attempt, "round 1 attempt number")
	require.NoError(t, g.env.dispatch.StartTask(ctx, g.tenant, g.taskID, r1.LeaseID))
	require.NoError(t, g.env.dispatch.AckTask(ctx, g.tenant, g.taskID, r1.LeaseID, "s3://ga05/r1", nil))
	st, rc := g.taskStatus(t, ctx)
	require.Equal(t, core.TaskStatusCompleted, st, "round 1 must settle completed")
	require.Equal(t, 0, rc)

	// Round 1 crossed the production publisher: its row reached 'published'.
	require.Equal(t, 1, g.requireAllPublished(t, ctx, "task_publish"), "round 1 publishes once")

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

	// Retry inside the LAST generation: the scheduler must promote it and the
	// redelivery must be a new attempt of the same generation.
	require.NoError(t, g.env.dispatch.NackTask(ctx, g.tenant, g.taskID, r3.LeaseID, true,
		&core.TaskError{Code: "TRANSIENT", Message: "ga05 round3 retry"}))
	st, _ = g.taskStatus(t, ctx)
	require.Equal(t, core.TaskStatusRetryScheduled, st, "round 3 retriable nack schedules a retry")
	require.NoError(t, withDeadline(t, 15*time.Second, func() error {
		s, _ := g.taskStatus(t, ctx)
		if s != core.TaskStatusQueued {
			return fmt.Errorf("retry scheduler has not promoted round 3 yet: %s", s)
		}
		return nil
	}), "retry scheduler must promote round 3")

	r3b, err := g.deliver(t, ctx, "round 3 retry attempt")
	require.NoError(t, err, "round 3 retry must redeliver")
	require.Equal(t, r3.Attempt+1, r3b.Attempt, "retry redelivery is a new attempt in the same generation")
	require.NoError(t, g.env.dispatch.StartTask(ctx, g.tenant, g.taskID, r3b.LeaseID))
	usage := &core.TokenUsage{PromptTokens: 120, CompletionTokens: 30, TotalTokens: 150}
	require.NoError(t, g.env.dispatch.AckTask(ctx, g.tenant, g.taskID, r3b.LeaseID, "s3://ga05/r3", usage))
	st, _ = g.taskStatus(t, ctx)
	require.Equal(t, core.TaskStatusCompleted, st, "round 3 must settle completed")

	// Settlement ledger reconciliation: each Replay cleared the prior
	// generation's rows, so exactly the settling attempt's two scopes remain,
	// carrying the acked usage and its derived cost.
	ledger := g.ledgerRows(t, ctx)
	require.Len(t, ledger, 2, "one ledger row per settlement scope for the settling attempt")
	scopes := map[string]string{}
	for _, row := range ledger {
		require.Equal(t, r3b.Attempt, row.attempt, "the settling attempt's rows only")
		require.Equal(t, int64(150), row.total, "acked usage must land in the ledger")
		require.InDelta(t, EstimateCostUSD(150), row.cost, 1e-9, "ledger cost derives from the acked tokens")
		scopes[row.scopeType] = row.scopeID
	}
	require.Equal(t, g.tenant, scopes["tenant"], "tenant scope settled")
	require.Equal(t, g.agent, scopes["agent"], "agent scope settled")

	// Final outbox reconciliation: every historical task_publish row kept,
	// one per delivery round, all published, all dedupe keys distinct.
	require.Equal(t, []string{
		"",
		core.BuildReplayDedupeKey(g.tenant, g.taskID, "replay", 1, 1),
		core.BuildReplayDedupeKey(g.tenant, g.taskID, "replay", 1, 2),
		core.BuildReplayDedupeKey(g.tenant, g.taskID, "replay", 2, 1),
		core.BuildReplayDedupeKey(g.tenant, g.taskID, "replay", 2, 2),
	}, g.taskPublishKeys(t, ctx), "outbox must retain one task_publish per round with per-generation keys")
	require.Equal(t, 5, g.requireAllPublished(t, ctx, "task_publish"), "every round's publish reached published")
	g.requireAllPublished(t, ctx, "event_publish")
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

	// Both DLQ legs crossed the production publisher: each dlq_publish row is
	// published and the broker's newest DLQ message carries the newest row id
	// as its JetStream dedupe id.
	require.Equal(t, 2, g.requireAllPublished(t, ctx, "dlq_publish"), "one dlq_publish per dead-letter round")
	g.requireBrokerDedupeID(t, ctx, "JANUS_"+g.tenant+"_DLQ",
		"janus."+g.tenant+".tasks_dlq."+g.mailbox, g.latestOutboxID(t, ctx, "dlq_publish"))
	require.Equal(t, 3, g.requireAllPublished(t, ctx, "task_publish"), "every round's publish reached published")
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
	require.Equal(t, 17, g.requireAllPublished(t, ctx, "event_publish"), "the publisher pushed every event row")

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
