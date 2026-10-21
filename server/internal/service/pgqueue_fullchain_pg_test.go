package service

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
	natsdriver "github.com/agentium-lab/Janus/server/internal/driver/nats"
	"github.com/agentium-lab/Janus/server/internal/driver/pgqueue"
	"github.com/agentium-lab/Janus/server/internal/outbox"
)

// B4 full-chain acceptance on REAL PostgreSQL + REAL NATS with pgqueue as the
// consumer-side driver: task create -> outbox row -> outbox Publisher ->
// NATS (JetStream round-trip asserted) -> pgqueue consumer fetch (claim) ->
// Start -> Ack -> task completed. Every hop is a real component; every state
// change is read back from SQL.
func TestB4_PGQueue_FullChain_OutboxToNATS_ToConsumerAck(t *testing.T) {
	natsURL := startPGQueueChainNATS(t)

	env := setupServiceTestEnv(t)
	ctx := context.Background()

	// Broker side: the outbox Publisher pushes to a real NATS server.
	natsDrv, derr := natsdriver.NewDriver(natsdriver.Config{URL: natsURL})
	require.NoError(t, derr)
	defer natsDrv.Close()
	require.NoError(t, natsDrv.EnsureTenant(ctx, "acme"))
	require.NoError(t, natsDrv.EnsureMailbox(ctx, core.MailboxSpec{
		TenantID: "acme", MailboxID: "mb-1", AgentID: "agent-1", ACKWaitSeconds: 30,
	}))
	require.NoError(t, natsDrv.EnsureConsumer(ctx, core.ConsumerSpec{
		TenantID: "acme", MailboxID: "mb-1", DurableName: "mb-1", ACKWaitSeconds: 30, MaxDeliver: 5,
	}))

	// Worker side: the consumer fetch claims the tasks table via pgqueue
	// (FOR UPDATE SKIP LOCKED), so taskSvc's direct queue publishes become
	// pgqueue no-ops and NATS traffic flows only through the Publisher.
	pgq := pgqueue.NewDriver(env.pool)
	dispatch := env.dispatch.WithQueueDriver(pgq)
	env.taskSvc = env.taskSvc.WithQueueDriver(pgq)

	env.outboxRepo.SetWorker("b4-pgq-chain", 5*time.Second)
	pubCtx, cancelPub := context.WithCancel(ctx)
	defer cancelPub()
	publisher := outbox.NewPublisher(env.outboxRepo, natsDrv)
	go publisher.Start(pubCtx, 20*time.Millisecond)
	defer publisher.Stop()

	// 1. Task create: task row + task_publish/event_publish outbox rows.
	const taskID = "b4-pgq-task"
	task := core.Task{
		ID: taskID, TenantID: "acme", SourceAgent: "agent-1",
		TargetType: core.TargetTypeMailbox, TargetValue: "mb-1", MailboxID: "mb-1",
		Status: core.TaskStatusQueued, Priority: core.PriorityNormal,
		Envelope: core.TaskEnvelope{
			JanusVersion: "0.3",
			TaskID: taskID, TenantID: "acme", SourceAgent: "agent-1",
			Target: core.Target{Type: "mailbox", Value: "mb-1"},
			Payload: core.Payload{Type: "text", Content: "b4-pgq"},
			Trace:   core.TraceContext{TraceID: "b4-pgq-trace"},
		},
	}
	created, cerr := env.taskSvc.Create(ctx, task)
	require.NoError(t, cerr)
	require.NotNil(t, created)

	var status string
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT status FROM tasks WHERE tenant_id=$1 AND id=$2`, "acme", taskID).Scan(&status))
	require.Equal(t, string(core.TaskStatusQueued), status, "task row must be queued after create")
	var taskPubRows int
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox_events WHERE tenant_id=$1 AND kind='task_publish'`, "acme").Scan(&taskPubRows))
	require.GreaterOrEqual(t, taskPubRows, 1, "create must write a task_publish outbox row")

	// 2. Outbox Publisher -> NATS: a real JetStream round-trip on the task
	// subject, bounded by the package's poll helper.
	nc, nerr := natsgo.Connect(natsURL)
	require.NoError(t, nerr)
	defer nc.Close()
	js, jerr := nc.JetStream()
	require.NoError(t, jerr)

	var natsTaskID string
	require.NoError(t, withDeadline(t, 15*time.Second, func() error {
		// Bind the durable the nats driver reconciled: the TASKS stream is a
		// WorkQueue-policy stream, so a second filtered consumer is rejected
		// ("filtered consumer not unique") — the delivery is drained through
		// the consumer a real worker would use.
		sub, serr := js.PullSubscribe(fmt.Sprintf("janus.%s.tasks.%s", "acme", "mb-1"), "consumer_acme_mb-1",
			natsgo.Bind("JANUS_acme_TASKS", "consumer_acme_mb-1"))
		if serr != nil {
			return serr
		}
		msgs, ferr := sub.Fetch(1, natsgo.MaxWait(1500*time.Millisecond))
		if ferr != nil || len(msgs) == 0 {
			return fmt.Errorf("task_publish not on NATS yet: %v", ferr)
		}
		msgs[0].Ack()
		natsTaskID = msgs[0].Header.Get("JANUS-Task-ID")
		return nil
	}), "the outbox Publisher must deliver the task message to real NATS")
	require.Equal(t, taskID, natsTaskID, "the NATS message must be the created task")

	require.NoError(t, withDeadline(t, 15*time.Second, func() error {
		var published int
		if perr := env.pool.QueryRow(ctx,
			`SELECT count(*) FROM outbox_events WHERE tenant_id=$1 AND kind='task_publish' AND status='published'`, "acme").Scan(&published); perr != nil {
			return perr
		}
		if published == 0 {
			return fmt.Errorf("task_publish row not yet marked published")
		}
		return nil
	}), "the outbox row must reach status=published after the NATS publish")

	// 3. pgqueue consumer fetch through the dispatch service: claim + attempt.
	var res *PullResult
	require.NoError(t, withDeadline(t, 15*time.Second, func() error {
		r, perr := dispatch.PullTask(ctx, "acme", "mb-1", "agent-1")
		if perr != nil {
			return perr
		}
		if r == nil {
			return fmt.Errorf("pgqueue fetch has not claimed the task yet")
		}
		res = r
		return nil
	}), "pgqueue consumer fetch must claim the queued task")
	require.Equal(t, taskID, res.Task.ID)
	require.NotEmpty(t, res.LeaseID)

	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT status FROM tasks WHERE tenant_id=$1 AND id=$2`, "acme", taskID).Scan(&status))
	require.Equal(t, string(core.TaskStatusClaimed), status, "task must be claimed after the pgqueue fetch")

	var deliveryRef string
	var leaseUntil *time.Time
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT a.delivery_ref, t.queue_lease_until FROM tasks t JOIN task_attempts a ON a.tenant_id=t.tenant_id AND a.task_id=t.id
		 WHERE t.tenant_id=$1 AND t.id=$2`, "acme", taskID).Scan(&deliveryRef, &leaseUntil))
	require.Contains(t, deliveryRef, taskID+":", "attempt carries the pgqueue-shaped delivery ref (taskID:attempt)")
	require.NotNil(t, leaseUntil, "pgqueue FetchTasks must set the queue lease on claim")

	// 4. Start -> running.
	require.NoError(t, dispatch.StartTask(ctx, "acme", taskID, res.LeaseID))
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT status FROM tasks WHERE tenant_id=$1 AND id=$2`, "acme", taskID).Scan(&status))
	require.Equal(t, string(core.TaskStatusRunning), status, "task must be running after Start")

	// 5. Ack -> completed, queue lease cleared (pgqueue AckTask), result ref set.
	require.NoError(t, dispatch.AckTask(ctx, "acme", taskID, res.LeaseID, "s3://b4-pgq", nil))
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT status FROM tasks WHERE tenant_id=$1 AND id=$2`, "acme", taskID).Scan(&status))
	require.Equal(t, string(core.TaskStatusCompleted), status, "task must be completed after Ack")

	var resultRef string
	var leaseAfter *time.Time
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT result_ref, queue_lease_until FROM tasks WHERE tenant_id=$1 AND id=$2`, "acme", taskID).Scan(&resultRef, &leaseAfter))
	require.Equal(t, "s3://b4-pgq", resultRef)
	require.Nil(t, leaseAfter, "the pgqueue AckTask must clear the queue lease once the task completes")

	// Terminal event flowed outbox -> Publisher -> NATS as well: bounded-wait
	// until every outbox row for the tenant is published.
	require.NoError(t, withDeadline(t, 15*time.Second, func() error {
		var unpublished int
		if uerr := env.pool.QueryRow(ctx,
			`SELECT count(*) FROM outbox_events WHERE tenant_id=$1 AND status != 'published'`, "acme").Scan(&unpublished); uerr != nil {
			return uerr
		}
		if unpublished != 0 {
			return fmt.Errorf("%d outbox rows not yet published", unpublished)
		}
		return nil
	}), "all lifecycle outbox rows (incl. task.completed) must be published to NATS")
}

// startPGQueueChainNATS spawns this test's OWN nats-server (JANUS_NATS_URL
// overrides; skips when no binary exists), on a fixed high port so it never
// collides with the 4222/4299 instances other suites spawn.
func startPGQueueChainNATS(t *testing.T) string {
	t.Helper()
	if u := os.Getenv("JANUS_NATS_URL"); u != "" {
		return u
	}
	bin := ""
	for _, cand := range []string{
		"nats-server",
		os.ExpandEnv("$HOME/.local/bin/nats-server"),
		os.ExpandEnv("$HOME/go/bin/nats-server"),
	} {
		if p, lerr := exec.LookPath(cand); lerr == nil {
			bin = p
			break
		}
	}
	if bin == "" {
		t.Skip("no nats-server binary (PATH, ~/.local/bin, ~/go/bin) and JANUS_NATS_URL unset; skipping B4 pgqueue full-chain test")
	}
	cmd := exec.Command(bin, "-p", "4287", "-js", "--store_dir", t.TempDir())
	stderr, perr := cmd.StderrPipe()
	require.NoError(t, perr)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() { cmd.Process.Signal(os.Interrupt); cmd.Wait() })
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
