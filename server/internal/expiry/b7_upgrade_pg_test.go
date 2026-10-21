package expiry

// B7 upgrade-coexistence pins against live PG (and, for the drain test, a
// real nats-server):
//
// The pre-4e9b272 expiry writer emitted outbox rows id'd
// `exp-<len(tenant)>-<tenant>-<task>` with payload event_id
// `obx-exp-<len(tenant)>-<tenant>-<task>` and guarded emission with NOT
// EXISTS over past events. The current writer claims via
// UPDATE...RETURNING (status predicate) and keys rows per generation:
// `exp-<len(tenant)>-<tenant>-<task>-g<replay_count>`. On upgrade day BOTH
// cohorts sit in outbox_events. These tests pin: the cohorts coexist
// without id collision, nothing is wrongfully suppressed (a replayed legacy
// task emits its -g1 round even though its old-format row exists), and the
// real outbox publisher drains BOTH cohorts to a real JetStream stream
// with distinct Nats-Msg-Ids (no dedupe swallow), replayable end to end.

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

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
	natsdrv "github.com/agentium-lab/Janus/server/internal/driver/nats"
	"github.com/agentium-lab/Janus/server/internal/driver/postgres"
	"github.com/agentium-lab/Janus/server/internal/outbox"
)

// seedLegacyExpiryOutboxRow reproduces what the pre-4e9b272 writer left
// behind for an already-expired task: an old-format outbox row (no
// generation suffix) plus the 'expired' status flip, in one step.
func seedLegacyExpiryOutboxRow(t *testing.T, pool *pgxpool.Pool, taskID string) {
	t.Helper()
	ctx := context.Background()
	rowID := fmt.Sprintf("exp-4-acme-%s", taskID)
	eventID := fmt.Sprintf("obx-exp-4-acme-%s", taskID)
	payload := fmt.Sprintf(`{"event_type":"task.expired","tenant_id":"acme","task_id":%q,"event_id":%q,"timestamp":"%s"}`,
		taskID, eventID, time.Now().UTC().Format(`2006-01-02T15:04:05Z`))
	_, err := pool.Exec(ctx,
		`INSERT INTO outbox_events (id, tenant_id, kind, payload) VALUES ($1, 'acme', 'event_publish', $2::jsonb)`,
		rowID, payload)
	require.NoError(t, err, "seed legacy outbox row %s", rowID)
	_, err = pool.Exec(ctx, `UPDATE tasks SET status='expired', updated_at=now() WHERE tenant_id='acme' AND id=$1`, taskID)
	require.NoError(t, err, "legacy writer also flipped the task to expired")
}

// allExpiryOutboxRows reads back every task.expired event_publish row.
func allExpiryOutboxRows(t *testing.T, pool *pgxpool.Pool) map[string]string {
	t.Helper()
	ctx := context.Background()
	rows, err := pool.Query(ctx, `
		SELECT id, payload->>'event_id' FROM outbox_events
		 WHERE kind = 'event_publish' AND payload->>'event_type' = 'task.expired'`)
	require.NoError(t, err)
	defer rows.Close()
	got := map[string]string{}
	for rows.Next() {
		var id, eventID string
		require.NoError(t, rows.Scan(&id, &eventID))
		got[id] = eventID
	}
	require.NoError(t, rows.Err())
	return got
}

// waitForOutboxStatus polls the observable (row status in PG) until every
// listed id reaches want, with a bounded deadline — no fixed sleeps.
func waitForOutboxStatus(t *testing.T, pool *pgxpool.Pool, want string, ids ...string) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		rows, err := pool.Query(ctx, `SELECT id, status FROM outbox_events WHERE id = ANY($1)`, ids)
		require.NoError(t, err)
		statuses := map[string]string{}
		for rows.Next() {
			var id, st string
			require.NoError(t, rows.Scan(&id, &st))
			statuses[id] = st
		}
		require.NoError(t, rows.Err())
		rows.Close()
		if len(statuses) == len(ids) {
			ok := true
			for _, st := range statuses {
				if st != want {
					ok = false
					break
				}
			}
			if ok {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("outbox rows %v did not all reach status %q within deadline", ids, want)
}

func TestB7_MixedFormatCoexistence_NoSuppression_NoCollision(t *testing.T) {
	pool := openExpiryTestDB(t)
	repo := postgres.NewTaskRepository(pool)
	ctx := context.Background()

	// Historical cohort: the old writer already expired task-legacy and
	// wrote its old-format row (no -g suffix).
	seedTask(t, pool, "task-legacy", "pending", -3600, 0, 0)
	seedLegacyExpiryOutboxRow(t, pool, "task-legacy")

	// Current cohort: the new writer expires task-fresh with a -g0 row.
	seedTask(t, pool, "task-fresh", "pending", -3600, 0, 0)
	n, err := repo.ExpireTasks(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, n, "only the non-terminal overdue task is claimed")

	// Coexistence read-back: both cohorts present, ids and event_ids all
	// distinct — the -g suffix makes a collision structurally impossible.
	rows := allExpiryOutboxRows(t, pool)
	assert.Equal(t, map[string]string{
		"exp-4-acme-task-legacy":      "obx-exp-4-acme-task-legacy",
		"exp-4-acme-task-fresh-g0":    "obx-exp-4-acme-task-fresh-g0",
	}, rows, "old-format and new-format rows must coexist verbatim")

	// Both cohorts are claimable by the real publisher fetch.
	entries, err := postgres.NewOutboxRepo(pool).FetchPending(ctx, 10)
	require.NoError(t, err)
	got := map[string]bool{}
	for _, e := range entries {
		got[e.ID] = true
	}
	assert.True(t, got["exp-4-acme-task-legacy"], "legacy row must be drainable")
	assert.True(t, got["exp-4-acme-task-fresh-g0"], "new-format row must be drainable")

	// No wrongful suppression: replay the LEGACY task and let its new round
	// go overdue — the old NOT EXISTS guard would have suppressed the new
	// round's event forever; the claim-based writer must emit it.
	replayTask(t, pool, repo, "task-legacy")
	assert.Equal(t, "created", taskStatus(t, pool, "task-legacy"))
	_, err = pool.Exec(ctx, `UPDATE tasks SET deadline = now() - interval '1 hour' WHERE tenant_id='acme' AND id='task-legacy'`)
	require.NoError(t, err)

	n, err = repo.ExpireTasks(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, n, "the replayed legacy task must expire again")
	assert.Equal(t, "expired", taskStatus(t, pool, "task-legacy"))

	rows = allExpiryOutboxRows(t, pool)
	require.Len(t, rows, 3, "old row + g0 row + the new round's g1 row")
	assert.Equal(t, "obx-exp-4-acme-task-legacy-g1", rows["exp-4-acme-task-legacy-g1"],
		"the replayed round emits its own generation-keyed event despite the existing old-format row")
	ids := expiryEventIDs(t, pool, "task-legacy")
	assert.ElementsMatch(t, []string{"exp-4-acme-task-legacy", "exp-4-acme-task-legacy-g1"}, ids)
}

var expiryListenAddrRe = regexp.MustCompile(`Listening for client connections on (.+)`)

// startExpiryNATS starts a private nats-server (or reuses JANUS_NATS_URL),
// mirroring the nats package's startNATSServer harness; skips when neither
// is available. The port is reserved explicitly: this server build treats
// `-p 0` as "default port 4222", which collides with the nats package's
// harness when go test runs the two package binaries in parallel.
func startExpiryNATS(t *testing.T) string {
	t.Helper()
	if url := os.Getenv("JANUS_NATS_URL"); url != "" {
		return url
	}
	var bin string
	for _, cand := range []string{os.ExpandEnv("$HOME/go/bin/nats-server"), "/home/silv/.local/bin/nats-server"} {
		if _, err := exec.LookPath(cand); err == nil {
			bin = cand
			break
		}
	}
	if bin == "" {
		t.Skipf("no nats-server binary and JANUS_NATS_URL unset; skipping end-to-end drain test")
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())

	cmd := exec.Command(bin, "-a", "127.0.0.1", "-p", fmt.Sprintf("%d", port), "-js", "--store_dir", t.TempDir())
	stderr, err := cmd.StderrPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		cmd.Process.Signal(os.Interrupt)
		cmd.Wait()
	})

	addrCh := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			if m := expiryListenAddrRe.FindStringSubmatch(scanner.Text()); len(m) > 1 {
				addrCh <- m[1]
				return
			}
		}
	}()
	select {
	case addr := <-addrCh:
		return "nats://" + addr
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for nats-server to start")
		return ""
	}
}

func TestB7_MixedFormatOutboxDrain_BothCohortsEmitEndToEnd(t *testing.T) {
	pool := openExpiryTestDB(t)
	repo := postgres.NewTaskRepository(pool)
	ctx := context.Background()

	url := startExpiryNATS(t)
	drv, err := natsdrv.NewDriver(natsdrv.Config{URL: url})
	require.NoError(t, err)
	t.Cleanup(func() { drv.Close() })
	require.NoError(t, drv.EnsureTenant(ctx, "acme"))

	// Mixed table: the legacy cohort plus a fresh current-code expiry.
	seedTask(t, pool, "task-legacy", "pending", -3600, 0, 0)
	seedLegacyExpiryOutboxRow(t, pool, "task-legacy")
	seedTask(t, pool, "task-fresh", "pending", -3600, 0, 0)
	_, err = repo.ExpireTasks(ctx)
	require.NoError(t, err)

	// The real publisher drains both cohorts to the real JetStream stream.
	pub := outbox.NewPublisher(postgres.NewOutboxRepo(pool), drv)
	pubCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pub.Start(pubCtx, 20*time.Millisecond)
	t.Cleanup(pub.Stop)

	waitForOutboxStatus(t, pool, "published", "exp-4-acme-task-legacy", "exp-4-acme-task-fresh-g0")

	ids := replayedEventIDs(t, drv, "acme")
	assert.ElementsMatch(t, []string{"obx-exp-4-acme-task-legacy", "obx-exp-4-acme-task-fresh-g0"}, ids,
		"both cohorts must land on the stream with distinct Nats-Msg-Ids — no dedupe swallow")

	// The replay round over the mixed set emits and drains too.
	replayTask(t, pool, repo, "task-legacy")
	_, err = pool.Exec(ctx, `UPDATE tasks SET deadline = now() - interval '1 hour' WHERE tenant_id='acme' AND id='task-legacy'`)
	require.NoError(t, err)
	_, err = repo.ExpireTasks(ctx)
	require.NoError(t, err)
	waitForOutboxStatus(t, pool, "published", "exp-4-acme-task-legacy-g1")

	ids = replayedEventIDs(t, drv, "acme")
	assert.ElementsMatch(t, []string{
		"obx-exp-4-acme-task-legacy", "obx-exp-4-acme-task-fresh-g0", "obx-exp-4-acme-task-legacy-g1",
	}, ids, "expiry replay over the mixed set keeps every generation distinct and deliverable")
}

func replayedEventIDs(t *testing.T, drv core.QueueEventDriver, tenantID string) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	iter, err := drv.ReplayEvents(ctx, core.EventReplayFilter{TenantID: tenantID})
	require.NoError(t, err)
	defer iter.Close()
	var ids []string
	for {
		ev, nerr := iter.Next(ctx)
		require.NoError(t, nerr)
		if ev == nil {
			break
		}
		ids = append(ids, ev.EventID)
	}
	return ids
}
