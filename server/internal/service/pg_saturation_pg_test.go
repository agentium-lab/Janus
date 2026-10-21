package service

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/driver/pgqueue"
	"github.com/agentium-lab/Janus/server/internal/driver/postgres"
	"github.com/agentium-lab/Janus/server/internal/outbox"
)

// B3 saturation acceptance (real PG, MaxConns=1): many CONCURRENT operations
// of every mailbox-adjacent kind — dual pulls across several mailboxes, task
// creates, direct outbox inserts, the outbox Publisher loop, the audit
// projector (tick-lock path), and MailboxService.UpdateConfig (advisory-lock
// path) — must all complete with zero errors inside a bounded context
// deadline. The narrow MaxConns=1 races prove single paths; this test proves
// the pool does not deadlock or self-starve when all of them run at once.
// A failure here means some path holds the one pooled connection while
// requesting a second one (the B3 class of bug), not a timing flake.
func TestSaturation_MaxConns1_MixedConcurrentOps_NoSelfStarvation(t *testing.T) {
	pool := openServiceTestDB(t)
	seedCtx := context.Background()

	// A second pool over the same test DB with exactly ONE connection.
	var dbName string
	require.NoError(t, pool.QueryRow(seedCtx, `SELECT current_database()`).Scan(&dbName))
	host := os.Getenv("JANUS_PG_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("JANUS_PG_PORT")
	if port == "" {
		port = "5432"
	}
	user := os.Getenv("JANUS_PG_USER")
	if user == "" {
		user = "janus"
	}
	password := os.Getenv("JANUS_PG_PASSWORD")
	cfg, err := pgxpool.ParseConfig(fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbName))
	require.NoError(t, err)
	cfg.MaxConns = 1
	single, err := pgxpool.NewWithConfig(seedCtx, cfg)
	require.NoError(t, err)
	t.Cleanup(single.Close)

	// Everything below runs on the size-1 pool.
	taskRepo := postgres.NewTaskRepository(single)
	attemptRepo := postgres.NewTaskAttemptRepository(single)
	mailboxRepo := postgres.NewMailboxRepository(single)
	outboxRepo := postgres.NewOutboxRepo(single)
	outboxRepo.SetWorker("saturation-test", 5*time.Second)
	budgetRepo := postgres.NewBudgetRepository(single)
	policyRepo := postgres.NewPolicyRuleRepository(single)
	budgetUsage := postgres.NewBudgetUsageRepo(single)

	budgetSvc := NewBudgetService(budgetRepo)
	policySvc := NewPolicyService(policyRepo)
	pgq := pgqueue.NewDriver(single)
	lifecycle := NewPGLifecycle(single)

	dispatch := NewDispatchService(taskRepo, attemptRepo, mailboxRepo, pgq, policySvc, budgetSvc).
		WithTxPath(lifecycle, outboxRepo, PGBudgetLedger(budgetUsage))
	taskSvc := NewTaskService(taskRepo, pgq, single, outboxRepo).
		WithPolicy(policySvc).
		WithLifecycle(lifecycle).
		WithAttemptRepo(attemptRepo)
	mailboxSvc := NewMailboxService(mailboxRepo, pgq)

	// Fixture: one tenant, 4 agents, 4 mailboxes (seeded via the normal pool
	// so the size-1 pool starts idle), plus 3 queued tasks per mailbox.
	const tenant = "sat"
	const numMailboxes = 4
	satAgent := func(i int) string { return fmt.Sprintf("sat-ag-%d", i) }
	satMB := func(i int) string { return fmt.Sprintf("sat-mb-%d", i) }

	_, err = pool.Exec(seedCtx, `INSERT INTO tenants (id, name) VALUES ($1, 'Saturation') ON CONFLICT DO NOTHING`, tenant)
	require.NoError(t, err)
	for i := 0; i < numMailboxes; i++ {
		_, err = pool.Exec(seedCtx,
			`INSERT INTO agents (tenant_id, id, display_name, protocol, status) VALUES ($1,$2,$3,'a2a','online') ON CONFLICT DO NOTHING`,
			tenant, satAgent(i), fmt.Sprintf("Sat %d", i))
		require.NoError(t, err)
		_, err = pool.Exec(seedCtx,
			`INSERT INTO mailboxes (tenant_id, id, agent_id, status, retry_policy) VALUES ($1,$2,$3,'active','{"max_attempts":5,"backoff_type":"exponential","initial_seconds":1,"max_seconds":10,"jitter":false}') ON CONFLICT DO NOTHING`,
			tenant, satMB(i), satAgent(i))
		require.NoError(t, err)
		for k := 0; k < 3; k++ {
			taskID := fmt.Sprintf("sat-seed-%d-%d", i, k)
			_, err = pool.Exec(seedCtx,
				`INSERT INTO tasks (tenant_id, id, source_agent, target_type, target_value, mailbox_id, status, priority, envelope)
				 VALUES ($1,$2,$3,'mailbox',$4,$4,'queued','normal',$5)`,
				tenant, taskID, satAgent(i), satMB(i),
				fmt.Sprintf(`{"task_id":%q,"tenant_id":%q,"source_agent":%q}`, taskID, tenant, satAgent(i)))
			require.NoError(t, err)
		}
	}

	// The saturation phase: everything below shares ONE pooled connection and
	// one 60s context. Every call takes ctx, so a wedged pool acquisition
	// surfaces as a context-deadline error instead of hanging the test.
	phaseCtx, cancelPhase := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelPhase()

	publisher := outbox.NewPublisher(outboxRepo, pgq)
	go publisher.Start(phaseCtx, 15*time.Millisecond)
	defer publisher.Stop()

	projector := outbox.NewAuditProjector(outboxRepo, &satAuditWriter{pool: single}).
		WithInterval(15 * time.Millisecond)
	go projector.Start(phaseCtx)
	defer projector.Stop()

	var (
		errMu        sync.Mutex
		opErrs       []string
		pullsClaimed atomic.Int64
		createsDone  atomic.Int64
		obInserts    atomic.Int64
	)
	recordErr := func(stage string, err error) bool {
		if err == nil {
			return false
		}
		errMu.Lock()
		opErrs = append(opErrs, fmt.Sprintf("%s: %v", stage, err))
		errMu.Unlock()
		return true
	}

	const (
		pullersPerMB = 2 // dual pull per mailbox
		pullsEach    = 6
		creators     = 3
		createsEach  = 6
		inserters    = 2
		insertsEach  = 8
		cfgUpdates   = 3
	)
	var wg sync.WaitGroup

	// Mailbox pulls (pgqueue claim path + claim transaction advisory lock).
	for m := 0; m < numMailboxes; m++ {
		for p := 0; p < pullersPerMB; p++ {
			wg.Add(1)
			go func(mi int) {
				defer wg.Done()
				for k := 0; k < pullsEach; k++ {
					res, perr := dispatch.PullTask(phaseCtx, tenant, satMB(mi), satAgent(mi))
					if recordErr(fmt.Sprintf("pull mb=%s k=%d", satMB(mi), k), perr) {
						return
					}
					if res != nil {
						pullsClaimed.Add(1)
					}
				}
			}(m)
		}
	}

	// Task creates (task row + 3 outbox rows in one lifecycle tx each).
	for c := 0; c < creators; c++ {
		wg.Add(1)
		go func(ci int) {
			defer wg.Done()
			for k := 0; k < createsEach; k++ {
				taskID := fmt.Sprintf("sat-task-c%d-%d", ci, k)
				mi := (ci + k) % numMailboxes
				task := core.Task{
					ID: taskID, TenantID: tenant, SourceAgent: satAgent(mi),
					TargetType: core.TargetTypeMailbox, TargetValue: satMB(mi), MailboxID: satMB(mi),
					Status: core.TaskStatusQueued, Priority: core.PriorityNormal,
					Envelope: core.TaskEnvelope{
						JanusVersion: "0.3",
						TaskID: taskID, TenantID: tenant, SourceAgent: satAgent(mi),
						Target: core.Target{Type: "mailbox", Value: satMB(mi)},
						Payload: core.Payload{Type: "text", Content: "saturation"},
						Trace:   core.TraceContext{TraceID: "sat-trace"},
					},
				}
				if _, cerr := taskSvc.Create(phaseCtx, task); recordErr(fmt.Sprintf("create %s", taskID), cerr) {
					return
				}
				createsDone.Add(1)
			}
		}(c)
	}

	// Direct outbox event inserts (event_publish rows the projector consumes).
	for w := 0; w < inserters; w++ {
		wg.Add(1)
		go func(wi int) {
			defer wg.Done()
			for k := 0; k < insertsEach; k++ {
				evt := core.JanusEvent{
					EventID:   fmt.Sprintf("sat-evt-%d-%d-%d", time.Now().UnixNano(), wi, k),
					TenantID:  tenant,
					EventType: core.EventTaskCreated,
					Payload:   json.RawMessage(`{"origin":"saturation"}`),
				}
				payload, merr := json.Marshal(evt)
				if recordErr("marshal event", merr) {
					return
				}
				if ierr := outboxRepo.InsertDirectWithDedupe(phaseCtx, evt.EventID, tenant, "event_publish", evt.EventID, payload); recordErr("outbox insert", ierr) {
					return
				}
				obInserts.Add(1)
			}
		}(w)
	}

	// Mailbox config updates (advisory-lock + lockedGet-on-executor path).
	for m := 0; m < numMailboxes; m++ {
		wg.Add(1)
		go func(mi int) {
			defer wg.Done()
			for k := 0; k < cfgUpdates; k++ {
				if uerr := mailboxSvc.UpdateConfig(phaseCtx, tenant, satMB(mi), 2, 30, 5, 3600); recordErr(fmt.Sprintf("update config mb=%s k=%d", satMB(mi), k), uerr) {
					return
				}
			}
		}(m)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		require.Empty(t, opErrs, "every concurrent operation must succeed with zero errors under MaxConns=1")
	case <-phaseCtx.Done():
		t.Fatalf("saturation phase did not finish within the 60s deadline: deadlock/self-starvation under MaxConns=1 (creates=%d pulls=%d outbox_inserts=%d, errs so far: %v)",
			createsDone.Load(), pullsClaimed.Load(), obInserts.Load(), opErrs)
	}

	// --- Effects visible in SQL (read back through the normal pool) ---

	// Tasks: 12 seeded + all creates, nothing lost.
	var tasks int
	require.NoError(t, pool.QueryRow(seedCtx,
		`SELECT count(*) FROM tasks WHERE tenant_id = $1`, tenant).Scan(&tasks))
	require.Equal(t, 12+creators*createsEach, tasks, "seeded + created task rows")

	// Attempts: exactly one per successful claim, and the 12 seeded tasks
	// were claimable from t=0, so claims must have happened.
	claimed := pullsClaimed.Load()
	require.GreaterOrEqual(t, claimed, int64(12), "the seeded tasks must have been claimed by the concurrent pullers")
	var attempts int
	require.NoError(t, pool.QueryRow(seedCtx,
		`SELECT count(*) FROM task_attempts WHERE tenant_id = $1`, tenant).Scan(&attempts))
	require.Equal(t, claimed, int64(attempts), "each successful pull creates exactly one attempt row")

	// Outbox: every row (creates x3, claims x1, direct inserts x1) published,
	// bounded-waited via the package's poll helper — no fixed sleep.
	expectedEventRows := int64(creators*createsEach*2) + claimed + int64(inserters*insertsEach)
	require.NoError(t, withDeadline(t, 30*time.Second, func() error {
		var pending, taskPub, eventPub int
		if err := pool.QueryRow(seedCtx,
			`SELECT count(*) FROM outbox_events WHERE tenant_id = $1 AND status != 'published'`, tenant).Scan(&pending); err != nil {
			return err
		}
		if err := pool.QueryRow(seedCtx,
			`SELECT count(*) FROM outbox_events WHERE tenant_id = $1 AND kind = 'task_publish'`, tenant).Scan(&taskPub); err != nil {
			return err
		}
		if err := pool.QueryRow(seedCtx,
			`SELECT count(*) FROM outbox_events WHERE tenant_id = $1 AND kind = 'event_publish'`, tenant).Scan(&eventPub); err != nil {
			return err
		}
		if taskPub != creators*createsEach {
			return fmt.Errorf("task_publish rows: want %d got %d", creators*createsEach, taskPub)
		}
		if int64(eventPub) != expectedEventRows {
			return fmt.Errorf("event_publish rows: want %d got %d", expectedEventRows, eventPub)
		}
		if pending != 0 {
			return fmt.Errorf("%d outbox rows still not published", pending)
		}
		return nil
	}), "the Publisher must drain the whole outbox")

	// Audit projection: every event_publish row projected, one audit row each
	// (bounded poll again — the projector's tick loop keeps running).
	require.NoError(t, withDeadline(t, 30*time.Second, func() error {
		var unprojected, auditRows int
		if err := pool.QueryRow(seedCtx,
			`SELECT count(*) FROM outbox_events WHERE tenant_id = $1 AND kind = 'event_publish' AND projected_at IS NULL`, tenant).Scan(&unprojected); err != nil {
			return err
		}
		if err := pool.QueryRow(seedCtx,
			`SELECT count(*) FROM audit_event_projection WHERE tenant_id = $1`, tenant).Scan(&auditRows); err != nil {
			return err
		}
		if unprojected != 0 {
			return fmt.Errorf("%d event_publish rows not yet projected", unprojected)
		}
		if int64(auditRows) != expectedEventRows {
			return fmt.Errorf("audit rows: want %d got %d", expectedEventRows, auditRows)
		}
		return nil
	}), "the AuditProjector must project every event_publish row")

	// Mailbox advisory-lock path: config version bumped on every mailbox.
	for i := 0; i < numMailboxes; i++ {
		var version int
		require.NoError(t, pool.QueryRow(seedCtx,
			`SELECT config_version FROM mailboxes WHERE tenant_id = $1 AND id = $2`, tenant, satMB(i)).Scan(&version))
		require.Equal(t, cfgUpdates, version, "config_version for %s", satMB(i))
	}
}

// satAuditWriter is the real-PG audit sink: writes the audit_event_projection
// table idempotently, through the tick-lock executor when the projector holds
// one (the B3 executor-sharing path), through the pool otherwise.
type satAuditWriter struct {
	pool *pgxpool.Pool
}

func (w *satAuditWriter) RecordIdempotent(ctx context.Context, evt core.JanusEvent) error {
	return w.record(ctx, w.pool, evt)
}

func (w *satAuditWriter) RecordIdempotentOn(ctx context.Context, exec postgres.Executor, evt core.JanusEvent) error {
	return w.record(ctx, exec, evt)
}

func (w *satAuditWriter) record(ctx context.Context, ex postgres.Executor, evt core.JanusEvent) error {
	_, err := ex.Exec(ctx,
		`INSERT INTO audit_event_projection (tenant_id, event_id, event_type, task_id, agent_id, trace_id, occurred_at, payload)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 ON CONFLICT (tenant_id, event_id) DO NOTHING`,
		evt.TenantID, evt.EventID, evt.EventType, evt.TaskID, evt.SourceAgent, evt.TraceID, evt.Timestamp, evt.Payload)
	return err
}
