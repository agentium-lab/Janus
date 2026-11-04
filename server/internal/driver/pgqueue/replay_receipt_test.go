package pgqueue

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/agentium-lab/Janus/core"
)

// GA-06 cross-round receipt: attempt numbers REPEAT after Replay
// (attempt_count resets to 0), so a round-0 callback delayed past a
// Replay+re-pull used to match the new round's rows (same attempt number)
// and clear its queue lease / touch retry_at. The ref now carries the
// replay round; every delivery side effect CASes on both.
func TestAckNack_ReplayRoundReceipt(t *testing.T) {
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
	db := os.Getenv("JANUS_PG_DBNAME")
	if db == "" {
		db = "janus_test"
	}
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, password, host, port, db)
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Skipf("pg not available: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(context.Background()); err != nil {
		t.Skipf("pg not reachable: %v", err)
	}

	ctx := context.Background()
	d := NewDriver(pool)
	const tenant, mb, task = "rcpt-a", "rcpt-mb", "rcpt-task"

	_, _ = pool.Exec(ctx, `DELETE FROM tasks WHERE tenant_id = $1`, tenant)
	defer func() { _, _ = pool.Exec(ctx, `DELETE FROM tasks WHERE tenant_id = $1`, tenant) }()
	_, err = pool.Exec(ctx,
		`INSERT INTO tasks (tenant_id,id,source_agent,target_type,target_value,mailbox_id,status,priority,envelope,created_at,updated_at)
		 VALUES ($1,$2,'agent-a','mailbox',$3,$3,'queued','normal','{"janus_version":"1.0","payload":{}}',now(),now())`,
		tenant, task, mb)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Round 0: fetch mints ref with replay=0; the service claim then bumps
	// attempt_count to 1 (ClaimWithCountCheckTx), the value the ref guards on.
	d0, err := d.FetchTasks(ctx, tenant, mb, core.FetchOptions{MaxMessages: 1})
	if err != nil || len(d0) != 1 {
		t.Fatalf("round-0 fetch: deliveries=%d err=%v", len(d0), err)
	}
	ref0 := d0[0].DeliveryRef
	if string(ref0) != task+":0:0" {
		t.Fatalf("round-0 ref = %q, want %q", ref0, task+":0:0")
	}
	if _, err := pool.Exec(ctx,
		`UPDATE tasks SET attempt_count = 1 WHERE tenant_id=$1 AND id=$2`, tenant, task); err != nil {
		t.Fatalf("simulate round-0 claim: %v", err)
	}

	// Replay: status back to queued, attempt_count resets to 0,
	// replay_count increments to 1; the round-1 claim bumps attempt to 1 —
	// the SAME number ref0 guards on, which is exactly the collision.
	if _, err := pool.Exec(ctx,
		`UPDATE tasks SET status='queued', attempt_count=0, replay_count=1,
		 queue_lease_until=NULL, retry_at=NULL WHERE tenant_id=$1 AND id=$2`, tenant, task); err != nil {
		t.Fatalf("simulate replay: %v", err)
	}
	d1, err := d.FetchTasks(ctx, tenant, mb, core.FetchOptions{MaxMessages: 1})
	if err != nil || len(d1) != 1 {
		t.Fatalf("round-1 fetch: deliveries=%d err=%v", len(d1), err)
	}
	ref1 := d1[0].DeliveryRef
	if string(ref1) != task+":0:1" {
		t.Fatalf("round-1 ref = %q, want %q", ref1, task+":0:1")
	}
	if _, err := pool.Exec(ctx,
		`UPDATE tasks SET attempt_count = 1 WHERE tenant_id=$1 AND id=$2`, tenant, task); err != nil {
		t.Fatalf("simulate round-1 claim: %v", err)
	}

	// Given the round-0 callback arriving now, it must clear NOTHING:
	// the round-1 lease stays, retry_at stays NULL.
	if err := d.AckTask(ctx, tenant, ref0); err != nil {
		t.Fatalf("stale ack should succeed as a no-op write, got: %v", err)
	}
	var leaseUntil *time.Time
	var retryAt *time.Time
	if err := pool.QueryRow(ctx,
		`SELECT queue_lease_until, retry_at FROM tasks WHERE tenant_id=$1 AND id=$2`, tenant, task).
		Scan(&leaseUntil, &retryAt); err != nil {
		t.Fatalf("read state after stale ack: %v", err)
	}
	if leaseUntil == nil {
		t.Fatal("CROSS-ROUND WRITE: round-0 ack cleared round-1's queue lease")
	}
	if retryAt != nil {
		t.Fatal("CROSS-ROUND WRITE: round-0 ack touched retry_at")
	}

	// Given the stale round-0 NACK, it must not schedule a retry either.
	if err := d.NackTask(ctx, tenant, ref0, core.NackRetriable); err != nil {
		t.Fatalf("stale nack: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT retry_at FROM tasks WHERE tenant_id=$1 AND id=$2`, tenant, task).Scan(&retryAt); err != nil {
		t.Fatalf("read retry_at after stale nack: %v", err)
	}
	if retryAt != nil {
		t.Fatal("CROSS-ROUND WRITE: round-0 nack set round-1's retry_at")
	}

	// Given the CURRENT round's ack, the lease clears — the guard must not
	// over-reject the live round.
	if err := d.AckTask(ctx, tenant, ref1); err != nil {
		t.Fatalf("current-round ack: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT queue_lease_until FROM tasks WHERE tenant_id=$1 AND id=$2`, tenant, task).
		Scan(&leaseUntil); err != nil {
		t.Fatalf("read lease after current ack: %v", err)
	}
	if leaseUntil != nil {
		t.Fatal("current-round ack failed to clear the lease")
	}
}
