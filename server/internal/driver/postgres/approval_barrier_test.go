package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// GA-03/04 barrier: a decision that waits on the approval row lock past its
// deadline must be rejected even when the blocking transaction commits
// WITHOUT modifying the tuple — PostgreSQL does not re-run the UPDATE's
// WHERE in that case, which is why UpdateDecisionTx locks first
// (SELECT … FOR UPDATE) and judges expires_at only after the lock is held.
func TestUpdateDecisionTx_BarrierLockWaitPastDeadline(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	ctx := context.Background()
	repo := NewApprovalRepo(pool)

	const tenant, appr = "t_barrier", "appr_barrier_1"
	_, err := pool.Exec(ctx,
		`INSERT INTO approvals (tenant_id, id, task_id, status, requested_by, expires_at)
		 VALUES ($1, $2, 'task_barrier', 'pending', 'seed', now() + interval '1 second')`,
		tenant, appr)
	require.NoError(t, err)

	tx1, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx1.Rollback(ctx) }() // releases the barrier on every exit path
	_, err = tx1.Exec(ctx, `SELECT id FROM approvals WHERE tenant_id = $1 AND id = $2 FOR UPDATE`, tenant, appr)
	require.NoError(t, err)

	tx2, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx2.Rollback(ctx) }()
	decided := make(chan error, 1)
	go func() {
		decided <- repo.UpdateDecisionTx(ctx, tx2, tenant, appr, "approved", "waiter", "")
	}()

	// Subscribe to the blocked state BEFORE letting the deadline pass: the
	// waiter must actually be parked on the lock, not merely en route.
	require.Eventually(t, func() bool {
		var waiting int
		err := pool.QueryRow(ctx,
			`SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&waiting)
		return err == nil && waiting >= 1
	}, 5*time.Second, 50*time.Millisecond, "decision tx must block on the tuple lock")

	time.Sleep(1500 * time.Millisecond) // expires_at passes while tx2 is parked
	require.NoError(t, tx1.Commit(ctx))  // release WITHOUT modifying the tuple

	select {
	case err := <-decided:
		require.Error(t, err, "decision waiting past the deadline must be rejected")
	case <-time.After(5 * time.Second):
		t.Fatal("UpdateDecisionTx did not return after the lock was released")
	}
	require.NoError(t, tx2.Rollback(ctx))

	var status string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT status FROM approvals WHERE tenant_id = $1 AND id = $2`, tenant, appr).Scan(&status))
	require.Equal(t, "pending", status, "no out-of-bounds write: approval must stay pending")

	// With the barrier gone and the deadline restored, a fresh decision on a
	// live approval still succeeds (the lock path must not over-reject).
	const appr2 = "appr_barrier_2"
	_, err = pool.Exec(ctx,
		`INSERT INTO approvals (tenant_id, id, task_id, status, requested_by, expires_at)
		 VALUES ($1, $2, 'task_barrier', 'pending', 'seed', now() + interval '1 hour')`,
		tenant, appr2)
	require.NoError(t, err)
	tx3, err := pool.Begin(ctx)
	require.NoError(t, err)
	require.NoError(t, repo.UpdateDecisionTx(ctx, tx3, tenant, appr2, "approved", "approver", ""))
	require.NoError(t, tx3.Commit(ctx))
}
