package retry

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// openSingleConnRetryDB opens a throwaway retry DB with MaxConns=1: any
// leaked transaction permanently starves the pool, making the leak
// deterministic instead of statistical.
func openSingleConnRetryDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	base := openRetryTestDB(t)
	cfg, err := pgxpool.ParseConfig(base.Config().ConnString())
	require.NoError(t, err)
	base.Close()
	cfg.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	require.NoError(t, err)
	require.NoError(t, pool.Ping(context.Background()))
	t.Cleanup(pool.Close)
	return pool
}

// A1 acceptance: the CAS race-loser branch must RETURN its pooled
// connection. The BEFORE UPDATE trigger returns NULL, so the scheduler's
// UPDATE affects zero rows — the deterministic loser path — while the task
// stays retry_scheduled, so every scan re-enters it. With MaxConns=1 a
// single leak starves the second scan's Begin; repeated loser cycles must
// neither accumulate acquisitions nor block, and the pool must close
// cleanly (the cleanup's pool.Close hangs on any leaked connection).
func TestProcessReadyRetries_RaceLoserReturnsConnection(t *testing.T) {
	pool := openSingleConnRetryDB(t)
	ctx := context.Background()
	seedRetryTask(t, pool, "task-race-loser", true)
	execRetry(t, pool, retrySkipFn)
	execRetry(t, pool, `CREATE TRIGGER retry_cov_skip_race BEFORE UPDATE ON tasks
		FOR EACH ROW EXECUTE FUNCTION retry_cov_skip()`)

	drv := &fakeQueueDriver{}
	sched := NewScheduler(pool, drv)
	sched.useOutbox = true

	baseline := int(pool.Stat().AcquiredConns())
	for i := 0; i < 5; i++ {
		scanCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		sched.processReadyRetries(scanCtx)
		cancel()
		acquired := int(pool.Stat().AcquiredConns())
		require.Equal(t, baseline, acquired,
			"loser scan %d: acquired conns %d != baseline %d (connection leaked)", i+1, acquired, baseline)
	}

	// With MaxConns=1 the pool still serves queries after five loser cycles.
	var one int
	require.NoError(t, pool.QueryRow(ctx, `SELECT 1`).Scan(&one))
	assert.Equal(t, 1, one)

	// And the loser left the row untouched: still retry_scheduled, no
	// outbox publish, no queue publish.
	var status string
	require.NoError(t, pool.QueryRow(ctx, `SELECT status FROM tasks WHERE id = 'task-race-loser'`).Scan(&status))
	assert.Equal(t, "retry_scheduled", status)
	var outboxCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM outbox_events WHERE kind = 'task_publish'`).Scan(&outboxCount))
	assert.Zero(t, outboxCount)
	assert.Empty(t, drv.publishedTasks)
}
