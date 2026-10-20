package service

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/driver/postgres"
)

// stubExecutor is a no-op postgres.Executor used as an identity token for
// executor-threading assertions.
type stubExecutor struct{}

func (stubExecutor) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}
func (stubExecutor) Query(context.Context, string, ...any) (pgx.Rows, error) { return nil, nil }
func (stubExecutor) QueryRow(context.Context, string, ...any) pgx.Row      { return nil }

// execLockMailboxRepo implements mailboxWriteLocker and hands its callback
// a fixed executor.
type execLockMailboxRepo struct {
	mockMailboxRepo
	held postgres.Executor
}

func (r *execLockMailboxRepo) AcquireMailboxLock(context.Context, string, string) (postgres.Executor, func(), bool, error) {
	return r.held, func() {}, true, nil
}

// execReadMailboxRepo implements execMailboxReader and records which
// executor the version-gate read ran on.
type execReadMailboxRepo struct {
	mockMailboxRepo
	lastExec postgres.Executor
}

func (r *execReadMailboxRepo) GetOnExec(_ context.Context, exec postgres.Executor, _, _ string) (*core.Mailbox, error) {
	r.lastExec = exec
	return &core.Mailbox{TenantID: "acme", ID: "mb-1", ConfigVersion: 1}, nil
}

// B3 regression: the executor bound to the connection holding the advisory
// lock must reach the locked callback — DB work under the lock runs on that
// same connection instead of borrowing a second pooled one.
func TestWithMailboxLock_PassesHeldConnExecutorToCallback(t *testing.T) {
	exec := &stubExecutor{}
	repo := &execLockMailboxRepo{held: exec}
	svc := NewMailboxService(repo, &mockQueueDriver{})

	var seen postgres.Executor
	require.NoError(t, svc.withMailboxLock(context.Background(), "acme", "mb-1", func(e postgres.Executor) {
		seen = e
	}))
	assert.Same(t, exec, seen, "callback must receive the lock's executor")
}

// Repos without the advisory locker keep the in-process mutex path and see
// a nil executor (they hold no connection to run on).
func TestWithMailboxLock_MemoryLockerPassesNilExecutor(t *testing.T) {
	svc := NewMailboxService(&mockMailboxRepo{}, &mockQueueDriver{})
	ran := false
	require.NoError(t, svc.withMailboxLock(context.Background(), "acme", "mb-1", func(e postgres.Executor) {
		ran = true
		assert.Nil(t, e, "memory fallback has no held connection")
	}))
	assert.True(t, ran)
}

// The version-gate read under the lock routes through GetOnExec when the
// repo supports it, so it never borrows a second pooled connection.
func TestLockedGet_UsesLockConnectionWhenAvailable(t *testing.T) {
	exec := &stubExecutor{}
	repo := &execReadMailboxRepo{}
	svc := NewMailboxService(repo, &mockQueueDriver{})

	mb, err := svc.lockedGet(context.Background(), exec, "acme", "mb-1")
	require.NoError(t, err)
	require.NotNil(t, mb)
	assert.Same(t, exec, repo.lastExec, "version-gate read must run on the lock's connection")
}
