package retry

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

func TestNewScheduler(t *testing.T) {
	s := NewScheduler(nil, nil)
	assert.NotNil(t, s)
	assert.False(t, s.useOutbox)
}

func TestScheduler_WithOutbox(t *testing.T) {
	s := NewScheduler(nil, nil)
	s2 := s.WithOutbox()
	assert.Same(t, s, s2)
	assert.True(t, s.useOutbox)
}

func TestScheduler_StartStop_ContextCancel(t *testing.T) {
	s := NewScheduler(nil, nil)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		s.Start(ctx, 1*time.Hour)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not exit after context cancel")
	}
}

func TestScheduler_StartStop_StopMethod(t *testing.T) {
	s := NewScheduler(nil, nil)
	ctx := context.Background()

	done := make(chan struct{})
	go func() {
		s.Start(ctx, 1*time.Hour)
		close(done)
	}()

	s.Stop()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not exit after Stop")
	}
}

func TestBuildRetryDedupeKey(t *testing.T) {
	assert.Equal(t, "task_publish:acme:task-1:3", buildRetryDedupeKey("acme", "task-1", 3))
	assert.Equal(t, "task_publish:t2:task-2:0", buildRetryDedupeKey("t2", "task-2", 0))
}

func TestBuildRetryDedupeKey_StableAndUnique(t *testing.T) {
	k1 := buildRetryDedupeKey("acme", "task-1", 1)
	k2 := buildRetryDedupeKey("acme", "task-1", 1)
	assert.Equal(t, k1, k2, "same inputs → same key")

	k3 := buildRetryDedupeKey("acme", "task-1", 2)
	assert.NotEqual(t, k1, k3, "different attempt → different key")
}

func TestBuildRetryMessage_ValidEnvelope(t *testing.T) {
	env := core.TaskEnvelope{
		JanusVersion: "1", TaskID: "task-1", TenantID: "acme",
		SourceAgent: "a1", Target: core.Target{Type: "mailbox", Value: "mb1"},
	}
	envJSON, _ := json.Marshal(env)

	msg, ok := buildRetryMessage("acme", "task-1", "mb1", core.PriorityNormal, envJSON)
	require.True(t, ok)
	assert.Equal(t, "acme", msg.TenantID)
	assert.Equal(t, "task-1", msg.TaskID)
	assert.Equal(t, "mb1", msg.MailboxID)
	assert.Equal(t, core.PriorityNormal, msg.Priority)
	assert.NotEmpty(t, msg.Payload)
}

func TestBuildRetryMessage_InvalidJSON(t *testing.T) {
	_, ok := buildRetryMessage("acme", "task-1", "mb1", core.PriorityNormal, []byte(`{invalid`))
	assert.False(t, ok)
}

func TestGenerateULID_NotEmpty(t *testing.T) {
	id := generateULID()
	assert.NotEmpty(t, id)
}

func TestGenerateULID_Unique(t *testing.T) {
	ids := make(map[string]bool)
	for i := 0; i < 100; i++ {
		ids[generateULID()] = true
	}
	assert.Len(t, ids, 100, "100 generated ULIDs should all be unique")
}

type mockRowsScanError struct{}

func (r *mockRowsScanError) Next() bool { return true }
func (r *mockRowsScanError) Scan(dest ...any) error { return fmt.Errorf("scan error") }
func (r *mockRowsScanError) Err() error { return nil }
func (r *mockRowsScanError) Close()    {}

type mockRowsErr struct{ closed bool }

func (r *mockRowsErr) Next() bool                            { return false }
func (r *mockRowsErr) Scan(dest ...any) error                { return nil }
func (r *mockRowsErr) Err() error                             { return fmt.Errorf("rows error") }
func (r *mockRowsErr) Close()                                { r.closed = true }

func TestScheduler_Start_TickerFires(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping ticker test in short mode")
	}
	pool := openRetryTestDBForTicker(t)
	sched := NewScheduler(pool, nil)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		sched.Start(ctx, 20*time.Millisecond)
		close(done)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not exit after context cancel")
	}
	pool.Close()
}

func openRetryTestDBForTicker(t *testing.T) *pgxpool.Pool {
	t.Helper()
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
	testDB := fmt.Sprintf("janus_retrytest_%d", time.Now().UnixNano())

	ctx := context.Background()
	adminDSN := fmt.Sprintf("host=%s port=%s user=%s dbname=janus_test sslmode=disable", host, port, user)
	adminConn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Skipf("postgres not reachable: %v", err)
	}
	_, err = adminConn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", testDB))
	require.NoError(t, err)
	adminConn.Close(ctx)

	dsn := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable", host, port, user, testDB)
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	require.NoError(t, pool.Ping(ctx))

	t.Cleanup(func() {
		pool.Close()
		c, err := pgx.Connect(ctx, adminDSN)
		if err == nil {
			c.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDB))
			c.Close(ctx)
		}
	})
	return pool
}

func TestProcessReadyRetries_UpdateErrorContinue(t *testing.T) {
	pool := openRetryTestDB(t)
	drv := &fakeQueueDriver{}
	sched := NewScheduler(pool, drv)
	sched.useOutbox = true
	ctx := context.Background()

	seedRetryTask(t, pool, "task-update-success", true)
	seedRetryTask(t, pool, "task-update-still-runs", true)

	sched.processReadyRetries(ctx)

	var status1, status2 string
	pool.QueryRow(ctx, "SELECT status FROM tasks WHERE id = $1", "task-update-success").Scan(&status1)
	pool.QueryRow(ctx, "SELECT status FROM tasks WHERE id = $1", "task-update-still-runs").Scan(&status2)

	assert.Equal(t, "queued", status1, "first task should be promoted")
	assert.Equal(t, "queued", status2, "second task should also be promoted")
}
