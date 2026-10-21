package handler

// End-to-end proof for task.lease_lost: an expired lease requeues the task
// (real lease scanner over real PostgreSQL) and writes a task.lease_lost
// event to the outbox in the same transaction; the real outbox.Publisher
// (over the real PG outbox repo) then hands it to the queue driver, which
// routes it into the FanoutBroadcaster exactly like the production event
// router, and a live WebSocket subscriber receives event_type task.lease_lost
// carrying the persisted outbox identity.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/driver/postgres"
	"github.com/agentium-lab/Janus/server/internal/lease"
	"github.com/agentium-lab/Janus/server/internal/outbox"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openLeaseLostStreamDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	host := os.Getenv("JANUS_PG_HOST")
	if host == "" {
		host = "/tmp"
	}
	port := os.Getenv("JANUS_PG_PORT")
	if port == "" {
		port = "5432"
	}
	user := os.Getenv("JANUS_PG_USER")
	password := os.Getenv("JANUS_PG_PASSWORD")
	if user == "" {
		user = "janus"
	}
	testDB := fmt.Sprintf("janus_llstream_%d", time.Now().UnixNano())

	ctx := context.Background()
	adminDSN := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=janus_test sslmode=disable", host, port, user, password)
	adminConn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Skipf("postgres not reachable (set JANUS_PG_HOST/PORT/USER to enable): %v", err)
	}
	_, err = adminConn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", testDB))
	require.NoError(t, err, "create test DB")
	adminConn.Close(ctx)

	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, testDB)
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	require.NoError(t, pool.Ping(ctx))

	migrationsDir := "../../../migrations"
	entries, _ := os.ReadDir(migrationsDir)
	for _, e := range entries {
		if !e.IsDir() && len(e.Name()) > 7 && e.Name()[len(e.Name())-7:] == ".up.sql" {
			up, _ := os.ReadFile(migrationsDir + "/" + e.Name())
			_, err := pool.Exec(ctx, string(up))
			require.NoError(t, err, "migration %s", e.Name())
		}
	}

	t.Cleanup(func() {
		pool.Close()
		ctx := context.Background()
		adminConn, err := pgx.Connect(ctx, adminDSN)
		if err == nil {
			adminConn.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDB))
			adminConn.Close(ctx)
		}
	})

	return pool
}

func seedLeaseLostStreamTask(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()

	// mailbox ack_wait_seconds=2; the attempt's heartbeat is 10 minutes old,
	// so the lease scanner must consider it expired.
	_, err := pool.Exec(ctx, `INSERT INTO tenants (id, name, created_at, updated_at) VALUES ('acme', 'Acme', now(), now())`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO agents (tenant_id, id, display_name, protocol, endpoint, status, max_concurrency, created_at, updated_at)
		VALUES ('acme', 'agent-1', 'Worker', 'a2a', 'http://localhost', 'active', 1, now(), now())`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO mailboxes (tenant_id, id, agent_id, status, priority, max_concurrency, ack_wait_seconds, max_deliver, retention_seconds, created_at, updated_at)
		VALUES ('acme', 'mb-1', 'agent-1', 'active', 'normal', 1, 2, 3, 3600, now(), now())`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO tasks (tenant_id, id, source_agent, target_type, target_value, mailbox_id, status, priority, envelope, attempt_count, created_at, updated_at)
		VALUES ('acme', 'task-1', 'agent-1', 'agent', 'agent-1', 'mb-1', 'claimed', 'normal', '{}'::jsonb, 1, now(), now())`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO task_attempts (tenant_id, task_id, attempt, agent_id, lease_id, status, started_at, heartbeat_at)
		VALUES ('acme', 'task-1', 1, 'agent-1', 'lease-1', 'claimed', now() - interval '10 minutes', now() - interval '10 minutes')`)
	require.NoError(t, err)
}

// streamDriver routes PublishEvent into the broadcaster's inbound channel —
// the same hand-off the production event router performs after the queue
// delivers an outbox event. Other driver methods are never called on this
// path (the outbox row kind is event_publish).
type streamDriver struct {
	core.QueueEventDriver
	publish func(core.JanusEvent) error
}

func (d *streamDriver) PublishEvent(ctx context.Context, event core.JanusEvent) error {
	return d.publish(event)
}

func TestLeaseLostEvent_ReachesLiveStreamSubscriber(t *testing.T) {
	pool := openLeaseLostStreamDB(t)
	seedLeaseLostStreamTask(t, pool)
	ctx := context.Background()

	broadcastCh := make(chan core.JanusEvent, 256)
	broadcaster := NewFanoutBroadcaster(broadcastCh)
	srv := httptest.NewServer(withWSAuth(NewWebSocketHandler(broadcaster)))
	defer srv.Close()

	ws := wsWaitSubscribed(t, srv, broadcastCh, "acme", "?tenant=acme")
	defer ws.Close()

	scanner := lease.NewScanner(pool, time.Hour)
	n, err := scanner.ExpireLeases(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	var taskStatus string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT status FROM tasks WHERE tenant_id='acme' AND id='task-1'`).Scan(&taskStatus))
	require.Equal(t, "retry_scheduled", taskStatus)

	// Persistence half: the task.lease_lost outbox row exists (SQL read-back).
	var outboxID string
	var rawPayload []byte
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT id, payload FROM outbox_events
		  WHERE tenant_id='acme' AND kind='event_publish'`).Scan(&outboxID, &rawPayload))
	var persisted core.JanusEvent
	require.NoError(t, json.Unmarshal(rawPayload, &persisted))
	require.Equal(t, core.EventLeaseLost, persisted.EventType)
	require.Equal(t, "task-1", persisted.TaskID)
	require.Equal(t, "acme", persisted.TenantID)
	require.NotEmpty(t, persisted.EventID)

	// Delivery half: real outbox publisher drains the real PG outbox and the
	// live WS subscriber receives the event with the persisted identity.
	driver := &streamDriver{publish: func(evt core.JanusEvent) error {
		broadcastCh <- evt
		return nil
	}}
	pub := outbox.NewPublisher(postgres.NewOutboxRepo(pool), driver)
	go pub.Start(ctx, 10*time.Millisecond)
	defer pub.Stop()

	ws.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, msg, err := ws.ReadMessage()
	require.NoError(t, err)

	var live core.JanusEvent
	require.NoError(t, json.Unmarshal(msg, &live))
	assert.Equal(t, core.EventLeaseLost, live.EventType)
	assert.Equal(t, "acme", live.TenantID)
	assert.Equal(t, "task-1", live.TaskID)
	assert.Equal(t, persisted.EventID, live.EventID,
		"the live stream event must carry the persisted outbox event identity")

	var payload struct {
		Reason  string `json:"reason"`
		Attempt int    `json:"attempt"`
		AgentID string `json:"agent_id"`
	}
	require.NoError(t, json.Unmarshal(live.Payload, &payload))
	assert.Equal(t, "lease_expired", payload.Reason)
	assert.Equal(t, 1, payload.Attempt)
	assert.Equal(t, "agent-1", payload.AgentID)

	require.Eventually(t, func() bool {
		var status string
		if err := pool.QueryRow(ctx,
			`SELECT status FROM outbox_events WHERE id = $1`, outboxID).Scan(&status); err != nil {
			return false
		}
		return status == "published"
	}, 10*time.Second, 50*time.Millisecond, "outbox row must be marked published after delivery")
}
