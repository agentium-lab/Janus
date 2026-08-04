package routing

// GOV-08 integration test: real-PostgreSQL group/human routing target resolution.
//
// Mapping-source determination (read router.go FIRST):
//   The Router resolves group/human targets through the AgentLookup interface
//   (GetGroupMailboxes / GetHumanMailboxes). There is currently NO production
//   PG-backed AgentLookup implementation and NO group/human->mailbox table in
//   the migrations. The interface is the seam the router actually reads, so
//   this test wires a PG-backed AgentLookup (defined below) that reads:
//     * mailbox validity from the REAL production `mailboxes` table, and
//     * group/human -> mailbox mappings from two test-managed tables
//       (routing_group_mailboxes / routing_human_mailboxes) created per test DB.
//   This exercises the REAL Router resolution logic (routeGroup / routeHuman /
//   ValidateMailbox) against tenant-scoped rows seeded into a real PostgreSQL
//   instance, mirroring what a future production PG adapter will read. The
//   adapter itself is test scaffolding, analogous to mockLookup in
//   router_test.go, but every read is a live round-trip to PostgreSQL.
//
// Tests are skipped (not failed) when PostgreSQL is unreachable, matching the
// convention used by server/internal/lease/scanner_pg_test.go.

import (
	"context"
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

func openRoutingTestDB(t *testing.T) *pgxpool.Pool {
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
	if user == "" {
		user = "janus"
	}
	testDB := fmt.Sprintf("janus_routingtest_%d", time.Now().UnixNano())

	ctx := context.Background()
	adminDSN := fmt.Sprintf("host=%s port=%s user=%s dbname=janus_test sslmode=disable", host, port, user)
	adminConn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Skipf("postgres not reachable (set JANUS_PG_HOST/PORT/USER to enable): %v", err)
	}
	_, err = adminConn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", testDB))
	require.NoError(t, err, "create test DB")
	adminConn.Close(ctx)

	dsn := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable", host, port, user, testDB)
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	require.NoError(t, pool.Ping(ctx))

	migrationsDir := findRoutingMigrationsDir()
	entries, err := os.ReadDir(migrationsDir)
	require.NoError(t, err, "read migrations dir %s", migrationsDir)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if len(name) > 7 && name[len(name)-7:] == ".up.sql" {
			up, err := os.ReadFile(migrationsDir + "/" + name)
			require.NoError(t, err, "read migration %s", name)
			_, err = pool.Exec(ctx, string(up))
			require.NoError(t, err, "apply migration %s", name)
		}
	}

	// NOT production schema: these tables exist only to seed group/human ->
	// mailbox mappings the Router resolves via the AgentLookup seam.
	_, err = pool.Exec(ctx, `
		CREATE TABLE routing_group_mailboxes (
			tenant_id  text NOT NULL,
			group_id   text NOT NULL,
			mailbox_id text NOT NULL,
			priority   integer NOT NULL DEFAULT 0,
			PRIMARY KEY (tenant_id, group_id, mailbox_id)
		);
		CREATE TABLE routing_human_mailboxes (
			tenant_id  text NOT NULL,
			human_id   text NOT NULL,
			mailbox_id text NOT NULL,
			PRIMARY KEY (tenant_id, human_id, mailbox_id)
		);
	`)
	require.NoError(t, err, "create routing mapping tables")

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

func findRoutingMigrationsDir() string {
	for _, d := range []string{"../../../../migrations", "../../../migrations", "../../migrations"} {
		if _, err := os.Stat(d); err == nil {
			return d
		}
	}
	return "../../../../migrations"
}

type pgLookup struct {
	pool *pgxpool.Pool
}

func (l *pgLookup) ListOnlineByCapability(ctx context.Context, tenantID, capability string) ([]AgentCandidate, error) {
	rows, err := l.pool.Query(ctx, `
		SELECT a.id, m.id, 0, 0, a.max_concurrency
		FROM agents a
		JOIN agent_capabilities ac
		  ON ac.tenant_id = a.tenant_id AND ac.agent_id = a.id
		LEFT JOIN mailboxes m
		  ON m.tenant_id = a.tenant_id AND m.agent_id = a.id AND m.status = 'active'
		WHERE a.tenant_id = $1 AND ac.capability = $2 AND a.status IN ('online','active')
		ORDER BY a.id`, tenantID, capability)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AgentCandidate
	for rows.Next() {
		var c AgentCandidate
		var mb *string
		if err := rows.Scan(&c.AgentID, &mb, &c.Backlog, &c.RunningCount, &c.MaxConcurrency); err != nil {
			return nil, err
		}
		if mb != nil {
			c.MailboxID = *mb
		}
		c.Status = "online"
		out = append(out, c)
	}
	return out, rows.Err()
}

func (l *pgLookup) GetAgentMailbox(ctx context.Context, tenantID, agentID string) (string, error) {
	var mailboxID string
	err := l.pool.QueryRow(ctx,
		`SELECT id FROM mailboxes WHERE tenant_id=$1 AND agent_id=$2 AND status='active' ORDER BY id LIMIT 1`,
		tenantID, agentID).Scan(&mailboxID)
	if err != nil {
		return "", fmt.Errorf("agent %s not found: %w", agentID, err)
	}
	return mailboxID, nil
}

func (l *pgLookup) ValidateMailbox(ctx context.Context, tenantID, mailboxID string) (bool, error) {
	var status string
	err := l.pool.QueryRow(ctx,
		`SELECT status FROM mailboxes WHERE tenant_id=$1 AND id=$2`,
		tenantID, mailboxID).Scan(&status)
	if err != nil {
		return false, err
	}
	return status == "active", nil
}

func (l *pgLookup) GetGroupMailboxes(ctx context.Context, tenantID, groupID string) ([]string, error) {
	rows, err := l.pool.Query(ctx,
		`SELECT mailbox_id FROM routing_group_mailboxes WHERE tenant_id=$1 AND group_id=$2 ORDER BY priority, mailbox_id`,
		tenantID, groupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (l *pgLookup) GetHumanMailboxes(ctx context.Context, tenantID, humanID string) ([]string, error) {
	rows, err := l.pool.Query(ctx,
		`SELECT mailbox_id FROM routing_human_mailboxes WHERE tenant_id=$1 AND human_id=$2 ORDER BY mailbox_id`,
		tenantID, humanID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func seedTenantAgentMailbox(t *testing.T, pool *pgxpool.Pool, tenant, agent, mailbox, status string) {
	t.Helper()
	ctx := context.Background()
	_, err := pool.Exec(ctx,
		`INSERT INTO tenants (id, name, created_at, updated_at) VALUES ($1, $1, now(), now())
		 ON CONFLICT (id) DO NOTHING`, tenant)
	require.NoError(t, err, "seed tenant %s", tenant)
	_, err = pool.Exec(ctx,
		`INSERT INTO agents (tenant_id, id, display_name, protocol, endpoint, status, max_concurrency, created_at, updated_at)
		 VALUES ($1, $2, $2, 'a2a', 'http://localhost', 'online', 1, now(), now())
		 ON CONFLICT (tenant_id, id) DO NOTHING`, tenant, agent)
	require.NoError(t, err, "seed agent %s/%s", tenant, agent)
	_, err = pool.Exec(ctx,
		`INSERT INTO mailboxes (tenant_id, id, agent_id, status, priority, max_concurrency, ack_wait_seconds, max_deliver, retention_seconds, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, 'normal', 1, 2, 3, 3600, now(), now())
		 ON CONFLICT (tenant_id, id) DO UPDATE SET status = EXCLUDED.status`, tenant, mailbox, agent, status)
	require.NoError(t, err, "seed mailbox %s/%s", tenant, mailbox)
}

// TestRoute_GroupActiveMailbox_PG seeds a tenant-scoped group with two mapped
// mailboxes (one inactive, one active) and asserts the Router resolves to the
// active one via the PG-backed lookup.
func TestRoute_GroupActiveMailbox_PG(t *testing.T) {
	pool := openRoutingTestDB(t)
	ctx := context.Background()
	seedTenantAgentMailbox(t, pool, "acme", "agent-a", "mb-a", "inactive")
	seedTenantAgentMailbox(t, pool, "acme", "agent-b", "mb-b", "active")

	_, err := pool.Exec(ctx,
		`INSERT INTO routing_group_mailboxes (tenant_id, group_id, mailbox_id, priority) VALUES
		 ('acme', 'dev-team', 'mb-a', 0),
		 ('acme', 'dev-team', 'mb-b', 1)`)
	require.NoError(t, err, "seed group mapping")

	r := NewRouter(&pgLookup{pool: pool}, nil, nil)
	result, err := r.Route(ctx, "acme",
		core.Target{Type: core.TargetTypeGroup, Value: "dev-team"},
		core.TaskEnvelope{})
	require.NoError(t, err)
	assert.Equal(t, core.TargetTypeGroup, result.TargetType)
	assert.Equal(t, "mb-b", result.MailboxID, "should skip inactive and route to active mailbox")
	assert.Equal(t, "group_mailbox:dev-team", result.Reason)
}

func TestRoute_GroupNoMapping_PG(t *testing.T) {
	pool := openRoutingTestDB(t)
	ctx := context.Background()
	seedTenantAgentMailbox(t, pool, "acme", "agent-x", "mb-x", "active")

	r := NewRouter(&pgLookup{pool: pool}, nil, nil)
	_, err := r.Route(ctx, "acme",
		core.Target{Type: core.TargetTypeGroup, Value: "unknown-group"},
		core.TaskEnvelope{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no group mailbox", "must match router.go fallback wording")
}

func TestRoute_HumanActiveMailbox_PG(t *testing.T) {
	pool := openRoutingTestDB(t)
	ctx := context.Background()
	seedTenantAgentMailbox(t, pool, "acme", "agent-h", "mb-alice", "active")

	_, err := pool.Exec(ctx,
		`INSERT INTO routing_human_mailboxes (tenant_id, human_id, mailbox_id) VALUES
		 ('acme', 'alice', 'mb-alice')`)
	require.NoError(t, err, "seed human mapping")

	r := NewRouter(&pgLookup{pool: pool}, nil, nil)
	_, err = r.Route(ctx, "acme",
		core.Target{Type: core.TargetTypeHuman, Value: "alice"},
		core.TaskEnvelope{})
	require.Error(t, err, "human routing should return error")
	assert.Contains(t, err.Error(), "not supported")
}

func TestRoute_HumanNoMapping_PG(t *testing.T) {
	pool := openRoutingTestDB(t)
	ctx := context.Background()
	seedTenantAgentMailbox(t, pool, "acme", "agent-x", "mb-x", "active")

	r := NewRouter(&pgLookup{pool: pool}, nil, nil)
	_, err := r.Route(ctx, "acme",
		core.Target{Type: core.TargetTypeHuman, Value: "nobody"},
		core.TaskEnvelope{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not supported")
}

func TestRoute_GroupTenantIsolation_PG(t *testing.T) {
	pool := openRoutingTestDB(t)
	ctx := context.Background()
	seedTenantAgentMailbox(t, pool, "acme", "agent-a", "mb-acme", "active")
	seedTenantAgentMailbox(t, pool, "globex", "agent-g", "mb-globex", "active")

	_, err := pool.Exec(ctx,
		`INSERT INTO routing_group_mailboxes (tenant_id, group_id, mailbox_id) VALUES
		 ('acme',   'shared-team', 'mb-acme'),
		 ('globex', 'shared-team', 'mb-globex')`)
	require.NoError(t, err, "seed tenant-scoped group mappings")

	r := NewRouter(&pgLookup{pool: pool}, nil, nil)
	result, err := r.Route(ctx, "globex",
		core.Target{Type: core.TargetTypeGroup, Value: "shared-team"},
		core.TaskEnvelope{})
	require.NoError(t, err)
	assert.Equal(t, "mb-globex", result.MailboxID, "must resolve the tenant-scoped row, not cross tenants")

	other, err := r.Route(ctx, "acme",
		core.Target{Type: core.TargetTypeGroup, Value: "shared-team"},
		core.TaskEnvelope{})
	require.NoError(t, err)
	assert.Equal(t, "mb-acme", other.MailboxID)
}
