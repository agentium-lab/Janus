package postgres

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func repoRoot() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..")
}

func migrationUpPath() string {
	return filepath.Join(repoRoot(), "migrations", "000001_initial_schema.up.sql")
}

func migrationDownPath() string {
	return filepath.Join(repoRoot(), "migrations", "000001_initial_schema.down.sql")
}

func dsn() string {
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
		user = "silv"
	}
	dbname := os.Getenv("JANUS_PG_DBNAME")
	if dbname == "" {
		dbname = "janus_test"
	}
	return fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable", host, port, user, dbname)
}

func openDB(t *testing.T) *sql.DB {
	db, err := sql.Open("pgx", dsn())
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func runUp(t *testing.T, db *sql.DB) {
	schemaSQL, err := os.ReadFile(migrationUpPath())
	require.NoError(t, err)
	_, err = db.Exec(string(schemaSQL))
	require.NoError(t, err)
}

func runDown(t *testing.T, db *sql.DB) {
	dropSQL, err := os.ReadFile(migrationDownPath())
	require.NoError(t, err)
	_, err = db.Exec(string(dropSQL))
	require.NoError(t, err)
}

func TestMigration_Up(t *testing.T) {
	db := openDB(t)
	runUp(t, db)
	t.Cleanup(func() { runDown(t, db) })

	var count int
	tables := []string{
		"tenants", "agents", "agent_capabilities", "mailboxes",
		"tasks", "task_attempts", "budgets", "policy_rules",
		"approvals", "audit_event_projection", "outbox_events",
	}
	for _, table := range tables {
		err := db.QueryRow(
			"SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1",
			table,
		).Scan(&count)
		assert.NoError(t, err, "checking table %s", table)
		assert.Equal(t, 1, count, "table %s should exist", table)
	}
}

func TestMigration_Down(t *testing.T) {
	db := openDB(t)
	runUp(t, db)
	runDown(t, db)

	var count int
	tables := []string{
		"tenants", "agents", "agent_capabilities", "mailboxes",
		"tasks", "task_attempts", "budgets", "policy_rules",
		"approvals", "audit_event_projection", "outbox_events",
	}
	for _, table := range tables {
		err := db.QueryRow(
			"SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1",
			table,
		).Scan(&count)
		assert.NoError(t, err)
		assert.Equal(t, 0, count, "table %s should not exist after down", table)
	}
}

func TestMigration_UpIdempotentAfterDown(t *testing.T) {
	db := openDB(t)
	runUp(t, db)
	runDown(t, db)
	runUp(t, db)
	t.Cleanup(func() { runDown(t, db) })

	var count int
	err := db.QueryRow(
		"SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'tenants'",
	).Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestMigration_Indexes(t *testing.T) {
	db := openDB(t)
	runUp(t, db)
	t.Cleanup(func() { runDown(t, db) })

	indexes := []string{
		"tasks_idempotency_idx",
		"tasks_status_idx",
		"audit_event_task_idx",
		"audit_event_trace_idx",
	}
	for _, idx := range indexes {
		var count int
		err := db.QueryRow(
			"SELECT count(*) FROM pg_indexes WHERE indexname = $1",
			idx,
		).Scan(&count)
		assert.NoError(t, err)
		assert.Equal(t, 1, count, "index %s should exist", idx)
	}
}

func TestInsertTenant(t *testing.T) {
	db := openDB(t)
	runUp(t, db)
	t.Cleanup(func() { runDown(t, db) })

	_, err := db.Exec("INSERT INTO tenants (id, name) VALUES ($1, $2)", "acme", "Acme Corp")
	require.NoError(t, err)

	var name string
	err = db.QueryRow("SELECT name FROM tenants WHERE id = $1", "acme").Scan(&name)
	assert.NoError(t, err)
	assert.Equal(t, "Acme Corp", name)
}

func TestInsertAgent(t *testing.T) {
	db := openDB(t)
	runUp(t, db)
	t.Cleanup(func() { runDown(t, db) })

	_, err := db.Exec("INSERT INTO tenants (id, name) VALUES ($1, $2)", "acme", "Acme Corp")
	require.NoError(t, err)

	_, err = db.Exec(
		"INSERT INTO agents (id, tenant_id, display_name, protocol, status) VALUES ($1, $2, $3, $4, $5)",
		"code-reviewer.team-a", "acme", "Code Reviewer", "a2a", "online",
	)
	require.NoError(t, err)

	var displayName string
	err = db.QueryRow(
		"SELECT display_name FROM agents WHERE tenant_id = $1 AND id = $2",
		"acme", "code-reviewer.team-a",
	).Scan(&displayName)
	assert.NoError(t, err)
	assert.Equal(t, "Code Reviewer", displayName)
}

func TestTaskIdempotency(t *testing.T) {
	db := openDB(t)
	runUp(t, db)
	t.Cleanup(func() { runDown(t, db) })

	_, err := db.Exec("INSERT INTO tenants (id, name) VALUES ($1, $2)", "acme", "Acme Corp")
	require.NoError(t, err)

	envelope := `{"janus_version":"0.1","task_id":"task_001"}`
	_, err = db.Exec(
		"INSERT INTO tasks (tenant_id, id, idempotency_key, source_agent, target_type, target_value, status, envelope) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
		"acme", "task_001", "repo-123-pr-456", "agent-a", "capability", "code_review", "created", envelope,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		"INSERT INTO tasks (tenant_id, id, idempotency_key, source_agent, target_type, target_value, status, envelope) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
		"acme", "task_002", "repo-123-pr-456", "agent-a", "capability", "code_review", "created", envelope,
	)
	assert.Error(t, err, "duplicate idempotency_key should fail")
}
