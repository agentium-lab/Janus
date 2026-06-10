package postgres

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
)

func repoRoot() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..")
}

func testDSN() string {
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

func openTestDB(t *testing.T) *sql.DB {
	db, err := sql.Open("pgx", testDSN())
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func runMigration(t *testing.T, db *sql.DB) {
	up, err := os.ReadFile(filepath.Join(repoRoot(), "migrations", "000001_initial_schema.up.sql"))
	require.NoError(t, err)
	_, err = db.Exec(string(up))
	require.NoError(t, err)
	t.Cleanup(func() {
		down, _ := os.ReadFile(filepath.Join(repoRoot(), "migrations", "000001_initial_schema.down.sql"))
		db.Exec(string(down))
	})
}
