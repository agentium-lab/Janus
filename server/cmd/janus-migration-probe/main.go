package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

func main() {
	pgHost := getenv("JANUS_PG_HOST", "localhost")
	pgPort := getenv("JANUS_PG_PORT", "5432")
	pgUser := getenv("JANUS_PG_USER", "janus")
	pgPassword := getenv("JANUS_PG_PASSWORD", "")
	pgDatabase := getenv("JANUS_PG_DATABASE", "janus")
	migrationPath := getenv("JANUS_MIGRATION_PATH", "migrations/")

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		pgUser, pgPassword, pgHost, pgPort, pgDatabase)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		log.Fatalf("failed to connect to postgres: %v", err)
	}
	defer conn.Close(context.Background())

	var dbVersion int
	err = conn.QueryRow(ctx, "SELECT version FROM schema_migrations LIMIT 1").Scan(&dbVersion)
	if err != nil {
		log.Fatalf("failed to query schema_migrations: %v", err)
	}

	srcDriver, err := source.Open("file://" + migrationPath)
	if err != nil {
		log.Fatalf("failed to open migration source: %v", err)
	}
	defer srcDriver.Close()

	v, err := srcDriver.First()
	if err != nil {
		log.Fatalf("failed to get first migration version: %v", err)
	}

	expectedVersion := v
	for {
		next, err := srcDriver.Next(v)
		if err != nil {
			break
		}
		expectedVersion = next
		v = next
	}

	if dbVersion != int(expectedVersion) {
		log.Fatalf("migration mismatch: database=%d, expected=%d", dbVersion, expectedVersion)
	}

	log.Printf("migrations up-to-date: version=%d", dbVersion)
}

func getenv(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}
