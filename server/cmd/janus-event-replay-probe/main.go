package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
)

func main() {
	pgHost := getenv("JANUS_PG_HOST", "localhost")
	pgPort := getenv("JANUS_PG_PORT", "5432")
	pgUser := getenv("JANUS_PG_USER", "janus")
	pgPassword := getenv("JANUS_PG_PASSWORD", "")
	pgDatabase := getenv("JANUS_PG_DATABASE", "janus")

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		pgUser, pgPassword, pgHost, pgPort, pgDatabase)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		log.Fatalf("failed to connect to postgres: %v", err)
	}
	defer conn.Close(context.Background())

	var count int
	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM audit_event_projection LIMIT 1").Scan(&count)
	if err != nil {
		log.Fatalf("failed to query audit_event_projection: %v", err)
	}

	var eventID, eventType string
	var occurredAt time.Time
	err = conn.QueryRow(ctx, `
		SELECT event_id, event_type, occurred_at
		FROM audit_event_projection
		ORDER BY occurred_at DESC
		LIMIT 1
	`).Scan(&eventID, &eventType, &occurredAt)
	if err != nil {
		log.Fatalf("failed to fetch latest audit event: %v", err)
	}

	var replayCount int
	err = conn.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM audit_event_projection
		WHERE occurred_at >= $1
	`, occurredAt.Add(-1*time.Minute)).Scan(&replayCount)
	if err != nil {
		log.Fatalf("failed to verify event replay window: %v", err)
	}

	if replayCount == 0 {
		log.Fatalf("event replay verification failed: no events in replay window")
	}

	log.Printf("event replay verified: latest_event_id=%s type=%s occurred_at=%s events_in_window=%d",
		eventID, eventType, occurredAt.Format(time.RFC3339), replayCount)
}

func getenv(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}
