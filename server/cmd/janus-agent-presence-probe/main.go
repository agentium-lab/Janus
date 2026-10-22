package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	dsn := getenv("JANUS_PG_DSN", "postgres://janus:janus@localhost:5432/janus?sslmode=disable")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("pg connect: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("pg ping: %v", err)
	}

	rows, err := pool.Query(ctx,
		`SELECT t.id, COUNT(a.id) FILTER (WHERE a.status = 'online') AS online_agents
		 FROM tenants t
		 LEFT JOIN agents a ON a.tenant_id = t.id
		 GROUP BY t.id
		 ORDER BY t.id`)
	if err != nil {
		log.Fatalf("query tenants: %v", err)
	}
	defer rows.Close()

	var zeroTenants []string
	totalTenants := 0
	for rows.Next() {
		var tenantID string
		var onlineCount int
		if err := rows.Scan(&tenantID, &onlineCount); err != nil {
			log.Fatalf("scan: %v", err)
		}
		totalTenants++
		if onlineCount == 0 {
			zeroTenants = append(zeroTenants, tenantID)
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("rows: %v", err)
	}

	log.Printf("agent presence probe: %d tenants checked", totalTenants)
	if len(zeroTenants) > 0 {
		log.Printf("WARNING: %d tenant(s) with zero online agents: %v", len(zeroTenants), zeroTenants)
		os.Exit(1)
	}
	log.Printf("all tenants have at least one online agent")
}

func getenv(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}
