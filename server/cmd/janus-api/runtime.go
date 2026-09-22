package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/config"
	_ "github.com/agentium-lab/Janus/server/internal/metrics"
)

// Process-level runtime helpers: DB pool, migrations, loopback checks, TLS.

func runMigration(cfg *config.Config) {
	migrationsPath, _ := filepath.Abs(cfg.Migration.Path)
	m, err := migrate.New("file://"+migrationsPath, cfg.Postgres.DSN())
	if err != nil {
		log.Fatalf("migrate init: %v", err)
	}
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		log.Fatalf("migrate up: %v", err)
	}
	m.Close()
	log.Println("migration completed")
}

func isLoopbackAddr(addr string) bool {
	host := addr
	if i := strings.LastIndex(addr, ":"); i >= 0 {
		host = addr[:i]
	}
	switch host {
	case "localhost", "127.0.0.1", "[::1]", "::1":
		return true
	}
	return false
}

func mustOpenPool(cfg *config.Config) *pgxpool.Pool {
	ctx := context.Background()
	poolConfig, err := pgxpool.ParseConfig(cfg.Postgres.DSN())
	if err != nil {
		log.Fatalf("pgx pool config: %v", err)
	}
	poolConfig.MaxConns = int32(cfg.Postgres.MaxConns)

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		log.Fatalf("pgx pool open: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("pgx pool ping: %v", err)
	}
	return pool
}

func isTerminalJanusEvent(evt core.JanusEvent) bool {
	switch evt.EventType {
	case core.EventTaskCompleted, core.EventTaskFailed,
		core.EventTaskCancelled, core.EventTaskDeadLettered, core.EventTaskExpired:
		return true
	}
	return false
}

// buildTLSConfig constructs a *tls.Config from the TLSConfig. When ClientCAFile
// is set, client certificates are required and verified (mTLS). MinVersion is
// TLS 1.2 and only strong AEAD cipher suites are enabled.
func buildTLSConfig(tlsCfg config.TLSConfig) (*tls.Config, error) {
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		},
	}
	if tlsCfg.ClientCAFile != "" {
		caCert, err := os.ReadFile(tlsCfg.ClientCAFile)
		if err != nil {
			return nil, fmt.Errorf("read client CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse client CA certificate")
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}
