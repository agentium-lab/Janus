package config

import (
	"os"
	"strings"
	"testing"
)

// Guards configs/janus.example.yaml against drifting from Config again (it
// sat stale for months): every documented default must survive a real
// config.Load of the shipped file.

func TestExampleYamlParsesThroughLoad(t *testing.T) {
	// The FILE is under test, not ambient configuration: clear every
	// JANUS_* override (CI pipelines export e.g. JANUS_PG_SSLMODE=disable
	// for live-PG jobs) so viper resolves purely from the shipped file.
	saved := map[string]string{}
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "JANUS_") {
			k := strings.SplitN(kv, "=", 2)[0]
			saved[k] = os.Getenv(k)
			os.Unsetenv(k)
		}
	}
	t.Cleanup(func() {
		for k, v := range saved {
			os.Setenv(k, v)
		}
	})
	os.Setenv("JANUS_CONFIG_FILE", "../../../configs/janus.example.yaml")
	t.Cleanup(func() { os.Unsetenv("JANUS_CONFIG_FILE") })
	cfg := Load()
	if cfg.Queue.Driver != "nats" || cfg.HTTPPort != 8080 || cfg.GRPCPort != 9090 {
		t.Fatalf("basics: %+v %+v %+v", cfg.Queue, cfg.HTTPPort, cfg.GRPCPort)
	}
	if cfg.Postgres.MaxConns != 20 || cfg.Postgres.SSLMode != "require" {
		t.Fatalf("postgres: %+v", cfg.Postgres)
	}
	if cfg.Outbox.WorkerInterval != "500ms" || cfg.Outbox.BatchSize != 50 || cfg.Outbox.MaxAttempts != 10 {
		t.Fatalf("outbox: %+v", cfg.Outbox)
	}
	if cfg.Budget.RateLimitFailureMode != "fail-open" || !cfg.Auth.Enabled || cfg.Mailbox.ReconcileInterval != "60s" {
		t.Fatalf("budget/auth/mailbox: %+v %+v %+v", cfg.Budget, cfg.Auth, cfg.Mailbox)
	}
	if cfg.TLS.Enabled || cfg.TLS.Gateway.Insecure || cfg.Metrics.Path != "/metrics" {
		t.Fatalf("tls/metrics: %+v %+v", cfg.TLS, cfg.Metrics)
	}
	if cfg.Artifact.Dir != "" || cfg.Artifact.MaxSizeMB != 10 || cfg.Artifact.TenantQuotaMB != 1024 {
		t.Fatalf("artifact: %+v", cfg.Artifact)
	}
	if cfg.LLM.Provider != "openai" || cfg.Redis.Addr != "localhost:6379" || cfg.Redis.EnableTLS {
		t.Fatalf("llm/redis: %+v %+v", cfg.LLM, cfg.Redis)
	}
}
