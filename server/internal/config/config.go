package config

import (
	"net/url"

	"fmt"
	"log"
	"os"
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	Queue     QueueConfig     `mapstructure:"queue"`
	HTTPPort  int             `mapstructure:"http_port"`
	HTTPHost  string          `mapstructure:"http_host"`
	GRPCPort  int             `mapstructure:"grpc_port"`
	Postgres  PostgresConfig  `mapstructure:"postgres"`
	NATS      NATSConfig      `mapstructure:"nats"`
	Redis     RedisConfig     `mapstructure:"redis"`
	Migration MigrationConfig `mapstructure:"migration"`
	Heartbeat HeartbeatConfig `mapstructure:"heartbeat"`
	Mailbox   MailboxConfig   `mapstructure:"mailbox"`
	Auth      AuthConfig      `mapstructure:"auth"`
	TLS       TLSConfig       `mapstructure:"tls"`
	CORS      CORSConfig      `mapstructure:"cors"`
	Metrics   MetricsConfig   `mapstructure:"metrics"`
	Tracing   TracingConfig   `mapstructure:"tracing"`
	Outbox    OutboxConfig    `mapstructure:"outbox"`
	Budget    BudgetConfig    `mapstructure:"budget"`
	LLM       LLMConfig       `mapstructure:"llm"`
}

type LLMConfig struct {
	Enabled        bool   `mapstructure:"enabled"`
	Provider       string `mapstructure:"provider"`
	Model          string `mapstructure:"model"`
	APIKey         string `mapstructure:"api_key"`
	BaseURL        string `mapstructure:"base_url"`
	MaxTokens      int    `mapstructure:"max_tokens"`
	TimeoutSeconds int    `mapstructure:"timeout_seconds"`
}

type PostgresConfig struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	Database string `mapstructure:"database"`
	MaxConns int    `mapstructure:"max_conns"`
	SSLMode  string `mapstructure:"sslmode"`
}

const defaultPgSSLMode = "require"

func (p PostgresConfig) sslMode() string {
	if p.SSLMode != "" {
		return p.SSLMode
	}
	return defaultPgSSLMode
}

func (p PostgresConfig) DSN() string {
	// Build through net/url so the password gets USERINFO encoding rules.
	// url.QueryEscape encoded space as '+', and net/url's userinfo unescape
	// (the path pgconn.ParseConfig takes via User.Password()) does NOT
	// reverse '+' — a password containing a space round-tripped as '+'.
	// url.UserPassword encodes space as %20 and round-trips exactly.
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(p.User, p.Password),
		Host:     fmt.Sprintf("%s:%d", p.Host, p.Port),
		Path:     "/" + p.Database,
		RawQuery: "sslmode=" + url.QueryEscape(p.sslMode()),
	}
	return u.String()
}

func (p PostgresConfig) ConnStr() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		p.Host, p.Port, p.User, p.Password, p.Database, p.sslMode())
}

type NATSConfig struct {
	URL string `mapstructure:"url"`
}

type QueueConfig struct {
	Driver string `mapstructure:"driver"` // "nats" (default) | "pg"
}

type RedisConfig struct {
	Addr      string `mapstructure:"addr"`
	Password  string `mapstructure:"password"`
	DB        int    `mapstructure:"db"`
	EnableTLS bool   `mapstructure:"enable_tls"`
}

type MigrationConfig struct {
	Auto bool   `mapstructure:"auto"`
	Path string `mapstructure:"path"`
}

type HeartbeatConfig struct {
	SweeperInterval string `mapstructure:"sweeper_interval"`
	TTL             string `mapstructure:"ttl"`
}

type MailboxConfig struct {
	// ReconcileInterval bounds how long PG-to-broker consumer drift can
	// persist (e.g. a lockless older replica committing a config the
	// broker never received during a rolling upgrade).
	ReconcileInterval string `mapstructure:"reconcile_interval"`
}

type AuthConfig struct {
	Enabled bool `mapstructure:"enabled"`
}

// TLSConfig configures optional TLS/mTLS for the HTTP and gRPC servers.
// When CertFile and KeyFile are both set, servers use TLS. When ClientCAFile
// is also set, client certificates are verified (mTLS).
type TLSConfig struct {
	Enabled      bool              `mapstructure:"enabled"`
	CertFile     string            `mapstructure:"cert_file"`
	KeyFile      string            `mapstructure:"key_file"`
	ClientCAFile string            `mapstructure:"client_ca_file"`
	// Gateway configures the grpc-gateway's CLIENT-side TLS connection to
	// the local gRPC server (verification + optional client keypair).
	Gateway GatewayTLSConfig `mapstructure:"gateway"`
}

// GatewayTLSConfig configures how the grpc-gateway — an in-process gRPC
// CLIENT dialing the local gRPC server — verifies that server and
// authenticates itself under mTLS.
type GatewayTLSConfig struct {
	// CACertFile is the CA bundle used to verify the gRPC server's
	// certificate. When empty the gateway cannot verify the server at all
	// and falls back to InsecureSkipVerify (warned at startup) — a
	// dev-only posture.
	CACertFile string `mapstructure:"ca_cert_file"`
	// ServerName overrides the DNS name verified against the server
	// certificate. Defaults to the dial address host ("localhost"); set it
	// when the server certificate carries no localhost SAN.
	ServerName string `mapstructure:"server_name"`
	// ClientCertFile and ClientKeyFile present a client certificate when
	// the gRPC server requires mTLS; both must be set together.
	ClientCertFile string `mapstructure:"client_cert_file"`
	ClientKeyFile  string `mapstructure:"client_key_file"`
}

// CORSConfig controls the Cross-Origin Resource Sharing policy. The default
// is deny (empty allowlist); configure AllowedOrigins to permit specific origins.
type CORSConfig struct {
	AllowedOrigins []string `mapstructure:"allowed_origins"`
}

type MetricsConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	Path    string `mapstructure:"path"`
}

type TracingConfig struct {
	Enabled      bool   `mapstructure:"enabled"`
	OTLPEndpoint string `mapstructure:"otlp_endpoint"`
	ServiceName  string `mapstructure:"service_name"`
}

type BudgetConfig struct {
	// fail-open keeps dispatch flowing when the redis-backed rate limiter
	// is unavailable; fail-closed rejects budget-gated requests instead.
	RateLimitFailureMode string `mapstructure:"rate_limit_failure_mode"`
}

type OutboxConfig struct {
	WorkerInterval string `mapstructure:"worker_interval"`
	BatchSize      int    `mapstructure:"batch_size"`
	LeaseDuration  string `mapstructure:"lease_duration"`
	MaxAttempts    int    `mapstructure:"max_attempts"`
}

func Load() *Config {
	v := viper.New()

	setDefaults(v)
	bindEnvVars(v)

	v.SetConfigName("janus")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")
	v.AddConfigPath("/etc/janus/")
	v.AddConfigPath("$HOME/.janus/")

	if configPath := os.Getenv("JANUS_CONFIG_FILE"); configPath != "" {
		v.SetConfigFile(configPath)
	}

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			log.Printf("warning: error reading config file: %v", err)
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		log.Fatalf("failed to unmarshal config: %v", err)
	}

	return &cfg
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("http_port", 8080)
	v.SetDefault("http_host", "")
	v.SetDefault("queue.driver", "nats")
	v.SetDefault("grpc_port", 9090)
	v.SetDefault("postgres.host", "localhost")
	v.SetDefault("postgres.port", 5432)
	v.SetDefault("postgres.user", "janus")
	v.SetDefault("postgres.password", "")
	v.SetDefault("postgres.database", "janus")
	v.SetDefault("postgres.max_conns", 20)
	v.SetDefault("postgres.sslmode", "require")
	v.SetDefault("nats.url", "nats://localhost:4222")
	v.SetDefault("redis.addr", "localhost:6379")
	v.SetDefault("redis.password", "")
	v.SetDefault("redis.db", 0)
	v.SetDefault("migration.auto", true)
	v.SetDefault("migration.path", "migrations/")
	v.SetDefault("heartbeat.sweeper_interval", "30s")
	v.SetDefault("mailbox.reconcile_interval", "60s")
	v.SetDefault("heartbeat.ttl", "60s")
	v.SetDefault("auth.enabled", true)
	v.SetDefault("tls.enabled", false)
	v.SetDefault("metrics.enabled", true)
	v.SetDefault("metrics.path", "/metrics")
	v.SetDefault("tracing.enabled", false)
	v.SetDefault("tracing.otlp_endpoint", "localhost:4317")
	v.SetDefault("tracing.service_name", "janus-api")
	v.SetDefault("outbox.worker_interval", "500ms")
	v.SetDefault("budget.rate_limit_failure_mode", "fail-open")
	v.SetDefault("outbox.batch_size", 50)
	v.SetDefault("outbox.lease_duration", "60s")
	v.SetDefault("outbox.max_attempts", 10)

	v.SetDefault("llm.enabled", false)
	v.SetDefault("llm.provider", "openai")
	v.SetDefault("llm.model", "gpt-4o-mini")
	v.SetDefault("llm.base_url", "https://api.openai.com/v1")
	v.SetDefault("llm.max_tokens", 200)
	v.SetDefault("llm.timeout_seconds", 5)
}

func bindEnvVars(v *viper.Viper) {
	envBindings := map[string]string{
		"JANUS_HTTP_PORT":                      "http_port",
		"JANUS_HTTP_HOST":                      "http_host",
		"JANUS_GRPC_PORT":                      "grpc_port",
		"JANUS_PG_HOST":                        "postgres.host",
		"JANUS_PG_PORT":                        "postgres.port",
		"JANUS_PG_USER":                        "postgres.user",
		"JANUS_PG_PASSWORD":                    "postgres.password",
		"JANUS_PG_DATABASE":                    "postgres.database",
		"JANUS_PG_MAX_CONNS":                   "postgres.max_conns",
		"JANUS_PG_SSLMODE":                     "postgres.sslmode",
		"PGSSLMODE":                            "postgres.sslmode",
		"JANUS_NATS_URL":                       "nats.url",
		"JANUS_REDIS_ADDR":                     "redis.addr",
		"JANUS_REDIS_PASSWORD":                 "redis.password",
		"JANUS_REDIS_DB":                       "redis.db",
		"JANUS_REDIS_ENABLE_TLS":               "redis.enable_tls",
		"JANUS_QUEUE_DRIVER":                   "queue.driver",
		"JANUS_MIGRATION_AUTO":                 "migration.auto",
		"JANUS_MIGRATION_PATH":                 "migration.path",
		"JANUS_HB_SWEEPER_INTERVAL":            "heartbeat.sweeper_interval",
		"JANUS_MAILBOX_RECONCILE_INTERVAL":     "mailbox.reconcile_interval",
		"JANUS_HB_TTL":                         "heartbeat.ttl",
		"JANUS_AUTH_ENABLED":                   "auth.enabled",
		"JANUS_TLS_ENABLED":                    "tls.enabled",
		"JANUS_TLS_CERT_FILE":                  "tls.cert_file",
		"JANUS_TLS_KEY_FILE":                   "tls.key_file",
		"JANUS_TLS_CLIENT_CA_FILE":             "tls.client_ca_file",
		"JANUS_TLS_GATEWAY_CA_CERT_FILE":       "tls.gateway.ca_cert_file",
		"JANUS_TLS_GATEWAY_SERVER_NAME":        "tls.gateway.server_name",
		"JANUS_TLS_GATEWAY_CLIENT_CERT_FILE":   "tls.gateway.client_cert_file",
		"JANUS_TLS_GATEWAY_CLIENT_KEY_FILE":    "tls.gateway.client_key_file",
		"JANUS_CORS_ALLOWED_ORIGINS":           "cors.allowed_origins",
		"JANUS_METRICS_ENABLED":                "metrics.enabled",
		"JANUS_METRICS_PATH":                   "metrics.path",
		"JANUS_TRACING_ENABLED":                "tracing.enabled",
		"JANUS_TRACING_OTLP_ENDPOINT":          "tracing.otlp_endpoint",
		"JANUS_TRACING_SERVICE_NAME":           "tracing.service_name",
		"JANUS_OUTBOX_WORKER_INTERVAL":         "outbox.worker_interval",
		"JANUS_BUDGET_RATE_LIMIT_FAILURE_MODE": "budget.rate_limit_failure_mode",
		"JANUS_OUTBOX_BATCH_SIZE":              "outbox.batch_size",
		"JANUS_OUTBOX_LEASE_DURATION":          "outbox.lease_duration",
		"JANUS_OUTBOX_MAX_ATTEMPTS":            "outbox.max_attempts",
		"JANUS_LLM_ENABLED":                    "llm.enabled",
		"JANUS_LLM_PROVIDER":                   "llm.provider",
		"JANUS_LLM_MODEL":                      "llm.model",
		"JANUS_LLM_API_KEY":                    "llm.api_key",
		"JANUS_LLM_BASE_URL":                   "llm.base_url",
		"JANUS_LLM_MAX_TOKENS":                 "llm.max_tokens",
		"JANUS_LLM_TIMEOUT_SECONDS":            "llm.timeout_seconds",
	}

	v.SetEnvPrefix("")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	for env, key := range envBindings {
		v.BindEnv(key, env)
	}
}
