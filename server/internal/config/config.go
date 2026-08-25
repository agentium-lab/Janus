package config

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	HTTPPort   int            `mapstructure:"http_port"`
	HTTPHost   string         `mapstructure:"http_host"`
	GRPCPort   int            `mapstructure:"grpc_port"`
	Postgres   PostgresConfig `mapstructure:"postgres"`
	NATS       NATSConfig     `mapstructure:"nats"`
	Redis      RedisConfig    `mapstructure:"redis"`
	Migration  MigrationConfig `mapstructure:"migration"`
	Heartbeat  HeartbeatConfig `mapstructure:"heartbeat"`
	Auth       AuthConfig     `mapstructure:"auth"`
	TLS        TLSConfig      `mapstructure:"tls"`
	CORS       CORSConfig     `mapstructure:"cors"`
	Log        LogConfig      `mapstructure:"log"`
	Metrics    MetricsConfig  `mapstructure:"metrics"`
	Tracing    TracingConfig  `mapstructure:"tracing"`
	Outbox     OutboxConfig    `mapstructure:"outbox"`
	Artifacts  ArtifactsConfig  `mapstructure:"artifacts"`
	LLM        LLMConfig        `mapstructure:"llm"`
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
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		p.User, p.Password, p.Host, p.Port, p.Database, p.sslMode())
}

func (p PostgresConfig) ConnStr() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		p.Host, p.Port, p.User, p.Password, p.Database, p.sslMode())
}

type NATSConfig struct {
	URL string `mapstructure:"url"`
}

type RedisConfig struct {
	Addr     string `mapstructure:"addr"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
}

type MigrationConfig struct {
	Auto bool   `mapstructure:"auto"`
	Path string `mapstructure:"path"`
}

type HeartbeatConfig struct {
	SweeperInterval string `mapstructure:"sweeper_interval"`
	TTL             string `mapstructure:"ttl"`
}

type AuthConfig struct {
	Enabled bool `mapstructure:"enabled"`
}

// TLSConfig configures optional TLS/mTLS for the HTTP and gRPC servers.
// When CertFile and KeyFile are both set, servers use TLS. When ClientCAFile
// is also set, client certificates are verified (mTLS).
type TLSConfig struct {
	Enabled      bool   `mapstructure:"enabled"`
	CertFile     string `mapstructure:"cert_file"`
	KeyFile      string `mapstructure:"key_file"`
	ClientCAFile string `mapstructure:"client_ca_file"`
}

// CORSConfig controls the Cross-Origin Resource Sharing policy. The default
// is deny (empty allowlist); configure AllowedOrigins to permit specific origins.
type CORSConfig struct {
	AllowedOrigins []string `mapstructure:"allowed_origins"`
}

type LogConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

type MetricsConfig struct {
	Enabled bool `mapstructure:"enabled"`
	Path    string `mapstructure:"path"`
}

type TracingConfig struct {
	Enabled      bool   `mapstructure:"enabled"`
	OTLPEndpoint string `mapstructure:"otlp_endpoint"`
	ServiceName  string `mapstructure:"service_name"`
}

type OutboxConfig struct {
	WorkerInterval  string `mapstructure:"worker_interval"`
	BatchSize       int    `mapstructure:"batch_size"`
	LeaseDuration   string `mapstructure:"lease_duration"`
	MaxAttempts     int    `mapstructure:"max_attempts"`
}

type ArtifactsConfig struct {
	StoreType string `mapstructure:"store_type"`
	LocalDir  string `mapstructure:"local_dir"`
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
	v.SetDefault("http_port", 8080)
	v.SetDefault("http_host", "")
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
	v.SetDefault("heartbeat.ttl", "60s")
	v.SetDefault("auth.enabled", true)
	v.SetDefault("tls.enabled", false)
	v.SetDefault("tls.min_version", "1.2")
	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "json")
	v.SetDefault("metrics.enabled", true)
	v.SetDefault("metrics.path", "/metrics")
	v.SetDefault("tracing.enabled", false)
	v.SetDefault("tracing.otlp_endpoint", "localhost:4317")
	v.SetDefault("tracing.service_name", "janus-api")
	v.SetDefault("outbox.worker_interval", "1s")
	v.SetDefault("outbox.batch_size", 50)
	v.SetDefault("outbox.lease_duration", "60s")
	v.SetDefault("outbox.max_attempts", 10)
	v.SetDefault("artifacts.store_type", "local")
	v.SetDefault("artifacts.local_dir", "/tmp/janus-artifacts")

	v.SetDefault("llm.enabled", false)
	v.SetDefault("llm.provider", "openai")
	v.SetDefault("llm.model", "gpt-4o-mini")
	v.SetDefault("llm.base_url", "https://api.openai.com/v1")
	v.SetDefault("llm.max_tokens", 200)
	v.SetDefault("llm.timeout_seconds", 5)
}

func bindEnvVars(v *viper.Viper) {
	envBindings := map[string]string{
		"JANUS_HTTP_PORT":             "http_port",
		"JANUS_HTTP_HOST":             "http_host",
		"JANUS_GRPC_PORT":             "grpc_port",
		"JANUS_PG_HOST":               "postgres.host",
		"JANUS_PG_PORT":               "postgres.port",
		"JANUS_PG_USER":               "postgres.user",
		"JANUS_PG_PASSWORD":           "postgres.password",
		"JANUS_PG_DATABASE":           "postgres.database",
		"JANUS_PG_MAX_CONNS":          "postgres.max_conns",
		"JANUS_PG_SSLMODE":            "postgres.sslmode",
		"PGSSLMODE":                   "postgres.sslmode",
		"JANUS_NATS_URL":              "nats.url",
		"JANUS_REDIS_ADDR":            "redis.addr",
		"JANUS_REDIS_PASSWORD":        "redis.password",
		"JANUS_REDIS_DB":              "redis.db",
		"JANUS_MIGRATION_AUTO":        "migration.auto",
		"JANUS_MIGRATION_PATH":        "migration.path",
		"JANUS_HB_SWEEPER_INTERVAL":   "heartbeat.sweeper_interval",
		"JANUS_HB_TTL":                "heartbeat.ttl",
		"JANUS_AUTH_ENABLED":          "auth.enabled",
		"JANUS_TLS_ENABLED":           "tls.enabled",
		"JANUS_TLS_CERT_FILE":         "tls.cert_file",
		"JANUS_TLS_KEY_FILE":          "tls.key_file",
		"JANUS_TLS_CLIENT_CA_FILE":    "tls.client_ca_file",
		"JANUS_CORS_ALLOWED_ORIGINS":  "cors.allowed_origins",
		"JANUS_LOG_LEVEL":             "log.level",
		"JANUS_LOG_FORMAT":            "log.format",
		"JANUS_METRICS_ENABLED":       "metrics.enabled",
		"JANUS_METRICS_PATH":          "metrics.path",
		"JANUS_TRACING_ENABLED":       "tracing.enabled",
		"JANUS_TRACING_OTLP_ENDPOINT": "tracing.otlp_endpoint",
		"JANUS_TRACING_SERVICE_NAME":  "tracing.service_name",
		"JANUS_OUTBOX_WORKER_INTERVAL": "outbox.worker_interval",
		"JANUS_OUTBOX_BATCH_SIZE":      "outbox.batch_size",
		"JANUS_OUTBOX_LEASE_DURATION":  "outbox.lease_duration",
		"JANUS_OUTBOX_MAX_ATTEMPTS":    "outbox.max_attempts",
		"JANUS_ARTIFACTS_STORE_TYPE":   "artifacts.store_type",
		"JANUS_ARTIFACTS_LOCAL_DIR":    "artifacts.local_dir",
		"JANUS_LLM_ENABLED":            "llm.enabled",
		"JANUS_LLM_PROVIDER":           "llm.provider",
		"JANUS_LLM_MODEL":              "llm.model",
		"JANUS_LLM_API_KEY":            "llm.api_key",
		"JANUS_LLM_BASE_URL":           "llm.base_url",
		"JANUS_LLM_MAX_TOKENS":         "llm.max_tokens",
		"JANUS_LLM_TIMEOUT_SECONDS":    "llm.timeout_seconds",
	}

	v.SetEnvPrefix("")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	for env, key := range envBindings {
		v.BindEnv(key, env)
	}
}
