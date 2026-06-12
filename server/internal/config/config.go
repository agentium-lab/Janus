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
	GRPCPort   int            `mapstructure:"grpc_port"`
	Postgres   PostgresConfig `mapstructure:"postgres"`
	NATS       NATSConfig     `mapstructure:"nats"`
	Redis      RedisConfig    `mapstructure:"redis"`
	Migration  MigrationConfig `mapstructure:"migration"`
	Heartbeat  HeartbeatConfig `mapstructure:"heartbeat"`
	Auth       AuthConfig     `mapstructure:"auth"`
}

type PostgresConfig struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	Database string `mapstructure:"database"`
	MaxConns int    `mapstructure:"max_conns"`
}

func (p PostgresConfig) DSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		p.User, p.Password, p.Host, p.Port, p.Database)
}

func (p PostgresConfig) ConnStr() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		p.Host, p.Port, p.User, p.Password, p.Database)
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
	v.SetDefault("grpc_port", 9090)
	v.SetDefault("postgres.host", "localhost")
	v.SetDefault("postgres.port", 5432)
	v.SetDefault("postgres.user", "janus")
	v.SetDefault("postgres.password", "")
	v.SetDefault("postgres.database", "janus")
	v.SetDefault("postgres.max_conns", 20)
	v.SetDefault("nats.url", "nats://localhost:4222")
	v.SetDefault("redis.addr", "localhost:6379")
	v.SetDefault("redis.password", "")
	v.SetDefault("redis.db", 0)
	v.SetDefault("migration.auto", true)
	v.SetDefault("migration.path", "migrations/")
	v.SetDefault("heartbeat.sweeper_interval", "30s")
	v.SetDefault("heartbeat.ttl", "60s")
	v.SetDefault("auth.enabled", false)
}

func bindEnvVars(v *viper.Viper) {
	envBindings := map[string]string{
		"JANUS_HTTP_PORT":             "http_port",
		"JANUS_GRPC_PORT":             "grpc_port",
		"JANUS_PG_HOST":               "postgres.host",
		"JANUS_PG_PORT":               "postgres.port",
		"JANUS_PG_USER":               "postgres.user",
		"JANUS_PG_PASSWORD":           "postgres.password",
		"JANUS_PG_DATABASE":           "postgres.database",
		"JANUS_PG_MAX_CONNS":          "postgres.max_conns",
		"JANUS_NATS_URL":              "nats.url",
		"JANUS_REDIS_ADDR":            "redis.addr",
		"JANUS_REDIS_PASSWORD":        "redis.password",
		"JANUS_REDIS_DB":              "redis.db",
		"JANUS_MIGRATION_AUTO":        "migration.auto",
		"JANUS_MIGRATION_PATH":        "migration.path",
		"JANUS_HB_SWEEPER_INTERVAL":   "heartbeat.sweeper_interval",
		"JANUS_HB_TTL":                "heartbeat.ttl",
		"JANUS_AUTH_ENABLED":          "auth.enabled",
	}

	v.SetEnvPrefix("")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	for env, key := range envBindings {
		v.BindEnv(key, env)
	}
}
