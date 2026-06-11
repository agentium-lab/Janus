package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	HTTPPort   int
	Postgres   PostgresConfig
	NATS       NATSConfig
	Redis      RedisConfig
	Migration  MigrationConfig
	Heartbeat  HeartbeatConfig
}

type PostgresConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	MaxConns int
}

func (p PostgresConfig) DSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		p.User, p.Password, p.Host, p.Port, p.Database)
}

type NATSConfig struct {
	URL string
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

type MigrationConfig struct {
	Auto bool
	Path string
}

type HeartbeatConfig struct {
	SweeperInterval string
	TTL             string
}

func (p PostgresConfig) ConnStr() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		p.Host, p.Port, p.User, p.Password, p.Database)
}

// Load reads configuration from environment variables with sensible defaults.
func Load() *Config {
	return &Config{
		HTTPPort: getEnvInt("JANUS_HTTP_PORT", 8080),
		Postgres: PostgresConfig{
			Host:     getEnv("JANUS_PG_HOST", "localhost"),
			Port:     getEnvInt("JANUS_PG_PORT", 5432),
			User:     getEnv("JANUS_PG_USER", "janus"),
			Password: getEnv("JANUS_PG_PASSWORD", ""),
			Database: getEnv("JANUS_PG_DATABASE", "janus"),
			MaxConns: getEnvInt("JANUS_PG_MAX_CONNS", 20),
		},
		NATS: NATSConfig{
			URL: getEnv("JANUS_NATS_URL", "nats://localhost:4222"),
		},
		Redis: RedisConfig{
			Addr:     getEnv("JANUS_REDIS_ADDR", "localhost:6379"),
			Password: getEnv("JANUS_REDIS_PASSWORD", ""),
			DB:       getEnvInt("JANUS_REDIS_DB", 0),
		},
		Migration: MigrationConfig{
			Auto: getEnvBool("JANUS_MIGRATION_AUTO", true),
			Path: getEnv("JANUS_MIGRATION_PATH", "migrations/"),
		},
		Heartbeat: HeartbeatConfig{
			SweeperInterval: getEnv("JANUS_HB_SWEEPER_INTERVAL", "30s"),
			TTL:             getEnv("JANUS_HB_TTL", "60s"),
		},
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}

func getEnvBool(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return fallback
}
