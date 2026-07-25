package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_Defaults(t *testing.T) {
	os.Unsetenv("JANUS_HTTP_PORT")
	os.Unsetenv("JANUS_PG_HOST")
	os.Unsetenv("JANUS_PG_USER")
	os.Unsetenv("JANUS_PG_DATABASE")
	os.Unsetenv("JANUS_NATS_URL")
	os.Unsetenv("JANUS_REDIS_ADDR")
	os.Unsetenv("JANUS_CONFIG_FILE")

	cfg := Load()
	assert.Equal(t, 8080, cfg.HTTPPort)
	assert.Equal(t, 9090, cfg.GRPCPort)
	assert.Equal(t, "localhost", cfg.Postgres.Host)
	assert.Equal(t, 5432, cfg.Postgres.Port)
	assert.Equal(t, "janus", cfg.Postgres.User)
	assert.Equal(t, "janus", cfg.Postgres.Database)
	assert.Equal(t, 20, cfg.Postgres.MaxConns)
	assert.Equal(t, "nats://localhost:4222", cfg.NATS.URL)
	assert.Equal(t, "localhost:6379", cfg.Redis.Addr)
	assert.False(t, cfg.Auth.Enabled)
	assert.True(t, cfg.Migration.Auto)
}

func TestLoad_EnvOverride(t *testing.T) {
	t.Setenv("JANUS_HTTP_PORT", "9090")
	t.Setenv("JANUS_PG_HOST", "db.example.com")
	t.Setenv("JANUS_PG_PORT", "6543")
	t.Setenv("JANUS_NATS_URL", "nats://nats:4222")
	t.Setenv("JANUS_AUTH_ENABLED", "true")

	cfg := Load()
	assert.Equal(t, 9090, cfg.HTTPPort)
	assert.Equal(t, "db.example.com", cfg.Postgres.Host)
	assert.Equal(t, 6543, cfg.Postgres.Port)
	assert.Equal(t, "nats://nats:4222", cfg.NATS.URL)
	assert.True(t, cfg.Auth.Enabled)
}

func TestPostgresConfig_DSN(t *testing.T) {
	p := PostgresConfig{Host: "h", Port: 5432, User: "u", Password: "p", Database: "d"}
	assert.Equal(t, "postgres://u:p@h:5432/d?sslmode=disable", p.DSN())
}

func TestPostgresConfig_ConnStr(t *testing.T) {
	p := PostgresConfig{Host: "h", Port: 5432, User: "u", Password: "p", Database: "d"}
	assert.Contains(t, p.ConnStr(), "host=h")
	assert.Contains(t, p.ConnStr(), "port=5432")
	assert.Contains(t, p.ConnStr(), "user=u")
	assert.Contains(t, p.ConnStr(), "dbname=d")
}

func TestLoad_ConfigFile(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "janus-test-*.yaml")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.WriteString("http_port: 7777\ngrpc_port: 8888\n")
	require.NoError(t, err)
	tmpFile.Close()

	t.Setenv("JANUS_CONFIG_FILE", tmpFile.Name())
	cfg := Load()
	assert.Equal(t, 7777, cfg.HTTPPort)
	assert.Equal(t, 8888, cfg.GRPCPort)
}
