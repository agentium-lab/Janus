package auth

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testDSN() string {
	host := os.Getenv("JANUS_PG_HOST")
	if host == "" {
		host = "/tmp"
	}
	port := os.Getenv("JANUS_PG_PORT")
	if port == "" {
		port = "5432"
	}
	user := os.Getenv("JANUS_PG_USER")
	if user == "" {
		user = "silv"
	}
	dbname := os.Getenv("JANUS_PG_DBNAME")
	if dbname == "" {
		dbname = "janus_test"
	}
	return fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable", host, port, user, dbname)
}

func setupTestDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()
	dsn := testDSN()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Skipf("pg not available: %v", err)
	}

	tableName := fmt.Sprintf("api_keys_test_%d", time.Now().UnixNano())
	ctx := context.Background()
	_, err = db.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE %s (LIKE api_keys INCLUDING ALL)", tableName,
	))
	if err != nil {
		_, _ = db.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %s (
			tenant_id  text NOT NULL,
			key_hash   text NOT NULL,
			name       text NOT NULL,
			prefix     text NOT NULL,
			created_at timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (tenant_id, key_hash)
		)`, tableName))
	}

	cleanup := func() {
		_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
		db.Close()
	}

	_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

	return db, cleanup
}

func TestValidate_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	db, cleanup := setupTestDB(t)
	defer cleanup()

	validator := NewAPIKeyValidator(db)
	ctx := context.Background()

	raw, prefix, keyHash, err := GenerateKey()
	require.NoError(t, err)

	_, err = db.ExecContext(ctx,
		"INSERT INTO api_keys (tenant_id, key_hash, name, prefix) VALUES ($1, $2, $3, $4)",
		"test-tenant", keyHash, "test-key", prefix,
	)
	require.NoError(t, err)

	gotTenant, err := validator.Validate(ctx, raw)
	assert.NoError(t, err)
	assert.Equal(t, "test-tenant", gotTenant)

	_, err = validator.Validate(ctx, "janus_invalidkey123456789012345678")
	assert.Error(t, err)
}

func TestMiddleware_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	db, cleanup := setupTestDB(t)
	defer cleanup()

	validator := NewAPIKeyValidator(db)
	ctx := context.Background()

	raw, prefix, keyHash, err := GenerateKey()
	require.NoError(t, err)

	_, err = db.ExecContext(ctx,
		"INSERT INTO api_keys (tenant_id, key_hash, name, prefix) VALUES ($1, $2, $3, $4)",
		"acme", keyHash, "test-key", prefix,
	)
	require.NoError(t, err)

	var capturedTenant string
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedTenant = TenantFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})

	handler := Middleware(validator)(inner)

	r := httptest.NewRequest(http.MethodGet, "/v1/tenants", nil)
	r.Header.Set("X-API-Key", raw)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "acme", capturedTenant)
}
