package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
)

type contextKey string

const (
	TenantCtxKey contextKey = "janus_tenant_id"
	APIKeyCtxKey contextKey = "janus_api_key_prefix"
)

func TenantFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(TenantCtxKey).(string); ok {
		return v
	}
	return ""
}

type APIKeyValidator struct {
	db *sql.DB
}

func NewAPIKeyValidator(db *sql.DB) *APIKeyValidator {
	return &APIKeyValidator{db: db}
}

func (v *APIKeyValidator) Validate(ctx context.Context, apiKey string) (tenantID string, err error) {
	if len(apiKey) < 8 {
		return "", fmt.Errorf("invalid api key format")
	}
	prefix := apiKey[:8]
	keyHash := hashKey(apiKey)

	var tid string
	err = v.db.QueryRowContext(ctx,
		"SELECT tenant_id FROM api_keys WHERE prefix = $1 AND key_hash = $2",
		prefix, keyHash,
	).Scan(&tid)
	if err == sql.ErrNoRows {
		return "", fmt.Errorf("invalid api key")
	}
	if err != nil {
		return "", fmt.Errorf("lookup api key: %w", err)
	}
	return tid, nil
}

func Middleware(validator *APIKeyValidator) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			apiKey := extractAPIKey(r)
			if apiKey == "" {
				http.Error(w, `{"error":"missing api key"}`, http.StatusUnauthorized)
				return
			}

			tenantID, err := validator.Validate(r.Context(), apiKey)
			if err != nil {
				http.Error(w, `{"error":"invalid api key"}`, http.StatusUnauthorized)
				return
			}

			ctx := context.WithValue(r.Context(), TenantCtxKey, tenantID)
			ctx = context.WithValue(ctx, APIKeyCtxKey, apiKey[:8]+"...")
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func extractAPIKey(r *http.Request) string {
	if key := r.Header.Get("X-API-Key"); key != "" {
		return strings.TrimSpace(key)
	}
	auth := r.Header.Get("Authorization")
	if strings.HasPrefix(auth, "Bearer ") {
		return strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
	}
	return ""
}

func GenerateKey() (rawKey, prefix, keyHash string, err error) {
	b := make([]byte, 32)
	if _, err = rand.Read(b); err != nil {
		return "", "", "", err
	}
	rawKey = "janus_" + hex.EncodeToString(b)
	prefix = rawKey[:8]
	keyHash = hashKey(rawKey)
	return
}

func hashKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:])
}

func TenantGuard(extractTenantFromPath func(string) string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authTenant := TenantFromContext(r.Context())
			if authTenant == "" {
				next.ServeHTTP(w, r)
				return
			}
			pathTenant := extractTenantFromPath(r.URL.Path)
			if pathTenant == "" {
				next.ServeHTTP(w, r)
				return
			}
			if pathTenant != authTenant {
				http.Error(w, `{"error":"tenant mismatch: authenticated tenant cannot access this resource"}`, http.StatusForbidden)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
