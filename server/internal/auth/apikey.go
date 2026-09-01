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
	TenantCtxKey     contextKey = "janus_tenant_id"
	APIKeyCtxKey     contextKey = "janus_api_key_prefix"
	PrincipalCtxKey  contextKey = "janus_principal"
	ActingUserCtxKey contextKey = "janus_acting_user"
)

// ActingUserHeader carries an optional human-attribution hint set by a
// trusted fronting proxy (e.g. authd). It is recorded as a non-authoritative
// claimed_actor annotation next to the authoritative key identity.
const ActingUserHeader = "X-Janus-Acting-User"

// Scope names carried by API keys. An empty scope set grants full access so
// keys created before scopes existed keep working unchanged.
const (
	ScopeAdmin     = "admin"
	ScopeTaskWrite = "task:write"
	ScopeTaskRead  = "task:read"
	ScopeAuditRead = "audit:read"
)

type Principal struct {
	TenantID  string
	Scopes    []string
	KeyPrefix string
}

func (p Principal) HasScope(scope string) bool {
	if len(p.Scopes) == 0 {
		return true
	}
	for _, s := range p.Scopes {
		if s == ScopeAdmin || s == scope {
			return true
		}
	}
	return false
}

func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(PrincipalCtxKey).(Principal)
	return p, ok
}

func ActingUserFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(ActingUserCtxKey).(string); ok {
		return v
	}
	return ""
}

func TenantFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(TenantCtxKey).(string); ok {
		return v
	}
	return ""
}

// scopeRule maps a request shape inside /v1/tenants/... to the scope it
// requires. segment must appear as a path segment; suffixes narrow the match.
// First matching rule wins; unmatched requests fall back to method defaults.
type scopeRule struct {
	segment  string
	suffixes []string
	scope    string
}

var scopeRules = []scopeRule{
	{segment: "approvals", suffixes: []string{"/approve", "/reject"}, scope: ScopeAdmin},
	{segment: "api-keys", scope: ScopeAdmin},
	{segment: "policy-rules", scope: ScopeAdmin},
	{segment: "budgets", scope: ScopeAdmin},
	{segment: "dlq", suffixes: []string{"/replay", "/discard"}, scope: ScopeAdmin},
	{segment: "traces", scope: ScopeAuditRead},
}

// RequiredScope resolves which scope a request demands. ok=false marks paths
// outside the versioned data-plane API (probes, gateways, ws) as needing no
// key scope.
func RequiredScope(method, path string) (scope string, ok bool) {
	if !strings.HasPrefix(path, "/v1/tenants/") &&
		!strings.HasPrefix(path, "/a2a/") &&
		!strings.HasPrefix(path, "/mcp") &&
		!strings.HasPrefix(path, "/acp/") {
		return "", false
	}
	for _, rule := range scopeRules {
		if !hasSegment(path, rule.segment) {
			continue
		}
		if len(rule.suffixes) > 0 && !hasAnySuffix(path, rule.suffixes) {
			continue
		}
		return rule.scope, true
	}
	if method == http.MethodGet || method == http.MethodHead {
		return ScopeTaskRead, true
	}
	return ScopeTaskWrite, true
}

func hasSegment(path, seg string) bool {
	for _, part := range strings.Split(path, "/") {
		if part == seg {
			return true
		}
	}
	return false
}

func hasAnySuffix(path string, suffixes []string) bool {
	for _, suf := range suffixes {
		if strings.HasSuffix(path, suf) {
			return true
		}
	}
	return false
}

type APIKeyValidator struct {
	db *sql.DB
}

func NewAPIKeyValidator(db *sql.DB) *APIKeyValidator {
	return &APIKeyValidator{db: db}
}

// Validate returns the authenticated tenant for callers that do not need
// scope information (gRPC interceptor).
func (v *APIKeyValidator) Validate(ctx context.Context, apiKey string) (tenantID string, err error) {
	p, err := v.ValidatePrincipal(ctx, apiKey)
	if err != nil {
		return "", err
	}
	return p.TenantID, nil
}

// ValidatePrincipal authenticates an API key and loads its scopes. Keys whose
// revoked_at is set are rejected. Scopes arrive comma-joined so the query
// stays driver-agnostic under database/sql.
func (v *APIKeyValidator) ValidatePrincipal(ctx context.Context, apiKey string) (Principal, error) {
	if len(apiKey) < 8 {
		return Principal{}, fmt.Errorf("invalid api key format")
	}
	var (
		p         Principal
		scopesRaw string
	)
	err := v.db.QueryRowContext(ctx,
		`SELECT tenant_id, array_to_string(scopes, ',') FROM api_keys WHERE prefix = $1 AND key_hash = $2 AND revoked_at IS NULL`,
		apiKey[:8], hashKey(apiKey),
	).Scan(&p.TenantID, &scopesRaw)
	if err == sql.ErrNoRows {
		return Principal{}, fmt.Errorf("invalid api key")
	}
	if err != nil {
		return Principal{}, fmt.Errorf("lookup api key: %w", err)
	}
	if scopesRaw != "" {
		p.Scopes = strings.Split(scopesRaw, ",")
	}
	p.KeyPrefix = apiKey[:8] + "..."
	return p, nil
}

// HasScope reports whether the authenticated principal (from ctx) carries the
// given scope. Used by the gRPC interceptor.
func (v *APIKeyValidator) HasScope(ctx context.Context, scope string) bool {
	p, ok := PrincipalFromContext(ctx)
	if !ok {
		return false
	}
	return p.HasScope(scope)
}

func Middleware(validator *APIKeyValidator) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			apiKey := extractAPIKey(r)
			if apiKey == "" {
				http.Error(w, `{"error":"missing api key"}`, http.StatusUnauthorized)
				return
			}

			principal, err := validator.ValidatePrincipal(r.Context(), apiKey)
			if err != nil {
				http.Error(w, `{"error":"invalid api key"}`, http.StatusUnauthorized)
				return
			}

			ctx := context.WithValue(r.Context(), TenantCtxKey, principal.TenantID)
			ctx = context.WithValue(ctx, PrincipalCtxKey, principal)
			ctx = context.WithValue(ctx, APIKeyCtxKey, principal.KeyPrefix)
			if actor := strings.TrimSpace(r.Header.Get(ActingUserHeader)); actor != "" {
				ctx = context.WithValue(ctx, ActingUserCtxKey, actor)
			}
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// ScopeGuard enforces RequiredScope against the authenticated principal. It
// must sit inside Middleware on the chain; requests without a principal
// (auth disabled in dev mode) pass straight through.
func ScopeGuard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := PrincipalFromContext(r.Context())
		if ok {
			scope, required := RequiredScope(r.Method, r.URL.Path)
			if required && !principal.HasScope(scope) {
				w.Header().Set("Content-Type", "application/json")
				http.Error(w, fmt.Sprintf(`{"error":"missing required scope %q"}`, scope), http.StatusForbidden)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
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
				http.Error(w, `{"error":"missing authenticated tenant"}`, http.StatusForbidden)
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
