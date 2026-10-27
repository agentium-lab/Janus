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

	// ScopePlatformAdmin gates control-plane operations that affect the
	// platform itself (tenant create/list). It is deliberately NOT implied
	// by ScopeAdmin: a tenant administrator must never manage other
	// tenants, so platform authority has to be granted explicitly.
	ScopePlatformAdmin = "platform:admin"
)

type Principal struct {
	TenantID     string
	Scopes       []string
	KeyPrefix    string
	BoundAgentID string
}

// CheckAgentIdentity enforces that a request claiming to act as claimedAgent
// matches the key's bound agent. Keys without a bound agent are unrestricted
// (backward compatible); bound keys may only act as themselves, so a leaked
// scoped key cannot impersonate other agents for attribution or progress.
func (p Principal) CheckAgentIdentity(claimedAgent string) error {
	if p.BoundAgentID == "" || claimedAgent == "" {
		return nil
	}
	if p.BoundAgentID != claimedAgent {
		return fmt.Errorf("api key is bound to agent %q and cannot act as %q", p.BoundAgentID, claimedAgent)
	}
	return nil
}

func (p Principal) HasScope(scope string) bool {
	// Legacy keys minted before scopes existed carry an empty set and keep
	// full DATA-PLANE access for compatibility — but they must NEVER
	// exercise platform authority: a tenant admin could mint an empty-scope
	// key and use it to reach the /v1/tenants control plane otherwise.
	if len(p.Scopes) == 0 {
		return scope != ScopePlatformAdmin
	}
	for _, s := range p.Scopes {
		if s == ScopeAdmin && scope != ScopePlatformAdmin {
			return true
		}
		if s == scope {
			return true
		}
	}
	return false
}

// GuardAgentIdentity enforces the caller's bound identity against a claimed
// agent. No principal in context (auth disabled / internal call) passes; an
// unbound principal passes (backward compatible); a bound principal may only
// act as the agent it is bound to. Every entrypoint that accepts an
// agent identity from the caller MUST call this.
func GuardAgentIdentity(ctx context.Context, claimedAgent string) error {
	p, ok := PrincipalFromContext(ctx)
	if !ok {
		return nil
	}
	return p.CheckAgentIdentity(claimedAgent)
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

// TenantRoute is one entry of the tenant-subtree route table: the method and
// net/http ServeMux pattern (Go 1.22+ syntax) of a documented route plus the
// scope that route demands. The table is the single source of truth shared
// by HTTP dispatch (cmd/janus-api/router.go) and RequiredScope below, so the
// scope a request needs can never disagree with the handler it reaches.
// Shapes absent from the table are undefined: no scope is required and the
// router rejects them with 404 before any handler runs.
type TenantRoute struct {
	Method  string
	Pattern string
	Scope   string
}

// TenantRoutes enumerates every canonical route under /v1/tenants/{tenantID}
// per docs/Janus-api-contract.md. Admin rows are governance and destructive
// operations; audit:read rows are trace/event reads; the rest follow the
// data-plane read/write split.
var TenantRoutes = []TenantRoute{
	// tenant
	{http.MethodGet, "/v1/tenants/{tenantID}", ScopeTaskRead},
	// agents
	{http.MethodPost, "/v1/tenants/{tenantID}/agents", ScopeTaskWrite},
	{http.MethodGet, "/v1/tenants/{tenantID}/agents", ScopeTaskRead},
	{http.MethodGet, "/v1/tenants/{tenantID}/agents/{agentID}", ScopeTaskRead},
	{http.MethodPost, "/v1/tenants/{tenantID}/agents/{agentID}/heartbeat", ScopeTaskWrite},
	// mailboxes
	{http.MethodPost, "/v1/tenants/{tenantID}/mailboxes", ScopeTaskWrite},
	{http.MethodGet, "/v1/tenants/{tenantID}/mailboxes/{mailboxID}", ScopeTaskRead},
	{http.MethodPatch, "/v1/tenants/{tenantID}/mailboxes/{mailboxID}", ScopeTaskWrite},
	{http.MethodPost, "/v1/tenants/{tenantID}/mailboxes/{mailboxID}/pause", ScopeTaskWrite},
	{http.MethodPost, "/v1/tenants/{tenantID}/mailboxes/{mailboxID}/resume", ScopeTaskWrite},
	{http.MethodPost, "/v1/tenants/{tenantID}/mailboxes/{mailboxID}/pull", ScopeTaskWrite},
	// dead letter queue
	{http.MethodGet, "/v1/tenants/{tenantID}/dlq", ScopeTaskRead},
	{http.MethodPost, "/v1/tenants/{tenantID}/dlq/{taskID}/replay", ScopeAdmin},
	{http.MethodPost, "/v1/tenants/{tenantID}/dlq/{taskID}/discard", ScopeAdmin},
	// tasks
	{http.MethodPost, "/v1/tenants/{tenantID}/tasks", ScopeTaskWrite},
	{http.MethodGet, "/v1/tenants/{tenantID}/tasks/{taskID}", ScopeTaskRead},
	{http.MethodPost, "/v1/tenants/{tenantID}/tasks/{taskID}/start", ScopeTaskWrite},
	{http.MethodPost, "/v1/tenants/{tenantID}/tasks/{taskID}/heartbeat", ScopeTaskWrite},
	{http.MethodPost, "/v1/tenants/{tenantID}/tasks/{taskID}/ack", ScopeTaskWrite},
	{http.MethodPost, "/v1/tenants/{tenantID}/tasks/{taskID}/nack", ScopeTaskWrite},
	{http.MethodPost, "/v1/tenants/{tenantID}/tasks/{taskID}/cancel", ScopeTaskWrite},
	{http.MethodPost, "/v1/tenants/{tenantID}/tasks/{taskID}/block", ScopeTaskWrite},
	{http.MethodPost, "/v1/tenants/{tenantID}/tasks/{taskID}/unblock", ScopeTaskWrite},
	{http.MethodPost, "/v1/tenants/{tenantID}/tasks/{taskID}/replay", ScopeTaskWrite},
	// Legacy force-status endpoints bypass the dispatch lease protocol:
	// unlike /dispatch/ack|nack they carry no lease CAS, so they are gated
	// to admin instead of any task:write key.
	{http.MethodPost, "/v1/tenants/{tenantID}/tasks/{taskID}/complete", ScopeAdmin},
	{http.MethodPost, "/v1/tenants/{tenantID}/tasks/{taskID}/fail", ScopeAdmin},
	{http.MethodPost, "/v1/tenants/{tenantID}/tasks/{taskID}/progress", ScopeTaskWrite},
	{http.MethodGet, "/v1/tenants/{tenantID}/tasks/{taskID}/stream", ScopeTaskRead},
	{http.MethodGet, "/v1/tenants/{tenantID}/tasks/{taskID}/events", ScopeTaskRead},
	{http.MethodGet, "/v1/tenants/{tenantID}/tasks/{taskID}/traces", ScopeAuditRead},
	{http.MethodGet, "/v1/tenants/{tenantID}/traces/{traceID}", ScopeAuditRead},
	{http.MethodGet, "/v1/tenants/{tenantID}/events", ScopeTaskRead},
	{http.MethodGet, "/v1/tenants/{tenantID}/catalog", ScopeTaskRead},
	// audit and outbox recovery
	{http.MethodPost, "/v1/tenants/{tenantID}/audit/replay", ScopeAdmin},
	{http.MethodPost, "/v1/tenants/{tenantID}/outbox/retry-dead", ScopeAdmin},
	// governance management
	{http.MethodPost, "/v1/tenants/{tenantID}/policy-rules", ScopeAdmin},
	{http.MethodGet, "/v1/tenants/{tenantID}/policy-rules", ScopeAdmin},
	{http.MethodPost, "/v1/tenants/{tenantID}/policy-rules/templates", ScopeAdmin},
	{http.MethodPost, "/v1/tenants/{tenantID}/budgets", ScopeAdmin},
	{http.MethodGet, "/v1/tenants/{tenantID}/budgets", ScopeAdmin},
	{http.MethodGet, "/v1/tenants/{tenantID}/budgets/{scopeType}", ScopeAdmin},
	{http.MethodGet, "/v1/tenants/{tenantID}/budgets/{scopeType}/{scopeID}", ScopeAdmin},
	{http.MethodPost, "/v1/tenants/{tenantID}/api-keys", ScopeAdmin},
	{http.MethodGet, "/v1/tenants/{tenantID}/api-keys", ScopeAdmin},
	{http.MethodPost, "/v1/tenants/{tenantID}/api-keys/{keyID}/revoke", ScopeAdmin},
	// approvals: decisions settle an approval, an operator-level privilege
	{http.MethodPost, "/v1/tenants/{tenantID}/approvals", ScopeTaskWrite},
	{http.MethodGet, "/v1/tenants/{tenantID}/approvals", ScopeTaskRead},
	{http.MethodGet, "/v1/tenants/{tenantID}/approvals/{approvalID}", ScopeTaskRead},
	{http.MethodPost, "/v1/tenants/{tenantID}/approvals/{approvalID}/approve", ScopeAdmin},
	{http.MethodPost, "/v1/tenants/{tenantID}/approvals/{approvalID}/reject", ScopeAdmin},
	// context refs
	{http.MethodPost, "/v1/tenants/{tenantID}/context-refs", ScopeTaskWrite},
	{http.MethodGet, "/v1/tenants/{tenantID}/context-refs/{refID}", ScopeTaskRead},
	{http.MethodDelete, "/v1/tenants/{tenantID}/context-refs/{refID}", ScopeTaskWrite},
	{http.MethodGet, "/v1/tenants/{tenantID}/tasks/{taskID}/context-refs", ScopeTaskRead},
}

var (
	tenantScopeMux      *http.ServeMux
	tenantScopeByTarget map[string]string
)

func init() {
	tenantScopeMux = http.NewServeMux()
	tenantScopeByTarget = make(map[string]string, len(TenantRoutes))
	stub := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	for _, rt := range TenantRoutes {
		target := rt.Method + " " + rt.Pattern
		tenantScopeMux.Handle(target, stub)
		tenantScopeByTarget[target] = rt.Scope
	}
}

// TenantRouteScope resolves the scope a tenant-subtree request demands by
// matching method+path against the same ServeMux patterns the router
// dispatches on. ok=false marks an undefined shape: no scope is required
// because the router 404s the request before any handler runs.
func TenantRouteScope(method, path string) (string, bool) {
	if p := normalizeTenantSubtreePath(path); p != path {
		path = p
	}
	req, err := http.NewRequest(method, path, nil)
	if err != nil {
		return "", false
	}
	_, target := tenantScopeMux.Handler(req)
	scope, ok := tenantScopeByTarget[target]
	return scope, ok
}

// normalizeTenantSubtreePath trims trailing slashes inside the tenant
// subtree so "POST /v1/tenants/t/tasks/" and "POST /v1/tenants/t/tasks"
// resolve to the same route in the scope lookup and in the router. The bare
// "/v1/tenants/" prefix root stays untouched (it is 404 by contract).
func normalizeTenantSubtreePath(path string) string {
	if !strings.HasPrefix(path, "/v1/tenants/") || path == "/v1/tenants/" {
		return path
	}
	trimmed := strings.TrimRight(path, "/")
	if trimmed == "" || trimmed == "/v1/tenants" {
		return path
	}
	return trimmed
}

// RequiredScope resolves which scope a request demands. ok=false marks paths
// outside the versioned data-plane API (probes, non-gateway paths) and
// undefined tenant-subtree shapes: the latter never reach a handler because
// the route table 404s them.
func RequiredScope(method, path string) (scope string, ok bool) {
	if path == "/v1/tenants" {
		return ScopePlatformAdmin, true // tenant management is the platform control plane
	}
	// The WS event stream is a data-plane READ of the tenant event plane:
	// subscribing requires task:read. Legacy empty-scope keys keep access
	// via Principal.HasScope; the WS handler re-checks the same scope
	// before upgrading.
	if path == "/ws" {
		return ScopeTaskRead, true
	}
	if strings.HasPrefix(path, "/v1/tenants/") {
		return TenantRouteScope(method, path)
	}
	if !strings.HasPrefix(path, "/a2a/") &&
		!strings.HasPrefix(path, "/mcp") &&
		!strings.HasPrefix(path, "/acp/") {
		return "", false
	}
	// Gateway prefixes (/a2a/, /mcp, /acp/) carry the method-default scope;
	// each gateway re-checks with protocol-level detail behind its own
	// surface.
	if method == http.MethodGet || method == http.MethodHead {
		return ScopeTaskRead, true
	}
	return ScopeTaskWrite, true
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
		`SELECT tenant_id, array_to_string(scopes, ','), bound_agent_id FROM api_keys WHERE prefix = $1 AND key_hash = $2 AND revoked_at IS NULL`,
		apiKey[:8], hashKey(apiKey),
	).Scan(&p.TenantID, &scopesRaw, &p.BoundAgentID)
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

// InjectPrincipal validates the key and stores the Principal in ctx.
func (v *APIKeyValidator) InjectPrincipal(ctx context.Context, apiKey string) context.Context {
	p, err := v.ValidatePrincipal(ctx, apiKey)
	if err != nil {
		return ctx
	}
	return context.WithValue(ctx, PrincipalCtxKey, p)
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

// PrincipalValidator is the auth dependency of Middleware. *APIKeyValidator
// satisfies it in production; tests inject an in-memory implementation to
// exercise the REAL middleware chain instead of a re-implementation.
type PrincipalValidator interface {
	ValidatePrincipal(ctx context.Context, apiKey string) (Principal, error)
}

func Middleware(validator PrincipalValidator) func(http.Handler) http.Handler {
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
