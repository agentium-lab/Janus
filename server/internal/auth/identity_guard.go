package auth

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

// IDENTITY INVARIANT (the contract this file enforces):
//
//	Any caller-declared agent identity — wherever it appears: query string,
//	JSON body top level, nested envelope, or metadata — must equal the
//	principal's BoundAgentID when the principal has one. Any mismatch is a
//	403 before any handler or side effect runs.
//
// Enforcement is structural: one middleware on the HTTP path and one check
// in the gRPC auth interceptor. Individual handlers MUST NOT be relied on
// to remember this; the per-entrypoint guards that exist are defense in
// depth, not the boundary.

const agentIdentityQueryKeys = "source_agent agent_id"

// agentIdentityBodyPaths lists every JSON path a caller can use to declare
// an agent identity. Adding a new carrier field requires extending this
// list AND the extractor tests, which enumerate each carrier explicitly.
var agentIdentityBodyPaths = []string{
	"source_agent",
	"agent_id",
	"envelope.source_agent",
	"metadata.source_agent",
	"metadata.agent_id",
}

// idAsIdentityPathSuffixes: on these paths the body field "id" DECLARES an
// agent identity (agent registration surfaces). Elsewhere "id" is an entity
// id (tasks etc.) and must NOT be treated as an identity claim — the table
// in the invariants suite covers both directions.
var idAsIdentityPathSuffixes = []string{
	"/agents",
	"/agent/card",
}

// ExtractAgentIdentity returns every non-empty agent identity the request
// declares across query and JSON body carriers. The body is restored after
// reading so downstream handlers are unaffected.
func ExtractAgentIdentity(r *http.Request) []string {
	var claims []string
	for _, key := range strings.Fields(agentIdentityQueryKeys) {
		if v := strings.TrimSpace(r.URL.Query().Get(key)); v != "" {
			claims = append(claims, v)
		}
	}
	if r.Body == nil || (r.Method != http.MethodPost && r.Method != http.MethodPut && r.Method != http.MethodPatch) {
		return claims
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	r.Body = io.NopCloser(bytes.NewReader(body))
	if err != nil || len(body) == 0 {
		return claims
	}
	var doc map[string]interface{}
	if json.Unmarshal(body, &doc) != nil {
		return claims
	}
	for _, path := range agentIdentityBodyPaths {
		if v, ok := lookupPath(doc, path); ok && v != "" {
			claims = append(claims, v)
		}
	}
	for _, suffix := range idAsIdentityPathSuffixes {
		if strings.HasSuffix(r.URL.Path, suffix) {
			if v, ok := lookupPath(doc, "id"); ok && v != "" {
				claims = append(claims, v)
			}
			break
		}
	}
	return claims
}

func lookupPath(doc map[string]interface{}, path string) (string, bool) {
	parts := strings.SplitN(path, ".", 2)
	cur, ok := doc[parts[0]]
	if !ok {
		return "", false
	}
	if len(parts) == 2 {
		sub, ok := cur.(map[string]interface{})
		if !ok {
			return "", false
		}
		cur, ok = sub[parts[1]]
		if !ok {
			return "", false
		}
	}
	s, ok := cur.(string)
	return s, ok
}

// CheckAgentIdentityRequest enforces the invariant for one request. It is
// transport-agnostic so the HTTP middleware and the gRPC interceptor share
// the exact same decision logic.
func CheckAgentIdentityRequest(boundAgent string, claims []string) error {
	if boundAgent == "" {
		return nil
	}
	for _, c := range claims {
		if err := (Principal{BoundAgentID: boundAgent}).CheckAgentIdentity(c); err != nil {
			return err
		}
	}
	return nil
}

// AgentIdentityMiddleware enforces the identity invariant on every HTTP
// request. Mount it AFTER auth.Middleware so the principal is present.
func AgentIdentityMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := PrincipalFromContext(r.Context())
		if !ok {
			next.ServeHTTP(w, r)
			return
		}
		if err := CheckAgentIdentityRequest(p.BoundAgentID, ExtractAgentIdentity(r)); err != nil {
			writeIdentityRejection(w, err)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeIdentityRejection(w http.ResponseWriter, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusForbidden)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error":   err.Error(),
		"code":    "PERMISSION_DENIED",
		"message": err.Error(),
		"status":  "403",
	})
}
