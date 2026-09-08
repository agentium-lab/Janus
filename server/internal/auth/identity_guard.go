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
//	JSON identity paths resolve CASE-INSENSITIVELY, mirroring
//	encoding/json's struct matching — a body that declares an identity via
//	"Envelope"/"SOURCE_AGENT" variant keys is resolved AND flagged: case
//	aliasing of any key is treated as hostile ambiguity and rejected 400.
//
// Enforcement is structural (one HTTP middleware + one gRPC check). The
// service layer additionally overwrites persisted identities from the
// principal (TaskService.Create) so a future extractor gap cannot split
// identities.

// agentIdentityLogicalPaths lists every logical JSON path a caller can use
// to declare an agent identity. Matched case-insensitively; adding a
// carrier requires extending this list and the invariants tests.
var agentIdentityLogicalPaths = map[string]bool{
	"source_agent":          true,
	"agent_id":              true,
	"envelope.source_agent": true,
	"metadata.source_agent": true,
	"metadata.agent_id":     true,
}

// idAsIdentityPathSuffixes: on these paths the body field "id" DECLARES an
// agent identity (agent registration surfaces). Elsewhere "id" is an entity
// id and must not be treated as a claim.
var idAsIdentityPathSuffixes = []string{
	"/agents",
	"/agent/card",
}

// MaxIdentityGuardBody bounds the body the guard inspects; larger bodies
// get 413 from the middleware — never a silent truncation.
const MaxIdentityGuardBody = 1 << 20

const agentIdentityQueryKeys = "source_agent agent_id"

type identityInspection struct {
	claims    []string // resolved identity declarations (case-insensitive)
	ambiguous bool     // case aliasing detected — reject 400
	oversized bool     // body exceeds MaxIdentityGuardBody — reject 413
}

// inspectAgentIdentity resolves every declared identity from query and body.
// The body is restored after reading so downstream handlers are unaffected.
func inspectAgentIdentity(r *http.Request) identityInspection {
	var ins identityInspection
	for _, key := range strings.Fields(agentIdentityQueryKeys) {
		if v := strings.TrimSpace(r.URL.Query().Get(key)); v != "" {
			ins.claims = append(ins.claims, v)
		}
	}
	if r.Body == nil || (r.Method != http.MethodPost && r.Method != http.MethodPut && r.Method != http.MethodPatch) {
		return ins
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, MaxIdentityGuardBody+1))
	r.Body = io.NopCloser(bytes.NewReader(body))
	if err != nil {
		return ins
	}
	if len(body) > MaxIdentityGuardBody {
		ins.oversized = true
		r.Body = http.NoBody
		return ins
	}
	if len(body) == 0 {
		return ins
	}
	var doc map[string]interface{}
	if json.Unmarshal(body, &doc) != nil {
		return ins
	}
	idIsIdentity := false
	for _, suffix := range idAsIdentityPathSuffixes {
		if strings.HasSuffix(r.URL.Path, suffix) {
			idIsIdentity = true
			break
		}
	}
	ins.claims, ins.ambiguous = walkIdentity(doc, "", idIsIdentity, ins.claims)
	return ins
}

// walkIdentity collects identity strings with case-insensitive path
// matching and flags case-aliased keys (same lowercase form, different
// physical spellings) at any level.
func walkIdentity(node map[string]interface{}, prefix string, idIsIdentity bool, claims []string) ([]string, bool) {
	ambiguous := false
	lowerCounts := make(map[string]int, len(node))
	for key, val := range node {
		lower := strings.ToLower(key)
		lowerCounts[lower]++
		// Identity-related keys must be spelled exactly as the canonical
		// JSON tags: "Envelope"/"SOURCE_AGENT" resolve (case-insensitive
		// matching keeps us safe) but are flagged as hostile ambiguity —
		// legitimate clients never case-variant these names.
		if key != lower && isIdentityRelatedName(lower, idIsIdentity) {
			ambiguous = true
		}
		if sub, ok := val.(map[string]interface{}); ok {
			path := lower
			if prefix != "" {
				path = prefix + "." + path
			}
			var subAmbig bool
			claims, subAmbig = walkIdentity(sub, path, idIsIdentity, claims)
			if subAmbig {
				ambiguous = true
			}
			continue
		}
		str, ok := val.(string)
		if !ok || str == "" {
			continue
		}
		path := lower
		if prefix != "" {
			path = prefix + "." + path
		}
		if agentIdentityLogicalPaths[path] || (idIsIdentity && path == "id") {
			claims = append(claims, str)
		}
	}
	for _, n := range lowerCounts {
		if n > 1 {
			ambiguous = true
		}
	}
	return claims, ambiguous
}

// InspectAgentIdentity is the test-facing entry for the invariant suite.
func InspectAgentIdentity(r *http.Request) (claims []string, ambiguous, oversized bool) {
	ins := inspectAgentIdentity(r)
	return ins.claims, ins.ambiguous, ins.oversized
}

func isIdentityRelatedName(lower string, idIsIdentity bool) bool {
	switch lower {
	case "source_agent", "agent_id", "envelope", "metadata":
		return true
	case "id":
		return idIsIdentity
	}
	return false
}

// CheckAgentIdentityRequest enforces the invariant for one request,
// transport-agnostic so HTTP middleware and gRPC share the decision logic.
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
		ins := inspectAgentIdentity(r)
		switch {
		case ins.oversized:
			http.Error(w, `{"error":"request body exceeds limit","code":"PAYLOAD_TOO_LARGE","status":413}`, http.StatusRequestEntityTooLarge)
			return
		case ins.ambiguous:
			http.Error(w, `{"error":"case-aliased JSON keys are not accepted","code":"INVALID_ARGUMENT","status":400}`, http.StatusBadRequest)
			return
		}
		if err := CheckAgentIdentityRequest(p.BoundAgentID, ins.claims); err != nil {
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
