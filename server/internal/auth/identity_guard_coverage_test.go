package auth

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckAgentIdentity(t *testing.T) {
	p := Principal{BoundAgentID: "a1"}
	assert.NoError(t, p.CheckAgentIdentity("a1"))
	assert.Error(t, p.CheckAgentIdentity("a2"))
	assert.NoError(t, p.CheckAgentIdentity(""))

	unbound := Principal{}
	assert.NoError(t, unbound.CheckAgentIdentity("anyone"))
}

func TestGuardAgentIdentity(t *testing.T) {
	ctx := context.WithValue(context.Background(), PrincipalCtxKey, Principal{BoundAgentID: "a1"})
	assert.NoError(t, GuardAgentIdentity(ctx, "a1"))
	assert.Error(t, GuardAgentIdentity(ctx, "a2"))

	emptyCtx := context.Background()
	assert.NoError(t, GuardAgentIdentity(emptyCtx, "anyone"))
}

func TestInspectAgentIdentity(t *testing.T) {
	t.Run("query source_agent", func(t *testing.T) {
		r := httptest.NewRequest("POST", "/x?source_agent=agent1", nil)
		claims, _, _ := InspectAgentIdentity(r)
		assert.Contains(t, claims, "agent1")
	})
	t.Run("query agent_id", func(t *testing.T) {
		r := httptest.NewRequest("POST", "/x?agent_id=agent2", nil)
		claims, _, _ := InspectAgentIdentity(r)
		assert.Contains(t, claims, "agent2")
	})
	t.Run("body top-level", func(t *testing.T) {
		r := httptest.NewRequest("POST", "/x", strings.NewReader(`{"source_agent":"sa"}`))
		claims, _, _ := InspectAgentIdentity(r)
		assert.Contains(t, claims, "sa")
	})
	t.Run("body envelope", func(t *testing.T) {
		r := httptest.NewRequest("POST", "/x", strings.NewReader(`{"envelope":{"source_agent":"env-sa"}}`))
		claims, _, _ := InspectAgentIdentity(r)
		assert.Contains(t, claims, "env-sa")
	})
	t.Run("body metadata", func(t *testing.T) {
		r := httptest.NewRequest("POST", "/x", strings.NewReader(`{"metadata":{"agent_id":"meta-id"}}`))
		claims, _, _ := InspectAgentIdentity(r)
		assert.Contains(t, claims, "meta-id")
	})
	t.Run("id on agents path", func(t *testing.T) {
		r := httptest.NewRequest("POST", "/v1/tenants/acme/agents", strings.NewReader(`{"id":"new-agent"}`))
		claims, _, _ := InspectAgentIdentity(r)
		assert.Contains(t, claims, "new-agent")
	})
	t.Run("id not on agents path", func(t *testing.T) {
		r := httptest.NewRequest("POST", "/v1/tenants/acme/tasks", strings.NewReader(`{"id":"task-1"}`))
		claims, _, _ := InspectAgentIdentity(r)
		assert.NotContains(t, claims, "task-1")
	})
	t.Run("case-variant flagged ambiguous", func(t *testing.T) {
		r := httptest.NewRequest("POST", "/x", strings.NewReader(`{"Source_Agent":"x"}`))
		_, ambiguous, _ := InspectAgentIdentity(r)
		assert.True(t, ambiguous)
	})
	t.Run("oversized flagged", func(t *testing.T) {
		big := bytes.Repeat([]byte("a"), MaxIdentityGuardBody+10)
		r := httptest.NewRequest("POST", "/x", bytes.NewReader(big))
		_, _, oversized := InspectAgentIdentity(r)
		assert.True(t, oversized)
	})
	t.Run("GET body ignored", func(t *testing.T) {
		r := httptest.NewRequest("GET", "/x", strings.NewReader(`{"source_agent":"x"}`))
		claims, _, _ := InspectAgentIdentity(r)
		assert.Empty(t, claims)
	})
}

func TestAgentIdentityMiddleware(t *testing.T) {
	reached := false
	sink := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached = true; w.WriteHeader(200) })

	t.Run("no principal passes", func(t *testing.T) {
		reached = false
		r := httptest.NewRequest("POST", "/x", strings.NewReader(`{"source_agent":"any"}`))
		w := httptest.NewRecorder()
		AgentIdentityMiddleware(sink).ServeHTTP(w, r)
		assert.True(t, reached)
		assert.Equal(t, 200, w.Code)
	})
	t.Run("bound + match passes", func(t *testing.T) {
		reached = false
		r := httptest.NewRequest("POST", "/x", strings.NewReader(`{"source_agent":"me"}`))
		r = r.WithContext(context.WithValue(r.Context(), PrincipalCtxKey, Principal{BoundAgentID: "me"}))
		w := httptest.NewRecorder()
		AgentIdentityMiddleware(sink).ServeHTTP(w, r)
		assert.True(t, reached)
	})
	t.Run("bound + mismatch 403", func(t *testing.T) {
		reached = false
		r := httptest.NewRequest("POST", "/x", strings.NewReader(`{"source_agent":"other"}`))
		r = r.WithContext(context.WithValue(r.Context(), PrincipalCtxKey, Principal{BoundAgentID: "me"}))
		w := httptest.NewRecorder()
		AgentIdentityMiddleware(sink).ServeHTTP(w, r)
		assert.False(t, reached)
		assert.Equal(t, 403, w.Code)
	})
	t.Run("ambiguous 400", func(t *testing.T) {
		reached = false
		r := httptest.NewRequest("POST", "/x", strings.NewReader(`{"Source_Agent":"x"}`))
		r = r.WithContext(context.WithValue(r.Context(), PrincipalCtxKey, Principal{BoundAgentID: "me"}))
		w := httptest.NewRecorder()
		AgentIdentityMiddleware(sink).ServeHTTP(w, r)
		assert.False(t, reached)
		assert.Equal(t, 400, w.Code)
	})
	t.Run("oversized 413", func(t *testing.T) {
		reached = false
		big := bytes.Repeat([]byte("a"), MaxIdentityGuardBody+10)
		r := httptest.NewRequest("POST", "/x", bytes.NewReader(big))
		r = r.WithContext(context.WithValue(r.Context(), PrincipalCtxKey, Principal{BoundAgentID: "me"}))
		w := httptest.NewRecorder()
		AgentIdentityMiddleware(sink).ServeHTTP(w, r)
		assert.False(t, reached)
		assert.Equal(t, 413, w.Code)
	})
	t.Run("body restored for downstream", func(t *testing.T) {
		r := httptest.NewRequest("POST", "/x", strings.NewReader(`{"source_agent":"me"}`))
		r = r.WithContext(context.WithValue(r.Context(), PrincipalCtxKey, Principal{BoundAgentID: "me"}))
		w := httptest.NewRecorder()
		AgentIdentityMiddleware(sink).ServeHTTP(w, r)
		buf := make([]byte, 64)
		n, _ := r.Body.Read(buf)
		require.True(t, n > 0, "body must be readable after middleware")
		assert.Contains(t, string(buf[:n]), "me")
	})
}

func TestWalkIdentity(t *testing.T) {
	doc := map[string]interface{}{
		"source_agent": "a",
		"envelope":     map[string]interface{}{"source_agent": "b"},
		"other":        "not-identity",
	}
	claims, _ := walkIdentity(doc, "", false, nil)
	assert.Contains(t, claims, "a")
	assert.Contains(t, claims, "b")
	assert.Len(t, claims, 2)

	_, ambig := walkIdentity(map[string]interface{}{"X": "1", "x": "2"}, "", false, nil)
	assert.True(t, ambig)
}

func TestIsIdentityRelatedName(t *testing.T) {
	assert.True(t, isIdentityRelatedName("source_agent", false))
	assert.True(t, isIdentityRelatedName("agent_id", false))
	assert.True(t, isIdentityRelatedName("envelope", false))
	assert.True(t, isIdentityRelatedName("metadata", false))
	assert.True(t, isIdentityRelatedName("id", true))
	assert.False(t, isIdentityRelatedName("id", false))
	assert.False(t, isIdentityRelatedName("other", false))
}
