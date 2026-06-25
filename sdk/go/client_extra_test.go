package janus

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestClientMux(handler http.HandlerFunc) *Client {
	srv := httptest.NewServer(handler)
	return NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"})
}

func TestClient_GetTenant(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/tenants/test-tenant", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"id":"test-tenant"}`))
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"})

	tenant, err := c.GetTenant(context.Background(), "test-tenant")
	require.NoError(t, err)
	assert.Equal(t, "test-tenant", tenant.ID)
}

func TestClient_ListAgents(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/tenants/test-tenant/agents", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"agents":[{"id":"a1"},{"id":"a2"}]}`))
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"})

	agents, err := c.ListAgents(context.Background())
	require.NoError(t, err)
	assert.Len(t, agents, 2)
	assert.Equal(t, "a1", agents[0].ID)
}

func TestClient_GetAgent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "/agents/a1")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"id":"a1","display_name":"Agent One"}`))
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"})

	agent, err := c.GetAgent(context.Background(), "a1")
	require.NoError(t, err)
	assert.Equal(t, "a1", agent.ID)
}

func TestClient_HeartbeatAgent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "/heartbeat")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"})

	err := c.HeartbeatAgent(context.Background(), "a1")
	require.NoError(t, err)
}

func TestClient_ReplayTask(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "/replay")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"id":"task-1","status":"queued"}`))
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"})

	task, err := c.ReplayTask(context.Background(), "task-1")
	require.NoError(t, err)
	assert.Equal(t, "task-1", task.ID)
}

func TestClient_CreateAPIKey(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "/api-keys")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"id":"key-1","key":"janus_secret_xxx","name":"ci"}`))
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"})

	key, err := c.CreateAPIKey(context.Background(), CreateAPIKeyRequest{Name: "ci"})
	require.NoError(t, err)
	assert.Equal(t, "key-1", key.ID)
	assert.Equal(t, "janus_secret_xxx", key.Key)
}

func TestClient_ListAPIKeys(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"api_keys":[{"id":"k1","name":"ci"}]}`))
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"})

	keys, err := c.ListAPIKeys(context.Background())
	require.NoError(t, err)
	require.Len(t, keys, 1)
	assert.Equal(t, "k1", keys[0].ID)
}

func TestClient_RevokeAPIKey(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "/revoke")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"id":"k1","name":"ci","status":"revoked"}`))
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"})

	key, err := c.RevokeAPIKey(context.Background(), "k1")
	require.NoError(t, err)
	assert.Equal(t, "k1", key.ID)
}

func TestClient_CreatePolicyRule(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "/policy-rules")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"id":"rule-1","name":"test"}`))
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"})

	rule, err := c.CreatePolicyRule(context.Background(), CreatePolicyRuleRequest{Name: "test"})
	require.NoError(t, err)
	assert.Equal(t, "rule-1", rule.ID)
}

func TestClient_CreatePolicyRuleFromTemplate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "/policy-rules/templates")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"id":"rule-tpl-1"}`))
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"})

	rule, err := c.CreatePolicyRuleFromTemplate(context.Background(), PolicyRuleTemplateRequest{Template: "allow_agent_capability"})
	require.NoError(t, err)
	assert.Equal(t, "rule-tpl-1", rule.ID)
}

func TestClient_ListPolicyRules(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"policy_rules":[{"id":"r1"}]}`))
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"})

	rules, err := c.ListPolicyRules(context.Background())
	require.NoError(t, err)
	require.Len(t, rules, 1)
	assert.Equal(t, "r1", rules[0].ID)
}

func TestClient_UpsertBudget(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "/budgets")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"tenant_id":"test-tenant","scope_type":"tenant","scope_id":"test-tenant","max_concurrency":10}`))
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"})

	budget, err := c.UpsertBudget(context.Background(), BudgetRequest{ScopeType: "tenant", MaxConcurrency: 10})
	require.NoError(t, err)
	assert.Equal(t, "test-tenant", budget.TenantID)
	assert.Equal(t, 10, budget.MaxConcurrency)
}

func TestClient_GetBudget(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "/budgets/tenant")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"tenant_id":"test-tenant","scope_type":"tenant","scope_id":"test-tenant","max_concurrency":5}`))
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"})

	budget, err := c.GetBudget(context.Background(), "tenant", "test-tenant")
	require.NoError(t, err)
	assert.Equal(t, 5, budget.MaxConcurrency)
}

func TestClient_ListBudgets(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"budgets":[{"tenant_id":"test-tenant","scope_type":"tenant","scope_id":"test-tenant"}]}`))
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"})

	budgets, err := c.ListBudgets(context.Background())
	require.NoError(t, err)
	require.Len(t, budgets, 1)
	assert.Equal(t, "test-tenant", budgets[0].TenantID)
}

func TestClient_DiscardDLQ(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "/discard")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"})

	err := c.DiscardDLQ(context.Background(), "task-1")
	require.NoError(t, err)
}

func TestClient_ReplayDLQ(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "/replay")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"id":"task-1","status":"created"}`))
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"})

	task, err := c.ReplayDLQ(context.Background(), "task-1")
	require.NoError(t, err)
	assert.Equal(t, "task-1", task.ID)
}

func TestClient_GetMailbox(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"id":"mb-1","tenant_id":"test-tenant","agent_id":"a1"}`))
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"})

	mb, err := c.GetMailbox(context.Background(), "mb-1")
	require.NoError(t, err)
	assert.Equal(t, "mb-1", mb.ID)
}

func TestClient_QueryDLQ(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "/dlq")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"tasks":[{"id":"task-1","status":"dead_lettered"}]}`))
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "test-tenant"})

	tasks, err := c.QueryDLQ(context.Background(), DLQQueryOptions{MailboxID: "mb-1"})
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, "task-1", tasks[0].ID)
}
