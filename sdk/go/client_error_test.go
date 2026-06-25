package janus

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func errServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"internal"}`))
	}))
}

func TestClient_GetTenant_Error(t *testing.T) {
	s := errServer()
	defer s.Close()
	c := NewClient(Config{BaseURL: s.URL, TenantID: "t"})
	_, err := c.GetTenant(context.Background(), "t1")
	require.Error(t, err)
}

func TestClient_ListAgents_Error(t *testing.T) {
	s := errServer()
	defer s.Close()
	c := NewClient(Config{BaseURL: s.URL, TenantID: "t"})
	_, err := c.ListAgents(context.Background())
	require.Error(t, err)
}

func TestClient_GetAgent_Error(t *testing.T) {
	s := errServer()
	defer s.Close()
	c := NewClient(Config{BaseURL: s.URL, TenantID: "t"})
	_, err := c.GetAgent(context.Background(), "a1")
	require.Error(t, err)
}

func TestClient_ReplayTask_Error(t *testing.T) {
	s := errServer()
	defer s.Close()
	c := NewClient(Config{BaseURL: s.URL, TenantID: "t"})
	_, err := c.ReplayTask(context.Background(), "task-1")
	require.Error(t, err)
}

func TestClient_CreateAPIKey_Error(t *testing.T) {
	s := errServer()
	defer s.Close()
	c := NewClient(Config{BaseURL: s.URL, TenantID: "t"})
	_, err := c.CreateAPIKey(context.Background(), CreateAPIKeyRequest{Name: "k"})
	require.Error(t, err)
}

func TestClient_ListAPIKeys_Error(t *testing.T) {
	s := errServer()
	defer s.Close()
	c := NewClient(Config{BaseURL: s.URL, TenantID: "t"})
	_, err := c.ListAPIKeys(context.Background())
	require.Error(t, err)
}

func TestClient_RevokeAPIKey_Error(t *testing.T) {
	s := errServer()
	defer s.Close()
	c := NewClient(Config{BaseURL: s.URL, TenantID: "t"})
	_, err := c.RevokeAPIKey(context.Background(), "k1")
	require.Error(t, err)
}

func TestClient_CreatePolicyRule_Error(t *testing.T) {
	s := errServer()
	defer s.Close()
	c := NewClient(Config{BaseURL: s.URL, TenantID: "t"})
	_, err := c.CreatePolicyRule(context.Background(), CreatePolicyRuleRequest{})
	require.Error(t, err)
}

func TestClient_CreatePolicyRuleFromTemplate_Error(t *testing.T) {
	s := errServer()
	defer s.Close()
	c := NewClient(Config{BaseURL: s.URL, TenantID: "t"})
	_, err := c.CreatePolicyRuleFromTemplate(context.Background(), PolicyRuleTemplateRequest{})
	require.Error(t, err)
}

func TestClient_ListPolicyRules_Error(t *testing.T) {
	s := errServer()
	defer s.Close()
	c := NewClient(Config{BaseURL: s.URL, TenantID: "t"})
	_, err := c.ListPolicyRules(context.Background())
	require.Error(t, err)
}

func TestClient_UpsertBudget_Error(t *testing.T) {
	s := errServer()
	defer s.Close()
	c := NewClient(Config{BaseURL: s.URL, TenantID: "t"})
	_, err := c.UpsertBudget(context.Background(), BudgetRequest{})
	require.Error(t, err)
}

func TestClient_GetBudget_Error(t *testing.T) {
	s := errServer()
	defer s.Close()
	c := NewClient(Config{BaseURL: s.URL, TenantID: "t"})
	_, err := c.GetBudget(context.Background(), "tenant", "t")
	require.Error(t, err)
}

func TestClient_ListBudgets_Error(t *testing.T) {
	s := errServer()
	defer s.Close()
	c := NewClient(Config{BaseURL: s.URL, TenantID: "t"})
	_, err := c.ListBudgets(context.Background())
	require.Error(t, err)
}

func TestClient_CreateMailboxWithConfig_Error(t *testing.T) {
	s := errServer()
	defer s.Close()
	c := NewClient(Config{BaseURL: s.URL, TenantID: "t"})
	_, err := c.CreateMailboxWithConfig(context.Background(), CreateMailboxRequest{})
	require.Error(t, err)
}

func TestClient_GetMailbox_Error(t *testing.T) {
	s := errServer()
	defer s.Close()
	c := NewClient(Config{BaseURL: s.URL, TenantID: "t"})
	_, err := c.GetMailbox(context.Background(), "mb1")
	require.Error(t, err)
}

func TestClient_GetTask_Error(t *testing.T) {
	s := errServer()
	defer s.Close()
	c := NewClient(Config{BaseURL: s.URL, TenantID: "t"})
	_, err := c.GetTask(context.Background(), "task-1")
	require.Error(t, err)
}

func TestClient_GetTaskEvents_Error(t *testing.T) {
	s := errServer()
	defer s.Close()
	c := NewClient(Config{BaseURL: s.URL, TenantID: "t"})
	_, err := c.GetTaskEvents(context.Background(), "task-1")
	require.Error(t, err)
}

func TestClient_ReplayDLQ_Error(t *testing.T) {
	s := errServer()
	defer s.Close()
	c := NewClient(Config{BaseURL: s.URL, TenantID: "t"})
	_, err := c.ReplayDLQ(context.Background(), "task-1")
	require.Error(t, err)
}

func TestClient_DiscardDLQ_Error(t *testing.T) {
	s := errServer()
	defer s.Close()
	c := NewClient(Config{BaseURL: s.URL, TenantID: "t"})
	err := c.DiscardDLQ(context.Background(), "task-1")
	require.Error(t, err)
}

func TestClient_QueryDLQ_Error(t *testing.T) {
	s := errServer()
	defer s.Close()
	c := NewClient(Config{BaseURL: s.URL, TenantID: "t"})
	_, err := c.QueryDLQ(context.Background(), DLQQueryOptions{})
	require.Error(t, err)
}
