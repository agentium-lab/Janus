package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

// --- APIKey handler ---

type fakeAPIKeySvc struct {
	createErr error
	listErr   error
	revokeErr error

	created   core.APIKey
	rawKey    string
	keys      []core.APIKey
	revoked   *core.APIKey
	gotTenant string
	gotName   string
	gotScopes []string
}

func (f *fakeAPIKeySvc) Create(_ context.Context, tenantID, name string, scopes []string) (core.APIKey, string, error) {
	if f.createErr != nil {
		return core.APIKey{}, "", f.createErr
	}
	f.gotTenant, f.gotName, f.gotScopes = tenantID, name, scopes
	return f.created, f.rawKey, nil
}

func (f *fakeAPIKeySvc) List(_ context.Context, tenantID string) ([]core.APIKey, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.keys, nil
}

func (f *fakeAPIKeySvc) Revoke(_ context.Context, tenantID, keyID string) (*core.APIKey, error) {
	if f.revokeErr != nil {
		return nil, f.revokeErr
	}
	return f.revoked, nil
}

func TestAPIKeyHandler_Create(t *testing.T) {
	svc := &fakeAPIKeySvc{
		created: core.APIKey{ID: "key-1", TenantID: "acme", Name: "ci-key"},
		rawKey:  "janus_raw_secret",
	}
	h := NewAPIKeyHandler(svc)

	body := `{"name":"ci-key","scopes":["tasks:read","tasks:write"]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/api-keys", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.Create(w, req)

	require.Equal(t, http.StatusCreated, w.Code)
	var resp struct {
		core.APIKey
		Key string `json:"key"`
	}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "janus_raw_secret", resp.Key)
	assert.Equal(t, "key-1", resp.ID)
	assert.Equal(t, "acme", svc.gotTenant)
	assert.Equal(t, "ci-key", svc.gotName)
	assert.Equal(t, []string{"tasks:read", "tasks:write"}, svc.gotScopes)
}

func TestAPIKeyHandler_Create_BadJSON(t *testing.T) {
	h := NewAPIKeyHandler(&fakeAPIKeySvc{})

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/api-keys", strings.NewReader("not-json"))
	w := httptest.NewRecorder()
	h.Create(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	var env errorEnvelope
	require.NoError(t, json.NewDecoder(w.Body).Decode(&env))
	assert.Equal(t, "INVALID_ARGUMENT", env.Code)
}

func TestAPIKeyHandler_Create_ServiceError(t *testing.T) {
	h := NewAPIKeyHandler(&fakeAPIKeySvc{createErr: fmt.Errorf("too many keys")})

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/api-keys", strings.NewReader(`{"name":"k"}`))
	w := httptest.NewRecorder()
	h.Create(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestAPIKeyHandler_List(t *testing.T) {
	svc := &fakeAPIKeySvc{keys: []core.APIKey{{ID: "key-1"}, {ID: "key-2"}}}
	h := NewAPIKeyHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/api-keys", nil)
	w := httptest.NewRecorder()
	h.List(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var resp map[string][]core.APIKey
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Len(t, resp["api_keys"], 2)
}

func TestAPIKeyHandler_List_Error(t *testing.T) {
	h := NewAPIKeyHandler(&fakeAPIKeySvc{listErr: fmt.Errorf("db error")})

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/api-keys", nil)
	w := httptest.NewRecorder()
	h.List(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestAPIKeyHandler_Revoke(t *testing.T) {
	now := core.APIKey{ID: "key-1", TenantID: "acme"}
	svc := &fakeAPIKeySvc{revoked: &now}
	h := NewAPIKeyHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/api-keys/key-1/revoke", nil)
	w := httptest.NewRecorder()
	h.Revoke(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var got core.APIKey
	require.NoError(t, json.NewDecoder(w.Body).Decode(&got))
	assert.Equal(t, "key-1", got.ID)
}

func TestAPIKeyHandler_Revoke_MissingKeyID(t *testing.T) {
	h := NewAPIKeyHandler(&fakeAPIKeySvc{})

	req := httptest.NewRequest(http.MethodPost, "/revoke", nil)
	w := httptest.NewRecorder()
	h.Revoke(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	var env errorEnvelope
	require.NoError(t, json.NewDecoder(w.Body).Decode(&env))
	assert.Equal(t, "INVALID_ARGUMENT", env.Code)
	assert.Contains(t, env.Message, "missing key id")
}

func TestAPIKeyHandler_Revoke_ServiceError(t *testing.T) {
	h := NewAPIKeyHandler(&fakeAPIKeySvc{revokeErr: fmt.Errorf("db error")})

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/api-keys/key-1/revoke", nil)
	w := httptest.NewRecorder()
	h.Revoke(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestAPIKeyHandler_Revoke_NotFound(t *testing.T) {
	h := NewAPIKeyHandler(&fakeAPIKeySvc{revoked: nil})

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/api-keys/key-1/revoke", nil)
	w := httptest.NewRecorder()
	h.Revoke(w, req)

	require.Equal(t, http.StatusNotFound, w.Code)
	var env errorEnvelope
	require.NoError(t, json.NewDecoder(w.Body).Decode(&env))
	assert.Equal(t, "NOT_FOUND", env.Code)
}

// --- Budget handler ---

type fakeBudgetSvc struct {
	upsertErr error
	getErr    error
	listErr   error

	spec    *core.BudgetSpec
	specs   []*core.BudgetSpec
	upsertd core.BudgetSpec

	gotTenant    string
	gotScopeType string
	gotScopeID   string
	gotGetType   string
	gotGetID     string
	gotRPM       int
	gotTPM       int
	gotMaxConc   int
	gotDaily     float64
	gotMonthly   float64
}

func (f *fakeBudgetSvc) Upsert(_ context.Context, tenantID, scopeType, scopeID string, rpm, tpm, maxConcurrency int, dailyCostUSD, monthlyCostUSD float64) (core.BudgetSpec, error) {
	if f.upsertErr != nil {
		return core.BudgetSpec{}, f.upsertErr
	}
	f.gotTenant, f.gotScopeType, f.gotScopeID = tenantID, scopeType, scopeID
	f.gotRPM, f.gotTPM, f.gotMaxConc = rpm, tpm, maxConcurrency
	f.gotDaily, f.gotMonthly = dailyCostUSD, monthlyCostUSD
	return f.upsertd, nil
}

func (f *fakeBudgetSvc) Get(_ context.Context, tenantID, scopeType, scopeID string) (*core.BudgetSpec, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	f.gotGetType, f.gotGetID = scopeType, scopeID
	return f.spec, nil
}

func (f *fakeBudgetSvc) List(_ context.Context, tenantID string) ([]*core.BudgetSpec, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.specs, nil
}

func TestBudgetHandler_Upsert(t *testing.T) {
	svc := &fakeBudgetSvc{upsertd: core.BudgetSpec{TenantID: "acme", ScopeType: "tenant", RPM: 100}}
	h := NewBudgetHandler(svc)

	body := `{"scope_type":"tenant","rpm":100,"tpm":1000,"max_concurrency":5,"daily_cost_usd":1.5,"monthly_cost_usd":30}`
	req := httptest.NewRequest(http.MethodPut, "/v1/tenants/acme/budgets", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.Upsert(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var got core.BudgetSpec
	require.NoError(t, json.NewDecoder(w.Body).Decode(&got))
	assert.Equal(t, core.BudgetScopeType("tenant"), got.ScopeType)
	assert.Equal(t, "acme", svc.gotTenant)
	assert.Equal(t, 100, svc.gotRPM)
	assert.Equal(t, 1000, svc.gotTPM)
	assert.Equal(t, 5, svc.gotMaxConc)
	assert.Equal(t, 1.5, svc.gotDaily)
	assert.Equal(t, 30.0, svc.gotMonthly)
}

func TestBudgetHandler_Upsert_BadJSON(t *testing.T) {
	h := NewBudgetHandler(&fakeBudgetSvc{})

	req := httptest.NewRequest(http.MethodPut, "/v1/tenants/acme/budgets", strings.NewReader("{oops"))
	w := httptest.NewRecorder()
	h.Upsert(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestBudgetHandler_Upsert_ServiceError(t *testing.T) {
	h := NewBudgetHandler(&fakeBudgetSvc{upsertErr: fmt.Errorf("invalid scope")})

	req := httptest.NewRequest(http.MethodPut, "/v1/tenants/acme/budgets", strings.NewReader(`{"scope_type":"tenant"}`))
	w := httptest.NewRecorder()
	h.Upsert(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestBudgetHandler_Get_TenantScope(t *testing.T) {
	svc := &fakeBudgetSvc{spec: &core.BudgetSpec{TenantID: "acme", ScopeType: "tenant"}}
	h := NewBudgetHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/budgets/tenant", nil)
	w := httptest.NewRecorder()
	h.Get(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "tenant", svc.gotGetType)
	assert.Equal(t, "", svc.gotGetID)
}

func TestBudgetHandler_Get_WithScopeID(t *testing.T) {
	svc := &fakeBudgetSvc{spec: &core.BudgetSpec{TenantID: "acme", ScopeType: "mailbox", ScopeID: "mb-1"}}
	h := NewBudgetHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/budgets/mailbox/mb-1", nil)
	w := httptest.NewRecorder()
	h.Get(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "mailbox", svc.gotGetType)
	assert.Equal(t, "mb-1", svc.gotGetID)
}

func TestBudgetHandler_Get_MissingScopeType(t *testing.T) {
	h := NewBudgetHandler(&fakeBudgetSvc{})

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/no-budgets-here", nil)
	w := httptest.NewRecorder()
	h.Get(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	var env errorEnvelope
	require.NoError(t, json.NewDecoder(w.Body).Decode(&env))
	assert.Contains(t, env.Message, "missing scope_type")
}

func TestBudgetHandler_Get_ServiceError(t *testing.T) {
	h := NewBudgetHandler(&fakeBudgetSvc{getErr: fmt.Errorf("db error")})

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/budgets/tenant", nil)
	w := httptest.NewRecorder()
	h.Get(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestBudgetHandler_Get_NotFound(t *testing.T) {
	h := NewBudgetHandler(&fakeBudgetSvc{spec: nil})

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/budgets/tenant", nil)
	w := httptest.NewRecorder()
	h.Get(w, req)

	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestBudgetHandler_List(t *testing.T) {
	svc := &fakeBudgetSvc{specs: []*core.BudgetSpec{{ScopeType: "tenant"}}}
	h := NewBudgetHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/budgets", nil)
	w := httptest.NewRecorder()
	h.List(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var resp map[string][]*core.BudgetSpec
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Len(t, resp["budgets"], 1)
}

func TestBudgetHandler_List_Error(t *testing.T) {
	h := NewBudgetHandler(&fakeBudgetSvc{listErr: fmt.Errorf("db error")})

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/budgets", nil)
	w := httptest.NewRecorder()
	h.List(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestBudgetScopeFromPath(t *testing.T) {
	cases := []struct {
		path       string
		scopeType  string
		scopeID    string
	}{
		{"/v1/tenants/acme/budgets/mailbox/mb-1", "mailbox", "mb-1"},
		{"/v1/tenants/acme/budgets/tenant", "tenant", ""},
		{"/v1/tenants/acme/budgets/agent/ag-1", "agent", "ag-1"},
		{"/v1/tenants/acme/budgets/", "", ""},
		{"/v1/tenants/acme/budgets", "", ""},
		{"/v1/tenants/acme/nothing", "", ""},
		{"/budgets/a/b/c", "a", "b"},
	}
	for _, tc := range cases {
		st, sid := budgetScopeFromPath(tc.path)
		assert.Equal(t, tc.scopeType, st, "scope_type for %q", tc.path)
		assert.Equal(t, tc.scopeID, sid, "scope_id for %q", tc.path)
	}
}

// --- Catalog handler ---

type fakeCatalogStore struct {
	err    error
	agents []*core.Agent
	gotTenant string
}

func (f *fakeCatalogStore) ListOnlineWithCapabilities(_ context.Context, tenantID string) ([]*core.Agent, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.gotTenant = tenantID
	return f.agents, nil
}

func TestCatalogHandler_List(t *testing.T) {
	svc := &fakeCatalogStore{agents: []*core.Agent{
		{ID: "a1", TenantID: "acme", Status: core.AgentStatusOnline},
	}}
	h := NewCatalogHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/catalog", nil)
	w := httptest.NewRecorder()
	h.List(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var resp struct {
		TenantID string        `json:"tenant_id"`
		Agents   []*core.Agent `json:"agents"`
	}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "acme", resp.TenantID)
	assert.Len(t, resp.Agents, 1)
	assert.Equal(t, "acme", svc.gotTenant)
}

func TestCatalogHandler_List_Error(t *testing.T) {
	h := NewCatalogHandler(&fakeCatalogStore{err: fmt.Errorf("db error")})

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/catalog", nil)
	w := httptest.NewRecorder()
	h.List(w, req)

	require.Equal(t, http.StatusInternalServerError, w.Code)
	var resp map[string]string
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "catalog query failed", resp["error"])
}

// --- Policy rule handler ---

type fakePolicyRuleSvc struct {
	createErr error
	listErr   error

	rules  []*core.PolicyRule
	created core.PolicyRule
	gotTenant string
	gotRule   core.PolicyRule
}

func (f *fakePolicyRuleSvc) Create(_ context.Context, tenantID string, rule core.PolicyRule) (core.PolicyRule, error) {
	if f.createErr != nil {
		return core.PolicyRule{}, f.createErr
	}
	f.gotTenant, f.gotRule = tenantID, rule
	return f.created, nil
}

func (f *fakePolicyRuleSvc) List(_ context.Context, tenantID string) ([]*core.PolicyRule, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.rules, nil
}

func TestPolicyRuleHandler_Create(t *testing.T) {
	svc := &fakePolicyRuleSvc{created: core.PolicyRule{ID: "rule-1", TenantID: "acme", Name: "deny-pii"}}
	h := NewPolicyRuleHandler(svc)

	body := `{"id":"rule-1","name":"deny-pii","status":"active","priority":10,"condition":{"field":"classification","op":"eq","value":"pii"},"action":{"effect":"deny"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/policy-rules", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.Create(w, req)

	require.Equal(t, http.StatusCreated, w.Code)
	var got core.PolicyRule
	require.NoError(t, json.NewDecoder(w.Body).Decode(&got))
	assert.Equal(t, "rule-1", got.ID)
	assert.Equal(t, "acme", svc.gotTenant)
	assert.Equal(t, "deny-pii", svc.gotRule.Name)
	assert.Equal(t, 10, svc.gotRule.Priority)
	assert.JSONEq(t, `{"field":"classification","op":"eq","value":"pii"}`, string(svc.gotRule.Condition))
	assert.JSONEq(t, `{"effect":"deny"}`, string(svc.gotRule.Action))
}

func TestPolicyRuleHandler_Create_BadJSON(t *testing.T) {
	h := NewPolicyRuleHandler(&fakePolicyRuleSvc{})

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/policy-rules", strings.NewReader("nope"))
	w := httptest.NewRecorder()
	h.Create(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestPolicyRuleHandler_Create_ServiceError(t *testing.T) {
	h := NewPolicyRuleHandler(&fakePolicyRuleSvc{createErr: fmt.Errorf("duplicate rule id")})

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/policy-rules", strings.NewReader(`{"id":"r1"}`))
	w := httptest.NewRecorder()
	h.Create(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestPolicyRuleHandler_List(t *testing.T) {
	svc := &fakePolicyRuleSvc{rules: []*core.PolicyRule{{ID: "r1"}, {ID: "r2"}}}
	h := NewPolicyRuleHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/policy-rules", nil)
	w := httptest.NewRecorder()
	h.List(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var resp map[string][]*core.PolicyRule
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Len(t, resp["policy_rules"], 2)
}

func TestPolicyRuleHandler_List_Error(t *testing.T) {
	h := NewPolicyRuleHandler(&fakePolicyRuleSvc{listErr: fmt.Errorf("db error")})

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/policy-rules", nil)
	w := httptest.NewRecorder()
	h.List(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestPolicyRuleHandler_CreateFromTemplate_NotImplemented(t *testing.T) {
	h := NewPolicyRuleHandler(&fakePolicyRuleSvc{})

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/policy-rules/from-template", strings.NewReader(`{"template":"deny-pii"}`))
	w := httptest.NewRecorder()
	h.CreateFromTemplate(w, req)

	require.Equal(t, http.StatusNotImplemented, w.Code)
	var env errorEnvelope
	require.NoError(t, json.NewDecoder(w.Body).Decode(&env))
	assert.Equal(t, "INTERNAL", env.Code)
	assert.Equal(t, http.StatusNotImplemented, env.Status)
}
