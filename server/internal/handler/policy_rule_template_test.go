package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/agentium-lab/Janus/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tool templates compile to action "tool.invoke", which no policy
// evaluation site carries: creating them must be refused (422) with a
// reason naming WHY, and nothing may reach the policy store.
func TestPolicyRuleHandler_CreateFromTemplate_ToolTemplatesRefused(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"require_approval_tool", `{"template":"require_approval_tool","tool":"mcp.exec"}`},
		{"allow_agent_tool", `{"template":"allow_agent_tool","agent_id":"a1","tool":"git.commit"}`},
		{"deny_agent_tool", `{"template":"deny_agent_tool","agent_id":"a1","tool":"git.commit"}`},
		{"allow_team_tool", `{"template":"allow_team_tool","team_id":"ops","tool":"git.commit"}`},
		{"deny_team_tool", `{"template":"deny_team_tool","team_id":"ops","tool":"git.commit"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := &fakePolicyRuleSvc{}
			h := NewPolicyRuleHandler(svc)

			req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/policy-rules/templates", strings.NewReader(tc.body))
			w := httptest.NewRecorder()
			h.CreateFromTemplate(w, req)

			require.Equal(t, http.StatusUnprocessableEntity, w.Code)
			var resp struct {
				Message string `json:"message"`
			}
			require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
			assert.Contains(t, resp.Message, "tool.invoke")
			assert.Contains(t, resp.Message, "task.publish")
			assert.Empty(t, svc.gotTenant, "refused rule must not reach the policy store")
		})
	}
}

func TestPolicyRuleHandler_CreateFromTemplate_TaskPublishTemplateStillCreated(t *testing.T) {
	svc := &fakePolicyRuleSvc{created: core.PolicyRule{ID: "rule-1", TenantID: "acme"}}
	h := NewPolicyRuleHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/policy-rules/templates",
		strings.NewReader(`{"template":"require_approval_capability","capability":"delete_db"}`))
	w := httptest.NewRecorder()
	h.CreateFromTemplate(w, req)

	require.Equal(t, http.StatusCreated, w.Code)
	assert.Equal(t, "acme", svc.gotTenant)
	assert.Contains(t, string(svc.gotRule.Condition), `"task.publish"`)
}

func TestPolicyRuleHandler_CreateFromTemplate_TaskRouteTemplateStillCreated(t *testing.T) {
	svc := &fakePolicyRuleSvc{created: core.PolicyRule{ID: "rule-2", TenantID: "acme"}}
	h := NewPolicyRuleHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/policy-rules/templates",
		strings.NewReader(`{"template":"deny_agent_data_classification","agent_id":"agent-1","data_classification":"confidential"}`))
	w := httptest.NewRecorder()
	h.CreateFromTemplate(w, req)

	require.Equal(t, http.StatusCreated, w.Code)
	assert.Equal(t, "acme", svc.gotTenant)
	assert.Contains(t, string(svc.gotRule.Condition), `"task.route"`)
}
