package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func toolPolicyProject() *ProjectConfig {
	return &ProjectConfig{
		DefaultTenant: "acme",
		Tenants: map[string]*ProjectTenant{
			"acme": {Policies: ProjectPolicies{Tools: ProjectToolPolicy{
				Allow: []ProjectToolBinding{{Agent: "a1", Tool: "git.commit"}},
			}}},
		},
	}
}

func projectSyncTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/budgets"):
			w.Write([]byte(`{"budgets":[]}`))
		case strings.HasSuffix(r.URL.Path, "/policy-rules"):
			w.Write([]byte(`{"policy_rules":[]}`))
		default:
			w.Write([]byte(`{"id":"acme","name":"acme"}`))
		}
	}))
}

func withProjectServerURL(t *testing.T, srv *httptest.Server) {
	t.Helper()
	old := serverURL
	serverURL = srv.URL
	t.Cleanup(func() { serverURL = old })
}

// janus project apply fails on tool.invoke templates with the same refusal
// message the broker's CreateFromTemplate returns.
func TestApplyProjectTenant_RefusesToolInvokeTemplates(t *testing.T) {
	srv := projectSyncTestServer(t)
	defer srv.Close()
	withProjectServerURL(t, srv)

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	err := applyProjectTenant(cmd, toolPolicyProject(), "acme")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "allow_agent_tool")
	assert.Contains(t, err.Error(), "tool.invoke")
	assert.Contains(t, err.Error(), "task.publish")
}

// janus project diff reports tool.invoke templates as unsupported instead
// of promising they would be created.
func TestDiffProjectTenant_MarksToolInvokeTemplatesUnsupported(t *testing.T) {
	srv := projectSyncTestServer(t)
	defer srv.Close()
	withProjectServerURL(t, srv)

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var out strings.Builder
	cmd.SetOut(&out)

	require.NoError(t, diffProjectTenant(cmd, toolPolicyProject(), "acme"))
	assert.Contains(t, out.String(), "allow_agent_tool")
	assert.Contains(t, out.String(), "not enforceable")
	assert.NotContains(t, out.String(), "+ policy", "diff must not offer to create an unenforceable rule")
}
