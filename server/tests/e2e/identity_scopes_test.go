package e2e

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/server/internal/auth"
	pgdriver "github.com/agentium-lab/Janus/server/internal/driver/postgres"
	"github.com/agentium-lab/Janus/server/internal/handler"
	"github.com/agentium-lab/Janus/server/internal/service"
)

const scopeTenant = "e2e-scope"

type identityStack struct {
	server    *httptest.Server
	fullRaw   string
	readerRaw string
}

type existenceViaRepo struct{ repo *pgdriver.AgentRepository }

func (e existenceViaRepo) AgentExists(ctx context.Context, tenantID, agentID string) (bool, error) {
	_, err := e.repo.Get(ctx, tenantID, agentID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func newIdentityStack(t *testing.T) *identityStack {
	t.Helper()
	ctx := context.Background()

	sqlDB, err := sql.Open("pgx", envOr("JANUS_PG_DSN", "postgres://janus:janus@localhost:5432/janus_test?sslmode=disable"))
	require.NoError(t, err)
	t.Cleanup(func() { sqlDB.Close() })

	_, err = sqlDB.ExecContext(ctx,
		`insert into tenants (id, name) values ($1, 'identity e2e') on conflict (id) do nothing`,
		scopeTenant)
	require.NoError(t, err)

	validator := auth.NewAPIKeyValidator(sqlDB)

	agentRepo := pgdriver.NewAgentRepository(pool)
	taskRepo := pgdriver.NewTaskRepository(pool)
	mailboxRepo := pgdriver.NewMailboxRepository(pool)
	policyRuleRepo := pgdriver.NewPolicyRuleRepository(pool)

	agentSvc := service.NewAgentService(agentRepo, mailboxRepo, redisDrv, natsDrv)
	policySvc := service.NewPolicyService(policyRuleRepo)
	taskSvc := service.NewTaskService(taskRepo, natsDrv, nil, nil).
		WithPolicy(policySvc).
		WithAgentExistence(existenceViaRepo{agentRepo})
	approvalSvc := service.NewApprovalService(pgdriver.NewApprovalRepo(pool), taskSvc, natsDrv)
	taskSvc.WithApproval(approvalSvc)
	apiKeySvc := service.NewAPIKeyService(pgdriver.NewAPIKeyRepo(pool))

	agentH := handler.NewAgentHandler(agentSvc)
	taskH := handler.NewTaskHandler(taskSvc)
	approvalH := handler.NewApprovalHandler(approvalSvc)
	apiKeyH := handler.NewAPIKeyHandler(apiKeySvc)
	echoActingUser := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(auth.ActingUserFromContext(r.Context())))
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/tenants/", func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, "/v1/tenants/"+scopeTenant)
		switch {
		case strings.HasSuffix(rest, "/approve"):
			postOnly(w, r, approvalH.Approve)
		case strings.HasSuffix(rest, "/revoke"):
			postOnly(w, r, apiKeyH.Revoke)
		case strings.HasSuffix(rest, "/acting-user"):
			getOnly(w, r, echoActingUser.ServeHTTP)
		case strings.Contains(rest, "api-keys") && r.Method == http.MethodGet:
			getOnly(w, r, apiKeyH.List)
		case strings.Contains(rest, "api-keys"):
			postOnly(w, r, apiKeyH.Create)
		case strings.HasSuffix(rest, "/agents") && r.Method == http.MethodPost:
			postOnly(w, r, agentH.Register)
		case strings.HasSuffix(rest, "/agents"):
			getOnly(w, r, agentH.List)
		case strings.Contains(rest, "/tasks"):
			postOnly(w, r, taskH.Create)
		default:
			http.NotFound(w, r)
		}
	})

	extract := func(path string) string {
		parts := strings.Split(strings.TrimPrefix(path, "/v1/tenants/"), "/")
		if len(parts) == 0 {
			return ""
		}
		return parts[0]
	}
	protected := auth.Middleware(validator)(auth.ScopeGuard(auth.TenantGuard(extract)(mux)))
	server := httptest.NewServer(protected)

	_, fullRaw, err := apiKeySvc.Create(ctx, scopeTenant, "full-access", nil, "")
	require.NoError(t, err)
	_, readerRaw, err := apiKeySvc.Create(ctx, scopeTenant, "reader", []string{auth.ScopeTaskRead}, "")
	require.NoError(t, err)

	st := &identityStack{server: server, fullRaw: fullRaw, readerRaw: readerRaw}
	t.Cleanup(func() {
		server.Close()
		cleanupScopeTenant()
	})
	return st
}

func cleanupScopeTenant() {
	ctx := context.Background()
	for _, q := range []string{
		`delete from api_keys where tenant_id=$1`,
		`delete from tasks where tenant_id=$1`,
		`delete from agents where tenant_id=$1`,
		`delete from mailboxes where tenant_id=$1`,
		`delete from approvals where tenant_id=$1`,
		`delete from tenants where id=$1`,
	} {
		_, _ = pool.Exec(ctx, q, scopeTenant)
	}
}

func TestE2E_IdentityScopes(t *testing.T) {
	st := newIdentityStack(t)
	srv := st.server

	do := func(method, path, key string, hdr map[string]string, body string) (*http.Response, string) {
		req, err := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		require.NoError(t, err)
		if key != "" {
			req.Header.Set("X-API-Key", key)
		}
		for k, v := range hdr {
			req.Header.Set(k, v)
		}
		resp, err := srv.Client().Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		buf := new(strings.Builder)
		_, _ = io.Copy(buf, resp.Body)
		return resp, buf.String()
	}

	t.Run("missing key rejected", func(t *testing.T) {
		resp, _ := do(http.MethodGet, "/v1/tenants/"+scopeTenant+"/agents", "", nil, "")
		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})

	t.Run("unknown key rejected", func(t *testing.T) {
		resp, _ := do(http.MethodGet, "/v1/tenants/"+scopeTenant+"/agents",
			"janus_unknown0000000000000000000000000000000000000000000000000000", nil, "")
		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})

	t.Run("revoked key rejected", func(t *testing.T) {
		ctx := context.Background()
		apiKeySvc := service.NewAPIKeyService(pgdriver.NewAPIKeyRepo(pool))
		victimRaw := mustCreateScopedKey(t, apiKeySvc, "victim", nil)
		resp, _ := do(http.MethodGet, "/v1/tenants/"+scopeTenant+"/agents", victimRaw, nil, "")
		require.Equal(t, http.StatusOK, resp.StatusCode)

		victimID := mustLookupKeyID(t, ctx, "victim")
		respR, bodyR := do(http.MethodPost,
			"/v1/tenants/"+scopeTenant+"/api-keys/"+victimID+"/revoke", st.fullRaw, nil, "")
		require.Equal(t, http.StatusOK, respR.StatusCode)
		assert.Contains(t, bodyR, "revoked_at")

		respAfter, _ := do(http.MethodGet, "/v1/tenants/"+scopeTenant+"/agents", victimRaw, nil, "")
		assert.Equal(t, http.StatusUnauthorized, respAfter.StatusCode)
	})

	t.Run("full key passes guards and enforces source agent ownership", func(t *testing.T) {
		resp, _ := do(http.MethodGet, "/v1/tenants/"+scopeTenant+"/agents", st.fullRaw, nil, "")
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		respG, bodyG := do(http.MethodPost, "/v1/tenants/"+scopeTenant+"/tasks", st.fullRaw, nil,
			`{"id":"sc-ghost","source_agent":"ghost","target_type":"mailbox","target_value":"mb","envelope":{"content_type":"application/json","body":"e30="}}`)
		assert.Equal(t, http.StatusBadRequest, respG.StatusCode)
		assert.Contains(t, bodyG, "unknown source_agent")
	})

	t.Run("acting user captured through middleware", func(t *testing.T) {
		path := "/v1/tenants/" + scopeTenant + "/acting-user"
		resp, body := do(http.MethodGet, path, st.fullRaw,
			map[string]string{auth.ActingUserHeader: "alice"}, "")
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "alice", body)

		_, bodyNone := do(http.MethodGet, path, st.fullRaw, nil, "")
		assert.Empty(t, bodyNone)
	})

	t.Run("reader key scope matrix", func(t *testing.T) {
		resp, _ := do(http.MethodGet, "/v1/tenants/"+scopeTenant+"/agents", st.readerRaw, nil, "")
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		respW, bodyW := do(http.MethodPost, "/v1/tenants/"+scopeTenant+"/tasks", st.readerRaw, nil,
			`{"id":"sc-w","source_agent":"a","target_type":"mailbox","target_value":"mb","envelope":{"content_type":"application/json","body":"e30="}}`)
		assert.Equal(t, http.StatusForbidden, respW.StatusCode)
		assert.Contains(t, bodyW, "task:write")

		respA, bodyA := do(http.MethodPost, "/v1/tenants/"+scopeTenant+"/approvals/a1/approve", st.readerRaw, nil, "{}")
		assert.Equal(t, http.StatusForbidden, respA.StatusCode)
		assert.Contains(t, bodyA, "admin")

		respK, bodyK := do(http.MethodPost, "/v1/tenants/"+scopeTenant+"/api-keys", st.readerRaw, nil,
			`{"name":"nope"}`)
		assert.Equal(t, http.StatusForbidden, respK.StatusCode)
		assert.Contains(t, bodyK, "admin")
	})
}

func mustCreateScopedKey(t *testing.T, svc *service.APIKeyService, name string, scopes []string) string {
	t.Helper()
	_, raw, err := svc.Create(context.Background(), scopeTenant, name, scopes, "")
	require.NoError(t, err)
	return raw
}

func mustLookupKeyID(t *testing.T, ctx context.Context, name string) string {
	t.Helper()
	var id string
	err := pool.QueryRow(ctx, `select id from api_keys where tenant_id=$1 and name=$2`, scopeTenant, name).Scan(&id)
	require.NoError(t, err)
	return id
}
