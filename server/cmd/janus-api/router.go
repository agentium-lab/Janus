package main

import (
	"fmt"
	"net/http"
	"strings"

	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/agentium-lab/Janus/server/internal/auth"
	"github.com/agentium-lab/Janus/server/internal/handler"
	_ "github.com/agentium-lab/Janus/server/internal/metrics"
)

// HTTP routing. The /v1/tenants/{tenantID} data plane is driven entirely by
// auth.TenantRoutes: every table entry registers one method+pattern on a
// net/http ServeMux (Go 1.22+ patterns) bound to its handler and wrapped
// with a scope guard carrying the entry's scope. Undefined path shapes match
// no pattern and get ServeMux's 404 — no scope check, no handler.

func newRouter(tenantH *handler.TenantHandler, agentH *handler.AgentHandler, taskH *handler.TaskHandler, mailboxH *handler.MailboxHandler, dispatchH *handler.DispatchHandler, auditH *handler.AuditHandler, approvalH *handler.ApprovalHandler, contextRefH *handler.ContextRefHandler, wsH *handler.WebSocketHandler, sseH *handler.SSEHandler, progressH *handler.ProgressHandler, a2aGw http.Handler, acpGw http.Handler, mcpGw http.Handler, dlqH *handler.DLQHandler, catalogH *handler.CatalogHandler, apiKeyH *handler.APIKeyHandler, policyH *handler.PolicyRuleHandler, budgetH *handler.BudgetHandler) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/ws", wsH.ServeHTTP)
	mux.Handle("/a2a/", a2aGw)
	// ACP is deprecated in favor of A2A (protocol merged upstream). The shell
	// keeps existing consumers working while advertising removal via headers.
	mux.Handle("/acp/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Deprecation", "true")
		w.Header().Set("Sunset", "Thu, 31 Dec 2026 23:59:59 GMT")
		w.Header().Set("Link", "</a2a/>; rel=\"deprecation\"")
		acpGw.ServeHTTP(w, r)
	}))
	mux.Handle("/mcp", mcpGw)
	mux.Handle("/mcp/", mcpGw)

	mux.HandleFunc("/v1/tenants", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			tenantH.Create(w, r)
		case http.MethodGet:
			tenantH.List(w, r)
		default:
			http.NotFound(w, r)
		}
	})

	handlers := tenantRouteHandlers(tenantH, agentH, taskH, mailboxH, dispatchH, auditH, approvalH, contextRefH, sseH, progressH, dlqH, catalogH, apiKeyH, policyH, budgetH)
	for _, rt := range auth.TenantRoutes {
		target := rt.Method + " " + rt.Pattern
		h, ok := handlers[target]
		if !ok {
			panic("janus-api: tenant route " + target + " has no handler binding")
		}
		mux.Handle(target, requireScope(rt.Scope, h))
	}

	return withTenantPathNormalization(mux)
}

// tenantRouteHandlers binds every route-table entry to its HTTP handler.
func tenantRouteHandlers(tenantH *handler.TenantHandler, agentH *handler.AgentHandler, taskH *handler.TaskHandler, mailboxH *handler.MailboxHandler, dispatchH *handler.DispatchHandler, auditH *handler.AuditHandler, approvalH *handler.ApprovalHandler, contextRefH *handler.ContextRefHandler, sseH *handler.SSEHandler, progressH *handler.ProgressHandler, dlqH *handler.DLQHandler, catalogH *handler.CatalogHandler, apiKeyH *handler.APIKeyHandler, policyH *handler.PolicyRuleHandler, budgetH *handler.BudgetHandler) map[string]http.HandlerFunc {
	return map[string]http.HandlerFunc{
		// tenant
		"GET /v1/tenants/{tenantID}": tenantH.Get,
		// agents
		"POST /v1/tenants/{tenantID}/agents":                     agentH.Register,
		"GET /v1/tenants/{tenantID}/agents":                      agentH.List,
		"GET /v1/tenants/{tenantID}/agents/{agentID}":            agentH.Get,
		"POST /v1/tenants/{tenantID}/agents/{agentID}/heartbeat": agentH.Heartbeat,
		// mailboxes
		"POST /v1/tenants/{tenantID}/mailboxes":                    mailboxH.Create,
		"GET /v1/tenants/{tenantID}/mailboxes/{mailboxID}":         mailboxH.Get,
		"PATCH /v1/tenants/{tenantID}/mailboxes/{mailboxID}":       mailboxH.Update,
		"POST /v1/tenants/{tenantID}/mailboxes/{mailboxID}/pause":  mailboxH.Pause,
		"POST /v1/tenants/{tenantID}/mailboxes/{mailboxID}/resume": mailboxH.Resume,
		"POST /v1/tenants/{tenantID}/mailboxes/{mailboxID}/pull":   dispatchH.Pull,
		// dead letter queue
		"GET /v1/tenants/{tenantID}/dlq":                   dlqH.Query,
		"POST /v1/tenants/{tenantID}/dlq/{taskID}/replay":  dlqH.Replay,
		"POST /v1/tenants/{tenantID}/dlq/{taskID}/discard": dlqH.Discard,
		// tasks
		"POST /v1/tenants/{tenantID}/tasks":                    taskH.Create,
		"GET /v1/tenants/{tenantID}/tasks/{taskID}":            taskH.Get,
		"POST /v1/tenants/{tenantID}/tasks/{taskID}/start":     dispatchH.Start,
		"POST /v1/tenants/{tenantID}/tasks/{taskID}/heartbeat": dispatchH.Heartbeat,
		"POST /v1/tenants/{tenantID}/tasks/{taskID}/ack":       dispatchH.Ack,
		"POST /v1/tenants/{tenantID}/tasks/{taskID}/nack":      dispatchH.Nack,
		"POST /v1/tenants/{tenantID}/tasks/{taskID}/cancel":    taskH.Cancel,
		"POST /v1/tenants/{tenantID}/tasks/{taskID}/block":     taskH.Block,
		"POST /v1/tenants/{tenantID}/tasks/{taskID}/unblock":   taskH.Unblock,
		"POST /v1/tenants/{tenantID}/tasks/{taskID}/replay":    taskH.Replay,
		"POST /v1/tenants/{tenantID}/tasks/{taskID}/complete":  taskH.Complete,
		"POST /v1/tenants/{tenantID}/tasks/{taskID}/fail":      taskH.Fail,
		"POST /v1/tenants/{tenantID}/tasks/{taskID}/progress":  progressH.Report,
		"GET /v1/tenants/{tenantID}/tasks/{taskID}/stream":     sseH.ServeHTTP,
		"GET /v1/tenants/{tenantID}/tasks/{taskID}/events":     auditH.QueryByTask,
		"GET /v1/tenants/{tenantID}/tasks/{taskID}/traces":     auditH.QueryByTrace,
		"GET /v1/tenants/{tenantID}/traces/{traceID}":          auditH.QueryByTrace,
		"GET /v1/tenants/{tenantID}/events":                    auditH.QueryByTenant,
		"GET /v1/tenants/{tenantID}/catalog":                   catalogH.List,
		// audit and outbox recovery
		"POST /v1/tenants/{tenantID}/audit/replay":      auditH.ReplayAudit,
		"POST /v1/tenants/{tenantID}/outbox/retry-dead": auditH.RetryOutboxDead,
		// governance management
		"POST /v1/tenants/{tenantID}/policy-rules":                 policyH.Create,
		"GET /v1/tenants/{tenantID}/policy-rules":                  policyH.List,
		"POST /v1/tenants/{tenantID}/policy-rules/templates":       policyH.CreateFromTemplate,
		"POST /v1/tenants/{tenantID}/budgets":                      budgetH.Upsert,
		"GET /v1/tenants/{tenantID}/budgets":                       budgetH.List,
		"GET /v1/tenants/{tenantID}/budgets/{scopeType}":           budgetH.Get,
		"GET /v1/tenants/{tenantID}/budgets/{scopeType}/{scopeID}": budgetH.Get,
		"POST /v1/tenants/{tenantID}/api-keys":                     apiKeyH.Create,
		"GET /v1/tenants/{tenantID}/api-keys":                      apiKeyH.List,
		"POST /v1/tenants/{tenantID}/api-keys/{keyID}/revoke":      apiKeyH.Revoke,
		// approvals
		"POST /v1/tenants/{tenantID}/approvals":                      approvalH.Request,
		"GET /v1/tenants/{tenantID}/approvals":                       approvalH.ListPending,
		"GET /v1/tenants/{tenantID}/approvals/{approvalID}":          approvalH.Get,
		"POST /v1/tenants/{tenantID}/approvals/{approvalID}/approve": approvalH.Approve,
		"POST /v1/tenants/{tenantID}/approvals/{approvalID}/reject":  approvalH.Reject,
		// context refs
		"POST /v1/tenants/{tenantID}/context-refs":               contextRefH.Attach,
		"GET /v1/tenants/{tenantID}/context-refs/{refID}":        contextRefH.Get,
		"DELETE /v1/tenants/{tenantID}/context-refs/{refID}":     contextRefH.Detach,
		"GET /v1/tenants/{tenantID}/tasks/{taskID}/context-refs": contextRefH.ListByTask,
	}
}

// requireScope enforces the route-table scope at dispatch: a request whose
// authenticated principal lacks the scope is refused with the same body
// auth.ScopeGuard produces (the middleware applies the same decision from
// the same table; this guard keeps scope and dispatch fused). No principal
// in context (auth disabled in dev mode) passes through.
func requireScope(scope string, next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if principal, ok := auth.PrincipalFromContext(r.Context()); ok && !principal.HasScope(scope) {
			w.Header().Set("Content-Type", "application/json")
			http.Error(w, fmt.Sprintf(`{"error":"missing required scope %q"}`, scope), http.StatusForbidden)
			return
		}
		next(w, r)
	})
}

// withTenantPathNormalization trims trailing slashes from tenant-subtree
// paths before pattern matching, preserving the legacy tolerance for forms
// real callers send (POST /v1/tenants/t/tasks/, GET /v1/tenants/t/). The
// bare "/v1/tenants/" root is left alone and stays 404.
func withTenantPathNormalization(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p := r.URL.Path; strings.HasPrefix(p, "/v1/tenants/") && p != "/v1/tenants/" {
			if trimmed := strings.TrimRight(p, "/"); trimmed != "" && trimmed != "/v1/tenants" {
				r.URL.Path = trimmed
			}
		}
		next.ServeHTTP(w, r)
	})
}

func extractTenantFromPath(path string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == "tenants" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

// buildTLSConfig constructs a *tls.Config from the TLSConfig. When ClientCAFile
// is set, client certificates are required and verified (mTLS). MinVersion is
// TLS 1.2 and only strong AEAD cipher suites are enabled.
