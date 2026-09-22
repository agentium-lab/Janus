package main

import (
	"net/http"
	"strings"

	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/agentium-lab/Janus/server/internal/handler"
	_ "github.com/agentium-lab/Janus/server/internal/metrics"
)

// HTTP routing table and method-guard helpers.

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

	mux.HandleFunc("/v1/tenants/", func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path

		switch {
		case hasSegment(p, "dlq") && hasSuffix(p, "/replay"):
			postOnly(w, r, dlqH.Replay)
		case hasSegment(p, "dlq") && hasSuffix(p, "/discard"):
			postOnly(w, r, dlqH.Discard)
		case hasSegment(p, "dlq") && r.Method == http.MethodGet:
			dlqH.Query(w, r)
		case hasSegment(p, "pull"):
			postOnly(w, r, dispatchH.Pull)
		case hasSegment(p, "traces"):
			getOnly(w, r, auditH.QueryByTrace)
		case hasSuffix(p, "/outbox/retry-dead"):
			postOnly(w, r, auditH.RetryOutboxDead)
		case hasSegment(p, "tasks") && hasSuffix(p, "/progress"):
			postOnly(w, r, progressH.Report)
		case hasSegment(p, "tasks") && hasSuffix(p, "/stream"):
			getOnly(w, r, sseH.ServeHTTP)
		case hasSegment(p, "mailboxes") && hasSuffix(p, "/pause"):
			postOnly(w, r, mailboxH.Pause)
		case hasSegment(p, "mailboxes") && hasSuffix(p, "/resume"):
			postOnly(w, r, mailboxH.Resume)
		case hasSegment(p, "mailboxes") && hasSuffix(p, "/mailboxes"):
			postOnly(w, r, mailboxH.Create)
		case hasSegment(p, "mailboxes") && r.Method == http.MethodPatch:
			mailboxH.Update(w, r)
		case hasSegment(p, "mailboxes"):
			getOnly(w, r, mailboxH.Get)
		case hasSegment(p, "heartbeat") && hasSegment(p, "agents"):
			postOnly(w, r, agentH.Heartbeat)
		case hasSegment(p, "agents") && !hasLastSegment(p, "agents"):
			getOnly(w, r, agentH.Get)
		case hasSegment(p, "agents") && hasLastSegment(p, "agents"):
			if r.Method == http.MethodPost {
				agentH.Register(w, r)
			} else {
				agentH.List(w, r)
			}
		case hasSuffix(p, "/catalog"):
			getOnly(w, r, catalogH.List)
		case hasSegment(p, "tasks") && hasSuffix(p, "/start"):
			postOnly(w, r, dispatchH.Start)
		case hasSegment(p, "tasks") && hasSuffix(p, "/heartbeat"):
			postOnly(w, r, dispatchH.Heartbeat)
		case hasSegment(p, "tasks") && hasSuffix(p, "/ack"):
			postOnly(w, r, dispatchH.Ack)
		case hasSegment(p, "tasks") && hasSuffix(p, "/nack"):
			postOnly(w, r, dispatchH.Nack)
		case hasSegment(p, "tasks") && hasSuffix(p, "/cancel"):
			postOnly(w, r, taskH.Cancel)
		case hasSegment(p, "tasks") && hasSuffix(p, "/block"):
			postOnly(w, r, taskH.Block)
		case hasSegment(p, "tasks") && hasSuffix(p, "/unblock"):
			postOnly(w, r, taskH.Unblock)
		case hasSegment(p, "policy-rules") && hasSuffix(p, "/templates"):
			postOnly(w, r, policyH.CreateFromTemplate)
		case hasSegment(p, "policy-rules") && r.Method == http.MethodGet:
			getOnly(w, r, policyH.List)
		case hasSegment(p, "policy-rules"):
			postOnly(w, r, policyH.Create)
		case hasSegment(p, "budgets") && hasSuffix(p, "/budgets") && r.Method == http.MethodGet:
			getOnly(w, r, budgetH.List)
		case hasSegment(p, "budgets") && hasSuffix(p, "/budgets"):
			postOnly(w, r, budgetH.Upsert)
		case hasSegment(p, "budgets") && r.Method == http.MethodGet:
			getOnly(w, r, budgetH.Get)
		case hasSegment(p, "api-keys") && hasSuffix(p, "/revoke"):
			postOnly(w, r, apiKeyH.Revoke)
		case hasSegment(p, "api-keys") && r.Method == http.MethodGet:
			getOnly(w, r, apiKeyH.List)
		case hasSegment(p, "api-keys"):
			postOnly(w, r, apiKeyH.Create)
		case hasSegment(p, "approvals") && hasSuffix(p, "/approve"):
			postOnly(w, r, approvalH.Approve)
		case hasSegment(p, "approvals") && hasSuffix(p, "/reject"):
			postOnly(w, r, approvalH.Reject)
		case hasSegment(p, "approvals") && !hasLastSegment(p, "approvals"):
			getOnly(w, r, approvalH.Get)
		case hasSegment(p, "approvals") && hasLastSegment(p, "approvals"):
			if r.Method == http.MethodPost {
				postOnly(w, r, approvalH.Request)
			} else {
				getOnly(w, r, approvalH.ListPending)
			}
		case hasSegment(p, "tasks") && hasSuffix(p, "/replay"):
			postOnly(w, r, taskH.Replay)
		case hasSegment(p, "tasks") && hasSuffix(p, "/complete"):
			postOnly(w, r, taskH.Complete)
		case hasSegment(p, "tasks") && hasSuffix(p, "/fail"):
			postOnly(w, r, taskH.Fail)
		case hasSegment(p, "tasks") && hasSuffix(p, "/events"):
			getOnly(w, r, auditH.QueryByTask)
		case hasSegment(p, "tasks") && !hasLastSegment(p, "tasks"):
			getOnly(w, r, taskH.Get)
		case hasSegment(p, "tasks") && hasLastSegment(p, "tasks"):
			postOnly(w, r, taskH.Create)
		case hasSegment(p, "events"):
			getOnly(w, r, auditH.QueryByTenant)
		case hasSuffix(p, "/audit/replay"):
			postOnly(w, r, auditH.ReplayAudit)
		case hasSegment(p, "context-refs") && hasSuffix(p, "/attach"):
			postOnly(w, r, contextRefH.Attach)
		case hasSegment(p, "context-refs") && hasSuffix(p, "/detach"):
			postOnly(w, r, contextRefH.Detach)
		case hasSegment(p, "context-refs") && hasSuffix(p, "/list"):
			getOnly(w, r, contextRefH.ListByTask)
		case hasSegment(p, "context-refs") && !hasLastSegment(p, "context-refs"):
			getOnly(w, r, contextRefH.Get)
		default:
			if r.Method == http.MethodGet {
				tenantH.Get(w, r)
			} else {
				http.NotFound(w, r)
			}
		}
	})

	return mux
}

func postOnly(w http.ResponseWriter, r *http.Request, fn http.HandlerFunc) {
	if r.Method == http.MethodPost {
		fn(w, r)
	} else {
		http.NotFound(w, r)
	}
}

func getOnly(w http.ResponseWriter, r *http.Request, fn http.HandlerFunc) {
	if r.Method == http.MethodGet {
		fn(w, r)
	} else {
		http.NotFound(w, r)
	}
}

func hasSegment(path, seg string) bool {
	for _, s := range strings.Split(path, "/") {
		if s == seg {
			return true
		}
	}
	return false
}

func hasLastSegment(path, seg string) bool {
	parts := strings.Split(strings.TrimRight(path, "/"), "/")
	return len(parts) > 0 && parts[len(parts)-1] == seg
}

func hasSuffix(path, suffix string) bool {
	return strings.HasSuffix(strings.TrimRight(path, "/"), suffix)
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
