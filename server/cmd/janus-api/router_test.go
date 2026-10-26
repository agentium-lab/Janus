package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/handler"
)

// Service fakes embed their interface so only methods the tested paths reach
// need bodies; each returns a marker payload so the response body records
// which handler the mux dispatched to.

type fakeTenantSvc struct{ handler.TenantService }

func (fakeTenantSvc) Get(context.Context, string) (*core.Tenant, error) {
	return &core.Tenant{ID: "MARKER-TENANT-GET"}, nil
}

type fakeMailboxSvc struct{ handler.MailboxService }

func (fakeMailboxSvc) Create(context.Context, core.Mailbox) error { return nil }

func (fakeMailboxSvc) Get(context.Context, string, string) (*core.Mailbox, error) {
	return &core.Mailbox{ID: "MARKER-MAILBOX-GET"}, nil
}

type fakeDLQSvc struct{ handler.DLQService }

func (fakeDLQSvc) QueryDLQ(context.Context, string, string, int) ([]*core.Task, error) {
	return []*core.Task{{ID: "MARKER-DLQ-QUERY"}}, nil
}

type fakeBudgetSvc struct{ handler.BudgetSpecAdminService }

func (fakeBudgetSvc) List(context.Context, string) ([]*core.BudgetSpec, error) {
	return []*core.BudgetSpec{{ScopeID: "MARKER-BUDGET-LIST"}}, nil
}

func (fakeBudgetSvc) Upsert(context.Context, string, string, string, int, int, int, float64, float64) (core.BudgetSpec, error) {
	return core.BudgetSpec{ScopeID: "MARKER-BUDGET-UPSERT"}, nil
}

type fakeCatalogStore struct{ handler.CatalogStore }

func (fakeCatalogStore) ListOnlineWithCapabilities(context.Context, string) ([]*core.Agent, error) {
	return []*core.Agent{{ID: "MARKER-CATALOG"}}, nil
}

type fakeAuditSvc struct{ handler.AuditService }

func (fakeAuditSvc) QueryByTrace(context.Context, string, string, int) (interface{}, error) {
	return map[string]string{"marker": "MARKER-AUDIT-TRACE"}, nil
}

type fakeDispatchSvc struct{ handler.DispatchService }

func (fakeDispatchSvc) PullTask(context.Context, string, string, string) (*handler.ServicePullResult, error) {
	return nil, nil
}

func newTestRouter() http.Handler {
	stub := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	return newRouter(
		handler.NewTenantHandler(fakeTenantSvc{}),
		nil, // agentH
		nil, // taskH
		handler.NewMailboxHandler(fakeMailboxSvc{}),
		handler.NewDispatchHandler(fakeDispatchSvc{}),
		handler.NewAuditHandler(fakeAuditSvc{}),
		nil, // approvalH
		nil, // contextRefH
		nil, // wsH
		nil, // sseH
		nil, // progressH
		stub,
		stub,
		stub,
		handler.NewDLQHandler(fakeDLQSvc{}),
		handler.NewCatalogHandler(fakeCatalogStore{}),
		nil, // apiKeyH
		nil, // policyH
		handler.NewBudgetHandler(fakeBudgetSvc{}),
	)
}

func doReq(t *testing.T, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	newTestRouter().ServeHTTP(rec, httptest.NewRequest(method, path, nil))
	return rec
}

// Tenants named like dispatch keywords must not hijack the switch: dispatch
// sees only the resource part after the tenant id.
func TestRouter_TenantNamedResourceWord_DoesNotHijackDispatch(t *testing.T) {
	rec := doReq(t, http.MethodGet, "/v1/tenants/dlq/mailboxes/m")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "MARKER-MAILBOX-GET") || strings.Contains(rec.Body.String(), "MARKER-DLQ-QUERY") {
		t.Fatalf("tenant dlq mailbox get: want mailbox handler, got code=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = doReq(t, http.MethodGet, "/v1/tenants/pull/mailboxes/m")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "MARKER-MAILBOX-GET") {
		t.Fatalf("tenant pull mailbox get: want mailbox handler, got code=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = doReq(t, http.MethodPost, "/v1/tenants/traces/mailboxes/mb/pull")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("tenant traces mailbox pull: want dispatch pull (204), got code=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestRouter_ResourceDispatchUnchanged(t *testing.T) {
	rec := doReq(t, http.MethodGet, "/v1/tenants/acme/dlq")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "MARKER-DLQ-QUERY") {
		t.Fatalf("dlq query: got code=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = doReq(t, http.MethodGet, "/v1/tenants/acme/tasks/t1/traces")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "MARKER-AUDIT-TRACE") {
		t.Fatalf("traces query: got code=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = doReq(t, http.MethodGet, "/v1/tenants/acme")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "MARKER-TENANT-GET") {
		t.Fatalf("tenant get (contract GET /v1/tenants/{id}): got code=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = doReq(t, http.MethodGet, "/v1/tenants/")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("bare /v1/tenants/: want 404, got %d", rec.Code)
	}
}

func TestRouter_BoundaryEqualResourceRoutes(t *testing.T) {
	// Routes whose resource part IS the whole path (no internal slash) must
	// still match their suffix cases after the tenant-prefix strip.
	cases := []struct {
		name    string
		method  string
		path    string
		code    int
		marker  string
	}{
		{"mailbox create", http.MethodPost, "/v1/tenants/acme/mailboxes", http.StatusCreated, "\"id\":\"bnd-mb\""},
		{"budget list", http.MethodGet, "/v1/tenants/acme/budgets", http.StatusOK, "MARKER-BUDGET-LIST"},
		{"budget upsert", http.MethodPost, "/v1/tenants/acme/budgets", http.StatusBadRequest, "invalid json"},
		{"outbox retry-dead", http.MethodPost, "/v1/tenants/acme/outbox/retry-dead", http.StatusServiceUnavailable, "UNAVAILABLE"},
		{"audit replay", http.MethodPost, "/v1/tenants/acme/audit/replay", http.StatusBadRequest, "from and to query parameters required"},
		{"catalog", http.MethodGet, "/v1/tenants/acme/catalog", http.StatusOK, "MARKER-CATALOG"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var rec *httptest.ResponseRecorder
			if tc.name == "mailbox create" {
				rec = doBody(t, tc.method, tc.path, "{\"id\":\"bnd-mb\",\"agent_id\":\"bnd-agent\"}")
			} else {
				rec = doReq(t, tc.method, tc.path)
			}
			if rec.Code != tc.code || !strings.Contains(rec.Body.String(), tc.marker) {
				t.Fatalf("%s %s: want code=%d marker %s, got code=%d body=%s",
					tc.method, tc.path, tc.code, tc.marker, rec.Code, rec.Body.String())
			}
		})
	}
}

func doBody(t *testing.T, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	newTestRouter().ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
	return rec
}
