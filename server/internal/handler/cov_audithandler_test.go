package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/agentium-lab/Janus/server/internal/outbox"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ReplayAudit parses from/to/limit, invokes the replayer, and maps statuses.
type fakeReplayer struct {
	gotFrom time.Time
	gotTo   time.Time
	gotLim  int
	result  outbox.ReplayResult
	err     error
}

func (f *fakeReplayer) Replay(_ context.Context, _ string, from, to time.Time, limit int) (outbox.ReplayResult, error) {
	f.gotFrom, f.gotTo, f.gotLim = from, to, limit
	return f.result, f.err
}

func TestAuditHandler_ReplayAudit_HappyPath(t *testing.T) {
	rep := &fakeReplayer{result: outbox.ReplayResult{Projected: 3, Status: "success"}}
	h := NewAuditHandler(nil).WithReplayer(rep)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/audit/replay?from=2026-01-01T00:00:00Z&to=2026-01-02T00:00:00Z&limit=100", nil)
	h.ReplayAudit(w, r)
	assert.Equal(t, http.StatusOK, w.Code)
	var body map[string]interface{}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, float64(3), body["projected"])
	assert.Equal(t, "success", body["status"])
}

func TestAuditHandler_ReplayAudit_BadParams(t *testing.T) {
	h := NewAuditHandler(nil).WithReplayer(&fakeReplayer{})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/audit/replay?from=notatime&to=2026-01-02T00:00:00Z", nil)
	h.ReplayAudit(w, r)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestAuditHandler_ReplayAudit_StatusMapping(t *testing.T) {
	h := NewAuditHandler(nil).WithReplayer(&fakeReplayer{result: outbox.ReplayResult{Failed: 2, Status: "all_failed"}})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/audit/replay?from=2026-01-01T00:00:00Z&to=2026-01-02T00:00:00Z", nil)
	h.ReplayAudit(w, r)
	assert.Equal(t, http.StatusUnprocessableEntity, w.Code)
}

// RetryOutboxDead requires the reviver; without one it is a 503.
func TestAuditHandler_RetryOutboxDead_NoReviver(t *testing.T) {
	h := NewAuditHandler(nil)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/outbox/retry-dead", nil)
	h.RetryOutboxDead(w, r)
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}

func TestAuditHandler_RetryOutboxDead_WithReviver(t *testing.T) {
	called := false
	h := NewAuditHandler(nil).WithOutboxRetryReviver(reviverFunc(func(ctx context.Context, tenantID string, limit int) (int64, error) {
		called = true
		assert.Equal(t, "acme", tenantID)
		return 7, nil
	}))
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/outbox/retry-dead?limit=50", nil)
	h.RetryOutboxDead(w, r)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.True(t, called)
}

type reviverFunc func(ctx context.Context, tenantID string, limit int) (int64, error)

func (f reviverFunc) RetryDead(ctx context.Context, tenantID string, limit int) (int64, error) {
	return f(ctx, tenantID, limit)
}
