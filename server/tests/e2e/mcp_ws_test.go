package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/agentium-lab/Janus/core"
	natsdriver "github.com/agentium-lab/Janus/server/internal/driver/nats"
	pgdriver "github.com/agentium-lab/Janus/server/internal/driver/postgres"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// GOV-14: MCP Governance — policy enforcement, context refs, audit trail
// ---------------------------------------------------------------------------

// TestE2E_MCP_Setup creates the shared tenant/agent/mailbox prerequisites that
// subsequent MCP and WS tests depend on. Idempotent — safe whether running the
// full suite or just the MCP/WS subset via -run filter.
func TestE2E_MCP_Setup(t *testing.T) {
	ensureSetup(t)
}

// TestE2E_MCP_ToolCallPolicyDeny verifies that a policy deny rule is honored
// when a tool invocation arrives through the MCP gateway. The MCP gateway must
// NOT bypass the policy service wired into TaskService.
func TestE2E_MCP_ToolCallPolicyDeny(t *testing.T) {
	const denyTarget = "mcp_deny_target"
	const allowTarget = "mcp_allow_target"
	const deniedCallID = "mcp-call-deny-1"
	const allowedCallID = "mcp-call-allow-1"

	policyRuleRepo := pgdriver.NewPolicyRuleRepository(pool)
	err := policyRuleRepo.Create(context.Background(), core.PolicyRule{
		TenantID:  testTenant,
		ID:        "rule-mcp-deny-e2e",
		Name:      "E2E Deny deny_target",
		Status:    "active",
		Priority:  10,
		Condition: json.RawMessage(`{"resource.value":"` + denyTarget + `"}`),
		Action:    json.RawMessage(`{"decision":"deny"}`),
	})
	require.NoError(t, err)

	// Denied call — policy must block task creation.
	resp := mcpToolCall(t, deniedCallID, denyTarget)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode,
		"policy-denied MCP tool call must return 500, not 201")
	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "policy denied",
		"response must explain the denial, not silently bypass")

	// Task must NOT exist.
	statusResp := mcpToolCallStatus(t, deniedCallID)
	defer statusResp.Body.Close()
	assert.Equal(t, http.StatusNotFound, statusResp.StatusCode,
		"denied task must not be persisted")

	// Allowed call — different target, no matching deny rule.
	resp2 := mcpToolCall(t, allowedCallID, allowTarget)
	defer resp2.Body.Close()
	require.Equal(t, http.StatusCreated, resp2.StatusCode,
		"non-denied MCP tool call must succeed")
	var created map[string]string
	json.NewDecoder(resp2.Body).Decode(&created)
	assert.Equal(t, allowedCallID, created["call_id"])

	// Task MUST exist.
	statusResp2 := mcpToolCallStatus(t, allowedCallID)
	defer statusResp2.Body.Close()
	assert.Equal(t, http.StatusOK, statusResp2.StatusCode,
		"allowed task must be persisted")
}

// TestE2E_MCP_ResourceClassification verifies that a context resource
// registered via the MCP gateway stores the caller-supplied classification.
func TestE2E_MCP_ResourceClassification(t *testing.T) {
	body := `{"uri":"file:///e2e/mcp-classification","hash":"sha256:deadbeef","classification":"confidential","access_scope":["team-alpha"]}`
	req, _ := http.NewRequest("POST", server.URL+"/mcp/resources?tenant_id="+testTenant, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	refID, ok := result["context_ref_id"].(string)
	require.True(t, ok, "response must include context_ref_id")
	assert.Equal(t, "file:///e2e/mcp-classification", result["uri"])

	contextRefRepo := pgdriver.NewContextRefRepo(pool)
	ref, err := contextRefRepo.Get(context.Background(), testTenant, refID)
	require.NoError(t, err)
	assert.Equal(t, "confidential", ref.Classification,
		"classification must be persisted as supplied by the MCP client")
	assert.Equal(t, "mcp_resource", ref.Type)
	assert.Equal(t, "sha256:deadbeef", ref.Hash)
	assert.Contains(t, ref.AccessScope, "team-alpha")
}

// TestE2E_MCP_AuditQuery verifies that after a successful MCP tool call, the
// task.created event is projected into audit_event_projection and is queryable
// via the tenant audit endpoint.
func TestE2E_MCP_AuditQuery(t *testing.T) {
	const callID = "mcp-call-audit-1"
	resp := mcpToolCall(t, callID, "mcp_audit_target")
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	events := waitForAuditEvents(t, callID, 5*time.Second)
	require.NotEmpty(t, events, "task.created event must be projected to audit store")

	var foundCreated bool
	for _, evt := range events {
		if evt.EventType == core.EventTaskCreated && evt.TaskID == callID {
			foundCreated = true
		}
	}
	assert.True(t, foundCreated, "audit trail must contain task.created for the MCP-originated task")
}

// TestE2E_MCP_ToolCallStatus verifies the MCP status endpoint returns the
// correct status for a task created via an MCP tool call.
func TestE2E_MCP_ToolCallStatus(t *testing.T) {
	const callID = "mcp-call-status-1"
	resp := mcpToolCall(t, callID, "mcp_status_target")
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	statusResp := mcpToolCallStatus(t, callID)
	defer statusResp.Body.Close()
	require.Equal(t, http.StatusOK, statusResp.StatusCode)

	var result map[string]interface{}
	json.NewDecoder(statusResp.Body).Decode(&result)
	assert.Equal(t, callID, result["call_id"])
	status, _ := result["status"].(string)
	assert.Contains(t, []string{"created", "queued"}, status)
}

// ---------------------------------------------------------------------------
// SDK-06: WebSocket Dashboard — real-time event delivery
// ---------------------------------------------------------------------------

// TestE2E_WS_DashboardEventDelivery connects a gorilla/websocket client to the
// dashboard endpoint, publishes a task via HTTP, and asserts that the
// corresponding task.created event is delivered on the WS stream.
func TestE2E_WS_DashboardEventDelivery(t *testing.T) {
	const wsTaskID = "ws-dashboard-task-1"

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws?tenant_id=" + testTenant
	ws, _, err := websocket.DefaultDialer.Dial(wsURL, http.Header{"Origin": []string{server.URL}})
	require.NoError(t, err)
	defer ws.Close()

	// Allow the subscription to register in the broadcaster before publishing.
	time.Sleep(200 * time.Millisecond)

	taskBody := map[string]interface{}{
		"id":              wsTaskID,
		"source_agent":    "agent-1",
		"target_type":     "agent",
		"target_value":    "agent-2",
		"mailbox_id":      "mb-1",
		"idempotency_key": "ws-dashboard-key-1",
		"envelope": map[string]interface{}{
			"janus_version": "0.1.0",
			"task_id":       wsTaskID,
			"tenant_id":     testTenant,
			"source_agent":  "agent-1",
			"target":        map[string]string{"type": "agent", "value": "agent-2"},
			"payload":       map[string]string{"type": "json", "content": `{"ws":"dashboard"}`},
			"trace":         map[string]string{"trace_id": "ws-trace-1"},
		},
	}
	createResp := mustRequest(t, "POST", "/v1/tenants/"+testTenant+"/tasks", taskBody)
	createResp.Body.Close()
	require.Equal(t, http.StatusCreated, createResp.StatusCode)

	// Read WS messages until we find our task event or time out.
	ws.SetReadDeadline(time.Now().Add(5 * time.Second))
	var matchedEvent *core.JanusEvent
	for {
		_, msg, err := ws.ReadMessage()
		if err != nil {
			break
		}
		var evt core.JanusEvent
		if json.Unmarshal(msg, &evt) != nil {
			continue
		}
		if evt.TaskID == wsTaskID && evt.TenantID == testTenant {
			matchedEvent = &evt
			break
		}
		ws.SetReadDeadline(time.Now().Add(5 * time.Second))
	}

	require.NotNil(t, matchedEvent, "WS client must receive event for the published task")
	assert.Equal(t, core.EventTaskCreated, matchedEvent.EventType,
		"first event for a new task must be task.created")
}

// TestE2E_WS_TenantIsolation verifies that a WS client subscribed to one tenant
// does not receive events published for a different tenant.
func TestE2E_WS_TenantIsolation(t *testing.T) {
	const otherTenant = "ws-iso-other"

	ctx := natsDriverCtx(otherTenant)
	err := natsDrv.EnsureTenant(ctx, otherTenant)
	require.NoError(t, err)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws?tenant_id=" + testTenant
	ws, _, err := websocket.DefaultDialer.Dial(wsURL, http.Header{"Origin": []string{server.URL}})
	require.NoError(t, err)
	defer ws.Close()
	time.Sleep(200 * time.Millisecond)

	err = natsDrv.PublishEvent(natsDriverCtx(otherTenant), core.JanusEvent{
		EventID:     "evt-ws-iso-1",
		EventType:   core.EventTaskCreated,
		TenantID:    otherTenant,
		TaskID:      "ws-iso-other-task",
		SourceAgent: "agent-x",
		Timestamp:   time.Now().UTC(),
		Payload:     []byte(`{}`),
	})
	require.NoError(t, err)

	ws.SetReadDeadline(time.Now().Add(1 * time.Second))
	_, _, err = ws.ReadMessage()
	assert.Error(t, err, "WS client for testTenant must not receive otherTenant events")
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func ensureSetup(t *testing.T) {
	t.Helper()
	resp := mustRequest(t, "POST", "/v1/tenants", map[string]string{"id": testTenant, "name": "E2E Test Tenant"})
	resp.Body.Close()

	resp = mustRequest(t, "POST", "/v1/tenants/"+testTenant+"/agents", map[string]interface{}{
		"id": "agent-1", "display_name": "Test Agent", "protocol": "a2a",
	})
	resp.Body.Close()

	resp = mustRequest(t, "POST", "/v1/tenants/"+testTenant+"/mailboxes", map[string]interface{}{
		"id": "mb-1", "agent_id": "agent-1", "max_concurrency": 5, "ack_wait_seconds": 300, "max_deliver": 3, "retention_seconds": 86400,
	})
	resp.Body.Close()
}

func natsDriverCtx(tenantID string) context.Context {
	return natsdriver.ContextWithTenant(context.Background(), tenantID)
}

func mcpToolCall(t *testing.T, callID, target string) *http.Response {
	t.Helper()
	body := fmt.Sprintf(`{"call_id":%q,"tool_name":"search","arguments":"test-query","target":%q}`, callID, target)
	req, err := http.NewRequest("POST", server.URL+"/mcp/tools/call?tenant_id="+testTenant+"&source_agent=mcp-e2e-agent", strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func mcpToolCallStatus(t *testing.T, callID string) *http.Response {
	t.Helper()
	req, err := http.NewRequest("GET", server.URL+"/mcp/tools/calls/"+callID+"/status?tenant_id="+testTenant, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func waitForAuditEvents(t *testing.T, taskID string, maxWait time.Duration) []*core.JanusEvent {
	t.Helper()
	deadline := time.Now().Add(maxWait)
	for time.Now().Before(deadline) {
		resp := mustRequest(t, "GET", fmt.Sprintf("/v1/tenants/%s/tasks/%s/events", testTenant, taskID), nil)
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var result struct {
			Events []*core.JanusEvent `json:"events"`
		}
		if json.Unmarshal(raw, &result) == nil && len(result.Events) > 0 {
			return result.Events
		}
		time.Sleep(200 * time.Millisecond)
	}
	return nil
}
