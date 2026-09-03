package janus

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/agentium-lab/Janus/core"
)

type Client struct {
	baseURL    string
	tenantID   string
	apiKey     string
	httpClient *http.Client
}

type APIKey struct {
	ID         string     `json:"id"`
	TenantID   string     `json:"tenant_id"`
	Name       string     `json:"name"`
	Prefix     string     `json:"prefix"`
	Scopes     []string   `json:"scopes,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
	LastUsedAt *time.Time `json:"last_used_at,omitempty"`
	RevokedAt  *time.Time `json:"revoked_at,omitempty"`
}

type CreateAPIKeyRequest struct {
	Name string `json:"name"`
}

type CreatedAPIKey struct {
	APIKey
	Key string `json:"key"`
}

type CreatePolicyRuleRequest struct {
	ID        string                 `json:"id"`
	Name      string                 `json:"name"`
	Status    string                 `json:"status,omitempty"`
	Priority  int                    `json:"priority,omitempty"`
	Condition map[string]interface{} `json:"condition"`
	Action    map[string]interface{} `json:"action"`
}

type PolicyRuleTemplateRequest = core.PolicyRuleTemplateRequest

type PolicyRuleListResponse struct {
	PolicyRules []core.PolicyRule `json:"policy_rules"`
}

type BudgetRequest struct {
	ScopeType      string  `json:"scope_type"`
	ScopeID        string  `json:"scope_id,omitempty"`
	RPM            int     `json:"rpm,omitempty"`
	TPM            int     `json:"tpm,omitempty"`
	MaxConcurrency int     `json:"max_concurrency,omitempty"`
	DailyCostUSD   float64 `json:"daily_cost_usd,omitempty"`
	MonthlyCostUSD float64 `json:"monthly_cost_usd,omitempty"`
}

type BudgetSpec struct {
	TenantID       string    `json:"tenant_id"`
	ScopeType      string    `json:"scope_type"`
	ScopeID        string    `json:"scope_id"`
	RPM            int       `json:"rpm,omitempty"`
	TPM            int       `json:"tpm,omitempty"`
	MaxConcurrency int       `json:"max_concurrency,omitempty"`
	DailyCostUSD   float64   `json:"daily_cost_usd,omitempty"`
	MonthlyCostUSD float64   `json:"monthly_cost_usd,omitempty"`
	CreatedAt      time.Time `json:"created_at,omitempty"`
	UpdatedAt      time.Time `json:"updated_at,omitempty"`
}

type BudgetListResponse struct {
	Budgets []BudgetSpec `json:"budgets"`
}

type Config struct {
	BaseURL  string
	TenantID string
	APIKey   string
}

type APIError struct {
	StatusCode int
	Code       string
	Message    string
}

func (e *APIError) Error() string {
	if e == nil {
		return ""
	}
	if e.Code != "" && e.Message != "" {
		return fmt.Sprintf("api error (%d %s): %s", e.StatusCode, e.Code, e.Message)
	}
	if e.Message != "" {
		return fmt.Sprintf("api error (%d): %s", e.StatusCode, e.Message)
	}
	return fmt.Sprintf("api error: status %d", e.StatusCode)
}

func NewClient(cfg Config) *Client {
	return &Client{
		baseURL:  cfg.BaseURL,
		tenantID: cfg.TenantID,
		apiKey:   cfg.APIKey,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *Client) CreateTenant(ctx context.Context, id, name string) error {
	return c.doPost(ctx, "/v1/tenants", map[string]string{"id": id, "name": name}, nil)
}

func (c *Client) GetTenant(ctx context.Context, id string) (*core.Tenant, error) {
	var tenant core.Tenant
	if err := c.doGet(ctx, "/v1/tenants/"+url.PathEscape(id), &tenant); err != nil {
		return nil, err
	}
	return &tenant, nil
}

func (c *Client) CreateMailbox(ctx context.Context, id, agentID string) error {
	_, err := c.CreateMailboxWithConfig(ctx, CreateMailboxRequest{ID: id, AgentID: agentID})
	return err
}

type RegisterAgentCapability struct {
	Capability  string `json:"capability"`
	Schema      string `json:"schema,omitempty"`
	Description string `json:"description,omitempty"`
}

type RegisterAgentRequest struct {
	ID             string                    `json:"id"`
	DisplayName    string                    `json:"display_name,omitempty"`
	TeamID         string                    `json:"team_id,omitempty"`
	Protocol       string                    `json:"protocol"`
	Endpoint       string                    `json:"endpoint,omitempty"`
	Description    string                    `json:"description,omitempty"`
	MaxConcurrency int                       `json:"max_concurrency,omitempty"`
	RPM            int                       `json:"rpm,omitempty"`
	TPM            int                       `json:"tpm,omitempty"`
	Capabilities   []RegisterAgentCapability `json:"capabilities,omitempty"`
}

func (c *Client) RegisterAgent(ctx context.Context, req RegisterAgentRequest) error {
	return c.doPost(ctx, "/v1/tenants/"+c.tenantID+"/agents", req, nil)
}

func (c *Client) ListAgents(ctx context.Context) ([]core.Agent, error) {
	var resp struct {
		Agents []core.Agent `json:"agents"`
	}
	if err := c.doGet(ctx, "/v1/tenants/"+c.tenantID+"/agents", &resp); err != nil {
		return nil, err
	}
	return resp.Agents, nil
}

func (c *Client) GetAgent(ctx context.Context, agentID string) (*core.Agent, error) {
	var agent core.Agent
	if err := c.doGet(ctx, "/v1/tenants/"+c.tenantID+"/agents/"+agentID, &agent); err != nil {
		return nil, err
	}
	return &agent, nil
}

func (c *Client) HeartbeatAgent(ctx context.Context, agentID string) error {
	return c.doPost(ctx, "/v1/tenants/"+c.tenantID+"/agents/"+agentID+"/heartbeat", nil, nil)
}

type PublishTaskRequest struct {
	ID             string            `json:"id"`
	SourceAgent    string            `json:"source_agent"`
	TargetType     string            `json:"target_type"`
	TargetValue    string            `json:"target_value"`
	MailboxID      string            `json:"mailbox_id"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	Priority       string            `json:"priority"`
	Envelope       core.TaskEnvelope `json:"envelope"`
}

type PublishTaskResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

func (c *Client) PublishTask(ctx context.Context, req PublishTaskRequest) (*PublishTaskResponse, error) {
	var resp PublishTaskResponse
	if err := c.doPost(ctx, "/v1/tenants/"+c.tenantID+"/tasks", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) GetTask(ctx context.Context, taskID string) (*core.Task, error) {
	var task core.Task
	if err := c.doGet(ctx, "/v1/tenants/"+c.tenantID+"/tasks/"+taskID, &task); err != nil {
		return nil, err
	}
	return &task, nil
}

type PullResult struct {
	Task  *core.Task `json:"task"`
	Lease struct {
		LeaseID   string      `json:"lease_id"`
		Attempt   int         `json:"attempt"`
		ExpiresAt interface{} `json:"expires_at"`
	} `json:"lease"`
}

func (c *Client) PullTask(ctx context.Context, mailboxID string, agentID string) (*PullResult, error) {
	if strings.TrimSpace(agentID) == "" {
		return nil, fmt.Errorf("agent id is required")
	}
	body := map[string]string{"agent_id": agentID}
	path := "/v1/tenants/" + c.tenantID + "/mailboxes/" + mailboxID + "/pull"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("X-API-Key", c.apiKey)
	}
	b, _ := json.Marshal(body)
	req.Body = io.NopCloser(bytes.NewReader(b))
	req.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(b)), nil }
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	}
	if resp.StatusCode >= 400 {
		return nil, decodeAPIError(resp)
	}

	var result PullResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &result, nil
}

func (c *Client) StartTask(ctx context.Context, taskID string, attempt int, leaseID string) error {
	body := map[string]interface{}{"attempt": attempt, "lease_id": leaseID}
	return c.doPost(ctx, "/v1/tenants/"+c.tenantID+"/tasks/"+taskID+"/start", body, nil)
}

func (c *Client) Heartbeat(ctx context.Context, taskID string, attempt int, leaseID string) error {
	body := map[string]interface{}{"attempt": attempt, "lease_id": leaseID}
	return c.doPost(ctx, "/v1/tenants/"+c.tenantID+"/tasks/"+taskID+"/heartbeat", body, nil)
}

type AckRequest struct {
	LeaseID    string           `json:"lease_id"`
	Attempt    int              `json:"attempt"`
	ResultRef  string           `json:"result_ref,omitempty"`
	TokenUsage *core.TokenUsage `json:"token_usage,omitempty"`
}

func (c *Client) AckTask(ctx context.Context, taskID string, req AckRequest) error {
	return c.doPost(ctx, "/v1/tenants/"+c.tenantID+"/tasks/"+taskID+"/ack", req, nil)
}

type NackRequest struct {
	LeaseID   string          `json:"lease_id"`
	Attempt   int             `json:"attempt"`
	Retriable bool            `json:"retriable"`
	Error     *core.TaskError `json:"error,omitempty"`
}

func (c *Client) NackTask(ctx context.Context, taskID string, req NackRequest) error {
	return c.doPost(ctx, "/v1/tenants/"+c.tenantID+"/tasks/"+taskID+"/nack", req, nil)
}

func (c *Client) CancelTask(ctx context.Context, taskID string) error {
	return c.doPost(ctx, "/v1/tenants/"+c.tenantID+"/tasks/"+taskID+"/cancel", nil, nil)
}

// ReportProgress sends a mid-task progress update visible to stream subscribers.
func (c *Client) ReportProgress(ctx context.Context, taskID, message, agentID string, percent *int, data map[string]interface{}) error {
	body := map[string]interface{}{
		"message":  message,
		"agent_id": agentID,
	}
	if percent != nil {
		body["percent"] = *percent
	}
	if data != nil {
		body["data"] = data
	}
	return c.doPost(ctx, "/v1/tenants/"+c.tenantID+"/tasks/"+taskID+"/progress", body, nil)
}

// StreamEvent is one SSE event from a task stream.
type StreamEvent struct {
	EventType string                 `json:"event_type"`
	TaskID    string                 `json:"task_id"`
	Payload   map[string]interface{} `json:"payload"`
}

// StreamTask subscribes to a task's SSE stream and calls fn for each event.
// Returns when the task reaches a terminal state or ctx is cancelled.
func (c *Client) StreamTask(ctx context.Context, taskID string, fn func(StreamEvent) error) error {
	url := c.baseURL + "/v1/tenants/" + c.tenantID + "/tasks/" + taskID + "/stream"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	if c.apiKey != "" {
		req.Header.Set("X-API-Key", c.apiKey)
	}
	req.Header.Set("Accept", "text/event-stream")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("stream failed: status %d", resp.StatusCode)
	}

	scanner := bufio.NewScanner(resp.Body)
	eventType := ""
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "event: ") {
			eventType = strings.TrimPrefix(line, "event: ")
		} else if strings.HasPrefix(line, "data: ") && eventType != "" {
			var evt StreamEvent
			evt.EventType = eventType
			if err := json.Unmarshal([]byte(line[6:]), &evt); err == nil {
				if err := fn(evt); err != nil {
					return err
				}
			}
			switch eventType {
			case "task.completed", "task.failed", "task.cancelled", "task.dead_lettered", "task.expired":
				return nil
			}
			eventType = ""
		}
	}
	return scanner.Err()
}

func (c *Client) ReplayTask(ctx context.Context, taskID string) (*core.Task, error) {
	var task core.Task
	if err := c.doPost(ctx, "/v1/tenants/"+c.tenantID+"/tasks/"+taskID+"/replay", nil, &task); err != nil {
		return nil, err
	}
	return &task, nil
}

func (c *Client) GetTaskEvents(ctx context.Context, taskID string) ([]*core.JanusEvent, error) {
	var resp struct {
		Events []*core.JanusEvent `json:"events"`
	}
	if err := c.doGet(ctx, "/v1/tenants/"+c.tenantID+"/tasks/"+taskID+"/events", &resp); err != nil {
		return nil, err
	}
	return resp.Events, nil
}

func (c *Client) CreateAPIKey(ctx context.Context, req CreateAPIKeyRequest) (*CreatedAPIKey, error) {
	var resp CreatedAPIKey
	if err := c.doPost(ctx, "/v1/tenants/"+c.tenantID+"/api-keys", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) ListAPIKeys(ctx context.Context) ([]APIKey, error) {
	var resp struct {
		APIKeys []APIKey `json:"api_keys"`
	}
	if err := c.doGet(ctx, "/v1/tenants/"+c.tenantID+"/api-keys", &resp); err != nil {
		return nil, err
	}
	return resp.APIKeys, nil
}

func (c *Client) RevokeAPIKey(ctx context.Context, keyID string) (*APIKey, error) {
	var resp APIKey
	if err := c.doPost(ctx, "/v1/tenants/"+c.tenantID+"/api-keys/"+keyID+"/revoke", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) CreatePolicyRule(ctx context.Context, req CreatePolicyRuleRequest) (*core.PolicyRule, error) {
	var resp core.PolicyRule
	if err := c.doPost(ctx, "/v1/tenants/"+c.tenantID+"/policy-rules", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) CreatePolicyRuleFromTemplate(ctx context.Context, req PolicyRuleTemplateRequest) (*core.PolicyRule, error) {
	var resp core.PolicyRule
	if err := c.doPost(ctx, "/v1/tenants/"+c.tenantID+"/policy-rules/templates", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) ListPolicyRules(ctx context.Context) ([]core.PolicyRule, error) {
	var resp PolicyRuleListResponse
	if err := c.doGet(ctx, "/v1/tenants/"+c.tenantID+"/policy-rules", &resp); err != nil {
		return nil, err
	}
	return resp.PolicyRules, nil
}

func (c *Client) UpsertBudget(ctx context.Context, req BudgetRequest) (*BudgetSpec, error) {
	var resp BudgetSpec
	if err := c.doPost(ctx, "/v1/tenants/"+c.tenantID+"/budgets", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) GetBudget(ctx context.Context, scopeType, scopeID string) (*BudgetSpec, error) {
	var resp BudgetSpec
	if err := c.doGet(ctx, "/v1/tenants/"+c.tenantID+"/budgets/"+url.PathEscape(scopeType)+"/"+url.PathEscape(scopeID), &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) ListBudgets(ctx context.Context) ([]BudgetSpec, error) {
	var resp BudgetListResponse
	if err := c.doGet(ctx, "/v1/tenants/"+c.tenantID+"/budgets", &resp); err != nil {
		return nil, err
	}
	return resp.Budgets, nil
}

type CreateMailboxRequest struct {
	ID               string `json:"id"`
	AgentID          string `json:"agent_id"`
	MaxConcurrency   int    `json:"max_concurrency,omitempty"`
	ACKWaitSeconds   int    `json:"ack_wait_seconds,omitempty"`
	MaxDeliver       int    `json:"max_deliver,omitempty"`
	RetentionSeconds int    `json:"retention_seconds,omitempty"`
}

type UpdateMailboxRequest struct {
	MaxConcurrency   *int `json:"max_concurrency,omitempty"`
	ACKWaitSeconds   *int `json:"ack_wait_seconds,omitempty"`
	MaxDeliver       *int `json:"max_deliver,omitempty"`
	RetentionSeconds *int `json:"retention_seconds,omitempty"`
}

type MailboxActionResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

func (c *Client) CreateMailboxWithConfig(ctx context.Context, req CreateMailboxRequest) (*MailboxActionResponse, error) {
	var resp MailboxActionResponse
	if err := c.doPost(ctx, "/v1/tenants/"+c.tenantID+"/mailboxes", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) GetMailbox(ctx context.Context, mailboxID string) (*core.Mailbox, error) {
	var mailbox core.Mailbox
	if err := c.doGet(ctx, "/v1/tenants/"+c.tenantID+"/mailboxes/"+mailboxID, &mailbox); err != nil {
		return nil, err
	}
	return &mailbox, nil
}

func (c *Client) UpdateMailbox(ctx context.Context, mailboxID string, req UpdateMailboxRequest) (*MailboxActionResponse, error) {
	var resp MailboxActionResponse
	if err := c.doPatch(ctx, "/v1/tenants/"+c.tenantID+"/mailboxes/"+mailboxID, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) PauseMailbox(ctx context.Context, mailboxID string) (*MailboxActionResponse, error) {
	var resp MailboxActionResponse
	if err := c.doPost(ctx, "/v1/tenants/"+c.tenantID+"/mailboxes/"+mailboxID+"/pause", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) ResumeMailbox(ctx context.Context, mailboxID string) (*MailboxActionResponse, error) {
	var resp MailboxActionResponse
	if err := c.doPost(ctx, "/v1/tenants/"+c.tenantID+"/mailboxes/"+mailboxID+"/resume", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

type DLQQueryOptions struct {
	MailboxID string
	Limit     int
}

type DLQQueryResponse struct {
	Tasks []*core.Task `json:"tasks"`
}

func (c *Client) QueryDLQ(ctx context.Context, opts DLQQueryOptions) ([]*core.Task, error) {
	path := "/v1/tenants/" + c.tenantID + "/dlq"
	query := url.Values{}
	if opts.MailboxID != "" {
		query.Set("mailbox", opts.MailboxID)
	}
	if opts.Limit > 0 {
		query.Set("limit", fmt.Sprintf("%d", opts.Limit))
	}
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}
	var resp DLQQueryResponse
	if err := c.doGet(ctx, path, &resp); err != nil {
		return nil, err
	}
	return resp.Tasks, nil
}

func (c *Client) ReplayDLQ(ctx context.Context, taskID string) (*core.Task, error) {
	var task core.Task
	if err := c.doPost(ctx, "/v1/tenants/"+c.tenantID+"/dlq/"+taskID+"/replay", nil, &task); err != nil {
		return nil, err
	}
	return &task, nil
}

func (c *Client) DiscardDLQ(ctx context.Context, taskID string) error {
	return c.doPost(ctx, "/v1/tenants/"+c.tenantID+"/dlq/"+taskID+"/discard", nil, nil)
}

func (c *Client) doGet(ctx context.Context, path string, result interface{}) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	return c.do(req, result)
}

func (c *Client) doPatch(ctx context.Context, path string, body interface{}, result interface{}) error {
	return c.doWithBody(ctx, http.MethodPatch, path, body, result)
}

func (c *Client) doPost(ctx context.Context, path string, body interface{}, result interface{}) error {
	return c.doWithBody(ctx, http.MethodPost, path, body, result)
}

func (c *Client) doWithBody(ctx context.Context, method string, path string, body interface{}, result interface{}) error {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal body: %w", err)
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return c.do(req, result)
}

func (c *Client) do(req *http.Request, result interface{}) error {
	if c.apiKey != "" {
		req.Header.Set("X-API-Key", c.apiKey)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil
	}

	if resp.StatusCode >= 400 {
		return decodeAPIError(resp)
	}

	if result != nil {
		if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}
	return nil
}

func decodeAPIError(resp *http.Response) error {
	apiErr := &APIError{
		StatusCode: resp.StatusCode,
		Code:       apiErrorCode(resp.StatusCode),
		Message:    http.StatusText(resp.StatusCode),
	}

	var errResp struct {
		Error   string `json:"error"`
		Code    string `json:"code"`
		Message string `json:"message"`
		Status  int    `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&errResp); err == nil {
		if errResp.Status != 0 {
			apiErr.StatusCode = errResp.Status
		}
		if errResp.Code != "" {
			apiErr.Code = errResp.Code
		}
		if errResp.Message != "" {
			apiErr.Message = errResp.Message
		} else if errResp.Error != "" {
			apiErr.Message = errResp.Error
		}
	}
	return apiErr
}

func apiErrorCode(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "INVALID_ARGUMENT"
	case http.StatusUnauthorized:
		return "UNAUTHENTICATED"
	case http.StatusForbidden:
		return "PERMISSION_DENIED"
	case http.StatusNotFound:
		return "NOT_FOUND"
	case http.StatusConflict:
		return "CONFLICT"
	case http.StatusTooManyRequests:
		return "RESOURCE_EXHAUSTED"
	case http.StatusServiceUnavailable:
		return "UNAVAILABLE"
	default:
		if status >= 500 {
			return "INTERNAL"
		}
		return "UNKNOWN"
	}
}
