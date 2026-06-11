package janus

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/agentium-lab/Janus/core"
)

type Client struct {
	baseURL    string
	tenantID   string
	httpClient *http.Client
}

type Config struct {
	BaseURL  string
	TenantID string
}

func NewClient(cfg Config) *Client {
	return &Client{
		baseURL:  cfg.BaseURL,
		tenantID: cfg.TenantID,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

type RegisterAgentRequest struct {
	ID          string `json:"id"`
	DisplayName string `json:"display_name"`
	Protocol    string `json:"protocol"`
	Endpoint    string `json:"endpoint,omitempty"`
	Description string `json:"description,omitempty"`
}

func (c *Client) RegisterAgent(ctx context.Context, req RegisterAgentRequest) error {
	return c.doPost(ctx, "/v1/tenants/"+c.tenantID+"/agents", req, nil)
}

type PublishTaskRequest struct {
	ID             string          `json:"id"`
	SourceAgent    string          `json:"source_agent"`
	TargetType     string          `json:"target_type"`
	TargetValue    string          `json:"target_value"`
	MailboxID      string          `json:"mailbox_id"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	Priority       string          `json:"priority"`
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
		ExpiresAt interface{} `json:"expires_at"`
	} `json:"lease"`
}

func (c *Client) PullTask(ctx context.Context, mailboxID string, agentID string) (*PullResult, error) {
	body := map[string]string{"agent_id": agentID}
	path := "/v1/tenants/" + c.tenantID + "/mailboxes/" + mailboxID + "/pull"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if body != nil {
		b, _ := json.Marshal(body)
		req.Body = io.NopCloser(bytes.NewReader(b))
		req.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(b)), nil }
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	}
	if resp.StatusCode >= 400 {
		var errResp struct {
			Error string `json:"error"`
		}
		if json.NewDecoder(resp.Body).Decode(&errResp) == nil && errResp.Error != "" {
			return nil, fmt.Errorf("api error (%d): %s", resp.StatusCode, errResp.Error)
		}
		return nil, fmt.Errorf("api error: status %d", resp.StatusCode)
	}

	var result PullResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &result, nil
}

func (c *Client) StartTask(ctx context.Context, taskID string, leaseID string) error {
	body := map[string]string{"lease_id": leaseID}
	return c.doPost(ctx, "/v1/tenants/"+c.tenantID+"/tasks/"+taskID+"/start", body, nil)
}

func (c *Client) Heartbeat(ctx context.Context, taskID string, leaseID string) error {
	body := map[string]string{"lease_id": leaseID}
	return c.doPost(ctx, "/v1/tenants/"+c.tenantID+"/tasks/"+taskID+"/heartbeat", body, nil)
}

type AckRequest struct {
	LeaseID    string           `json:"lease_id"`
	ResultRef  string           `json:"result_ref,omitempty"`
	TokenUsage *core.TokenUsage `json:"token_usage,omitempty"`
}

func (c *Client) AckTask(ctx context.Context, taskID string, req AckRequest) error {
	return c.doPost(ctx, "/v1/tenants/"+c.tenantID+"/tasks/"+taskID+"/ack", req, nil)
}

type NackRequest struct {
	LeaseID   string         `json:"lease_id"`
	Retriable bool           `json:"retriable"`
	Error     *core.TaskError `json:"error,omitempty"`
}

func (c *Client) NackTask(ctx context.Context, taskID string, req NackRequest) error {
	return c.doPost(ctx, "/v1/tenants/"+c.tenantID+"/tasks/"+taskID+"/nack", req, nil)
}

func (c *Client) CancelTask(ctx context.Context, taskID string) error {
	return c.doPost(ctx, "/v1/tenants/"+c.tenantID+"/tasks/"+taskID+"/cancel", nil, nil)
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

func (c *Client) doGet(ctx context.Context, path string, result interface{}) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	return c.do(req, result)
}

func (c *Client) doPost(ctx context.Context, path string, body interface{}, result interface{}) error {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal body: %w", err)
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, reader)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return c.do(req, result)
}

func (c *Client) do(req *http.Request, result interface{}) error {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil
	}

	if resp.StatusCode >= 400 {
		var errResp struct {
			Error string `json:"error"`
		}
		if json.NewDecoder(resp.Body).Decode(&errResp) == nil && errResp.Error != "" {
			return fmt.Errorf("api error (%d): %s", resp.StatusCode, errResp.Error)
		}
		return fmt.Errorf("api error: status %d", resp.StatusCode)
	}

	if result != nil {
		if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}
	return nil
}
