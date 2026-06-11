package a2a

import (
	"fmt"
	"time"

	"github.com/agentium-lab/Janus/core"
)

type AgentCard struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	URL         string            `json:"url"`
	Provider    AgentProvider     `json:"provider"`
	Version     string            `json:"version"`
	Capabilities []AgentCapability `json:"capabilities"`
	Auth        *AuthInfo         `json:"auth,omitempty"`
}

type AgentProvider struct {
	Organization string `json:"organization"`
	URL          string `json:"url"`
}

type AgentCapability struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	URL         string `json:"url"`
	Input       *Schema `json:"input,omitempty"`
	Output      *Schema `json:"output,omitempty"`
}

type Schema struct {
	Type       string                 `json:"type"`
	Properties map[string]interface{} `json:"properties,omitempty"`
}

type AuthInfo struct {
	Schemes []string `json:"schemes"`
}

type SendMessageRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	ID      string          `json:"id"`
	Params  SendMessageParams `json:"params"`
}

type SendMessageParams struct {
	Message AgentMessage `json:"message"`
}

type AgentMessage struct {
	Role        string      `json:"role"`
	Parts       []MessagePart `json:"parts"`
	TaskID      string      `json:"taskId,omitempty"`
	ContextID   string      `json:"contextId,omitempty"`
	ReferenceTaskID string `json:"referenceTaskId,omitempty"`
}

type MessagePart struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}

type TaskStatus struct {
	TaskID string `json:"taskId"`
	State  string `json:"state"`
}

type TaskStatusEvent struct {
	TaskID    string `json:"taskId"`
	State     string `json:"state"`
	UpdatedAt string `json:"updatedAt"`
}

func CardToAgent(tenantID string, card AgentCard) core.Agent {
	return core.Agent{
		TenantID:    tenantID,
		ID:          card.ID,
		DisplayName: card.Name,
		Protocol:    "a2a",
		Endpoint:    card.URL,
		Status:      core.AgentStatusOnline,
		Description: card.Description,
	}
}

func CardToCapabilities(tenantID, agentID string, card AgentCard) []CapabilityEntry {
	var caps []CapabilityEntry
	for _, c := range card.Capabilities {
		caps = append(caps, CapabilityEntry{
			TenantID:    tenantID,
			AgentID:     agentID,
			Capability:  c.Name,
			Description: c.Description,
		})
	}
	return caps
}

type CapabilityEntry struct {
	TenantID    string
	AgentID     string
	Capability  string
	Description string
}

func MessageToTask(msg SendMessageRequest, tenantID, sourceAgent, mailboxID string) core.Task {
	taskID := msg.ID
	if taskID == "" {
		taskID = generateID()
	}

	var content string
	for _, p := range msg.Params.Message.Parts {
		if p.Type == "text" && p.Text != "" {
			content = p.Text
			break
		}
	}

	traceID := msg.Params.Message.ContextID
	if traceID == "" {
		traceID = generateID()
	}

	return core.Task{
		TenantID:    tenantID,
		ID:          taskID,
		SourceAgent: sourceAgent,
		TargetType:  core.TargetTypeMailbox,
		TargetValue: mailboxID,
		MailboxID:   mailboxID,
		Status:      core.TaskStatusCreated,
		Priority:    core.PriorityNormal,
		Envelope: core.TaskEnvelope{
			JanusVersion: "0.1",
			TaskID:       taskID,
			TenantID:     tenantID,
			SourceAgent:  sourceAgent,
			Target:       core.Target{Type: core.TargetTypeMailbox, Value: mailboxID},
			Priority:     core.PriorityNormal,
			Payload:      core.Payload{Type: "a2a_message", Content: content},
			Trace:        core.TraceContext{TraceID: traceID},
		},
	}
}

func JanusStatusToA2A(status core.TaskStatus) string {
	switch status {
	case core.TaskStatusCreated, core.TaskStatusQueued, core.TaskStatusClaimed, core.TaskStatusRetryScheduled:
		return "submitted"
	case core.TaskStatusRunning:
		return "working"
	case core.TaskStatusBlocked, core.TaskStatusApprovalPending:
		return "input-required"
	case core.TaskStatusCompleted:
		return "completed"
	case core.TaskStatusFailed, core.TaskStatusDeadLettered, core.TaskStatusExpired:
		return "failed"
	case core.TaskStatusCancelled:
		return "canceled"
	default:
		return "unknown"
	}
}

func generateID() string {
	return fmt.Sprintf("task_%d", time.Now().UnixNano())
}
