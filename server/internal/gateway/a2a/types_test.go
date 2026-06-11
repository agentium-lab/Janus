package a2a

import (
	"testing"

	"github.com/agentium-lab/Janus/core"
	"github.com/stretchr/testify/assert"
)

func TestCardToAgent(t *testing.T) {
	card := AgentCard{
		ID:          "reviewer.team-a",
		Name:        "Code Reviewer",
		Description: "Reviews code for quality and security",
		URL:         "https://reviewer.internal/a2a",
		Version:     "1.0",
		Capabilities: []AgentCapability{
			{Name: "code_review", Description: "Review code changes"},
			{Name: "security_scan", Description: "Scan for vulnerabilities"},
		},
	}

	agent := CardToAgent("acme", card)
	assert.Equal(t, "acme", agent.TenantID)
	assert.Equal(t, "reviewer.team-a", agent.ID)
	assert.Equal(t, "Code Reviewer", agent.DisplayName)
	assert.Equal(t, core.AgentProtocol("a2a"), agent.Protocol)
	assert.Equal(t, "https://reviewer.internal/a2a", agent.Endpoint)
	assert.Equal(t, core.AgentStatusOnline, agent.Status)
}

func TestCardToCapabilities(t *testing.T) {
	card := AgentCard{
		ID: "reviewer",
		Capabilities: []AgentCapability{
			{Name: "code_review", Description: "Review code"},
			{Name: "security", Description: "Security scan"},
		},
	}

	caps := CardToCapabilities("acme", "reviewer", card)
	assert.Len(t, caps, 2)
	assert.Equal(t, "code_review", caps[0].Capability)
	assert.Equal(t, "security", caps[1].Capability)
}

func TestJanusStatusToA2A(t *testing.T) {
	tests := []struct {
		janus   core.TaskStatus
		a2a     string
	}{
		{core.TaskStatusCreated, "submitted"},
		{core.TaskStatusQueued, "submitted"},
		{core.TaskStatusClaimed, "submitted"},
		{core.TaskStatusRetryScheduled, "submitted"},
		{core.TaskStatusRunning, "working"},
		{core.TaskStatusBlocked, "input-required"},
		{core.TaskStatusApprovalPending, "input-required"},
		{core.TaskStatusCompleted, "completed"},
		{core.TaskStatusFailed, "failed"},
		{core.TaskStatusDeadLettered, "failed"},
		{core.TaskStatusExpired, "failed"},
		{core.TaskStatusCancelled, "canceled"},
	}

	for _, tt := range tests {
		t.Run(string(tt.janus), func(t *testing.T) {
			got := JanusStatusToA2A(tt.janus)
			assert.Equal(t, tt.a2a, got)
		})
	}
}

func TestMessageToTask(t *testing.T) {
	msg := SendMessageRequest{
		JSONRPC: "2.0",
		Method:  "message/send",
		ID:      "msg-001",
		Params: SendMessageParams{
			Message: AgentMessage{
				Role: "user",
				Parts: []MessagePart{
					{Type: "text", Text: "Review this PR for security issues"},
				},
				ContextID: "trace-001",
			},
		},
	}

	task := MessageToTask(msg, "acme", "product-agent", "reviewer-mailbox")
	assert.Equal(t, "msg-001", task.ID)
	assert.Equal(t, "acme", task.TenantID)
	assert.Equal(t, "product-agent", task.SourceAgent)
	assert.Equal(t, core.TargetTypeMailbox, task.TargetType)
	assert.Equal(t, "reviewer-mailbox", task.MailboxID)
	assert.Equal(t, "Review this PR for security issues", task.Envelope.Payload.Content)
	assert.Equal(t, "trace-001", task.Envelope.Trace.TraceID)
}
