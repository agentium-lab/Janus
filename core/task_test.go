package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTaskStatus_IsTerminal(t *testing.T) {
	tests := []struct {
		name     string
		status   TaskStatus
		expected bool
	}{
		{"completed is terminal", TaskStatusCompleted, true},
		{"dead_lettered is terminal", TaskStatusDeadLettered, true},
		{"expired is terminal", TaskStatusExpired, true},
		{"cancelled is terminal", TaskStatusCancelled, true},
		{"created is not terminal", TaskStatusCreated, false},
		{"queued is not terminal", TaskStatusQueued, false},
		{"claimed is not terminal", TaskStatusClaimed, false},
		{"running is not terminal", TaskStatusRunning, false},
		{"failed is not terminal", TaskStatusFailed, false},
		{"retry_scheduled is not terminal", TaskStatusRetryScheduled, false},
		{"blocked is not terminal", TaskStatusBlocked, false},
		{"approval_pending is not terminal", TaskStatusApprovalPending, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.status.IsTerminal())
		})
	}
}

func TestCanTransition(t *testing.T) {
	valid := []struct {
		from, to TaskStatus
	}{
		{TaskStatusCreated, TaskStatusQueued},
		{TaskStatusQueued, TaskStatusClaimed},
		{TaskStatusQueued, TaskStatusApprovalPending},
		{TaskStatusQueued, TaskStatusExpired},
		{TaskStatusQueued, TaskStatusCancelled},
		{TaskStatusApprovalPending, TaskStatusQueued},
		{TaskStatusApprovalPending, TaskStatusCancelled},
		{TaskStatusClaimed, TaskStatusRunning},
		{TaskStatusRunning, TaskStatusCompleted},
		{TaskStatusRunning, TaskStatusFailed},
		{TaskStatusRunning, TaskStatusBlocked},
		{TaskStatusRunning, TaskStatusCancelled},
		{TaskStatusBlocked, TaskStatusRunning},
		{TaskStatusBlocked, TaskStatusCancelled},
		{TaskStatusFailed, TaskStatusRetryScheduled},
		{TaskStatusFailed, TaskStatusDeadLettered},
		{TaskStatusRetryScheduled, TaskStatusQueued},
	}

	for _, tt := range valid {
		t.Run(string(tt.from)+"->"+string(tt.to), func(t *testing.T) {
			assert.True(t, CanTransition(tt.from, tt.to),
				"expected transition from %s to %s to be valid", tt.from, tt.to)
		})
	}
}

func TestCanTransition_Invalid(t *testing.T) {
	invalid := []struct {
		from, to TaskStatus
	}{
		{TaskStatusCreated, TaskStatusRunning},
		{TaskStatusCreated, TaskStatusCompleted},
		{TaskStatusQueued, TaskStatusRunning},
		{TaskStatusClaimed, TaskStatusCompleted},
		{TaskStatusClaimed, TaskStatusFailed},
		{TaskStatusCompleted, TaskStatusQueued},
		{TaskStatusCompleted, TaskStatusRunning},
		{TaskStatusDeadLettered, TaskStatusQueued},
		{TaskStatusExpired, TaskStatusQueued},
		{TaskStatusCancelled, TaskStatusQueued},
		{TaskStatusRunning, TaskStatusQueued},
		{TaskStatusRunning, TaskStatusClaimed},
	}

	for _, tt := range invalid {
		t.Run(string(tt.from)+"->"+string(tt.to), func(t *testing.T) {
			assert.False(t, CanTransition(tt.from, tt.to),
				"expected transition from %s to %s to be invalid", tt.from, tt.to)
		})
	}
}

func TestCanTransition_SelfTransition(t *testing.T) {
	statuses := []TaskStatus{
		TaskStatusCreated, TaskStatusQueued, TaskStatusRunning,
		TaskStatusCompleted, TaskStatusFailed,
	}
	for _, s := range statuses {
		t.Run(string(s)+"->"+string(s), func(t *testing.T) {
			assert.False(t, CanTransition(s, s))
		})
	}
}

func TestTaskEnvelope_Validate_RequiredFields(t *testing.T) {
	future := time.Now().Add(24 * time.Hour)
	validEnvelope := TaskEnvelope{
		JanusVersion: "0.1",
		TaskID:       "task_01HZY8Q9ZWZK3F8R0V8Y5B7M2Q",
		TenantID:     "acme",
		SourceAgent:  "product-agent.team-a",
		Target:       Target{Type: TargetTypeCapability, Value: "code_review"},
		Priority:     PriorityNormal,
		Payload:      Payload{Type: "code_review_request", Content: "Review this PR"},
		Trace:        TraceContext{TraceID: "trace_01HZY8Q9ZWZK3F8R0V8Y5B7M2Q"},
		Deadline:     &future,
	}

	t.Run("valid envelope passes", func(t *testing.T) {
		err := validEnvelope.Validate()
		assert.NoError(t, err)
	})

	missingFieldTests := []struct {
		name   string
		mutate func(*TaskEnvelope)
		field  string
	}{
		{
			"missing janus_version",
			func(e *TaskEnvelope) { e.JanusVersion = "" },
			"janus_version",
		},
		{
			"missing task_id",
			func(e *TaskEnvelope) { e.TaskID = "" },
			"task_id",
		},
		{
			"missing tenant_id",
			func(e *TaskEnvelope) { e.TenantID = "" },
			"tenant_id",
		},
		{
			"missing source_agent",
			func(e *TaskEnvelope) { e.SourceAgent = "" },
			"source_agent",
		},
		{
			"missing target type",
			func(e *TaskEnvelope) { e.Target.Type = "" },
			"target.type",
		},
		{
			"missing target value",
			func(e *TaskEnvelope) { e.Target.Value = "" },
			"target.value",
		},
		{
			"missing payload type",
			func(e *TaskEnvelope) { e.Payload.Type = "" },
			"payload.type",
		},
		{
			"missing trace_id",
			func(e *TaskEnvelope) { e.Trace.TraceID = "" },
			"trace.trace_id",
		},
	}

	for _, tt := range missingFieldTests {
		t.Run(tt.name, func(t *testing.T) {
			e := validEnvelope
			tt.mutate(&e)
			err := e.Validate()
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.field)
		})
	}
}

func TestTaskEnvelope_Validate_InvalidTargetType(t *testing.T) {
	envelope := validTestEnvelope()
	envelope.Target.Type = "invalid_type"
	err := envelope.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid target type")
}

func TestTaskEnvelope_Validate_InvalidPriority(t *testing.T) {
	envelope := validTestEnvelope()
	envelope.Priority = "urgent"
	err := envelope.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid priority")
}

func TestTaskEnvelope_Validate_DeadlineInPast(t *testing.T) {
	envelope := validTestEnvelope()
	past := time.Now().Add(-1 * time.Hour)
	envelope.Deadline = &past
	err := envelope.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "deadline")
}

func TestTaskEnvelope_Validate_NegativeTTL(t *testing.T) {
	envelope := validTestEnvelope()
	envelope.TTLSeconds = -1
	err := envelope.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ttl_seconds")
}

func TestTaskEnvelope_Validate_NegativeBudget(t *testing.T) {
	envelope := validTestEnvelope()
	envelope.Budget = &Budget{MaxTokens: -100}
	err := envelope.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "budget.max_tokens")
}

func TestTaskEnvelope_Validate_NegativeCost(t *testing.T) {
	envelope := validTestEnvelope()
	envelope.Budget = &Budget{MaxCostUSD: -1.0}
	err := envelope.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "budget.max_cost_usd")
}

func validTestEnvelope() TaskEnvelope {
	return TaskEnvelope{
		JanusVersion: "0.1",
		TaskID:       "task_01HZY8Q9ZWZK3F8R0V8Y5B7M2Q",
		TenantID:     "acme",
		SourceAgent:  "product-agent.team-a",
		Target:       Target{Type: TargetTypeCapability, Value: "code_review"},
		Priority:     PriorityNormal,
		Payload:      Payload{Type: "code_review_request", Content: "Review this PR"},
		Trace:        TraceContext{TraceID: "trace_01HZY8Q9ZWZK3F8R0V8Y5B7M2Q"},
	}
}
