package a2a

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/agentium-lab/Janus/core"
)

// ─────────────────────────────────────────────────────────────────────────────
// A2A protocol v1.0 data types (HTTP+JSON binding).
//
// Wire format is camelCase per the canonical a2a.proto. StreamResponse is a
// single-field discriminated union: exactly one of task/message/statusUpdate/
// artifactUpdate is emitted. There is NO `final` field in v1.0 — terminality
// is signaled by status.state reaching a terminal value.
// ─────────────────────────────────────────────────────────────────────────────

// V1TaskState enum values (a2a.proto TaskState).
const (
	V1StateUnspecified   = "TASK_STATE_UNSPECIFIED"
	V1StateSubmitted     = "TASK_STATE_SUBMITTED"
	V1StateWorking       = "TASK_STATE_WORKING"
	V1StateCompleted     = "TASK_STATE_COMPLETED"
	V1StateFailed        = "TASK_STATE_FAILED"
	V1StateCanceled      = "TASK_STATE_CANCELED"
	V1StateInputRequired = "TASK_STATE_INPUT_REQUIRED"
	V1StateRejected      = "TASK_STATE_REJECTED"
	V1StateAuthRequired  = "TASK_STATE_AUTH_REQUIRED"
)

// V1SendMessageRequest is the flat REST body for message:send / message:stream.
type V1SendMessageRequest struct {
	Message       V1Message              `json:"message"`
	Configuration *V1SendConfiguration   `json:"configuration,omitempty"`
	Metadata      map[string]interface{} `json:"metadata,omitempty"`
}

type V1SendConfiguration struct {
	AcceptedOutputModes []string `json:"acceptedOutputModes,omitempty"`
	HistoryLength       int32    `json:"historyLength,omitempty"`
	ReturnImmediately   bool     `json:"returnImmediately,omitempty"`
}

// V1Message is an A2A Message (role + parts).
type V1Message struct {
	MessageID string                 `json:"messageId"`
	Role      string                 `json:"role"` // ROLE_USER | ROLE_AGENT
	Parts     []V1Part               `json:"parts"`
	ContextID string                 `json:"contextId,omitempty"`
	TaskID    string                 `json:"taskId,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

// V1Part is a flattened Part oneof: exactly one of text/data/raw/url.
type V1Part struct {
	Text      string                 `json:"text,omitempty"`
	Data      map[string]interface{} `json:"data,omitempty"`
	Raw       []byte                 `json:"raw,omitempty"` // base64 in JSON
	URL       string                 `json:"url,omitempty"`
	MediaType string                 `json:"mimeType,omitempty"`
	Filename  string                 `json:"filename,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

// V1Task is an A2A Task snapshot.
type V1Task struct {
	ID        string                 `json:"id"`
	ContextID string                 `json:"contextId"`
	Status    V1TaskStatus           `json:"status"`
	Artifacts []V1Artifact           `json:"artifacts,omitempty"`
	History   []V1Message            `json:"history,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

type V1TaskStatus struct {
	State     string     `json:"state"`
	Message   *V1Message `json:"message,omitempty"`
	Timestamp *time.Time `json:"timestamp,omitempty"`
}

type V1Artifact struct {
	ArtifactID  string                 `json:"artifactId"`
	Parts       []V1Part               `json:"parts"`
	Name        string                 `json:"name,omitempty"`
	Description string                 `json:"description,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// V1TaskStatusUpdateEvent is a streaming status transition.
type V1TaskStatusUpdateEvent struct {
	TaskID    string                 `json:"taskId"`
	ContextID string                 `json:"contextId"`
	Status    V1TaskStatus           `json:"status"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

// V1TaskArtifactUpdateEvent streams incremental artifacts.
type V1TaskArtifactUpdateEvent struct {
	TaskID    string     `json:"taskId"`
	ContextID string     `json:"contextId"`
	Artifact  V1Artifact `json:"artifact"`
	Append    bool       `json:"append,omitempty"`
	LastChunk bool       `json:"lastChunk,omitempty"`
}

// V1StreamResponse is the oneof wrapper. MarshalJSON emits exactly one key.
type V1StreamResponse struct {
	Task           *V1Task                    `json:"-"`
	Message        *V1Message                 `json:"-"`
	StatusUpdate   *V1TaskStatusUpdateEvent   `json:"-"`
	ArtifactUpdate *V1TaskArtifactUpdateEvent `json:"-"`
}

func (r V1StreamResponse) MarshalJSON() ([]byte, error) {
	switch {
	case r.Task != nil:
		return json.Marshal(map[string]*V1Task{"task": r.Task})
	case r.Message != nil:
		return json.Marshal(map[string]*V1Message{"message": r.Message})
	case r.StatusUpdate != nil:
		return json.Marshal(map[string]*V1TaskStatusUpdateEvent{"statusUpdate": r.StatusUpdate})
	case r.ArtifactUpdate != nil:
		return json.Marshal(map[string]*V1TaskArtifactUpdateEvent{"artifactUpdate": r.ArtifactUpdate})
	default:
		return nil, fmt.Errorf("empty stream response")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Conversions: Janus internal model → A2A v1.0
// ─────────────────────────────────────────────────────────────────────────────

// JanusStatusToV1State maps Janus task status to the v1.0 TaskState enum.
func JanusStatusToV1State(s core.TaskStatus) string {
	switch s {
	case core.TaskStatusCreated, core.TaskStatusQueued, core.TaskStatusClaimed,
		core.TaskStatusRetryScheduled:
		return V1StateSubmitted
	case core.TaskStatusRunning:
		return V1StateWorking
	case core.TaskStatusBlocked, core.TaskStatusApprovalPending:
		return V1StateInputRequired
	case core.TaskStatusCompleted:
		return V1StateCompleted
	case core.TaskStatusFailed, core.TaskStatusDeadLettered, core.TaskStatusExpired:
		return V1StateFailed
	case core.TaskStatusCancelled:
		return V1StateCanceled
	default:
		return V1StateUnspecified
	}
}

// V1StateIsTerminal reports whether a v1.0 state ends the stream.
func V1StateIsTerminal(state string) bool {
	switch state {
	case V1StateCompleted, V1StateFailed, V1StateCanceled, V1StateRejected:
		return true
	}
	return false
}

// JanusTaskToV1 converts an internal task snapshot to a v1.0 Task.
func JanusTaskToV1(t *core.Task) *V1Task {
	if t == nil {
		return nil
	}
	ts := t.UpdatedAt
	if ts.IsZero() {
		ts = t.CreatedAt
	}
	v1 := &V1Task{
		ID:        t.ID,
		ContextID: t.Envelope.Trace.TraceID,
		Status: V1TaskStatus{
			State:     JanusStatusToV1State(t.Status),
			Timestamp: &ts,
		},
	}
	if t.ResultRef != "" {
		v1.Artifacts = []V1Artifact{{
			ArtifactID: "result",
			Parts:      []V1Part{{Text: t.ResultRef}},
			Name:       "result_ref",
		}}
	}
	return v1
}

// JanusEventToV1Update translates a broker event into a v1.0 statusUpdate.
// Returns nil for events that have no v1.0 representation.
func JanusEventToV1Update(evt core.JanusEvent) *V1TaskStatusUpdateEvent {
	var state string
	switch evt.EventType {
	case core.EventTaskCreated, core.EventTaskQueued, core.EventTaskClaimed,
		core.EventTaskRetryScheduled:
		state = V1StateSubmitted
	case core.EventTaskStarted:
		state = V1StateWorking
	case core.EventTaskProgress, core.EventTaskHeartbeat:
		state = V1StateWorking
	case core.EventTaskBlocked, core.EventTaskApprovalPending:
		state = V1StateInputRequired
	case core.EventTaskCompleted:
		state = V1StateCompleted
	case core.EventTaskFailed:
		state = V1StateFailed
	case core.EventTaskDeadLettered, core.EventTaskExpired:
		state = V1StateFailed
	case core.EventTaskCancelled:
		state = V1StateCanceled
	default:
		return nil // agent/policy/budget events are not task-scoped
	}
	ts := evt.Timestamp
	upd := &V1TaskStatusUpdateEvent{
		TaskID:    evt.TaskID,
		ContextID: evt.TraceID,
		Status: V1TaskStatus{
			State:     state,
			Timestamp: &ts,
		},
	}
	// task.progress payloads carry agent-authored status text — surface it.
	if evt.EventType == core.EventTaskProgress && len(evt.Payload) > 0 {
		var prog core.TaskProgress
		if json.Unmarshal(evt.Payload, &prog) == nil && prog.Message != "" {
			upd.Status.Message = &V1Message{
				MessageID: fmt.Sprintf("%s-progress", evt.EventID),
				Role:      "ROLE_AGENT",
				Parts:     []V1Part{{Text: prog.Message}},
				ContextID: evt.TraceID,
				TaskID:    evt.TaskID,
			}
		}
	}
	return upd
}

// V1MessageToTask converts a v1.0 send request into an internal task.
func V1MessageToTask(req V1SendMessageRequest, tenantID, sourceAgent, mailboxID string) core.Task {
	taskID := generateID()
	contextID := req.Message.ContextID
	if contextID == "" {
		contextID = generateID()
	}

	var content string
	var structured json.RawMessage
	for _, p := range req.Message.Parts {
		if p.Text != "" && content == "" {
			content = p.Text
		}
		if p.Data != nil {
			if raw, err := json.Marshal(p.Data); err == nil {
				structured = raw
			}
		}
	}
	payloadType := "a2a_message"
	if structured != nil {
		payloadType = "a2a_structured"
		content = string(structured)
	}

	// Tenant/source routing may arrive via metadata (multi-tenant brokers).
	if req.Metadata != nil {
		if mb, ok := req.Metadata["mailbox_id"].(string); ok && mb != "" {
			mailboxID = mb
		}
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
			Payload:      core.Payload{Type: payloadType, Content: content},
			Trace:        core.TraceContext{TraceID: contextID},
		},
	}
}
