package routing

import (
	"github.com/agentium-lab/Janus/core"
)

type RoutingEvent struct {
	EventType string `json:"event_type"`
	TenantID  string `json:"tenant_id"`
	TaskID    string `json:"task_id"`
	AgentID   string `json:"agent_id"`
	MailboxID string `json:"mailbox_id"`
	Reason    string `json:"reason"`
	Score     int    `json:"score"`
}

const (
	EventRoutingSelected = "routing.selected"
	EventRoutingFailed   = "routing.failed"
)

func SelectedEvent(tenantID, taskID string, result *RouterResult) RoutingEvent {
	return RoutingEvent{
		EventType: EventRoutingSelected,
		TenantID:  tenantID,
		TaskID:    taskID,
		AgentID:   result.AgentID,
		MailboxID: result.MailboxID,
		Reason:    result.Reason,
		Score:     result.Score,
	}
}

func FailedEvent(tenantID, taskID, reason string, filtered []FilteredCandidate) RoutingEvent {
	detail := reason
	if len(filtered) > 0 {
		detail += " (filtered: "
		for i, f := range filtered {
			if i > 0 {
				detail += ", "
			}
			detail += f.AgentID + ":" + f.Reason
		}
		detail += ")"
	}
	return RoutingEvent{
		EventType: EventRoutingFailed,
		TenantID:  tenantID,
		TaskID:    taskID,
		Reason:    detail,
	}
}

var _ core.Priority = core.PriorityNormal
