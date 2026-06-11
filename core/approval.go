package core

import "time"

type Approval struct {
	TenantID    string     `json:"tenant_id"`
	ID          string     `json:"id"`
	TaskID      string     `json:"task_id"`
	Status      string     `json:"status"`
	RequestedBy string     `json:"requested_by"`
	Approver    string     `json:"approver,omitempty"`
	Reason      string     `json:"reason,omitempty"`
	Decision    string     `json:"decision,omitempty"`
	ExpiresAt   time.Time  `json:"expires_at"`
	CreatedAt   time.Time  `json:"created_at"`
	DecidedAt   *time.Time `json:"decided_at,omitempty"`
}
