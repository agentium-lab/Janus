package core

import "time"

// APIKey is the tenant-scoped credential record. The raw secret is shown once
// at creation and never stored; lookups use prefix + SHA-256 hash pairs.
type APIKey struct {
	ID           string     `json:"id"`
	TenantID     string     `json:"tenant_id"`
	Name         string     `json:"name"`
	Prefix       string     `json:"prefix"`
	Scopes       []string   `json:"scopes,omitempty"`
	BoundAgentID string     `json:"bound_agent_id,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	LastUsedAt   *time.Time `json:"last_used_at,omitempty"`
	RevokedAt    *time.Time `json:"revoked_at,omitempty"`
}
