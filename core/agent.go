package core

import "time"

// AgentStatus represents the current status of an agent.
type AgentStatus string

const (
	AgentStatusOnline    AgentStatus = "online"
	AgentStatusOffline   AgentStatus = "offline"
	AgentStatusDegraded  AgentStatus = "degraded"
)

// AgentProtocol represents the communication protocol an agent supports.
type AgentProtocol string

const (
	ProtocolA2A       AgentProtocol = "a2a"
	ProtocolACP       AgentProtocol = "acp"
	ProtocolCustomSDK AgentProtocol = "custom-sdk"
)

// AgentCapability represents a single capability of an agent.
type AgentCapability struct {
	TenantID    string
	AgentID     string
	Capability  string
	Schema      string
	Description string
}

// Agent represents a registered agent in Janus.
type Agent struct {
	ID              string         `json:"id"`
	TenantID        string         `json:"tenant_id"`
	TeamID          string         `json:"team_id,omitempty"`
	DisplayName     string         `json:"display_name"`
	Protocol        AgentProtocol  `json:"protocol"`
	Endpoint        string         `json:"endpoint"`
	Status          AgentStatus    `json:"status"`
	Description     string         `json:"description"`
	Capabilities    []AgentCapability `json:"capabilities"`
	MaxConcurrency  int            `json:"max_concurrency"`
	RPM             int            `json:"rpm"`
	TPM             int            `json:"tpm"`
	CreatedAt       time.Time      `json:"created_at"`
	UpdatedAt       time.Time      `json:"updated_at"`
	LastHeartbeatAt *time.Time     `json:"last_heartbeat_at"`
}
