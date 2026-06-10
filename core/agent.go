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
	ID             string
	TenantID       string
	DisplayName    string
	Protocol       AgentProtocol
	Endpoint       string
	Status         AgentStatus
	Description    string
	Capabilities   []AgentCapability
	MaxConcurrency int
	RPM            int
	TPM            int
	CreatedAt      time.Time
	UpdatedAt      time.Time
	LastHeartbeatAt *time.Time
}
