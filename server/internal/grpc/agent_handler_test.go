package grpc

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb "github.com/agentium-lab/Janus/proto/gen/janus/v1"
	"github.com/agentium-lab/Janus/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockAgentService struct {
	agents map[string]*core.Agent
	err    error
}

func (m *mockAgentService) Register(_ context.Context, a core.Agent) error {
	if m.err != nil {
		return m.err
	}
	if m.agents == nil {
		m.agents = make(map[string]*core.Agent)
	}
	m.agents[a.TenantID+":"+a.ID] = &a
	return nil
}

func (m *mockAgentService) Get(_ context.Context, tenantID, agentID string) (*core.Agent, error) {
	if m.err != nil {
		return nil, m.err
	}
	a, ok := m.agents[tenantID+":"+agentID]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return a, nil
}

func (m *mockAgentService) UpdateStatus(_ context.Context, tenantID, agentID string, status core.AgentStatus) error {
	if m.err != nil {
		return m.err
	}
	if a, ok := m.agents[tenantID+":"+agentID]; ok {
		a.Status = status
	}
	return nil
}

func (m *mockAgentService) Heartbeat(_ context.Context, tenantID, agentID string) error {
	return m.err
}

func (m *mockAgentService) List(_ context.Context, tenantID string) ([]*core.Agent, error) {
	if m.err != nil {
		return nil, m.err
	}
	var result []*core.Agent
	for _, a := range m.agents {
		if a.TenantID == tenantID {
			result = append(result, a)
		}
	}
	return result, nil
}

func (m *mockAgentService) ListByStatus(_ context.Context, tenantID string, status core.AgentStatus) ([]*core.Agent, error) {
	return nil, m.err
}

func makeTestAgent(tenantID, id string) *core.Agent {
	return &core.Agent{
		TenantID:    tenantID,
		ID:          id,
		DisplayName: "test-agent",
		Protocol:    core.ProtocolA2A,
		Endpoint:    "http://localhost:8080",
		Status:      core.AgentStatusOnline,
		Capabilities: []core.AgentCapability{
			{Capability: "chat", Description: "chat capability"},
		},
		MaxConcurrency: 5,
		RPM:            100,
		TPM:            10000,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}
}

func TestAgentServiceServer_RegisterAgent(t *testing.T) {
	mock := &mockAgentService{}
	s := &AgentServiceServer{svc: mock}

	resp, err := s.RegisterAgent(context.Background(), &pb.RegisterAgentRequest{
		TenantId:    "acme",
		Id:          "agent-1",
		DisplayName: "test-agent",
		Protocol:    "http",
		Endpoint:    "http://localhost:8080",
		Capabilities: []*pb.AgentCapability{
			{Capability: "chat", Description: "chat capability"},
		},
		MaxConcurrency: 5,
		Rpm:            100,
		Tpm:            10000,
	})
	require.NoError(t, err)
	assert.Equal(t, "agent-1", resp.Id)
	assert.Equal(t, "acme", resp.TenantId)
	assert.Equal(t, "test-agent", resp.DisplayName)
	assert.Len(t, resp.Capabilities, 1)
}

func TestAgentServiceServer_RegisterAgent_Error(t *testing.T) {
	mock := &mockAgentService{err: assert.AnError}
	s := &AgentServiceServer{svc: mock}

	_, err := s.RegisterAgent(context.Background(), &pb.RegisterAgentRequest{
		TenantId: "acme", Id: "agent-1",
	})
	assert.Error(t, err)
}

func TestAgentServiceServer_GetAgent(t *testing.T) {
	mock := &mockAgentService{agents: map[string]*core.Agent{
		"acme:agent-1": makeTestAgent("acme", "agent-1"),
	}}
	s := &AgentServiceServer{svc: mock}

	resp, err := s.GetAgent(context.Background(), &pb.GetAgentRequest{
		TenantId: "acme", AgentId: "agent-1",
	})
	require.NoError(t, err)
	assert.Equal(t, "agent-1", resp.Id)
}

func TestAgentServiceServer_GetAgent_NotFound(t *testing.T) {
	mock := &mockAgentService{}
	s := &AgentServiceServer{svc: mock}

	_, err := s.GetAgent(context.Background(), &pb.GetAgentRequest{
		TenantId: "acme", AgentId: "nonexistent",
	})
	assert.Error(t, err)
}

func TestAgentServiceServer_ListAgents(t *testing.T) {
	mock := &mockAgentService{agents: map[string]*core.Agent{
		"acme:a1": makeTestAgent("acme", "a1"),
		"acme:a2": makeTestAgent("acme", "a2"),
		"other:a3": makeTestAgent("other", "a3"),
	}}
	s := &AgentServiceServer{svc: mock}

	resp, err := s.ListAgents(context.Background(), &pb.ListAgentsRequest{TenantId: "acme"})
	require.NoError(t, err)
	assert.Len(t, resp.Agents, 2)
}

func TestAgentServiceServer_Heartbeat(t *testing.T) {
	mock := &mockAgentService{}
	s := &AgentServiceServer{svc: mock}

	resp, err := s.Heartbeat(context.Background(), &pb.HeartbeatRequest{
		TenantId: "acme", AgentId: "agent-1",
	})
	require.NoError(t, err)
	assert.NotNil(t, resp.ServerTime)
}

func TestAgentServiceServer_UpdateAgent_Status(t *testing.T) {
	mock := &mockAgentService{agents: map[string]*core.Agent{
		"acme:agent-1": makeTestAgent("acme", "agent-1"),
	}}
	s := &AgentServiceServer{svc: mock}

	status := "offline"
	resp, err := s.UpdateAgent(context.Background(), &pb.UpdateAgentRequest{
		TenantId: "acme",
		AgentId:  "agent-1",
		Status:   &status,
	})
	require.NoError(t, err)
	assert.Equal(t, "offline", resp.Status)
}

func TestAgentServiceServer_UpdateAgent_Error(t *testing.T) {
	mock := &mockAgentService{err: assert.AnError}
	s := &AgentServiceServer{svc: mock}

	status := "offline"
	_, err := s.UpdateAgent(context.Background(), &pb.UpdateAgentRequest{
		TenantId: "acme", AgentId: "agent-1", Status: &status,
	})
	assert.Error(t, err)
}

func TestAgentServiceServer_UpdateAgent_GetError(t *testing.T) {
	mock := &mockAgentService{err: assert.AnError}
	s := &AgentServiceServer{svc: mock}

	_, err := s.UpdateAgent(context.Background(), &pb.UpdateAgentRequest{
		TenantId: "acme", AgentId: "agent-1",
	})
	assert.Error(t, err)
}

func TestAgentServiceServer_Heartbeat_Error(t *testing.T) {
	mock := &mockAgentService{err: assert.AnError}
	s := &AgentServiceServer{svc: mock}

	_, err := s.Heartbeat(context.Background(), &pb.HeartbeatRequest{
		TenantId: "acme", AgentId: "agent-1",
	})
	assert.Error(t, err)
}

func TestAgentServiceServer_ListAgents_Error(t *testing.T) {
	mock := &mockAgentService{err: assert.AnError}
	s := &AgentServiceServer{svc: mock}

	_, err := s.ListAgents(context.Background(), &pb.ListAgentsRequest{TenantId: "acme"})
 	assert.Error(t, err)
 }

