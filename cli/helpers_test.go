package main

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/agentium-lab/Janus/core"
	janus "github.com/agentium-lab/Janus/sdk/go"
)

func TestSortedAgentIDs(t *testing.T) {
	tenant := &ProjectTenant{
		Agents: map[string]*ProjectAgent{
			"zeta":  {},
			"alpha": {},
			"beta":  {},
		},
	}
	ids := sortedAgentIDs(tenant)
	assert.Equal(t, []string{"alpha", "beta", "zeta"}, ids)
}

func TestSortedAgentIDs_Empty(t *testing.T) {
	tenant := &ProjectTenant{Agents: map[string]*ProjectAgent{}}
	ids := sortedAgentIDs(tenant)
	assert.Empty(t, ids)
}

func TestIsAPIStatus_NilError(t *testing.T) {
	assert.False(t, isAPIStatus(nil, 500))
}

func TestIsAPIStatus_NonAPIError(t *testing.T) {
	assert.False(t, isAPIStatus(errors.New("generic"), 500))
}

func TestIsAPIStatus_MatchingStatus(t *testing.T) {
	apiErr := &janus.APIError{StatusCode: 500}
	assert.True(t, isAPIStatus(apiErr, 500))
}

func TestIsAPIStatus_NonMatchingStatus(t *testing.T) {
	apiErr := &janus.APIError{StatusCode: 404}
	assert.False(t, isAPIStatus(apiErr, 500))
}

func TestIntPtr_Zero(t *testing.T) {
	assert.Nil(t, intPtr(0))
}

func TestIntPtr_Negative(t *testing.T) {
	assert.Nil(t, intPtr(-1))
}

func TestIntPtr_Positive(t *testing.T) {
	p := intPtr(5)
	assert.NotNil(t, p)
	assert.Equal(t, 5, *p)
}

func TestAppendUniquePolicyBinding_NewValue(t *testing.T) {
	values := []ProjectPolicyBinding{{Agent: "a", Capability: "cap1"}}
	result := appendUniquePolicyBinding(values, ProjectPolicyBinding{Agent: "b", Capability: "cap2"})
	assert.Len(t, result, 2)
}

func TestAppendUniquePolicyBinding_DuplicateValue(t *testing.T) {
	values := []ProjectPolicyBinding{{Agent: "a", Capability: "cap1"}}
	result := appendUniquePolicyBinding(values, ProjectPolicyBinding{Agent: "a", Capability: "cap1"})
	assert.Len(t, result, 1)
}

func TestAppendUniqueToolBinding_NewValue(t *testing.T) {
	values := []ProjectToolBinding{{Agent: "a", Tool: "t1"}}
	result := appendUniqueToolBinding(values, ProjectToolBinding{Agent: "b", Tool: "t2"})
	assert.Len(t, result, 2)
}

func TestAppendUniqueToolBinding_DuplicateValue(t *testing.T) {
	values := []ProjectToolBinding{{Agent: "a", Tool: "t1"}}
	result := appendUniqueToolBinding(values, ProjectToolBinding{Agent: "a", Tool: "t1"})
	assert.Len(t, result, 1)
}

func TestAppendUniqueClassificationBinding_NewValue(t *testing.T) {
	values := []ProjectClassificationBinding{{Agent: "a", Classification: "public"}}
	result := appendUniqueClassificationBinding(values, ProjectClassificationBinding{Agent: "b", Classification: "internal"})
	assert.Len(t, result, 2)
}

func TestAppendUniqueClassificationBinding_DuplicateValue(t *testing.T) {
	values := []ProjectClassificationBinding{{Agent: "a", Classification: "public"}}
	result := appendUniqueClassificationBinding(values, ProjectClassificationBinding{Agent: "a", Classification: "public"})
	assert.Len(t, result, 1)
}

func TestStringValue_Nil(t *testing.T) {
	assert.Equal(t, "", stringValue(nil))
}

func TestStringValue_String(t *testing.T) {
	assert.Equal(t, "hello", stringValue("hello"))
}

func TestStringValue_Int(t *testing.T) {
	assert.Equal(t, "42", stringValue(42))
}

func TestProjectAgentFromCore(t *testing.T) {
	agent := core.Agent{
		ID: "a1", DisplayName: "Agent 1", TeamID: "team1",
		Protocol: core.ProtocolA2A, Endpoint: "http://a1",
		Description: "Test agent", MaxConcurrency: 3, RPM: 100, TPM: 1000,
		Capabilities: []core.AgentCapability{
			{Capability: "review", Description: "Code review"},
		},
	}
	pa := projectAgentFromCore(agent)
	assert.Equal(t, "Agent 1", pa.Name)
	assert.Equal(t, "team1", pa.Team)
	assert.Equal(t, "a2a", pa.Protocol)
	assert.Equal(t, 3, pa.Concurrency)
	assert.Len(t, pa.Capabilities, 1)
	assert.Equal(t, "review", pa.Capabilities[0].ID)
}

func TestProjectCapabilityFromCore(t *testing.T) {
	cap := core.AgentCapability{
		Capability:   "review",
		Description:  "Code review",
		Schema:       `{"allowed_data_classifications":["public","internal"]}`,
	}
	pc := projectCapabilityFromCore(cap)
	assert.Equal(t, "review", pc.ID)
	assert.Equal(t, "Code review", pc.Description)
	assert.Contains(t, pc.DataClassifications, "public")
	assert.Contains(t, pc.DataClassifications, "internal")
}

func TestProjectCapabilityFromCore_NoSchema(t *testing.T) {
	cap := core.AgentCapability{Capability: "review"}
	pc := projectCapabilityFromCore(cap)
	assert.Equal(t, "review", pc.ID)
	assert.Empty(t, pc.DataClassifications)
}

func TestCleanStringSlice(t *testing.T) {
	result := cleanStringSlice([]string{"b", " a ", "", "a", "b"})
	assert.Equal(t, []string{"a", "b"}, result)
}

func TestAppendUniqueString_NewValue(t *testing.T) {
	result := appendUniqueString([]string{"a"}, "b")
	assert.Equal(t, []string{"a", "b"}, result)
}

func TestAppendUniqueString_DuplicateValue(t *testing.T) {
	result := appendUniqueString([]string{"a"}, "a")
	assert.Equal(t, []string{"a"}, result)
}

func TestAppendUniqueString_EmptyValue(t *testing.T) {
	result := appendUniqueString([]string{"a"}, "  ")
	assert.Equal(t, []string{"a"}, result)
}
