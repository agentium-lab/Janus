package routing

import (
	"context"
	"errors"
	"testing"

	"github.com/agentium-lab/Janus/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- fakes with error injection knobs ---

type covLookup struct {
	candidates   []AgentCandidate
	candidateErr error
	mailboxes    map[string]bool
	validateErr  error
	agentMailbox map[string]string
	agentMbxErr  error
	groupMap     map[string][]string
	groupErr     error
	humanMap     map[string][]string
}

func (m *covLookup) ListOnlineByCapability(_ context.Context, _, _ string) ([]AgentCandidate, error) {
	return m.candidates, m.candidateErr
}
func (m *covLookup) GetAgentMailbox(_ context.Context, _, _ string) (string, error) {
	if m.agentMbxErr != nil {
		return "", m.agentMbxErr
	}
	if mb, ok := m.agentMailbox["agent-1"]; ok {
		return mb, nil
	}
	return "", errors.New("not found")
}
func (m *covLookup) ValidateMailbox(_ context.Context, _, _ string) (bool, error) {
	return false, m.validateErr
}
func (m *covLookup) GetGroupMailboxes(_ context.Context, _, _ string) ([]string, error) {
	return m.groupMap["g"], m.groupErr
}
func (m *covLookup) GetHumanMailboxes(_ context.Context, _, _ string) ([]string, error) {
	return m.humanMap["h"], nil
}

type covPolicy struct {
	denied map[string]bool
	err    error
}

func (m *covPolicy) CheckRoute(_ context.Context, _, agentID, _ string) (bool, error) {
	if m.err != nil {
		return false, m.err
	}
	return !m.denied[agentID], nil
}

type covBudget struct {
	denied map[string]bool
	err    error
}

func (m *covBudget) CheckCapacity(_ context.Context, _, agentID string, _, _ int) (bool, error) {
	if m.err != nil {
		return false, m.err
	}
	return !m.denied[agentID], nil
}

// --- routeAgent / routeMailbox error branches ---

func TestCov_RouteAgent_LookupError(t *testing.T) {
	r := NewRouter(&covLookup{agentMbxErr: errors.New("db down")}, nil, nil)
	_, err := r.Route(context.Background(), "acme", core.Target{Type: core.TargetTypeAgent, Value: "agent-x"}, core.TaskEnvelope{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "agent agent-x not found")
	assert.Contains(t, err.Error(), "db down")
}

func TestCov_RouteMailbox_ValidateError(t *testing.T) {
	r := NewRouter(&covLookup{validateErr: errors.New("boom")}, nil, nil)
	_, err := r.Route(context.Background(), "acme", core.Target{Type: core.TargetTypeMailbox, Value: "mb-1"}, core.TaskEnvelope{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "validation failed")
}

// --- routeCapability filter branches ---

func TestCov_RouteCapability_OfflineFiltered(t *testing.T) {
	r := NewRouter(&covLookup{candidates: []AgentCandidate{{
		AgentID: "a1", MailboxID: "mb-1", Status: "offline",
	}}}, nil, nil)
	res, err := r.Route(context.Background(), "acme", core.Target{Type: core.TargetTypeCapability, Value: "code_review"}, core.TaskEnvelope{})
	require.Error(t, err)
	require.NotNil(t, res)
	require.Len(t, res.FilteredOut, 1)
	assert.Equal(t, "offline", res.FilteredOut[0].Reason)
	assert.Equal(t, "all_candidates_filtered", res.Reason)
}

func TestCov_RouteCapability_DataClassMismatch(t *testing.T) {
	r := NewRouter(&covLookup{candidates: []AgentCandidate{{
		AgentID: "a1", MailboxID: "mb-1", Status: "online", DataClass: "public",
	}}}, nil, nil)
	env := core.TaskEnvelope{Policy: &core.PolicyContext{DataClassification: "confidential"}}
	res, err := r.Route(context.Background(), "acme", core.Target{Type: core.TargetTypeCapability, Value: "code_review"}, env)
	require.Error(t, err)
	require.NotNil(t, res)
	require.Len(t, res.FilteredOut, 1)
	assert.Equal(t, "data_classification_mismatch", res.FilteredOut[0].Reason)
}

func TestCov_RouteCapability_PolicyCheckErrorNotFiltered(t *testing.T) {
	// A policy checker error must not filter the candidate (fail-open).
	r := NewRouter(&covLookup{candidates: []AgentCandidate{{
		AgentID: "a1", MailboxID: "mb-1", Status: "online",
		Capabilities: []core.AgentCapability{{Capability: "code_review"}},
	}}}, &covPolicy{err: errors.New("policy svc down")}, nil)
	res, err := r.Route(context.Background(), "acme", core.Target{Type: core.TargetTypeCapability, Value: "code_review"}, core.TaskEnvelope{})
	require.NoError(t, err)
	assert.Equal(t, "a1", res.AgentID)
}

func TestCov_RouteCapability_PolicyDenied(t *testing.T) {
	r := NewRouter(&covLookup{candidates: []AgentCandidate{{
		AgentID: "a1", MailboxID: "mb-1", Status: "online",
		Capabilities: []core.AgentCapability{{Capability: "code_review"}},
	}}}, &covPolicy{denied: map[string]bool{"a1": true}}, nil)
	res, err := r.Route(context.Background(), "acme", core.Target{Type: core.TargetTypeCapability, Value: "code_review"}, core.TaskEnvelope{})
	require.Error(t, err)
	require.NotNil(t, res)
	require.Len(t, res.FilteredOut, 1)
	assert.Equal(t, "policy_denied", res.FilteredOut[0].Reason)
}

func TestCov_RouteCapability_BudgetDenied(t *testing.T) {
	r := NewRouter(&covLookup{candidates: []AgentCandidate{{
		AgentID: "a1", MailboxID: "mb-1", Status: "online",
		Capabilities: []core.AgentCapability{{Capability: "code_review"}},
	}}}, nil, &covBudget{denied: map[string]bool{"a1": true}})
	res, err := r.Route(context.Background(), "acme", core.Target{Type: core.TargetTypeCapability, Value: "code_review"}, core.TaskEnvelope{})
	require.Error(t, err)
	require.NotNil(t, res)
	require.Len(t, res.FilteredOut, 1)
	assert.Equal(t, "budget_exceeded", res.FilteredOut[0].Reason)
}

func TestCov_RouteCapability_BudgetCheckErrorNotFiltered(t *testing.T) {
	// A budget checker error must not filter the candidate (fail-open).
	r := NewRouter(&covLookup{candidates: []AgentCandidate{{
		AgentID: "a1", MailboxID: "mb-1", Status: "online",
		Capabilities: []core.AgentCapability{{Capability: "code_review"}},
	}}}, nil, &covBudget{err: errors.New("budget svc down")})
	res, err := r.Route(context.Background(), "acme", core.Target{Type: core.TargetTypeCapability, Value: "code_review"}, core.TaskEnvelope{})
	require.NoError(t, err)
	assert.Equal(t, "a1", res.AgentID)
}

func TestCov_RouteCapability_ModelClassMismatch(t *testing.T) {
	r := NewRouter(&covLookup{candidates: []AgentCandidate{
		{AgentID: "a1", MailboxID: "mb-1", Status: "online", ModelClasses: []string{"gpt"}},
	}}, nil, nil)
	env := core.TaskEnvelope{Budget: &core.Budget{ModelClasses: []string{"claude"}}}
	res, err := r.Route(context.Background(), "acme", core.Target{Type: core.TargetTypeCapability, Value: "code_review"}, env)
	require.Error(t, err)
	require.NotNil(t, res)
	require.Len(t, res.FilteredOut, 1)
	assert.Equal(t, "model_class_mismatch", res.FilteredOut[0].Reason)
}

func TestCov_RouteCapability_ModelClassIntersection(t *testing.T) {
	r := NewRouter(&covLookup{candidates: []AgentCandidate{{
		AgentID: "a1", MailboxID: "mb-1", Status: "online",
		ModelClasses: []string{"gpt", "claude"},
		Capabilities: []core.AgentCapability{{Capability: "code_review"}},
	}}}, nil, nil)
	env := core.TaskEnvelope{Budget: &core.Budget{ModelClasses: []string{"claude"}}}
	res, err := r.Route(context.Background(), "acme", core.Target{Type: core.TargetTypeCapability, Value: "code_review"}, env)
	require.NoError(t, err)
	assert.Equal(t, "a1", res.AgentID)
}

func TestCov_RouteCapability_ZeroScoreReason(t *testing.T) {
	// Candidate passes filters but has no matching capability: score stays 0.
	r := NewRouter(&covLookup{candidates: []AgentCandidate{{
		AgentID: "a1", MailboxID: "mb-1", Status: "online",
		Capabilities: []core.AgentCapability{{Capability: "deploy"}},
	}}}, nil, nil)
	res, err := r.Route(context.Background(), "acme", core.Target{Type: core.TargetTypeCapability, Value: "code_review"}, core.TaskEnvelope{})
	require.NoError(t, err)
	assert.Equal(t, "a1", res.AgentID)
	assert.Equal(t, 0, res.Score)
	assert.Equal(t, "capability_filtered", res.Reason)
}

func TestCov_RouteCapability_ListError(t *testing.T) {
	r := NewRouter(&covLookup{candidateErr: errors.New("lookup boom")}, nil, nil)
	_, err := r.Route(context.Background(), "acme", core.Target{Type: core.TargetTypeCapability, Value: "code_review"}, core.TaskEnvelope{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "list capability candidates")
}

func TestCov_RouteGroup_LookupError(t *testing.T) {
	r := NewRouter(&covLookup{groupErr: errors.New("db down")}, nil, nil)
	_, err := r.Route(context.Background(), "acme", core.Target{Type: core.TargetTypeGroup, Value: "dev-team"}, core.TaskEnvelope{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no group mailbox mapping")
}

// --- scoring ---

func TestCov_ScoreCandidate_DescriptionKeyword(t *testing.T) {
	c := AgentCandidate{
		Capabilities: []core.AgentCapability{
			{Capability: "deploy", Description: "also performs code_review checks"},
		},
	}
	score := scoreCandidate(c, "code_review", core.TaskEnvelope{})
	assert.Greater(t, score, 0, "description keyword should add score")
}

func TestCov_ScoreCandidate_AllowedToolsAlias(t *testing.T) {
	c := AgentCandidate{
		Capabilities: []core.AgentCapability{
			{Capability: "code_review"},
			{Capability: "deploy"},
		},
	}
	env := core.TaskEnvelope{Policy: &core.PolicyContext{AllowedTools: []string{"deploy"}}}
	score := scoreCandidate(c, "unrelated", env)
	assert.Greater(t, score, 0, "allowed-tools alias match should add score")
}

func TestCov_ScoreCandidate_BacklogClampToZero(t *testing.T) {
	c := AgentCandidate{
		Capabilities: []core.AgentCapability{{Capability: "code_review"}},
		Backlog:      100,
	}
	score := scoreCandidate(c, "code_review", core.TaskEnvelope{})
	assert.Equal(t, 0, score, "negative score must clamp to zero")
}

// --- events ---

func TestCov_FailedEvent_MultipleFiltered(t *testing.T) {
	filtered := []FilteredCandidate{
		{AgentID: "a1", Reason: "offline"},
		{AgentID: "a2", Reason: "policy_denied"},
	}
	evt := FailedEvent("acme", "task-1", "no_candidates", filtered)
	assert.Contains(t, evt.Reason, "a1:offline")
	assert.Contains(t, evt.Reason, "a2:policy_denied")
}
