package routing

import (
	"context"
	"errors"
	"testing"

	"github.com/agentium-lab/Janus/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func admissionLookup(owner *AgentCandidate) *mockLookup {
	return &mockLookup{
		mailboxes:    map[string]bool{"mb-1": true},
		mailboxOwner: map[string]*AgentCandidate{"mb-1": owner},
	}
}

func TestRoute_Mailbox_AdmissionGate(t *testing.T) {
	owner := &AgentCandidate{AgentID: "agent-1", MailboxID: "mb-1", TeamID: "team-a"}

	t.Run("policy denies owner", func(t *testing.T) {
		r := NewRouter(admissionLookup(owner), &mockPolicy{denyAgents: map[string]bool{"agent-1": true}}, nil)
		result, err := r.Route(context.Background(), "acme",
			core.Target{Type: core.TargetTypeMailbox, Value: "mb-1"}, core.TaskEnvelope{})
		require.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "agent-1 denied by policy")
	})

	t.Run("policy allows owner", func(t *testing.T) {
		r := NewRouter(admissionLookup(owner), &mockPolicy{}, nil)
		result, err := r.Route(context.Background(), "acme",
			core.Target{Type: core.TargetTypeMailbox, Value: "mb-1"}, core.TaskEnvelope{})
		require.NoError(t, err)
		assert.Equal(t, "mb-1", result.MailboxID)
		assert.Equal(t, "agent-1", result.AgentID)
	})

	t.Run("ownerless fails closed when governed", func(t *testing.T) {
		r := NewRouter(admissionLookup(nil), &mockPolicy{}, nil)
		_, err := r.Route(context.Background(), "acme",
			core.Target{Type: core.TargetTypeMailbox, Value: "mb-1"}, core.TaskEnvelope{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no resolvable owner")
	})

	t.Run("ownerless stays routable in dev mode", func(t *testing.T) {
		r := NewRouter(admissionLookup(nil), nil, nil)
		result, err := r.Route(context.Background(), "acme",
			core.Target{Type: core.TargetTypeMailbox, Value: "mb-1"}, core.TaskEnvelope{})
		require.NoError(t, err)
		assert.Equal(t, "mb-1", result.MailboxID)
	})

	t.Run("owner lookup error fails closed", func(t *testing.T) {
		lookup := admissionLookup(owner)
		lookup.ownerLookupEr = errors.New("db down")
		r := NewRouter(lookup, &mockPolicy{}, nil)
		_, err := r.Route(context.Background(), "acme",
			core.Target{Type: core.TargetTypeMailbox, Value: "mb-1"}, core.TaskEnvelope{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "owner lookup")
	})

	t.Run("target team mismatch rejects", func(t *testing.T) {
		r := NewRouter(admissionLookup(owner), &mockPolicy{}, nil)
		_, err := r.Route(context.Background(), "acme",
			core.Target{Type: core.TargetTypeMailbox, Value: "mb-1"},
			core.TaskEnvelope{Policy: &core.PolicyContext{TargetTeamID: "team-b"}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not match requested target team")
	})

	t.Run("policy checker error fails closed", func(t *testing.T) {
		r := NewRouter(admissionLookup(owner), &errorPolicy{err: errors.New("policy store down")}, nil)
		_, err := r.Route(context.Background(), "acme",
			core.Target{Type: core.TargetTypeMailbox, Value: "mb-1"}, core.TaskEnvelope{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "policy check")
	})
}

func TestRoute_Group_AdmissionGate(t *testing.T) {
	groupLookup := func() *mockLookup {
		return &mockLookup{
			mailboxes: map[string]bool{"mb-1": true, "mb-2": true},
			mailboxOwner: map[string]*AgentCandidate{
				"mb-1": {AgentID: "agent-1", MailboxID: "mb-1", TeamID: "team-a"},
				"mb-2": {AgentID: "agent-2", MailboxID: "mb-2", TeamID: "team-a"},
			},
			groupMap: map[string][]string{"dev-team": {"mb-1", "mb-2"}},
		}
	}

	t.Run("first owner denied second wins", func(t *testing.T) {
		r := NewRouter(groupLookup(), &mockPolicy{denyAgents: map[string]bool{"agent-1": true}}, nil)
		result, err := r.Route(context.Background(), "acme",
			core.Target{Type: core.TargetTypeGroup, Value: "dev-team"}, core.TaskEnvelope{})
		require.NoError(t, err)
		assert.Equal(t, "mb-2", result.MailboxID)
		assert.Equal(t, "agent-2", result.AgentID)
	})

	t.Run("all denied returns all-filtered shape", func(t *testing.T) {
		r := NewRouter(groupLookup(), &mockPolicy{denyAgents: map[string]bool{"agent-1": true, "agent-2": true}}, nil)
		result, err := r.Route(context.Background(), "acme",
			core.Target{Type: core.TargetTypeGroup, Value: "dev-team"}, core.TaskEnvelope{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "all candidates filtered")
		require.NotNil(t, result)
		assert.Equal(t, "all_candidates_filtered", result.Reason)
		require.Len(t, result.FilteredOut, 2)
		for _, f := range result.FilteredOut {
			assert.Equal(t, "policy_denied", f.Reason)
		}
	})

	t.Run("ownerless group mailbox filtered with reason", func(t *testing.T) {
		lookup := groupLookup()
		delete(lookup.mailboxOwner, "mb-2")
		r := NewRouter(lookup, &mockPolicy{denyAgents: map[string]bool{"agent-1": true}}, nil)
		result, err := r.Route(context.Background(), "acme",
			core.Target{Type: core.TargetTypeGroup, Value: "dev-team"}, core.TaskEnvelope{})
		require.Error(t, err)
		require.NotNil(t, result)
		require.Len(t, result.FilteredOut, 2)
		assert.Equal(t, "policy_denied", result.FilteredOut[0].Reason)
		assert.Equal(t, "ownerless", result.FilteredOut[1].Reason)
	})

	t.Run("dev mode keeps first-active behavior", func(t *testing.T) {
		r := NewRouter(groupLookup(), nil, nil)
		result, err := r.Route(context.Background(), "acme",
			core.Target{Type: core.TargetTypeGroup, Value: "dev-team"}, core.TaskEnvelope{})
		require.NoError(t, err)
		assert.Equal(t, "mb-1", result.MailboxID)
	})
}

func TestCheckMailboxAdmission(t *testing.T) {
	owner := &AgentCandidate{AgentID: "agent-1", MailboxID: "mb-1", TeamID: "team-a"}

	t.Run("nil policy stays permissive", func(t *testing.T) {
		r := NewRouter(admissionLookup(nil), nil, nil)
		require.NoError(t, r.CheckMailboxAdmission(context.Background(), "acme", "mb-1", core.TaskEnvelope{}))
	})

	t.Run("denied owner refused", func(t *testing.T) {
		r := NewRouter(admissionLookup(owner), &mockPolicy{denyAgents: map[string]bool{"agent-1": true}}, nil)
		err := r.CheckMailboxAdmission(context.Background(), "acme", "mb-1", core.TaskEnvelope{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "denied by policy")
	})

	t.Run("allowed owner passes", func(t *testing.T) {
		r := NewRouter(admissionLookup(owner), &mockPolicy{}, nil)
		require.NoError(t, r.CheckMailboxAdmission(context.Background(), "acme", "mb-1", core.TaskEnvelope{}))
	})

	t.Run("ownerless refused", func(t *testing.T) {
		r := NewRouter(admissionLookup(nil), &mockPolicy{}, nil)
		err := r.CheckMailboxAdmission(context.Background(), "acme", "mb-1", core.TaskEnvelope{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no resolvable owner")
	})
}
