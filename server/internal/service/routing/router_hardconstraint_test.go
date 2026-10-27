package routing

import (
	"context"
	"testing"

	"github.com/agentium-lab/Janus/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func classifiedEnvelope(classification string) core.TaskEnvelope {
	return core.TaskEnvelope{Policy: &core.PolicyContext{DataClassification: classification}}
}

func modelClassEnvelope(classes ...string) core.TaskEnvelope {
	return core.TaskEnvelope{Budget: &core.Budget{ModelClasses: classes}}
}

// T1: capability entrance — one shared hard-constraint check, including the
// missing-attribute rejection.
func TestRoute_Capability_HardConstraints_Classification(t *testing.T) {
	candidate := func(dataClass string) *mockLookup {
		return &mockLookup{candidates: []AgentCandidate{{
			AgentID: "agent-1", MailboxID: "mb-1", Status: "online", DataClass: dataClass,
			Capabilities: []core.AgentCapability{{Capability: "review"}},
		}}}
	}
	route := func(lookup *mockLookup) (*RouterResult, error) {
		r := NewRouter(lookup, &mockPolicy{}, nil)
		return r.Route(context.Background(), "acme",
			core.Target{Type: core.TargetTypeCapability, Value: "review"}, classifiedEnvelope("confidential"))
	}

	t.Run("classified task vs candidate without attribute is required-rejected", func(t *testing.T) {
		res, err := route(candidate(""))
		require.Error(t, err)
		require.NotNil(t, res)
		require.Len(t, res.FilteredOut, 1)
		assert.Equal(t, "agent-1", res.FilteredOut[0].AgentID)
		assert.Equal(t, "data_classification_required", res.FilteredOut[0].Reason)
	})

	t.Run("classified task vs different class is mismatch-rejected", func(t *testing.T) {
		res, err := route(candidate("public"))
		require.Error(t, err)
		require.NotNil(t, res)
		require.Len(t, res.FilteredOut, 1)
		assert.Equal(t, "data_classification_mismatch", res.FilteredOut[0].Reason)
	})

	t.Run("classified task vs matching class passes", func(t *testing.T) {
		res, err := route(candidate("confidential"))
		require.NoError(t, err)
		assert.Equal(t, "agent-1", res.AgentID)
		assert.Empty(t, res.FilteredOut)
	})
}

// T2: direct agent target — nil policy (dev mode) must NOT bypass hard
// constraints: they are baseline governance, not policy.
func TestRoute_AgentTarget_HardConstraints_Classification(t *testing.T) {
	routerFor := func(dataClass string) *Router {
		return NewRouter(&mockLookup{
			candidates:   []AgentCandidate{{AgentID: "agent-1", Status: "online", DataClass: dataClass}},
			agentMailbox: map[string]string{"agent-1": "mb-1"},
		}, nil, nil)
	}
	target := core.Target{Type: core.TargetTypeAgent, Value: "agent-1"}

	t.Run("classified task vs agent without attribute rejected", func(t *testing.T) {
		_, err := routerFor("").Route(context.Background(), "acme", target, classifiedEnvelope("confidential"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "rejected by hard constraint")
		assert.Contains(t, err.Error(), "data_classification_required")
	})

	t.Run("classified task vs different class rejected", func(t *testing.T) {
		_, err := routerFor("public").Route(context.Background(), "acme", target, classifiedEnvelope("confidential"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "data_classification_mismatch")
	})

	t.Run("classified task vs matching class routes", func(t *testing.T) {
		res, err := routerFor("confidential").Route(context.Background(), "acme", target, classifiedEnvelope("confidential"))
		require.NoError(t, err)
		assert.Equal(t, "mb-1", res.MailboxID)
	})
}

// T3: mailbox owner admission — via the direct mailbox target and via
// CheckMailboxAdmission (the replay/DLQ path inherits the same gate).
func TestRoute_MailboxTarget_HardConstraints_Classification(t *testing.T) {
	routerFor := func(dataClass string) *Router {
		return NewRouter(admissionLookup(&AgentCandidate{
			AgentID: "agent-1", MailboxID: "mb-1", TeamID: "team-a", DataClass: dataClass,
		}), &mockPolicy{}, nil)
	}
	target := core.Target{Type: core.TargetTypeMailbox, Value: "mb-1"}

	t.Run("classified task vs owner without attribute rejected", func(t *testing.T) {
		_, err := routerFor("").Route(context.Background(), "acme", target, classifiedEnvelope("confidential"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "rejected by hard constraint")
		assert.Contains(t, err.Error(), "data_classification_required")
	})

	t.Run("classified task vs owner different class rejected", func(t *testing.T) {
		_, err := routerFor("public").Route(context.Background(), "acme", target, classifiedEnvelope("confidential"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "data_classification_mismatch")
	})

	t.Run("classified task vs owner matching class routes", func(t *testing.T) {
		res, err := routerFor("confidential").Route(context.Background(), "acme", target, classifiedEnvelope("confidential"))
		require.NoError(t, err)
		assert.Equal(t, "mb-1", res.MailboxID)
		assert.Equal(t, "agent-1", res.AgentID)
	})
}

func TestCheckMailboxAdmission_HardConstraints_Classification(t *testing.T) {
	routerFor := func(dataClass string) *Router {
		return NewRouter(admissionLookup(&AgentCandidate{
			AgentID: "agent-1", MailboxID: "mb-1", TeamID: "team-a", DataClass: dataClass,
		}), &mockPolicy{}, nil)
	}

	t.Run("classified task vs owner without attribute refused", func(t *testing.T) {
		err := routerFor("").CheckMailboxAdmission(context.Background(), "acme", "mb-1", classifiedEnvelope("confidential"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "data_classification_required")
	})

	t.Run("classified task vs owner different class refused", func(t *testing.T) {
		err := routerFor("public").CheckMailboxAdmission(context.Background(), "acme", "mb-1", classifiedEnvelope("confidential"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "data_classification_mismatch")
	})

	t.Run("classified task vs owner matching class admitted", func(t *testing.T) {
		err := routerFor("confidential").CheckMailboxAdmission(context.Background(), "acme", "mb-1", classifiedEnvelope("confidential"))
		require.NoError(t, err)
	})
}

// T4: model classes — required-but-empty and no-intersection on all three
// entrances.
func TestRoute_HardConstraints_ModelClasses(t *testing.T) {
	t.Run("capability required-but-empty filtered", func(t *testing.T) {
		r := NewRouter(&mockLookup{candidates: []AgentCandidate{{
			AgentID: "agent-1", MailboxID: "mb-1", Status: "online",
		}}}, nil, nil)
		res, err := r.Route(context.Background(), "acme",
			core.Target{Type: core.TargetTypeCapability, Value: "review"}, modelClassEnvelope("gpt-4"))
		require.Error(t, err)
		require.NotNil(t, res)
		require.Len(t, res.FilteredOut, 1)
		assert.Equal(t, "model_class_required", res.FilteredOut[0].Reason)
	})

	t.Run("capability no-intersection filtered", func(t *testing.T) {
		r := NewRouter(&mockLookup{candidates: []AgentCandidate{{
			AgentID: "agent-1", MailboxID: "mb-1", Status: "online", ModelClasses: []string{"claude-3"},
		}}}, nil, nil)
		res, err := r.Route(context.Background(), "acme",
			core.Target{Type: core.TargetTypeCapability, Value: "review"}, modelClassEnvelope("gpt-4"))
		require.Error(t, err)
		require.NotNil(t, res)
		require.Len(t, res.FilteredOut, 1)
		assert.Equal(t, "model_class_mismatch", res.FilteredOut[0].Reason)
	})

	agentRouter := func(modelClasses []string) *Router {
		return NewRouter(&mockLookup{
			candidates:   []AgentCandidate{{AgentID: "agent-1", Status: "online", ModelClasses: modelClasses}},
			agentMailbox: map[string]string{"agent-1": "mb-1"},
		}, nil, nil)
	}
	agentTarget := core.Target{Type: core.TargetTypeAgent, Value: "agent-1"}

	t.Run("agent target required-but-empty rejected", func(t *testing.T) {
		_, err := agentRouter(nil).Route(context.Background(), "acme", agentTarget, modelClassEnvelope("gpt-4"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "model_class_required")
	})

	t.Run("agent target no-intersection rejected", func(t *testing.T) {
		_, err := agentRouter([]string{"claude-3"}).Route(context.Background(), "acme", agentTarget, modelClassEnvelope("gpt-4"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "model_class_mismatch")
	})

	mailboxRouter := func(modelClasses []string) *Router {
		return NewRouter(admissionLookup(&AgentCandidate{
			AgentID: "agent-1", MailboxID: "mb-1", TeamID: "team-a", ModelClasses: modelClasses,
		}), &mockPolicy{}, nil)
	}
	mailboxTarget := core.Target{Type: core.TargetTypeMailbox, Value: "mb-1"}

	t.Run("mailbox target required-but-empty rejected", func(t *testing.T) {
		_, err := mailboxRouter(nil).Route(context.Background(), "acme", mailboxTarget, modelClassEnvelope("gpt-4"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "model_class_required")
	})

	t.Run("mailbox target no-intersection rejected", func(t *testing.T) {
		_, err := mailboxRouter([]string{"claude-3"}).Route(context.Background(), "acme", mailboxTarget, modelClassEnvelope("gpt-4"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "model_class_mismatch")
	})

	t.Run("agent and mailbox targets with intersection route", func(t *testing.T) {
		env := modelClassEnvelope("gpt-4", "claude-3")
		res, err := agentRouter([]string{"gpt-4"}).Route(context.Background(), "acme", agentTarget, env)
		require.NoError(t, err)
		assert.Equal(t, "mb-1", res.MailboxID)
		res, err = mailboxRouter([]string{"claude-3"}).Route(context.Background(), "acme", mailboxTarget, env)
		require.NoError(t, err)
		assert.Equal(t, "mb-1", res.MailboxID)
	})
}

// T5: back-compat — unclassified, unconstrained traffic passes candidates
// without attributes on every entrance.
func TestRoute_UnclassifiedTask_NoConstraints_BackCompat(t *testing.T) {
	t.Run("capability passes attribute-less candidates", func(t *testing.T) {
		r := NewRouter(&mockLookup{candidates: []AgentCandidate{{
			AgentID: "agent-1", MailboxID: "mb-1", Status: "online",
			Capabilities: []core.AgentCapability{{Capability: "review"}},
		}}}, nil, nil)
		res, err := r.Route(context.Background(), "acme",
			core.Target{Type: core.TargetTypeCapability, Value: "review"}, core.TaskEnvelope{})
		require.NoError(t, err)
		assert.Equal(t, "agent-1", res.AgentID)
	})

	t.Run("agent target passes attribute-less agent", func(t *testing.T) {
		r := NewRouter(&mockLookup{
			candidates:   []AgentCandidate{{AgentID: "agent-1", Status: "online"}},
			agentMailbox: map[string]string{"agent-1": "mb-1"},
		}, &mockPolicy{}, nil)
		res, err := r.Route(context.Background(), "acme",
			core.Target{Type: core.TargetTypeAgent, Value: "agent-1"}, core.TaskEnvelope{})
		require.NoError(t, err)
		assert.Equal(t, "mb-1", res.MailboxID)
	})

	t.Run("mailbox target passes attribute-less owner", func(t *testing.T) {
		r := NewRouter(admissionLookup(&AgentCandidate{
			AgentID: "agent-1", MailboxID: "mb-1", TeamID: "team-a",
		}), &mockPolicy{}, nil)
		res, err := r.Route(context.Background(), "acme",
			core.Target{Type: core.TargetTypeMailbox, Value: "mb-1"}, core.TaskEnvelope{})
		require.NoError(t, err)
		assert.Equal(t, "mb-1", res.MailboxID)
	})
}
