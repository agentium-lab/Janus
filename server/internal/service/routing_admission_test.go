package service

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/service/routing"
)

type admitLookup struct {
	candidates   []routing.AgentCandidate
	mailboxes    map[string]bool
	agentMailbox map[string]string
	mailboxOwner map[string]*routing.AgentCandidate
	groupMap     map[string][]string
}

func (m *admitLookup) ListOnlineByCapability(_ context.Context, _, _ string) ([]routing.AgentCandidate, error) {
	return m.candidates, nil
}
func (m *admitLookup) GetAgent(_ context.Context, _, agentID string) (*routing.AgentCandidate, error) {
	for _, c := range m.candidates {
		if c.AgentID == agentID {
			return &c, nil
		}
	}
	return nil, errors.New("not found")
}
func (m *admitLookup) GetAgentMailbox(_ context.Context, _, agentID string) (string, error) {
	if mb, ok := m.agentMailbox[agentID]; ok {
		return mb, nil
	}
	return "", errors.New("not found")
}
func (m *admitLookup) ValidateMailbox(_ context.Context, _, mailboxID string) (bool, error) {
	return m.mailboxes[mailboxID], nil
}
func (m *admitLookup) GetMailboxOwner(_ context.Context, _, mailboxID string) (*routing.AgentCandidate, error) {
	return m.mailboxOwner[mailboxID], nil
}
func (m *admitLookup) GetGroupMailboxes(_ context.Context, _, groupID string) ([]string, error) {
	return m.groupMap[groupID], nil
}
func (m *admitLookup) GetHumanMailboxes(_ context.Context, _, _ string) ([]string, error) {
	return nil, nil
}

type admitCall struct{ agentID, dataClass, teamID string }

type admitPolicy struct {
	denied map[string]bool
	calls  []admitCall
}

func (p *admitPolicy) CheckRoute(_ context.Context, _, agentID, dataClass, teamID string) (bool, error) {
	p.calls = append(p.calls, admitCall{agentID, dataClass, teamID})
	return !p.denied[agentID], nil
}

func admissionLookup(owner *routing.AgentCandidate) *admitLookup {
	return &admitLookup{
		mailboxes:    map[string]bool{"mb-1": true},
		mailboxOwner: map[string]*routing.AgentCandidate{"mb-1": owner},
	}
}

func mailboxTask(id string) core.Task {
	task := core.Task{
		TenantID: "acme", ID: id, SourceAgent: "agent-a",
		TargetType: core.TargetTypeMailbox, TargetValue: "mb-1",
	}
	env := makeTestEnvelope(id, "acme")
	env.Target = core.Target{Type: core.TargetTypeMailbox, Value: "mb-1"}
	task.Envelope = env
	return task
}

func TestCreate_MailboxTarget_AdmissionGate(t *testing.T) {
	ctx := context.Background()
	owner := &routing.AgentCandidate{AgentID: "agent-1", MailboxID: "mb-1", TeamID: "team-a"}

	t.Run("owner denied by policy rejects create", func(t *testing.T) {
		repo := &mockTaskRepo{}
		qd := &mockQueueDriver{}
		svc := NewTaskService(repo, qd, nil, nil).
			WithRouter(routing.NewRouter(admissionLookup(owner), &admitPolicy{denied: map[string]bool{"agent-1": true}}, nil))
		_, err := svc.Create(ctx, mailboxTask("t-mb-deny"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "routing")
		assert.Contains(t, err.Error(), "agent-1 denied by policy")
		assert.Empty(t, repo.tasks, "denied create must persist nothing")
		assert.Empty(t, qd.publishedTasks, "denied create must not publish")
	})

	t.Run("owner allowed routes and queues", func(t *testing.T) {
		repo := &mockTaskRepo{}
		qd := &mockQueueDriver{}
		svc := NewTaskService(repo, qd, nil, nil).
			WithRouter(routing.NewRouter(admissionLookup(owner), &admitPolicy{}, nil))
		created, err := svc.Create(ctx, mailboxTask("t-mb-ok"))
		require.NoError(t, err)
		assert.Equal(t, "mb-1", created.MailboxID)
		assert.Equal(t, core.TaskStatusQueued, repo.tasks["acme:t-mb-ok"].Status)
		assert.Len(t, qd.publishedTasks, 1)
	})

	t.Run("ownerless mailbox refused when governed", func(t *testing.T) {
		svc := NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil).
			WithRouter(routing.NewRouter(admissionLookup(nil), &admitPolicy{}, nil))
		_, err := svc.Create(ctx, mailboxTask("t-mb-ownerless"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no resolvable owner")
	})
}

func TestCreate_GroupTarget_AdmissionGate(t *testing.T) {
	ctx := context.Background()
	groupLookup := func() *admitLookup {
		return &admitLookup{
			mailboxes: map[string]bool{"mb-1": true, "mb-2": true},
			mailboxOwner: map[string]*routing.AgentCandidate{
				"mb-1": {AgentID: "agent-1", MailboxID: "mb-1", TeamID: "team-a"},
				"mb-2": {AgentID: "agent-2", MailboxID: "mb-2", TeamID: "team-a"},
			},
			groupMap: map[string][]string{"dev-team": {"mb-1", "mb-2"}},
		}
	}
	groupTask := func(id string) core.Task {
		task := core.Task{
			TenantID: "acme", ID: id, SourceAgent: "agent-a",
			TargetType: core.TargetTypeGroup, TargetValue: "dev-team",
		}
		env := makeTestEnvelope(id, "acme")
		env.Target = core.Target{Type: core.TargetTypeGroup, Value: "dev-team"}
		task.Envelope = env
		return task
	}

	t.Run("first owner denied second wins", func(t *testing.T) {
		repo := &mockTaskRepo{}
		svc := NewTaskService(repo, &mockQueueDriver{}, nil, nil).
			WithRouter(routing.NewRouter(groupLookup(), &admitPolicy{denied: map[string]bool{"agent-1": true}}, nil))
		created, err := svc.Create(ctx, groupTask("t-grp-skip"))
		require.NoError(t, err)
		assert.Equal(t, "mb-2", created.MailboxID)
	})

	t.Run("all owners denied fails create all-filtered", func(t *testing.T) {
		repo := &mockTaskRepo{}
		qd := &mockQueueDriver{}
		svc := NewTaskService(repo, qd, nil, nil).
			WithRouter(routing.NewRouter(groupLookup(), &admitPolicy{denied: map[string]bool{"agent-1": true, "agent-2": true}}, nil))
		_, err := svc.Create(ctx, groupTask("t-grp-all"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "routing")
		assert.Contains(t, err.Error(), "all candidates filtered")
		assert.Empty(t, repo.tasks)
		assert.Empty(t, qd.publishedTasks)
	})
}

func TestCreate_ClassificationGovernance(t *testing.T) {
	ctx := context.Background()

	classifiedRef := func(repo *mockContextRefRepo, id, classification string) {
		repo.refs[id] = &core.ContextRef{
			ID: id, TenantID: "acme", Type: "file",
			URI: "s3://" + id, Classification: classification,
		}
	}

	t.Run("declared under-classification rejected", func(t *testing.T) {
		repo := newMockContextRefRepo()
		classifiedRef(repo, "cr-1", "confidential")
		taskRepo := &mockTaskRepo{}
		qd := &mockQueueDriver{}
		svc := NewTaskService(taskRepo, qd, nil, nil).
			WithContextRefService(NewContextRefService(repo)).
			WithRouter(routing.NewRouter(admissionLookup(&routing.AgentCandidate{AgentID: "agent-1", MailboxID: "mb-1"}), &admitPolicy{}, nil))
		task := mailboxTask("t-under")
		task.Envelope.Policy = &core.PolicyContext{DataClassification: "public"}
		task.Envelope.ContextRefs = []core.ContextRef{{TenantID: "acme", ID: "cr-1"}}
		_, err := svc.Create(ctx, task)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "data classification mismatch")
		assert.Contains(t, err.Error(), "public")
		assert.Contains(t, err.Error(), "confidential")
		assert.Empty(t, taskRepo.tasks, "mismatched create must persist nothing")
		assert.Empty(t, qd.publishedTasks)
	})

	t.Run("ref classification inherited and governs routing", func(t *testing.T) {
		repo := newMockContextRefRepo()
		classifiedRef(repo, "cr-1", "confidential")
		pol := &admitPolicy{}
		lookup := &admitLookup{
			candidates: []routing.AgentCandidate{
				{AgentID: "agent-x", MailboxID: "mb-x", Status: "online", DataClass: "public"},
				{AgentID: "agent-y", MailboxID: "mb-y", Status: "online", DataClass: "confidential"},
			},
		}
		svc := NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil).
			WithContextRefService(NewContextRefService(repo)).
			WithRouter(routing.NewRouter(lookup, pol, nil))
		task := core.Task{
			TenantID: "acme", ID: "t-inherit", SourceAgent: "agent-a",
			TargetType: core.TargetTypeCapability, TargetValue: "review",
			Envelope: makeTestEnvelope("t-inherit", "acme"),
		}
		task.Envelope.ContextRefs = []core.ContextRef{{TenantID: "acme", ID: "cr-1"}}
		created, err := svc.Create(ctx, task)
		require.NoError(t, err)
		assert.Equal(t, "mb-y", created.MailboxID, "candidate with DataClass public must be filtered by the trusted confidential classification")
		require.NotEmpty(t, pol.calls)
		assert.Equal(t, "confidential", pol.calls[0].dataClass, "policy CheckRoute must receive the trusted ref classification")
		assert.Equal(t, "confidential", created.Envelope.Policy.DataClassification, "effective classification must be persisted in the envelope")
	})

	t.Run("refs disagree rejected", func(t *testing.T) {
		repo := newMockContextRefRepo()
		classifiedRef(repo, "cr-1", "internal")
		classifiedRef(repo, "cr-2", "confidential")
		svc := NewTaskService(&mockTaskRepo{}, &mockQueueDriver{}, nil, nil).
			WithContextRefService(NewContextRefService(repo)).
			WithRouter(routing.NewRouter(admissionLookup(&routing.AgentCandidate{AgentID: "agent-1", MailboxID: "mb-1"}), &admitPolicy{}, nil))
		task := mailboxTask("t-disagree")
		task.Envelope.ContextRefs = []core.ContextRef{{TenantID: "acme", ID: "cr-1"}, {TenantID: "acme", ID: "cr-2"}}
		_, err := svc.Create(ctx, task)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "data classification mismatch")
	})

	t.Run("declared matches ref passes through", func(t *testing.T) {
		repo := newMockContextRefRepo()
		classifiedRef(repo, "cr-1", "confidential")
		repo2 := &mockTaskRepo{}
		svc := NewTaskService(repo2, &mockQueueDriver{}, nil, nil).
			WithContextRefService(NewContextRefService(repo)).
			WithRouter(routing.NewRouter(admissionLookup(&routing.AgentCandidate{AgentID: "agent-1", MailboxID: "mb-1", DataClass: "confidential"}), &admitPolicy{}, nil))
		task := mailboxTask("t-agree")
		task.Envelope.Policy = &core.PolicyContext{DataClassification: "confidential"}
		task.Envelope.ContextRefs = []core.ContextRef{{TenantID: "acme", ID: "cr-1"}}
		created, err := svc.Create(ctx, task)
		require.NoError(t, err)
		assert.Equal(t, "mb-1", created.MailboxID)
	})
}

func TestReplay_MailboxAdmissionGate(t *testing.T) {
	ctx := context.Background()
	owner := &routing.AgentCandidate{AgentID: "agent-1", MailboxID: "mb-1", TeamID: "team-a"}
	terminalTask := func() *mockTaskRepo {
		return &mockTaskRepo{tasks: map[string]*core.Task{
			"acme:t1": {
				ID: "t1", TenantID: "acme", Status: core.TaskStatusCompleted,
				MailboxID: "mb-1", Envelope: core.TaskEnvelope{TaskID: "t1"},
			},
		}}
	}

	t.Run("owner now denied refuses replay", func(t *testing.T) {
		qd := &mockQueueDriver{}
		svc := NewTaskService(terminalTask(), qd, nil, nil).
			WithRouter(routing.NewRouter(admissionLookup(owner), &admitPolicy{denied: map[string]bool{"agent-1": true}}, nil))
		_, err := svc.Replay(ctx, "acme", "t1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "replay admission")
		assert.Contains(t, err.Error(), "denied by policy")
		assert.Empty(t, qd.publishedTasks, "refused replay must not republish")
	})

	t.Run("owner still allowed replays", func(t *testing.T) {
		qd := &mockQueueDriver{}
		svc := NewTaskService(terminalTask(), qd, nil, nil).
			WithRouter(routing.NewRouter(admissionLookup(owner), &admitPolicy{}, nil))
		result, err := svc.Replay(ctx, "acme", "t1")
		require.NoError(t, err)
		assert.Equal(t, core.TaskStatusQueued, result.Status)
		assert.Len(t, qd.publishedTasks, 1)
	})

	t.Run("ownerless mailbox refuses replay", func(t *testing.T) {
		svc := NewTaskService(terminalTask(), &mockQueueDriver{}, nil, nil).
			WithRouter(routing.NewRouter(admissionLookup(nil), &admitPolicy{}, nil))
		_, err := svc.Replay(ctx, "acme", "t1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "replay admission")
		assert.Contains(t, err.Error(), "no resolvable owner")
	})

	t.Run("approval verdict still gates before admission", func(t *testing.T) {
		svc := NewTaskService(terminalTask(), &mockQueueDriver{}, nil, nil).
			WithApproval(NewApprovalService(&gateApprovalRepo{latest: &core.Approval{Status: "pending"}}, nil, nil)).
			WithRouter(routing.NewRouter(admissionLookup(owner), &admitPolicy{denied: map[string]bool{"agent-1": true}}, nil))
		_, err := svc.Replay(ctx, "acme", "t1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "still pending")
		assert.NotContains(t, err.Error(), "replay admission", "the approval gate must fire before the admission gate")
	})
}
