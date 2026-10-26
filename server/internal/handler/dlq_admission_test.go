package handler

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

type fakeDLQGate struct {
	approvalErr  error
	policyErr    error
	admissionErr error
	calls        []string
}

func (g *fakeDLQGate) CheckApproval(_ context.Context, _, _ string) error {
	g.calls = append(g.calls, "approval")
	return g.approvalErr
}

func (g *fakeDLQGate) CheckPolicy(_ context.Context, _, _, _, _ string) error {
	g.calls = append(g.calls, "policy")
	return g.policyErr
}

func (g *fakeDLQGate) CheckAdmission(_ context.Context, _, _ string, _ core.TaskEnvelope) error {
	g.calls = append(g.calls, "admission")
	return g.admissionErr
}

func TestDLQServiceAdapter_ReplayDLQ_MailboxAdmissionGate(t *testing.T) {
	ctx := context.Background()
	dlqTask := func() *mockDLQTaskRepo {
		return &mockDLQTaskRepo{tasks: map[string]*core.Task{
			"acme:t1": {
				ID: "t1", TenantID: "acme", Status: core.TaskStatusDeadLettered,
				MailboxID: "mb-1", Priority: core.PriorityNormal,
				Envelope: core.TaskEnvelope{TaskID: "t1"},
			},
		}}
	}

	t.Run("admission denied refuses dlq replay", func(t *testing.T) {
		gate := &fakeDLQGate{admissionErr: errors.New("replay admission: mailbox mb-1 owner agent agent-1 denied by policy")}
		driver := &mockDLQQueueDriver{}
		adapter := NewDLQServiceAdapter(dlqTask(), driver).WithReplayGate(gate)
		_, err := adapter.ReplayDLQ(ctx, "acme", "t1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "dlq replay admission gate")
		assert.Contains(t, err.Error(), "denied by policy")
		assert.Equal(t, 0, driver.publishedTasks, "refused dlq replay must not republish")
	})

	t.Run("gates run in order approval policy admission", func(t *testing.T) {
		gate := &fakeDLQGate{}
		adapter := NewDLQServiceAdapter(dlqTask(), &mockDLQQueueDriver{}).WithReplayGate(gate)
		_, err := adapter.ReplayDLQ(ctx, "acme", "t1")
		require.NoError(t, err)
		assert.Equal(t, []string{"approval", "policy", "admission"}, gate.calls)
	})

	t.Run("admission passing proceeds to publish", func(t *testing.T) {
		driver := &mockDLQQueueDriver{}
		adapter := NewDLQServiceAdapter(dlqTask(), driver).WithReplayGate(&fakeDLQGate{})
		task, err := adapter.ReplayDLQ(ctx, "acme", "t1")
		require.NoError(t, err)
		assert.NotNil(t, task)
		assert.Equal(t, 1, driver.publishedTasks)
	})
}
