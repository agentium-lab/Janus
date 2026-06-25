package bootstrap

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

type mockTenantLister struct {
	ids []string
	err error
}

func (m *mockTenantLister) ListIDs(_ context.Context) ([]string, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.ids, nil
}

type mockQueueEnsurer struct {
	ensured []string
	err     error
}

func (m *mockQueueEnsurer) EnsureTenant(_ context.Context, tenantID string) error {
	if m.err != nil {
		return m.err
	}
	m.ensured = append(m.ensured, tenantID)
	return nil
}

type mockMailboxEnsurer struct {
	mailboxes []core.MailboxSpec
	consumers []core.ConsumerSpec
	err       error
}

func (m *mockMailboxEnsurer) EnsureMailbox(_ context.Context, spec core.MailboxSpec) error {
	if m.err != nil {
		return m.err
	}
	m.mailboxes = append(m.mailboxes, spec)
	return nil
}

func (m *mockMailboxEnsurer) EnsureConsumer(_ context.Context, spec core.ConsumerSpec) error {
	if m.err != nil {
		return m.err
	}
	m.consumers = append(m.consumers, spec)
	return nil
}

func TestBootstrap_Run(t *testing.T) {
	tl := &mockTenantLister{ids: []string{"acme", "globex"}}
	qe := &mockQueueEnsurer{}

	result, err := Run(context.Background(), Options{
		TenantLister: tl,
		QueueEnsurer: qe,
	})
	require.NoError(t, err)
	assert.Equal(t, 2, result.TenantsEnsured)
	assert.Empty(t, result.Errors)
	assert.Equal(t, []string{"acme", "globex"}, qe.ensured)
}

func TestBootstrap_RunEmpty(t *testing.T) {
	result, err := Run(context.Background(), Options{
		TenantLister: &mockTenantLister{ids: nil},
		QueueEnsurer: &mockQueueEnsurer{},
	})
	require.NoError(t, err)
	assert.Equal(t, 0, result.TenantsEnsured)
}

func TestBootstrap_RunTenantError(t *testing.T) {
	_, err := Run(context.Background(), Options{
		TenantLister: &mockTenantLister{err: fmt.Errorf("db down")},
		QueueEnsurer: &mockQueueEnsurer{},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "list tenants")
}

func TestBootstrap_RunPartialFailure(t *testing.T) {
	tl := &mockTenantLister{ids: []string{"ok", "bad"}}
	qe := &mockQueueEnsurer{err: fmt.Errorf("nats error")}

	result, err := Run(context.Background(), Options{
		TenantLister: tl,
		QueueEnsurer: qe,
	})
	require.NoError(t, err)
	assert.Equal(t, 0, result.TenantsEnsured)
	assert.Len(t, result.Errors, 2)
}

func TestBootstrap_EnsureMailboxConsumer(t *testing.T) {
	me := &mockMailboxEnsurer{}
	mb := &core.Mailbox{
		TenantID:       "acme",
		ID:             "mb-1",
		AgentID:        "agent-1",
		MaxConcurrency: 3,
		ACKWaitSeconds: 30,
		MaxDeliver:     5,
	}

	err := EnsureMailboxConsumer(context.Background(), me, mb)
	require.NoError(t, err)
	assert.Len(t, me.mailboxes, 1)
	assert.Equal(t, "mb-1", me.mailboxes[0].MailboxID)
	assert.Len(t, me.consumers, 1)
	assert.Equal(t, "mb-1", me.consumers[0].DurableName)
}

func TestBootstrap_EnsureMailboxConsumer_NilEnsurer(t *testing.T) {
	err := EnsureMailboxConsumer(context.Background(), nil, &core.Mailbox{ID: "x"})
	require.NoError(t, err)
}

func TestBootstrap_EnsureMailboxConsumer_NilMailbox(t *testing.T) {
	err := EnsureMailboxConsumer(context.Background(), &mockMailboxEnsurer{}, nil)
	require.NoError(t, err)
}

func TestBootstrap_RunMissingLister(t *testing.T) {
	_, err := Run(context.Background(), Options{})
	require.Error(t, err)
}

func TestBootstrap_RunMissingEnsurer(t *testing.T) {
	_, err := Run(context.Background(), Options{
		TenantLister: &mockTenantLister{},
	})
	require.Error(t, err)
}
