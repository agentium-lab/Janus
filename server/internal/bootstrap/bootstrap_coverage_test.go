package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

type bsEnsurer struct {
	mu       sync.Mutex
	failures map[string]int
	calls    []string
}

func (e *bsEnsurer) EnsureTenant(_ context.Context, tenantID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls = append(e.calls, tenantID)
	if e.failures[tenantID] > 0 {
		e.failures[tenantID]--
		return fmt.Errorf("broker unreachable for %s", tenantID)
	}
	return nil
}

func (e *bsEnsurer) callCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.calls)
}

func waitForDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("retry loop did not terminate")
	}
}

func TestRetryLoopProgress_ConvergesFailedTenants(t *testing.T) {
	ensurer := &bsEnsurer{failures: map[string]int{"t1": 1}}
	var prog RetryProgress
	done := RetryLoopProgress(context.Background(), Options{QueueEnsurer: ensurer}, []string{"t1"}, time.Millisecond, &prog)
	waitForDone(t, done)
	assert.Equal(t, 1, prog.Total)
	assert.Equal(t, 0, prog.Pending())
	assert.GreaterOrEqual(t, ensurer.callCount(), 2, "tenant must be retried after the first failure")
}

func TestRetryLoopProgress_ContextCancelStops(t *testing.T) {
	ensurer := &bsEnsurer{failures: map[string]int{"t1": 1000}}
	ctx, cancel := context.WithCancel(context.Background())
	var prog RetryProgress
	done := RetryLoopProgress(ctx, Options{QueueEnsurer: ensurer}, []string{"t1"}, time.Millisecond, &prog)
	require.Eventually(t, func() bool { return prog.Pending() == 1 }, 2*time.Second, time.Millisecond)
	cancel()
	waitForDone(t, done)
	assert.Equal(t, 1, prog.Pending(), "cancellation keeps the pending count truthful")
}

func TestRetryLoop_ConvergesWithoutProgress(t *testing.T) {
	ensurer := &bsEnsurer{}
	done := RetryLoop(context.Background(), Options{QueueEnsurer: ensurer}, []string{"t1"}, time.Millisecond)
	waitForDone(t, done)
	assert.Equal(t, 1, ensurer.callCount())
}

func TestRetryProgress_PendingNilReceiver(t *testing.T) {
	var p *RetryProgress
	assert.Equal(t, 0, p.Pending())
}

type bsMailboxEnsurer struct {
	mailboxErr error
	consumerErr error
}

func (e *bsMailboxEnsurer) EnsureMailbox(context.Context, core.MailboxSpec) error { return e.mailboxErr }
func (e *bsMailboxEnsurer) EnsureConsumer(context.Context, core.ConsumerSpec) error { return e.consumerErr }

func TestEnsureMailboxConsumer_GuardPaths(t *testing.T) {
	ctx := context.Background()
	assert.NoError(t, EnsureMailboxConsumer(ctx, nil, &core.Mailbox{ID: "mb1"}))
	assert.NoError(t, EnsureMailboxConsumer(ctx, &bsMailboxEnsurer{}, nil))
	require.ErrorContains(t,
		EnsureMailboxConsumer(ctx, &bsMailboxEnsurer{mailboxErr: errors.New("stream gone")}, &core.Mailbox{ID: "mb1"}),
		"ensure mailbox mb1")
	require.ErrorContains(t,
		EnsureMailboxConsumer(ctx, &bsMailboxEnsurer{consumerErr: errors.New("consumer gone")}, &core.Mailbox{ID: "mb1"}),
		"ensure consumer mb1")
}
