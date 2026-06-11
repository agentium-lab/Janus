package expiry

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type mockExpirer struct {
	count int64
	err   error
	calls atomic.Int64
}

func (m *mockExpirer) ExpireTasks(_ context.Context) (int64, error) {
	m.calls.Add(1)
	return m.count, m.err
}

func TestScanner_StartStop(t *testing.T) {
	exp := &mockExpirer{count: 3}
	s := NewScanner(exp, 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	go s.Start(ctx)
	time.Sleep(50 * time.Millisecond)
	cancel()

	assert.GreaterOrEqual(t, exp.calls.Load(), int64(2))
}

func TestScanner_Stop(t *testing.T) {
	exp := &mockExpirer{count: 1}
	s := NewScanner(exp, 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	go s.Start(ctx)
	time.Sleep(30 * time.Millisecond)
	s.Stop()
	cancel()
}

func TestScanner_ExpireError(t *testing.T) {
	exp := &mockExpirer{err: fmt.Errorf("db error")}
	s := NewScanner(exp, 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	go s.Start(ctx)
	time.Sleep(30 * time.Millisecond)
	cancel()
}

func TestScanner_ZeroExpired(t *testing.T) {
	exp := &mockExpirer{count: 0}
	s := NewScanner(exp, 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	go s.Start(ctx)
	time.Sleep(30 * time.Millisecond)
	cancel()

	assert.GreaterOrEqual(t, exp.calls.Load(), int64(1))
}
