package lease

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewScanner(t *testing.T) {
	s := NewScanner(nil, 5*time.Second)
	assert.NotNil(t, s)
	assert.Equal(t, 5*time.Second, s.interval)
}

func TestScanner_StartStop_ContextCancel(t *testing.T) {
	s := NewScanner(nil, 1*time.Hour)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		s.Start(ctx)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not exit after context cancel")
	}
}

func TestScanner_StartStop_StopMethod(t *testing.T) {
	s := NewScanner(nil, 1*time.Hour)
	ctx := context.Background()

	done := make(chan struct{})
	go func() {
		s.Start(ctx)
		close(done)
	}()

	s.Stop()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not exit after Stop")
	}
}

func TestBackoff(t *testing.T) {
	assert.Equal(t, 10*time.Second, backoff(0))
	assert.Equal(t, 20*time.Second, backoff(1))
	assert.Equal(t, 40*time.Second, backoff(2))
	assert.Equal(t, 80*time.Second, backoff(3))
}

func TestBackoff_Capped(t *testing.T) {
	assert.Equal(t, 15*time.Minute, backoff(100))
}

func TestBackoff_ExponentialGrowth(t *testing.T) {
	d0 := backoff(0)
	d1 := backoff(1)
	d2 := backoff(2)
	assert.Equal(t, d0*2, d1)
	assert.Equal(t, d1*2, d2)
}
