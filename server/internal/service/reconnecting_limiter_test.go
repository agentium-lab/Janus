package service

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	redisdriver "github.com/agentium-lab/Janus/server/internal/driver/redis"
)

type staticLimiter struct{ calls int32 }

func (s *staticLimiter) CheckRPM(context.Context, string, string, string, int) error {
	atomic.AddInt32(&s.calls, 1)
	return nil
}

func (s *staticLimiter) CheckTPM(context.Context, string, string, string, int, int) error {
	atomic.AddInt32(&s.calls, 1)
	return nil
}

// The promote path: once the injected connect succeeds, checks delegate to
// the live limiter instead of reporting unavailable.
func TestReconnectingLimiter_PromotesToLiveDriver(t *testing.T) {
	var attempts int32
	live := &staticLimiter{}
	u := newReconnectingLimiter(redisdriver.Config{}, nil, func(redisdriver.Config) (RateLimiter, error) {
		if atomic.AddInt32(&attempts, 1) < 2 {
			return nil, errors.New("still down")
		}
		return live, nil
	})
	inner := u.(*unreachableLimiter)
	go inner.reconnectEvery(10 * time.Millisecond)

	deadline := time.After(2 * time.Second)
	for {
		if err := u.CheckRPM(context.Background(), "acme", "agent", "a1", 5); err == nil {
			return
		}
		select {
		case <-deadline:
			t.Fatal("limiter never promoted to the live driver")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
}
