package service

import (
	"context"
	"log"
	"sync"
	"time"

	redisdriver "github.com/agentium-lab/Janus/server/internal/driver/redis"
)

// unreachableLimiter stands in for a Redis-backed rate limiter that could
// not be created at startup. A nil limiter would silently SKIP every
// RPM/TPM check, turning a configured fail-closed policy into an
// accidental fail-open; this stub keeps reporting the limiter as
// unavailable so the policy actually applies, and promotes itself to the
// real driver as soon as one becomes reachable.
type unreachableLimiter struct {
	cfg     redisdriver.Config
	connect func(cfg redisdriver.Config) (RateLimiter, error)

	mu       sync.RWMutex
	delegate RateLimiter
	stop     chan struct{}
}

// NewReconnectingLimiter wraps an initial connection failure (realDriver
// == nil) or an already-working driver. It keeps retrying the connection
// in the background and swaps in the live driver on success.
func NewReconnectingLimiter(cfg redisdriver.Config, realDriver *redisdriver.Driver) RateLimiter {
	return newReconnectingLimiter(cfg, realDriver, func(cfg redisdriver.Config) (RateLimiter, error) {
		d, err := redisdriver.NewDriver(cfg)
		if err != nil {
			return nil, err
		}
		return d, nil
	})
}

func newReconnectingLimiter(cfg redisdriver.Config, realDriver *redisdriver.Driver, connect func(redisdriver.Config) (RateLimiter, error)) RateLimiter {
	u := &unreachableLimiter{cfg: cfg, connect: connect, stop: make(chan struct{})}
	if realDriver != nil {
		u.delegate = realDriver
	}
	go u.reconnectLoop()
	return u
}

func (u *unreachableLimiter) reconnectLoop() {
	u.reconnectEvery(15 * time.Second)
}

func (u *unreachableLimiter) reconnectEvery(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-u.stop:
			return
		case <-ticker.C:
			u.mu.RLock()
			current := u.delegate
			u.mu.RUnlock()
			if current != nil {
				return // promoted already
			}
			drv, err := u.connect(u.cfg)
			if err != nil {
				continue
			}
			u.mu.Lock()
			u.delegate = drv
			u.mu.Unlock()
			log.Printf("budget: rate limiter reconnected after startup failure (fail-closed policy now enforced by live redis)")
			return
		}
	}
}

func (u *unreachableLimiter) CheckRPM(ctx context.Context, tenantID, scopeType, scopeID string, limit int) error {
	if d := u.current(); d != nil {
		return d.CheckRPM(ctx, tenantID, scopeType, scopeID, limit)
	}
	return redisdriver.ErrThrottleUnavailable
}

func (u *unreachableLimiter) CheckTPM(ctx context.Context, tenantID, scopeType, scopeID string, limit, tokenCount int) error {
	if d := u.current(); d != nil {
		return d.CheckTPM(ctx, tenantID, scopeType, scopeID, limit, tokenCount)
	}
	return redisdriver.ErrThrottleUnavailable
}

func (u *unreachableLimiter) current() RateLimiter {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.delegate
}
