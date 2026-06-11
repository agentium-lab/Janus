package expiry

import (
	"context"
	"log"
	"time"
)

// TaskExpirer scans for tasks that have exceeded their deadline or TTL
// and transitions them to the expired state.
type TaskExpirer interface {
	ExpireTasks(ctx context.Context) (int64, error)
}

// Scanner periodically scans for expired tasks and transitions them.
type Scanner struct {
	expirer  TaskExpirer
	interval time.Duration
	stopCh   chan struct{}
}

// NewScanner creates a new expiry scanner.
func NewScanner(expirer TaskExpirer, interval time.Duration) *Scanner {
	return &Scanner{
		expirer:  expirer,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

// Start begins the periodic scan loop. Blocks until context is cancelled or Stop is called.
func (s *Scanner) Start(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.scan(ctx)
		}
	}
}

// Stop signals the scanner to stop.
func (s *Scanner) Stop() {
	close(s.stopCh)
}

func (s *Scanner) scan(ctx context.Context) {
	n, err := s.expirer.ExpireTasks(ctx)
	if err != nil {
		log.Printf("expiry scanner: %v", err)
		return
	}
	if n > 0 {
		log.Printf("expiry scanner: expired %d tasks", n)
	}
}
