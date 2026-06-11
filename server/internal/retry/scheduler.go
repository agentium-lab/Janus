package retry

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Scheduler struct {
	pool *pgxpool.Pool
	done chan struct{}
}

func NewScheduler(pool *pgxpool.Pool) *Scheduler {
	return &Scheduler{
		pool: pool,
		done: make(chan struct{}),
	}
}

func (s *Scheduler) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		case <-ticker.C:
			s.processReadyRetries(ctx)
		}
	}
}

func (s *Scheduler) Stop() {
	close(s.done)
}

func (s *Scheduler) processReadyRetries(ctx context.Context) {
	tag, err := s.pool.Exec(ctx,
		`UPDATE tasks SET status = 'queued', retry_at = NULL, updated_at = now()
		 WHERE status = 'retry_scheduled' AND retry_at IS NOT NULL AND retry_at <= now()`)
	if err != nil {
		log.Printf("retry scheduler: %v", err)
		return
	}
	if tag.RowsAffected() > 0 {
		log.Printf("retry scheduler: promoted %d tasks to queued", tag.RowsAffected())
	}
}
