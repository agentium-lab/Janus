package retry

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/agentium-lab/Janus/core"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Scheduler struct {
	pool      *pgxpool.Pool
	queue     core.QueueEventDriver
	useOutbox bool
	done      chan struct{}
}

func NewScheduler(pool *pgxpool.Pool, queue core.QueueEventDriver) *Scheduler {
	return &Scheduler{
		pool:  pool,
		queue: queue,
		done:  make(chan struct{}),
	}
}

func (s *Scheduler) WithOutbox() *Scheduler {
	s.useOutbox = true
	return s
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
	rows, err := s.pool.Query(ctx,
		`SELECT tenant_id, id, mailbox_id, priority, envelope
		 FROM tasks
		 WHERE status = 'retry_scheduled' AND retry_at IS NOT NULL AND retry_at <= now()`)
	if err != nil {
		log.Printf("retry scheduler query: %v", err)
		return
	}
	defer rows.Close()

	type retryTask struct {
		TenantID  string
		ID        string
		MailboxID string
		Priority  core.Priority
		Envelope  []byte
	}

	var tasks []retryTask
	for rows.Next() {
		var t retryTask
		if err := rows.Scan(&t.TenantID, &t.ID, &t.MailboxID, &t.Priority, &t.Envelope); err != nil {
			log.Printf("retry scheduler scan: %v", err)
			return
		}
		tasks = append(tasks, t)
	}
	if err := rows.Err(); err != nil {
		log.Printf("retry scheduler rows: %v", err)
		return
	}

	for _, t := range tasks {
		_, err := s.pool.Exec(ctx,
			`UPDATE tasks SET status = 'queued', retry_at = NULL, updated_at = now()
			 WHERE tenant_id = $1 AND id = $2 AND status = 'retry_scheduled'`,
			t.TenantID, t.ID,
		)
		if err != nil {
			log.Printf("retry scheduler update %s: %v", t.ID, err)
			continue
		}

		if t.MailboxID != "" {
			var env core.TaskEnvelope
			if err := json.Unmarshal(t.Envelope, &env); err == nil {
				payload, _ := json.Marshal(env)
				msg := core.TaskMessage{
					TenantID:  t.TenantID,
					MailboxID: t.MailboxID,
					TaskID:    t.ID,
					Priority:  t.Priority,
					Payload:   payload,
				}
				if s.useOutbox {
					queuePayload, _ := json.Marshal(msg)
					_, _ = s.pool.Exec(ctx,
						`INSERT INTO outbox_events (id, tenant_id, kind, payload) VALUES ($1, $2, 'task_publish', $3)`,
						generateULID(), t.TenantID, queuePayload,
					)
				} else if s.queue != nil {
					_ = s.queue.PublishTask(ctx, msg)
				}
			}
		}
	}

	if len(tasks) > 0 {
		log.Printf("retry scheduler: promoted %d tasks to queued", len(tasks))
	}
}

func generateULID() string {
	now := time.Now()
	t := uint64(now.UnixMilli())
	b := make([]byte, 10)
	for i := 9; i >= 0; i-- {
		b[i] = byte(t & 0xff)
		t >>= 8
	}
	randBytes := make([]byte, 6)
	for i := range randBytes {
		randBytes[i] = byte(t & 0xff)
		t = t>>8 + uint64(i)
	}
	return fmt.Sprintf("%x%x", b, randBytes)
}
