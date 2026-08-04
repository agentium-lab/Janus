package retry

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	cryptorand "crypto/rand"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/metrics"
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
		`SELECT tenant_id, id, mailbox_id, priority, envelope, attempt_count
		 FROM tasks
		 WHERE status = 'retry_scheduled' AND retry_at IS NOT NULL AND retry_at <= now()`)
	if err != nil {
		log.Printf("retry scheduler query: %v", err)
		return
	}
	defer rows.Close()

	type retryTask struct {
		TenantID     string
		ID           string
		MailboxID    string
		Priority     core.Priority
		Envelope     []byte
		AttemptCount int
	}

	var tasks []retryTask
	for rows.Next() {
		var t retryTask
		if err := rows.Scan(&t.TenantID, &t.ID, &t.MailboxID, &t.Priority, &t.Envelope, &t.AttemptCount); err != nil {
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
		func(t retryTask) {
			tx, err := s.pool.Begin(ctx)
			if err != nil {
				log.Printf("retry scheduler tx begin %s: %v", t.ID, err)
				return
			}
			committed := false
			defer func() {
				if !committed {
					tx.Rollback(ctx)
				}
			}()

			tag, err := tx.Exec(ctx,
				`UPDATE tasks SET status = 'queued', retry_at = NULL, updated_at = now()
				 WHERE tenant_id = $1 AND id = $2 AND status = 'retry_scheduled'`,
				t.TenantID, t.ID,
			)
			if err != nil {
				log.Printf("retry scheduler update %s: %v", t.ID, err)
				return
			}
			if tag.RowsAffected() == 0 {
				committed = true
				return
			}

			if t.MailboxID != "" {
				msg, ok := buildRetryMessage(t.TenantID, t.ID, t.MailboxID, t.Priority, t.Envelope)
				if ok {
					if s.useOutbox {
						queuePayload, _ := json.Marshal(msg)
						dedupeKey := buildRetryDedupeKey(t.TenantID, t.ID, t.AttemptCount)
						if _, err := tx.Exec(ctx,
							`INSERT INTO outbox_events (id, tenant_id, kind, payload, dedupe_key)
							 VALUES ($1, $2, 'task_publish', $3, $4)
							 ON CONFLICT (tenant_id, dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING`,
							generateULID(), t.TenantID, queuePayload, dedupeKey,
						); err != nil {
							log.Printf("retry scheduler outbox %s: %v", t.ID, err)
							return
						}
					} else if s.queue != nil {
						_ = s.queue.PublishTask(ctx, msg)
					}
				}
			}

		if err := tx.Commit(ctx); err != nil {
			log.Printf("retry scheduler commit %s: %v", t.ID, err)
			return
		}
		committed = true
		metrics.RetryAttempted.Inc()
	}(t)
	}

	if len(tasks) > 0 {
		log.Printf("retry scheduler: promoted %d tasks to queued", len(tasks))
	}
}

// buildRetryDedupeKey constructs a stable dedupe key for a retry-driven
// re-publish so the same delivery cannot be enqueued twice.
func buildRetryDedupeKey(tenantID, taskID string, attemptCount int) string {
	return fmt.Sprintf("task_publish:%s:%s:%d", tenantID, taskID, attemptCount)
}

// buildRetryMessage unmarshals a task envelope and constructs the
// TaskMessage to re-publish. Returns ok=false if the envelope is invalid.
func buildRetryMessage(tenantID, taskID, mailboxID string, priority core.Priority, envelopeJSON []byte) (core.TaskMessage, bool) {
	var env core.TaskEnvelope
	if err := json.Unmarshal(envelopeJSON, &env); err != nil {
		return core.TaskMessage{}, false
	}
	payload, _ := json.Marshal(env)
	return core.TaskMessage{
		TenantID:  tenantID,
		MailboxID: mailboxID,
		TaskID:    taskID,
		Priority:  priority,
		Payload:   payload,
	}, true
}

func generateULID() string {
	now := time.Now()
	t := uint64(now.UnixMilli())
	b := make([]byte, 10)
	for i := 9; i >= 0; i-- {
		b[i] = byte(t & 0xff)
		t >>= 8
	}
	// Use crypto/rand for the 6-byte entropy portion so IDs generated within
	// the same millisecond do not collide.
	randBytes := make([]byte, 6)
	_, _ = cryptorand.Read(randBytes)
	return fmt.Sprintf("%x%x", b, randBytes)
}
