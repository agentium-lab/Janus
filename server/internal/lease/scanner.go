package lease

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/agentium-lab/Janus/core"
)

// Scanner periodically finds claimed/running task attempts whose lease has
// expired (no heartbeat within ack_wait_seconds) and transitions them to
// retry_scheduled (if retries remain) or dead_lettered. This recovers tasks
// whose agent crashed or stopped heartbeating.
type Scanner struct {
	pool     *pgxpool.Pool
	interval time.Duration
	stopCh   chan struct{}
}

// NewScanner creates a lease scanner. interval is how often to scan.
func NewScanner(pool *pgxpool.Pool, interval time.Duration) *Scanner {
	return &Scanner{
		pool:     pool,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

// Start runs the scan loop until ctx is cancelled or Stop is called.
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
	if s.pool == nil {
		return
	}
	n, err := s.ExpireLeases(ctx)
	if err != nil {
		log.Printf("lease scanner: %v", err)
		return
	}
	if n > 0 {
		log.Printf("lease scanner: expired %d leases", n)
	}
}

// ExpireLeases finds claimed/running attempts whose lease has expired (using
// the mailbox's ack_wait_seconds measured from heartbeat_at or started_at) and
// transitions them. Returns the number of attempts expired.
//
// The transition follows the mailbox retry policy: if attempt_count has not
// exceeded max_deliver, the task is moved to retry_scheduled with a retry_at;
// otherwise it is dead_lettered. Attempt status is set to 'failed'.
func (s *Scanner) ExpireLeases(ctx context.Context) (int, error) {
	// Select expired in-flight attempts along with the mailbox ack_wait and
	// retry policy needed to decide retry vs DLQ.
	rows, err := s.pool.Query(ctx,
		`SELECT ta.tenant_id, ta.task_id, ta.attempt, ta.agent_id,
		        t.mailbox_id, t.attempt_count, t.priority, t.envelope,
		        m.ack_wait_seconds, m.max_deliver
		 FROM task_attempts ta
		 JOIN tasks t ON ta.tenant_id = t.tenant_id AND ta.task_id = t.id
		 LEFT JOIN mailboxes m ON t.tenant_id = m.tenant_id AND t.mailbox_id = m.id
		 WHERE ta.status IN ('claimed', 'running')
		   AND t.status IN ('claimed', 'running')
		   AND COALESCE(ta.heartbeat_at, ta.started_at) + (COALESCE(m.ack_wait_seconds, 300) || ' seconds')::interval < now()`)
	if err != nil {
		return 0, fmt.Errorf("query expired leases: %w", err)
	}
	defer rows.Close()

	type expired struct {
		TenantID, TaskID, AgentID, MailboxID string
		Attempt, AttemptCount                int
		Priority                             core.Priority
		Envelope                             []byte
		AckWait, MaxDeliver                  int
	}
	var list []expired
	for rows.Next() {
		var e expired
		if err := rows.Scan(&e.TenantID, &e.TaskID, &e.Attempt, &e.AgentID,
			&e.MailboxID, &e.AttemptCount, &e.Priority, &e.Envelope, &e.AckWait, &e.MaxDeliver); err != nil {
			return 0, fmt.Errorf("scan expired lease: %w", err)
		}
		list = append(list, e)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("expired lease rows: %w", err)
	}

	count := 0
	for _, e := range list {
		// Mark attempt failed (CAS: only if still claimed/running).
		tag, err := s.pool.Exec(ctx,
			`UPDATE task_attempts SET status = 'failed', finished_at = now()
			 WHERE tenant_id = $1 AND task_id = $2 AND attempt = $3 AND status IN ('claimed', 'running')`,
			e.TenantID, e.TaskID, e.Attempt,
		)
		if err != nil {
			log.Printf("lease scanner: mark attempt failed %s/%s: %v", e.TenantID, e.TaskID, err)
			continue
		}
		if tag.RowsAffected() == 0 {
			continue // already moved by a concurrent ACK/NACK
		}

		// Decide retry vs DLQ based on mailbox max_deliver.
		canRetry := e.MaxDeliver <= 0 || e.AttemptCount < e.MaxDeliver
		if canRetry {
			retryAt := time.Now().Add(backoff(e.AttemptCount))
			if _, err := s.pool.Exec(ctx,
				`UPDATE tasks SET status = 'retry_scheduled', retry_at = $1, updated_at = now()
				 WHERE tenant_id = $2 AND id = $3 AND status IN ('claimed', 'running')`,
				retryAt, e.TenantID, e.TaskID,
			); err != nil {
				log.Printf("lease scanner: set retry_scheduled %s/%s: %v", e.TenantID, e.TaskID, err)
			}
		} else {
			if _, err := s.pool.Exec(ctx,
				`UPDATE tasks SET status = 'dead_lettered', updated_at = now()
				 WHERE tenant_id = $1 AND id = $2 AND status IN ('claimed', 'running')`,
				e.TenantID, e.TaskID,
			); err != nil {
				log.Printf("lease scanner: set dead_lettered %s/%s: %v", e.TenantID, e.TaskID, err)
			}
		}
		count++
	}
	return count, nil
}

// backoff returns a retry delay for the given attempt number. Exponential with
// jitter-free base: 10s, 20s, 40s, ... capped at 15m.
func backoff(attempt int) time.Duration {
	d := 10 * time.Second
	for i := 0; i < attempt && d < 15*time.Minute; i++ {
		d *= 2
	}
	if d > 15*time.Minute {
		d = 15 * time.Minute
	}
	return d
}
