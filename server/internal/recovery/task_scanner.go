package recovery

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/metrics"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TaskScanner recovers queued tasks whose NATS messages were lost.
// It finds tasks in 'queued' status that are older than a threshold
// and re-publishes them to the queue.
type TaskScanner struct {
	pool     *pgxpool.Pool
	queue    core.QueueEventDriver
	interval time.Duration
	// minAge is the minimum age of a queued task before recovery attempts it.
	// This prevents racing with normal publish.
	minAge time.Duration
	stopCh chan struct{}
	// lastProcessedAt and lastProcessedID track the cursor for keyset pagination
	// to prevent starvation of newer tasks when there are many old stuck tasks.
	lastProcessedAt time.Time
	lastProcessedID string
}

func NewTaskScanner(pool *pgxpool.Pool, queue core.QueueEventDriver, interval time.Duration) *TaskScanner {
	return &TaskScanner{
		pool:     pool,
		queue:    queue,
		interval: interval,
		minAge:   5 * time.Minute, // Don't recover tasks younger than 5 minutes
		stopCh:   make(chan struct{}),
	}
}

func (s *TaskScanner) Start(ctx context.Context) {
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

func (s *TaskScanner) Stop() {
	close(s.stopCh)
}

func (s *TaskScanner) scan(ctx context.Context) {
	if s.pool == nil || s.queue == nil {
		return
	}

	// Find queued tasks older than minAge that may have lost their NATS message.
	// Use keyset pagination to prevent starvation: if there are many old stuck tasks,
	// we need to eventually process newer tasks too.
	// The cursor is (created_at, id) to handle ties in created_at.
	var rows pgx.Rows
	var err error

	if s.lastProcessedAt.IsZero() {
		// First scan: start from the oldest tasks
		rows, err = s.pool.Query(ctx,
			`SELECT tenant_id, id, mailbox_id, priority, envelope, created_at
			 FROM tasks
			 WHERE status = 'queued'
			   AND created_at < now() - $1::interval
			 ORDER BY created_at ASC, id ASC
			 LIMIT 100`,
			s.minAge.String(),
		)
	} else {
		// Subsequent scans: continue from where we left off
		rows, err = s.pool.Query(ctx,
			`SELECT tenant_id, id, mailbox_id, priority, envelope, created_at
			 FROM tasks
			 WHERE status = 'queued'
			   AND created_at < now() - $1::interval
			   AND (created_at, id) > ($2, $3)
			 ORDER BY created_at ASC, id ASC
			 LIMIT 100`,
			s.minAge.String(), s.lastProcessedAt, s.lastProcessedID,
		)
	}
	if err != nil {
		log.Printf("recovery task scanner query: %v", err)
		return
	}
	defer rows.Close()

	type queuedTask struct {
		TenantID  string
		ID        string
		MailboxID string
		Priority  core.Priority
		Envelope  []byte
		CreatedAt time.Time
	}

	var tasks []queuedTask
	for rows.Next() {
		var t queuedTask
		if err := rows.Scan(&t.TenantID, &t.ID, &t.MailboxID, &t.Priority, &t.Envelope, &t.CreatedAt); err != nil {
			log.Printf("recovery task scanner scan: %v", err)
			return
		}
		tasks = append(tasks, t)
	}
	if err := rows.Err(); err != nil {
		log.Printf("recovery task scanner rows: %v", err)
		return
	}

	if len(tasks) == 0 {
		// No more tasks to process, reset cursor to start from oldest again
		s.lastProcessedAt = time.Time{}
		s.lastProcessedID = ""
		return
	}

	log.Printf("recovery task scanner: found %d queued tasks older than %v, attempting recovery", len(tasks), s.minAge)

	recovered := 0
	for _, t := range tasks {
		if s.recoverTask(ctx, t) {
			recovered++
		}
	}

	// Update cursor to the last task we processed
	lastTask := tasks[len(tasks)-1]
	s.lastProcessedAt = lastTask.CreatedAt
	s.lastProcessedID = lastTask.ID

	if recovered > 0 {
		log.Printf("recovery task scanner: recovered %d tasks", recovered)
		metrics.TasksRecovered.Add(float64(recovered))
	}
}

func (s *TaskScanner) recoverTask(ctx context.Context, t struct {
	TenantID  string
	ID        string
	MailboxID string
	Priority  core.Priority
	Envelope  []byte
	CreatedAt time.Time
}) bool {
	if t.MailboxID == "" {
		return false
	}

	var env core.TaskEnvelope
	if err := json.Unmarshal(t.Envelope, &env); err != nil {
		log.Printf("recovery task scanner: task %s envelope invalid: %v", t.ID, err)
		return false
	}

	payload, _ := json.Marshal(env)
	msg := core.TaskMessage{
		TenantID:  t.TenantID,
		MailboxID: t.MailboxID,
		TaskID:    t.ID,
		Priority:  t.Priority,
		Payload:   payload,
		DedupeKey: fmt.Sprintf("recovery:%s:%s", t.TenantID, t.ID),
	}

	if err := s.queue.PublishTask(ctx, msg); err != nil {
		log.Printf("recovery task scanner: task %s publish failed: %v", t.ID, err)
		return false
	}

	return true
}
