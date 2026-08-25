// Package pgqueue implements core.QueueEventDriver entirely on PostgreSQL,
// enabling single-dependency deployments without NATS. Delivery rows are the
// tasks table itself; claims use FOR UPDATE SKIP LOCKED so concurrent pulls
// never double-claim, and retries reuse the existing retry_at column.
package pgqueue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/agentium-lab/Janus/core"
)

type Driver struct {
	mu   sync.Mutex
	subs []chan<- core.JanusEvent
	pool *pgxpool.Pool
}

func NewDriver(pool *pgxpool.Pool) *Driver {
	return &Driver{pool: pool}
}

// SubscribeEvents registers an in-process event channel. Durability comes
// from the outbox rows that precede every PublishEvent call; this bus only
// carries live fan-out to WS broadcasters and audit projectors.
func (d *Driver) SubscribeEvents(ctx context.Context, ch chan<- core.JanusEvent) (*Subscription, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	sub := &Subscription{ch: ch, driver: d}
	d.subs = append(d.subs, ch)
	go func() {
		<-ctx.Done()
		d.remove(ch)
	}()
	return sub, nil
}

type Subscription struct {
	ch     chan<- core.JanusEvent
	driver *Driver
}

func (s *Subscription) Unsubscribe() { s.driver.remove(s.ch) }

func (d *Driver) remove(ch chan<- core.JanusEvent) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i, c := range d.subs {
		if c == ch {
			d.subs = append(d.subs[:i], d.subs[i+1:]...)
			return
		}
	}
}

var ErrNotSupported = errors.New("pgqueue: not supported in postgres-only mode")

func ref(taskID string, attempt int) core.DeliveryRef {
	return core.DeliveryRef(fmt.Sprintf("%s:%d", taskID, attempt))
}

func splitRef(r core.DeliveryRef) (taskID string, attempt int, err error) {
	parts := strings.Split(string(r), ":")
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("malformed delivery ref %q", r)
	}
	_, err = fmt.Sscanf(parts[1], "%d", &attempt)
	if err != nil {
		return "", 0, fmt.Errorf("malformed delivery ref %q", r)
	}
	return parts[0], attempt, nil
}

// PublishTask is a no-op: in PG mode the task row inserted by TaskService IS
// the queue entry; FetchTasks reads it directly.
func (d *Driver) PublishTask(_ context.Context, _ core.TaskMessage) error { return nil }

func (d *Driver) FetchTasks(ctx context.Context, mailbox string, opts core.FetchOptions) ([]core.TaskDelivery, error) {
	limit := opts.MaxMessages
	if limit <= 0 {
		limit = 1
	}
	deliveries := []core.TaskDelivery{}
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin fetch: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		WITH picked AS (
			SELECT id FROM tasks
			WHERE mailbox_id = $1 AND status = 'queued'
			  AND (retry_at IS NULL OR retry_at <= now())
			ORDER BY priority DESC, created_at
			FOR UPDATE SKIP LOCKED
			LIMIT $2
		)
		UPDATE tasks t SET status = 'claimed', updated_at = now(), attempt_count = attempt_count + 1
		FROM picked WHERE t.id = picked.id
		RETURNING t.id, t.attempt_count`, mailbox, limit)
	if err != nil {
		return nil, fmt.Errorf("claim query: %w", err)
	}
	type claimed struct {
		id      string
		attempt int
	}
	var got []claimed
	for rows.Next() {
		var c claimed
		if err := rows.Scan(&c.id, &c.attempt); err != nil {
			rows.Close()
			return nil, err
		}
		got = append(got, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for _, c := range got {
		ref := ref(c.id, c.attempt)
		if _, ierr := tx.Exec(ctx,
			`INSERT INTO task_attempts (tenant_id, task_id, attempt, lease_id, status, started_at, delivery_ref)
			 SELECT tenant_id, id, $2, $3, 'claimed', now(), $4 FROM tasks WHERE id = $1`,
			c.id, c.attempt, ref, ref); ierr != nil {
			return nil, fmt.Errorf("insert attempt %s: %w", ref, ierr)
		}
		env := []byte("{}")
		var tenantID, srcAgent, ctype string
		var payload []byte
		if qerr := tx.QueryRow(ctx,
			`SELECT tenant_id, source_agent, envelope->>'content_type',
			        COALESCE(envelope->'payload','null'::jsonb)::text
			 FROM tasks WHERE id=$1`, c.id).
			Scan(&tenantID, &srcAgent, &ctype, &payload); qerr == nil {
			env, _ = jsonEnvelope(tenantID, c.id, srcAgent, ctype, payload)
		}
		deliveries = append(deliveries, core.TaskDelivery{
			TaskID:      c.id,
			Attempt:     c.attempt,
			Payload:     env,
			DeliveryRef: ref,
		})
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit fetch: %w", err)
	}
	return deliveries, nil
}

func jsonEnvelope(tenant, taskID, srcAgent, ctype string, payload []byte) ([]byte, error) {
	return fmt.Appendf(nil,
		`{"janus_version":"1.0","task_id":%q,"tenant_id":%q,"source_agent":%q,"content_type":%q,"payload":%s}`,
		taskID, tenant, srcAgent, ctype, payload), nil
}

// AckTask finalizes the attempt row; authoritative task completion still runs
// through DispatchService.AckTask's own guarded transaction.
func (d *Driver) AckTask(ctx context.Context, r core.DeliveryRef) error {
	taskID, attempt, err := splitRef(r)
	if err != nil {
		return err
	}
	_, err = d.pool.Exec(ctx,
		`UPDATE task_attempts SET status='completed', finished_at=now()
		 WHERE task_id=$1 AND attempt=$2 AND status='claimed'`, taskID, attempt)
	return err
}

func (d *Driver) NackTask(ctx context.Context, r core.DeliveryRef, reason core.NackReason) error {
	taskID, attempt, err := splitRef(r)
	if err != nil {
		return err
	}
	if _, err := d.pool.Exec(ctx,
		`UPDATE task_attempts SET status='nacked', finished_at=now()
		 WHERE task_id=$1 AND attempt=$2 AND status='claimed'`, taskID, attempt); err != nil {
		return err
	}
	if reason == core.NackNonRetriable {
		_, err = d.pool.Exec(ctx,
			`UPDATE tasks SET status='dead_letter', updated_at=now() WHERE id=$1`, taskID)
		return err
	}
	// Retriable: back to queued immediately; retry_at scheduling stays owned by
	// the existing retry policy columns.
	_, err = d.pool.Exec(ctx,
		`UPDATE tasks SET status='queued', updated_at=now() WHERE id=$1 AND status='claimed'`, taskID)
	return err
}

func (d *Driver) PublishDLQ(ctx context.Context, msg core.TaskMessage, errPayload []byte) error {
	_, err := d.pool.Exec(ctx,
		`UPDATE tasks SET status='dead_letter', error=$2::jsonb, updated_at=now() WHERE id=$1`,
		msg.TaskID, string(errPayload))
	return err
}

// PublishEvent fans the event out to in-process subscribers (WS broadcaster,
// audit projector). Durability is owned by the outbox row written before this
// call, so dropping a notification on a full channel never loses the fact.
func (d *Driver) PublishEvent(_ context.Context, event core.JanusEvent) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, ch := range d.subs {
		select {
		case ch <- event:
		default:
		}
	}
	return nil
}

func (d *Driver) ReplayEvents(ctx context.Context, _ core.EventReplayFilter) (core.EventIterator, error) {
	return nil, errors.New("pgqueue: historical replay pending; use the outbox table directly")
}

func (d *Driver) EnsureTenant(_ context.Context, _ string) error              { return nil }
func (d *Driver) EnsureMailbox(_ context.Context, _ core.MailboxSpec) error   { return nil }
func (d *Driver) EnsureConsumer(_ context.Context, _ core.ConsumerSpec) error { return nil }
func (d *Driver) Close() error                                                { return nil }
