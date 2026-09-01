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

func (d *Driver) FetchTasks(ctx context.Context, tenantID, mailbox string, opts core.FetchOptions) ([]core.TaskDelivery, error) {
	limit := opts.MaxMessages
	if limit <= 0 {
		limit = 1
	}
	rows, err := d.pool.Query(ctx, `
		WITH picked AS (
			SELECT id, attempt_count FROM tasks
			WHERE mailbox_id = $1 AND status = 'queued'
			  AND (retry_at IS NULL OR retry_at <= now())
			  AND (queue_lease_until IS NULL OR queue_lease_until <= now())
			ORDER BY priority DESC, created_at
			FOR UPDATE SKIP LOCKED
			LIMIT $2
		)
		UPDATE tasks t SET queue_lease_until = now() + interval '45 seconds', updated_at = now()
		FROM picked WHERE t.id = picked.id
		RETURNING t.id, t.tenant_id, t.source_agent, t.attempt_count,
		          COALESCE(t.envelope->>'content_type',''), COALESCE(t.envelope->'payload','{}'::jsonb)`, mailbox, limit)
	if err != nil {
		return nil, fmt.Errorf("claim query: %w", err)
	}
	defer rows.Close()

	deliveries := []core.TaskDelivery{}
	for rows.Next() {
		var (
			id, tenantID, srcAgent, ctype string
			attempt                       int
			payload                       []byte
		)
		if err := rows.Scan(&id, &tenantID, &srcAgent, &attempt, &ctype, &payload); err != nil {
			return nil, err
		}
		ref := ref(id, attempt)
		env, _ := jsonEnvelope(tenantID, id, srcAgent, ctype, payload)
		deliveries = append(deliveries, core.TaskDelivery{
			TaskID: id, Attempt: attempt, Payload: env, DeliveryRef: ref,
		})
	}
	return deliveries, rows.Err()
}

func jsonEnvelope(tenant, taskID, srcAgent, ctype string, payload []byte) ([]byte, error) {
	return fmt.Appendf(nil,
		`{"janus_version":"1.0","task_id":%q,"tenant_id":%q,"source_agent":%q,"content_type":%q,"payload":%s}`,
		taskID, tenant, srcAgent, ctype, payload), nil
}

// AckTask finalizes the attempt row; authoritative task completion still runs
// through DispatchService.AckTask's own guarded transaction.
func (d *Driver) AckTask(ctx context.Context, tenantID string, r core.DeliveryRef) error {
	taskID, _, err := splitRef(r)
	if err != nil {
		return err
	}
	_, err = d.pool.Exec(ctx,
		`UPDATE tasks SET queue_lease_until = NULL WHERE id=$1 AND tenant_id=$2`, taskID, tenantID)
	return err
}

func (d *Driver) NackTask(ctx context.Context, tenantID string, r core.DeliveryRef, reason core.NackReason) error {
	taskID, _, err := splitRef(r)
	if err != nil {
		return err
	}
	d.clearLease(ctx, tenantID, taskID)
	if reason == core.NackNonRetriable {
		_, err = d.pool.Exec(ctx,
			`UPDATE tasks SET status='dead_letter', updated_at=now() WHERE id=$1 AND tenant_id=$2`, taskID, tenantID)
		return err
	}
	// Retriable: release the lease immediately; retry_at scheduling stays
	// owned by the existing retry policy columns.
	_, err = d.pool.Exec(ctx,
		`UPDATE tasks SET retry_at = COALESCE(retry_at, now()) WHERE id=$1 AND tenant_id=$2`, taskID, tenantID)
	return err
}

func (d *Driver) clearLease(ctx context.Context, tenantID, taskID string) {
	_, _ = d.pool.Exec(ctx, `UPDATE tasks SET queue_lease_until=NULL WHERE id=$1 AND tenant_id=$2`, taskID, tenantID)
}

func (d *Driver) PublishDLQ(ctx context.Context, msg core.TaskMessage, errPayload []byte) error {
	_, err := d.pool.Exec(ctx,
		`UPDATE tasks SET status='dead_letter', error=$3::jsonb, updated_at=now() WHERE id=$1 AND tenant_id=$2`,
		msg.TaskID, msg.TenantID, string(errPayload))
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
