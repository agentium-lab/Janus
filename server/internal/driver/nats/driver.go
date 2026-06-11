package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/agentium-lab/Janus/core"
)

type contextKey string

const tenantCtxKey contextKey = "janus_tenant"

func ContextWithTenant(ctx context.Context, tenantID string) context.Context {
	return context.WithValue(ctx, tenantCtxKey, tenantID)
}

func tenantFromCtx(ctx context.Context) string {
	if v, ok := ctx.Value(tenantCtxKey).(string); ok {
		return v
	}
	return "default"
}

type Config struct {
	URL string
}

type Driver struct {
	nc      *nats.Conn
	js      jetstream.JetStream
	mu      sync.RWMutex
	tenant  map[string]*tenantStreams
	pending map[core.DeliveryRef]jetstream.Msg
}

type tenantStreams struct {
	taskStream  jetstream.Stream
	eventStream jetstream.Stream
	retryStream jetstream.Stream
	dlqStreams  map[string]jetstream.Stream
	consumers   map[string]jetstream.Consumer
}

func NewDriver(cfg Config) (*Driver, error) {
	nc, err := nats.Connect(cfg.URL,
		nats.ReconnectWait(2*time.Second),
		nats.MaxReconnects(60),
		nats.Timeout(5*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to NATS: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("create jetstream: %w", err)
	}

	return &Driver{
		nc:      nc,
		js:      js,
		tenant:  make(map[string]*tenantStreams),
		pending: make(map[core.DeliveryRef]jetstream.Msg),
	}, nil
}

func (d *Driver) PublishTask(ctx context.Context, msg core.TaskMessage) error {
	subject := taskSubject(msg.TenantID, msg.MailboxID)

	nmsg := nats.NewMsg(subject)
	nmsg.Data = msg.Payload
	nmsg.Header.Set("JANUS-Task-ID", msg.TaskID)
	nmsg.Header.Set("JANUS-Tenant-ID", msg.TenantID)
	nmsg.Header.Set("JANUS-Mailbox-ID", msg.MailboxID)
	nmsg.Header.Set("JANUS-Priority", string(msg.Priority))
	for k, v := range msg.Headers {
		nmsg.Header.Set(k, v)
	}

	_, err := d.js.PublishMsg(ctx, nmsg)
	if err != nil {
		return fmt.Errorf("publish task to %s: %w", subject, err)
	}
	return nil
}

func (d *Driver) PublishDLQ(ctx context.Context, msg core.TaskMessage, errPayload []byte) error {
	subject := dlqSubject(msg.TenantID, msg.MailboxID)

	nmsg := nats.NewMsg(subject)
	nmsg.Data = msg.Payload
	nmsg.Header.Set("JANUS-Task-ID", msg.TaskID)
	nmsg.Header.Set("JANUS-Tenant-ID", msg.TenantID)
	nmsg.Header.Set("JANUS-Mailbox-ID", msg.MailboxID)
	nmsg.Header.Set("JANUS-DLQ-Error", string(errPayload))

	_, err := d.js.PublishMsg(ctx, nmsg)
	if err != nil {
		return fmt.Errorf("publish dlq to %s: %w", subject, err)
	}
	return nil
}

func (d *Driver) FetchTasks(ctx context.Context, mailbox string, opts core.FetchOptions) ([]core.TaskDelivery, error) {
	tenantID := tenantFromCtx(ctx)
	consumerKey := consumerName(tenantID, mailbox)

	ts, ok := d.getTenant(tenantID)
	if !ok {
		return nil, fmt.Errorf("tenant %s not initialized", tenantID)
	}

	d.mu.RLock()
	cons, ok := ts.consumers[consumerKey]
	d.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("consumer %s not found", consumerKey)
	}

	maxMsgs := opts.MaxMessages
	if maxMsgs <= 0 {
		maxMsgs = 1
	}

	fetchOpts := []jetstream.FetchOpt{}
	if opts.WaitTime > 0 {
		fetchOpts = append(fetchOpts, jetstream.FetchMaxWait(opts.WaitTime))
	}

	batch, err := cons.Fetch(maxMsgs, fetchOpts...)
	if err != nil {
		return nil, fmt.Errorf("fetch from %s: %w", consumerKey, err)
	}

	var deliveries []core.TaskDelivery
	for msg := range batch.Messages() {
		meta, err := msg.Metadata()
		if err != nil {
			msg.Nak()
			continue
		}
		ref := core.DeliveryRef(fmt.Sprintf("%s:%d", msg.Subject(), meta.Sequence.Stream))
		d.StorePending(ref, msg)
		deliveries = append(deliveries, core.TaskDelivery{
			TaskID:          msg.Headers().Get("JANUS-Task-ID"),
			Payload:         msg.Data(),
			DeliveryRef:     ref,
			RedeliveryCount: int(meta.NumDelivered) - 1,
		})
	}
	return deliveries, nil
}

func (d *Driver) AckTask(_ context.Context, ref core.DeliveryRef) error {
	msg, ok := d.popPending(ref)
	if !ok {
		return fmt.Errorf("delivery ref not found: %s", ref)
	}
	return msg.Ack()
}

func (d *Driver) NackTask(_ context.Context, ref core.DeliveryRef, reason core.NackReason) error {
	msg, ok := d.popPending(ref)
	if !ok {
		return fmt.Errorf("delivery ref not found: %s", ref)
	}
	if reason == core.NackNonRetriable {
		return msg.Term()
	}
	return msg.Nak()
}

func (d *Driver) PublishEvent(ctx context.Context, event core.JanusEvent) error {
	subject := eventSubject(event.TenantID, string(event.EventType))
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	_, err = d.js.Publish(ctx, subject, data)
	if err != nil {
		return fmt.Errorf("publish event to %s: %w", subject, err)
	}
	return nil
}

func (d *Driver) ReplayEvents(ctx context.Context, filter core.EventReplayFilter) (core.EventIterator, error) {
	_, ok := d.getTenant(filter.TenantID)
	if !ok {
		return nil, fmt.Errorf("tenant %s not initialized", filter.TenantID)
	}

	sName := streamName(filter.TenantID, "EVENTS")
	cName := fmt.Sprintf("replay_%d", time.Now().UnixNano())

	cfg := jetstream.ConsumerConfig{
		Durable:       cName,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	}
	if len(filter.EventTypes) > 0 {
		filterSubjects := make([]string, 0, len(filter.EventTypes))
		for _, et := range filter.EventTypes {
			filterSubjects = append(filterSubjects, eventSubject(filter.TenantID, string(et)))
		}
		cfg.FilterSubjects = filterSubjects
	}

	cons, err := d.js.CreateConsumer(ctx, sName, cfg)
	if err != nil {
		return nil, fmt.Errorf("create replay consumer: %w", err)
	}

	batch, err := cons.Fetch(256, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		d.js.DeleteConsumer(ctx, sName, cName)
		return nil, fmt.Errorf("fetch for replay: %w", err)
	}

	return &eventIterator{
		msgs:   batch.Messages(),
		js:     d.js,
		stream: sName,
		name:   cName,
	}, nil
}

func (d *Driver) EnsureTenant(ctx context.Context, tenantID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.tenant[tenantID]; ok {
		return nil
	}

	taskStream, err := d.js.CreateStream(ctx, jetstream.StreamConfig{
		Name:       streamName(tenantID, "TASKS"),
		Subjects:   []string{fmt.Sprintf("janus.%s.tasks.>", tenantID)},
		Retention:  jetstream.WorkQueuePolicy,
		MaxAge:     7 * 24 * time.Hour,
		Storage:    jetstream.FileStorage,
		Duplicates: 2 * time.Minute,
	})
	if err != nil {
		return fmt.Errorf("create task stream for tenant %s: %w", tenantID, err)
	}

	eventStream, err := d.js.CreateStream(ctx, jetstream.StreamConfig{
		Name:       streamName(tenantID, "EVENTS"),
		Subjects:   []string{fmt.Sprintf("janus.%s.events.>", tenantID)},
		Retention:  jetstream.LimitsPolicy,
		MaxAge:     30 * 24 * time.Hour,
		Storage:    jetstream.FileStorage,
		Duplicates: 2 * time.Minute,
	})
	if err != nil {
		return fmt.Errorf("create event stream for tenant %s: %w", tenantID, err)
	}

	retryStream, err := d.js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      streamName(tenantID, "RETRY"),
		Subjects:  []string{fmt.Sprintf("janus.%s.tasks_retry.>", tenantID)},
		Retention: jetstream.LimitsPolicy,
		MaxAge:    24 * time.Hour,
		Storage:   jetstream.FileStorage,
	})
	if err != nil {
		return fmt.Errorf("create retry stream for tenant %s: %w", tenantID, err)
	}

	d.tenant[tenantID] = &tenantStreams{
		taskStream:  taskStream,
		eventStream: eventStream,
		retryStream: retryStream,
		dlqStreams:  make(map[string]jetstream.Stream),
		consumers:   make(map[string]jetstream.Consumer),
	}
	return nil
}

func (d *Driver) EnsureMailbox(ctx context.Context, spec core.MailboxSpec) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	ts, ok := d.tenant[spec.TenantID]
	if !ok {
		return fmt.Errorf("tenant %s not initialized", spec.TenantID)
	}

	if _, ok := ts.dlqStreams[spec.MailboxID]; ok {
		return nil
	}

	dlqStream, err := d.js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      streamName(spec.TenantID, "DLQ_"+sanitize(spec.MailboxID)),
		Subjects:  []string{dlqSubject(spec.TenantID, spec.MailboxID)},
		Retention: jetstream.LimitsPolicy,
		MaxAge:    30 * 24 * time.Hour,
		MaxMsgs:   10000,
		Storage:   jetstream.FileStorage,
	})
	if err != nil {
		return fmt.Errorf("create DLQ stream for mailbox %s: %w", spec.MailboxID, err)
	}
	ts.dlqStreams[spec.MailboxID] = dlqStream
	return nil
}

func (d *Driver) EnsureConsumer(ctx context.Context, spec core.ConsumerSpec) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	ts, ok := d.tenant[spec.TenantID]
	if !ok {
		return fmt.Errorf("tenant %s not initialized", spec.TenantID)
	}

	cname := consumerName(spec.TenantID, spec.MailboxID)
	if _, ok := ts.consumers[cname]; ok {
		return nil
	}

	maxAckPending := spec.MaxACKPending
	if maxAckPending <= 0 {
		maxAckPending = 100
	}

	ackWait := time.Duration(spec.ACKWaitSeconds) * time.Second
	if ackWait <= 0 {
		ackWait = 300 * time.Second
	}

	maxDeliver := spec.MaxDeliver
	if maxDeliver <= 0 {
		maxDeliver = 5
	}

	cons, err := d.js.CreateConsumer(ctx, streamName(spec.TenantID, "TASKS"), jetstream.ConsumerConfig{
		Durable:        cname,
		FilterSubjects: []string{taskSubject(spec.TenantID, spec.MailboxID)},
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        ackWait,
		MaxDeliver:     maxDeliver,
		MaxAckPending:  maxAckPending,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
	})
	if err != nil {
		return fmt.Errorf("create consumer %s: %w", cname, err)
	}
	ts.consumers[cname] = cons
	return nil
}

func (d *Driver) Close() error {
	d.nc.Close()
	return nil
}

func (d *Driver) Conn() *nats.Conn {
	return d.nc
}

func (d *Driver) SubscribeEvents(ctx context.Context, ch chan<- core.JanusEvent) (*nats.Subscription, error) {
	sub, err := d.nc.Subscribe("janus.*.events.>", func(msg *nats.Msg) {
		var event core.JanusEvent
		if err := json.Unmarshal(msg.Data, &event); err != nil {
			return
		}
		select {
		case ch <- event:
		default:
		}
	})
	if err != nil {
		return nil, fmt.Errorf("subscribe events: %w", err)
	}
	return sub, nil
}

func (d *Driver) StorePending(ref core.DeliveryRef, msg jetstream.Msg) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.pending[ref] = msg
}

func (d *Driver) popPending(ref core.DeliveryRef) (jetstream.Msg, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	msg, ok := d.pending[ref]
	if ok {
		delete(d.pending, ref)
	}
	return msg, ok
}

func (d *Driver) getTenant(tenantID string) (*tenantStreams, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	ts, ok := d.tenant[tenantID]
	return ts, ok
}

func streamName(tenantID, suffix string) string {
	return fmt.Sprintf("JANUS_%s_%s", sanitize(tenantID), sanitize(suffix))
}

func taskSubject(tenantID, mailboxID string) string {
	return fmt.Sprintf("janus.%s.tasks.%s", tenantID, mailboxID)
}

func eventSubject(tenantID, eventType string) string {
	return fmt.Sprintf("janus.%s.events.%s", tenantID, eventType)
}

func dlqSubject(tenantID, mailboxID string) string {
	return fmt.Sprintf("janus.%s.tasks_dlq.%s", tenantID, mailboxID)
}

func consumerName(tenantID, mailboxID string) string {
	return fmt.Sprintf("consumer_%s_%s", sanitize(tenantID), sanitize(mailboxID))
}

func sanitize(s string) string {
	result := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-' {
			result = append(result, c)
		} else {
			result = append(result, '_')
		}
	}
	return string(result)
}

type eventIterator struct {
	msgs   <-chan jetstream.Msg
	js     jetstream.JetStream
	stream string
	name   string
}

func (it *eventIterator) Next(ctx context.Context) (*core.JanusEvent, error) {
	select {
	case msg, ok := <-it.msgs:
		if !ok {
			return nil, nil
		}
		msg.Ack()
		var event core.JanusEvent
		if err := json.Unmarshal(msg.Data(), &event); err != nil {
			return nil, fmt.Errorf("unmarshal event: %w", err)
		}
		return &event, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (it *eventIterator) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	it.js.DeleteConsumer(ctx, it.stream, it.name)
	return nil
}
