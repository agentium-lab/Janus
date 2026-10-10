package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/agentium-lab/Janus/core"
)

// maxPendingEntries caps the in-flight pending map. When the cap is reached,
// the oldest entries are evicted (their Msg is Nak()'d back to JetStream so
// redelivery happens rather than silent loss). This prevents unbounded growth
// if a consumer fetches but never ACKs (e.g. agent crash, slow processing).
const maxPendingEntries = 4096

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
	// Unlimited reconnects: a bounded MaxReconnects turns any broker outage
	// longer than the retry window into a PERMANENTLY closed connection —
	// the process never recovers without a restart (observed in the nightly
	// chaos run: docker stop/start nats left the API degraded forever).
	// A reconnecting client costs one SYN per interval.
	nc, err := nats.Connect(cfg.URL,
		nats.ReconnectWait(2*time.Second),
		nats.MaxReconnects(-1),
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
	if msg.DedupeKey != "" {
		nmsg.Header.Set("Nats-Msg-Id", msg.DedupeKey)
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
	if msg.DedupeKey != "" {
		nmsg.Header.Set("Nats-Msg-Id", msg.DedupeKey)
	}

	_, err := d.js.PublishMsg(ctx, nmsg)
	if err != nil {
		return fmt.Errorf("publish dlq to %s: %w", subject, err)
	}
	return nil
}

func (d *Driver) FetchTasks(ctx context.Context, tenantID, mailbox string, opts core.FetchOptions) ([]core.TaskDelivery, error) {
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

func (d *Driver) AckTask(_ context.Context, _ string, ref core.DeliveryRef) error {
	msg, ok := d.popPending(ref)
	if !ok {
		return fmt.Errorf("delivery ref not found: %s", ref)
	}
	return msg.Ack()
}

func (d *Driver) NackTask(_ context.Context, _ string, ref core.DeliveryRef, reason core.NackReason) error {
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
	msgID := event.EventID
	if msgID == "" {
		msgID = fmt.Sprintf("%s-%s-%d", event.TenantID, event.TaskID, event.Timestamp.UnixNano())
	}
	_, err = d.js.PublishMsg(ctx, &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": []string{msgID}},
	})
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

// ReconcileTenant verifies the tenant's streams actually exist on the
// broker and recreates any that were lost (e.g. JetStream storage wiped).
// The in-process tenant cache makes EnsureTenant a no-op once seen, so a
// runtime stream loss would otherwise never heal on this instance.
// Stream configs are SHARED by initial creation and reconcile so a
// restored stream always matches what EnsureTenant would have built (the
// restore path previously capped EVENTS at 10k msgs while the initial
// build had no cap — silently shrinking the broker replay window).
func taskStreamConfig(tenantID string) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:       streamName(tenantID, "TASKS"),
		Subjects:   []string{fmt.Sprintf("janus.%s.tasks.>", tenantID)},
		Retention:  jetstream.WorkQueuePolicy,
		MaxAge:     7 * 24 * time.Hour,
		Storage:    jetstream.FileStorage,
		Duplicates: 2 * time.Minute,
	}
}

func eventStreamConfig(tenantID string) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:       streamName(tenantID, "EVENTS"),
		Subjects:   []string{fmt.Sprintf("janus.%s.events.>", tenantID)},
		Retention:  jetstream.LimitsPolicy,
		MaxAge:     30 * 24 * time.Hour,
		Storage:    jetstream.FileStorage,
		Duplicates: 2 * time.Minute,
	}
}

func retryStreamConfig(tenantID string) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:      streamName(tenantID, "RETRY"),
		Subjects:  []string{fmt.Sprintf("janus.%s.tasks_retry.>", tenantID)},
		Retention: jetstream.LimitsPolicy,
		MaxAge:    24 * time.Hour,
		Storage:   jetstream.FileStorage,
	}
}

// streamConfigDrifted reports whether the broker's live config differs
// from the desired config on the limits that matter for parity.
func streamConfigDrifted(live, want jetstream.StreamConfig) bool {
	return live.MaxMsgs != want.MaxMsgs ||
		live.MaxAge != want.MaxAge ||
		live.Retention != want.Retention ||
		live.Duplicates != want.Duplicates
}

func dlqStreamConfig(tenantID, mailboxID string) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:      streamName(tenantID, "DLQ_"+sanitize(mailboxID)),
		Subjects:  []string{dlqSubject(tenantID, mailboxID)},
		Retention: jetstream.LimitsPolicy,
		MaxAge:    30 * 24 * time.Hour,
		MaxMsgs:   10000,
		Storage:   jetstream.FileStorage,
	}
}

func (d *Driver) ReconcileTenant(ctx context.Context, tenantID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Incremental repair on the EXISTING cache entry: replace only the
	// stream handles that are missing on the broker, and never touch the
	// dlqStreams/consumers maps — rebuilding the entry from scratch evicted
	// every live consumer handle and broke pulls until they were lazily
	// re-created.
	ts, ok := d.tenant[tenantID]
	if !ok {
		// Build on a LOCAL entry and publish it to the map only after all
		// required streams are confirmed — publishing an empty/partial
		// entry first made EnsureTenant report success via cache hit even
		// when stream creation failed midway (readyz false-green).
		ts = &tenantStreams{
			dlqStreams: make(map[string]jetstream.Stream),
			consumers:  make(map[string]jetstream.Consumer),
		}
		defer func() {
			if ts.taskStream != nil && ts.eventStream != nil && ts.retryStream != nil {
				d.tenant[tenantID] = ts
			}
		}()
	}
	fix := func(name string, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
		if st, err := d.js.Stream(ctx, name); err == nil {
			// Legacy drift repair: streams created by an older version may
			// carry different limits (e.g. EVENTS once capped at 10k msgs
			// by the pre-parity restore path). Verify and UPDATE the config
			// in place so existing streams converge to the current spec —
			// not just newly created ones.
			if info, ierr := st.Info(ctx); ierr == nil && streamConfigDrifted(info.Config, cfg) {
				updated, uerr := d.js.UpdateStream(ctx, cfg)
				if uerr != nil {
					return nil, fmt.Errorf("update drifted stream %s: %w", name, uerr)
				}
				log.Printf("nats: updated drifted stream config %s for tenant %s", name, tenantID)
				return updated, nil
			}
			return st, nil
		}
		st, err := d.js.CreateStream(ctx, cfg)
		if err != nil {
			return nil, err
		}
		log.Printf("nats: recreated missing stream %s for tenant %s", name, tenantID)
		return st, nil
	}

	tasks, err := fix(streamName(tenantID, "TASKS"), taskStreamConfig(tenantID))
	if err != nil {
		return fmt.Errorf("reconcile task stream for tenant %s: %w", tenantID, err)
	}
	ts.taskStream = tasks

	events, err := fix(streamName(tenantID, "EVENTS"), eventStreamConfig(tenantID))
	if err != nil {
		return fmt.Errorf("reconcile event stream for tenant %s: %w", tenantID, err)
	}
	ts.eventStream = events

	retry, err := fix(streamName(tenantID, "RETRY"), retryStreamConfig(tenantID))
	if err != nil {
		return fmt.Errorf("reconcile retry stream for tenant %s: %w", tenantID, err)
	}
	ts.retryStream = retry

	return nil
}

func (d *Driver) EnsureTenant(ctx context.Context, tenantID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.tenant[tenantID]; ok {
		return nil
	}

	taskStream, err := d.js.CreateStream(ctx, taskStreamConfig(tenantID))
	if err != nil {
		return fmt.Errorf("create task stream for tenant %s: %w", tenantID, err)
	}

	eventStream, err := d.js.CreateStream(ctx, eventStreamConfig(tenantID))
	if err != nil {
		return fmt.Errorf("create event stream for tenant %s: %w", tenantID, err)
	}

	retryStream, err := d.js.CreateStream(ctx, retryStreamConfig(tenantID))
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

// ReconcileMailboxDLQ verifies the mailbox's DLQ stream exists on the
// broker and recreates it when lost; the dlqStreams cache makes
// EnsureMailbox a no-op once seen, so a runtime stream loss would never
// heal and dead-letter publishes would fail forever.
func (d *Driver) ReconcileMailboxDLQ(ctx context.Context, tenantID, mailboxID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	ts, ok := d.tenant[tenantID]
	if !ok {
		return nil // tenant reconcile owns initialization
	}
	name := streamName(tenantID, "DLQ_"+sanitize(mailboxID))
	if st, err := d.js.Stream(ctx, name); err == nil {
		ts.dlqStreams[mailboxID] = st
		return nil
	}
	st, err := d.js.CreateStream(ctx, dlqStreamConfig(tenantID, mailboxID))
	if err != nil {
		return fmt.Errorf("reconcile DLQ stream for mailbox %s: %w", mailboxID, err)
	}
	ts.dlqStreams[mailboxID] = st
	log.Printf("nats: recreated missing DLQ stream %s", name)
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

	dlqStream, err := d.js.CreateStream(ctx, dlqStreamConfig(spec.TenantID, spec.MailboxID))
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

	maxDeliver := runawayBackstopDeliver(spec.MaxDeliver)

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

// runawayBackstopDeliver is the NATS MaxDeliver policy: PG
// RetryPolicy.MaxAttempts is the single delivery-lifetime authority (the
// lease scanner decides retry vs dead-letter), so NATS MaxDeliver is only a
// runaway-redelivery backstop for crash loops and must sit well above the
// mailbox's business retry budget — a small mailbox MaxDeliver used to
// terminate deliveries in JetStream while PG was still legitimately
// retrying.
func runawayBackstopDeliver(mailboxMaxDeliver int) int {
	if mailboxMaxDeliver <= 0 {
		mailboxMaxDeliver = 5
	}
	if mailboxMaxDeliver < 100 {
		return 100
	}
	return mailboxMaxDeliver
}

// ReconcileConsumer applies the CURRENT spec to an existing consumer.
// EnsureConsumer deliberately no-ops when its in-process cache hits, so
// mailbox config changes (ack_wait / max_deliver / max_ack_pending) never
// reached NATS — PG and the broker drifted apart. UpdateConfig calls this
// after persisting to PG.
func (d *Driver) ReconcileConsumer(ctx context.Context, spec core.ConsumerSpec) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	ts, ok := d.tenant[spec.TenantID]
	if !ok {
		return fmt.Errorf("tenant %s not initialized", spec.TenantID)
	}
	cname := consumerName(spec.TenantID, spec.MailboxID)

	maxAckPending := spec.MaxACKPending
	if maxAckPending <= 0 {
		maxAckPending = 100
	}
	ackWait := time.Duration(spec.ACKWaitSeconds) * time.Second
	if ackWait <= 0 {
		ackWait = 300 * time.Second
	}
	maxDeliver := runawayBackstopDeliver(spec.MaxDeliver)

	// The periodic reconcile calls this for EVERY mailbox; skipping the
	// broker write when the durable config already matches turns the pass
	// into read-only verification instead of N unconditional consumer
	// updates serialized on the driver mutex (write amplification).
	if cons, ok := ts.consumers[cname]; ok {
		if info, err := cons.Info(ctx); err == nil && consumerConfigMatches(info.Config, cname, spec.TenantID, spec.MailboxID, ackWait, maxDeliver, maxAckPending) {
			return nil
		}
	}

	// Bound the broker write: a hung call would otherwise hold this
	// goroutine (and any advisory lock the caller took) indefinitely.
	writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cons, err := d.js.CreateOrUpdateConsumer(writeCtx, streamName(spec.TenantID, "TASKS"), jetstream.ConsumerConfig{
		Durable:        cname,
		FilterSubjects: []string{taskSubject(spec.TenantID, spec.MailboxID)},
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        ackWait,
		MaxDeliver:     maxDeliver,
		MaxAckPending:  maxAckPending,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
	})
	if err != nil {
		return fmt.Errorf("reconcile consumer %s: %w", cname, err)
	}
	ts.consumers[cname] = cons
	return nil
}

func consumerConfigMatches(cfg jetstream.ConsumerConfig, cname, tenantID, mailboxID string, ackWait time.Duration, maxDeliver, maxAckPending int) bool {
	return cfg.Durable == cname &&
		cfg.AckWait == ackWait &&
		cfg.MaxDeliver == maxDeliver &&
		cfg.MaxAckPending == maxAckPending &&
		len(cfg.FilterSubjects) == 1 &&
		cfg.FilterSubjects[0] == taskSubject(tenantID, mailboxID)
}

func (d *Driver) Close() error {
	d.nc.Close()
	return nil
}

func (d *Driver) Conn() *nats.Conn {
	return d.nc
}

func isTerminalEvent(evt core.JanusEvent) bool {
	switch evt.EventType {
	case core.EventTaskCompleted, core.EventTaskFailed,
		core.EventTaskCancelled, core.EventTaskDeadLettered, core.EventTaskExpired:
		return true
	}
	return false
}

func (d *Driver) SubscribeEvents(ctx context.Context, ch chan<- core.JanusEvent) (*nats.Subscription, error) {
	sub, err := d.nc.Subscribe("janus.*.events.>", func(msg *nats.Msg) {
		var event core.JanusEvent
		if err := json.Unmarshal(msg.Data, &event); err != nil {
			return
		}
		if isTerminalEvent(event) {
			select {
			case ch <- event:
			case <-time.After(5 * time.Second):
				// Not an error return (NATS callback), but make the loss
				// visible: error log + counter so alerting can catch it.
				log.Printf("[ERROR] nats driver: terminal event %s (%s) hand-off timed out — event LOST to in-memory subscribers; task state remains durable, clients recover via GetTask", event.EventID, event.EventType)
			}
		} else {
			select {
			case ch <- event:
			default:
			}
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
	if len(d.pending) >= maxPendingEntries {
		for k, m := range d.pending {
			if m != nil {
				_ = m.Nak()
			}
			delete(d.pending, k)
			if len(d.pending) < maxPendingEntries/2 {
				break
			}
		}
	}
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
	msgs      <-chan jetstream.Msg
	js        jetstream.JetStream
	stream    string
	name      string
	closeOnce sync.Once
}

func (it *eventIterator) Next(ctx context.Context) (*core.JanusEvent, error) {
	select {
	case msg, ok := <-it.msgs:
		if !ok {
			// Channel closed: batch exhausted. Release the consumer so the
			// ephemeral replay consumer does not linger on the stream.
			_ = it.Close()
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
	it.closeOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = it.js.DeleteConsumer(ctx, it.stream, it.name)
	})
	return nil
}
