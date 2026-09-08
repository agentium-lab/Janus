package handler

import (
	"log"
	"sync"
	"time"

	"github.com/agentium-lab/Janus/core"
)

// terminalDeliveryWindow bounds how long a terminal event will wait for a
// slow subscriber before giving up (and logging). Terminal events must never
// be dropped silently: SSE streams hang forever if the client misses them.
const terminalDeliveryWindow = 5 * time.Second

func isTerminalEvent(evt core.JanusEvent) bool {
	switch evt.EventType {
	case core.EventTaskCompleted, core.EventTaskFailed,
		core.EventTaskCancelled, core.EventTaskDeadLettered, core.EventTaskExpired:
		return true
	}
	return false
}

type FanoutBroadcaster struct {
	mu      sync.Mutex
	fans    map[string][]chan core.JanusEvent
	inbound chan core.JanusEvent

	dedupeMu   sync.Mutex
	dedupeSeen map[string]struct{}
	dedupeRing []string
	dedupePos  int
}

func NewFanoutBroadcaster(inbound <-chan core.JanusEvent) *FanoutBroadcaster {
	b := &FanoutBroadcaster{
		fans:       make(map[string][]chan core.JanusEvent),
		inbound:    make(chan core.JanusEvent, 256),
		dedupeSeen: make(map[string]struct{}),
		dedupeRing: make([]string, 1024),
	}
	go func() {
		for event := range inbound {
			b.inbound <- event
		}
		close(b.inbound)
	}()
	go b.run()
	return b
}

// Publish pushes an event into the fanout pipeline (EventPublisher impl).
// Non-terminal events are dropped when the pipeline is full (backpressure);
// terminal events block up to terminalDeliveryWindow — dropping them would
// strand SSE subscribers.
func (b *FanoutBroadcaster) Publish(evt core.JanusEvent) {
	if !isTerminalEvent(evt) {
		select {
		case b.inbound <- evt:
		default:
		}
		return
	}
	// END-TO-END NO-LOSS: when the internal pipeline is full, bypass it —
	// deliver directly to subscribers (non-blocking; full ones are evicted).
	// A terminal event must never be silently dropped: SSE clients hang on
	// a missed terminal state.
	select {
	case b.inbound <- evt:
	default:
		b.fanoutDirect(evt)
	}
}

// fanoutDirect is the overflow path when the inbound pipeline is full:
// deliver under the same single critical section as run().
func (b *FanoutBroadcaster) fanoutDirect(evt core.JanusEvent) {
	if b.seenBefore(evt) {
		return
	}
	b.fanoutLocked(evt)
}

// seenBefore records the event ID and reports whether it was already seen.
// Events without an EventID always pass through (they cannot be deduped).
func (b *FanoutBroadcaster) seenBefore(evt core.JanusEvent) bool {
	if evt.EventID == "" {
		return false
	}
	b.dedupeMu.Lock()
	defer b.dedupeMu.Unlock()
	if _, dup := b.dedupeSeen[evt.EventID]; dup {
		return true
	}
	if old := b.dedupeRing[b.dedupePos]; old != "" {
		delete(b.dedupeSeen, old)
	}
	b.dedupeRing[b.dedupePos] = evt.EventID
	b.dedupeSeen[evt.EventID] = struct{}{}
	b.dedupePos = (b.dedupePos + 1) % len(b.dedupeRing)
	return false
}

// run owns the inbound pipeline. Channel sends AND closes happen inside
// one critical section so a concurrent evict (from fanoutDirect) can never
// close a channel this loop is about to send on — the ninth review's
// send-on-closed-channel race. Sends are non-blocking, so lock hold time
// stays negligible.
func (b *FanoutBroadcaster) run() {
	for event := range b.inbound {
		if b.seenBefore(event) {
			continue
		}
		b.fanoutLocked(event)
	}
}

// fanoutLocked delivers to the tenant's subscribers under mu. Terminal
// events evict full subscribers immediately (bounded fan-out, seventh
// review); non-terminal overflow drops (backpressure).
func (b *FanoutBroadcaster) fanoutLocked(event core.JanusEvent) {
	terminal := isTerminalEvent(event)
	b.mu.Lock()
	defer b.mu.Unlock()
	subs := b.fans[event.TenantID]
	// iterate over a copy so eviction can mutate the original slice safely
	for _, ch := range append([]chan core.JanusEvent(nil), subs...) {
		select {
		case ch <- event:
		default:
			if terminal {
				b.evictLocked(event.TenantID, ch)
				log.Printf("broadcaster: evicted slow subscriber, terminal event for task %s undeliverable", event.TaskID)
			}
		}
	}
}

// pendingInbound reports the backlog of the internal pipeline; tests use it
// to know when the pump has drained pre-loaded events.
func (b *FanoutBroadcaster) pendingInbound() int { return len(b.inbound) }

// evict removes and closes a fan channel. Closing wakes blocked readers so
// their streams terminate instead of hanging. Callers MUST hold b.mu.
func (b *FanoutBroadcaster) evictLocked(tenantID string, target chan core.JanusEvent) {
	subs := b.fans[tenantID]
	for i, ch := range subs {
		if ch == target {
			b.fans[tenantID] = append(subs[:i], subs[i+1:]...)
			if len(b.fans[tenantID]) == 0 {
				delete(b.fans, tenantID)
			}
			close(ch)
			return
		}
	}
}

func (b *FanoutBroadcaster) Subscribe(tenantID string) <-chan core.JanusEvent {
	ch := make(chan core.JanusEvent, 64)
	b.mu.Lock()
	b.fans[tenantID] = append(b.fans[tenantID], ch)
	b.mu.Unlock()
	return ch
}

func (b *FanoutBroadcaster) Unsubscribe(tenantID string, sub <-chan core.JanusEvent) {
	b.mu.Lock()
	subs := b.fans[tenantID]
	for i, ch := range subs {
		if ch == sub {
			b.fans[tenantID] = append(subs[:i], subs[i+1:]...)
			break
		}
	}
	if len(b.fans[tenantID]) == 0 {
		delete(b.fans, tenantID)
	}
	b.mu.Unlock()
}
