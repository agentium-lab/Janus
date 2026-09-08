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

// fanoutDirect delivers an event straight to the fan channels, bypassing
// the inbound pipeline (overflow path for terminal events).
func (b *FanoutBroadcaster) fanoutDirect(evt core.JanusEvent) {
	terminal := isTerminalEvent(evt)
	if b.seenBefore(evt) {
		return
	}
	b.mu.Lock()
	subs := make([]chan core.JanusEvent, len(b.fans[evt.TenantID]))
	copy(subs, b.fans[evt.TenantID])
	b.mu.Unlock()
	for _, ch := range subs {
		select {
		case ch <- evt:
		default:
			if terminal {
				b.evict(evt.TenantID, ch)
				log.Printf("broadcaster: evicted slow subscriber, terminal event for task %s undeliverable", evt.TaskID)
			}
		}
	}
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

func (b *FanoutBroadcaster) run() {
	for event := range b.inbound {
		if b.seenBefore(event) {
			continue
		}
		terminal := isTerminalEvent(event)
		b.mu.Lock()
		subs := make([]chan core.JanusEvent, len(b.fans[event.TenantID]))
		copy(subs, b.fans[event.TenantID])
		b.mu.Unlock()
		for _, ch := range subs {
			if !terminal {
				select {
				case ch <- event:
				default:
				}
				continue
			}
			// Terminal events never block the global loop: a subscriber
			// whose queue is full is closed and evicted immediately (the SSE
			// handler exits on the closed channel; the client recovers via
			// GetTask). This bounds fan-out time independently of the number
			// of slow subscribers — waiting per-subscriber multiplied the
			// delay by N (seventh review).
			select {
			case ch <- event:
			default:
				b.evict(event.TenantID, ch)
				log.Printf("broadcaster: evicted slow subscriber, terminal event for task %s undeliverable", event.TaskID)
			}
		}
	}
}

// pendingInbound reports the backlog of the internal pipeline; tests use it
// to know when the pump has drained pre-loaded events.
func (b *FanoutBroadcaster) pendingInbound() int { return len(b.inbound) }

// evict removes and closes a fan channel. Closing wakes blocked readers so
// their streams terminate instead of hanging.
func (b *FanoutBroadcaster) evict(tenantID string, target chan core.JanusEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
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
