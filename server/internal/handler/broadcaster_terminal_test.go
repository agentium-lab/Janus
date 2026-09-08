package handler

import (
	"fmt"
	"testing"
	"time"

	"github.com/agentium-lab/Janus/core"
)

// Terminal semantics after F42: a subscriber with a full queue is closed and
// evicted IMMEDIATELY (no 5s wait) — its stream ends and the client recovers
// via GetTask. Delivery-when-full was replaced by eviction-when-full to
// bound fan-out time independently of slow-subscriber count.
func TestFanoutBroadcaster_TerminalEvictsFullSubscriberImmediately(t *testing.T) {
	inbound := make(chan core.JanusEvent, 256)
	b := NewFanoutBroadcaster(inbound)
	ch := b.Subscribe("acme")
	for i := 0; i < 64; i++ {
		b.inbound <- core.JanusEvent{TenantID: "acme", TaskID: "t1", EventType: core.EventTaskProgress, EventID: fmt.Sprintf("f-%d", i)}
	}
	deadline := time.Now().Add(5 * time.Second)
	for b.pendingInbound() > 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // let the run loop finish fanning fills out

	// The subscriber never reads: its queue stays full so the terminal event
	// cannot fit and the subscriber must be evicted. Publishing then waiting
	// for the run loop to process it BEFORE draining proves the eviction.
	b.inbound <- core.JanusEvent{TenantID: "acme", TaskID: "t1", EventType: core.EventTaskCompleted, EventID: "term-1"}
	time.Sleep(200 * time.Millisecond)

	closed := false
	drained := 0
	for !closed {
		select {
		case _, ok := <-ch:
			if !ok {
				closed = true
			} else {
				drained++
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("full subscriber was not evicted (channel not closed) within 3s; drained=%d", drained)
		}
	}
	if drained != 64 {
		t.Fatalf("expected the 64 buffered fills then close, drained=%d", drained)
	}
}

func TestFanoutBroadcaster_NonTerminalStillDropsWhenFull(t *testing.T) {
	// Backpressure for non-terminal events must not block the publisher.
	b := &FanoutBroadcaster{fans: map[string][]chan core.JanusEvent{}, inbound: make(chan core.JanusEvent, 1)}
	b.inbound <- core.JanusEvent{EventType: core.EventTaskProgress}
	done := make(chan struct{})
	go func() {
		b.Publish(core.JanusEvent{EventType: core.EventTaskProgress})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("non-terminal publish blocked on full pipeline")
	}
	<-b.inbound
}

// FAN-OUT TIME BOUND (seventh review): one terminal event broadcast to N
// subscribers with FULL queues must complete in bounded time — eviction is
// immediate, never 5s x N. The global loop must stay free for later events.
func TestFanoutBroadcaster_TerminalFanoutTimeBounded(t *testing.T) {
	for _, n := range []int{10, 100, 1000} {
		t.Run(fmt.Sprintf("subscribers_%d", n), func(t *testing.T) {
			inbound := make(chan core.JanusEvent, 4)
			b := NewFanoutBroadcaster(inbound)
			subs := make([]<-chan core.JanusEvent, 0, n)
			for i := 0; i < n; i++ {
				ch := b.Subscribe("acme")
				subs = append(subs, ch)
				// fill each subscriber queue so the terminal event cannot fit
				for j := 0; j < 64; j++ {
					b.inbound <- core.JanusEvent{TenantID: "acme", EventType: core.EventTaskProgress, EventID: fmt.Sprintf("fill-%d-%d", i, j)}
				}
			}
			// let the pump drain fills into the subscriber queues
			deadline := time.Now().Add(5 * time.Second)
			for b.pendingInbound() > 0 && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			// pendingInbound()==0 only means the run loop TOOK the events;
			// give it time to finish fanning them out before publishing.
			time.Sleep(100 * time.Millisecond)

			start := time.Now()
			b.inbound <- core.JanusEvent{TenantID: "acme", EventType: core.EventTaskCompleted, EventID: "term"}

			// STRONG assertion (eighth review): the registry must empty —
			// every full subscriber evicted. Poll fans directly: draining
			// subscriber queues would free capacity and change the outcome.
			fansCleared := time.After(5 * time.Second)
			for {
				b.mu.Lock()
				remaining := len(b.fans["acme"])
				b.mu.Unlock()
				if remaining == 0 {
					break
				}
				select {
				case <-fansCleared:
					t.Fatalf("%d/%d subscribers still registered after terminal event", remaining, n)
				default:
					time.Sleep(5 * time.Millisecond)
				}
			}
			// eviction closed every channel: reads must observe the close
			// after the buffered fills drain out.
			for i, ch := range subs {
				if !drainUntilClosed(ch, 5*time.Second) {
					t.Fatalf("subscriber %d channel was not closed by eviction (n=%d)", i, n)
				}
			}
			if elapsed := time.Since(start); elapsed > 2*time.Second {
				t.Fatalf("terminal fan-out to %d full subscribers took %v; must be immediate", n, elapsed)
			}
		})
	}
}

func drainUntilClosed(ch <-chan core.JanusEvent, timeout time.Duration) bool {
	deadline := time.After(timeout)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return true
			}
		case <-deadline:
			return false
		}
	}
}
