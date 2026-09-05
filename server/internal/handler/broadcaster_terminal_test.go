package handler

import (
	"testing"
	"time"

	"github.com/agentium-lab/Janus/core"
)

func TestFanoutBroadcaster_TerminalEventNotDroppedWhenFull(t *testing.T) {
	// Fill the subscriber channel (64 buffer) with non-terminal events, then
	// verify a terminal event still gets through: dropping it would strand
	// SSE subscribers forever.
	inbound := make(chan core.JanusEvent, 1)
	b := NewFanoutBroadcaster(inbound)
	ch := b.Subscribe("acme")
	for i := 0; i < 80; i++ {
		b.Publish(core.JanusEvent{TenantID: "acme", TaskID: "t1", EventType: core.EventTaskProgress})
	}
	delivered := make(chan struct{})
	go func() {
		b.Publish(core.JanusEvent{TenantID: "acme", TaskID: "t1", EventType: core.EventTaskCompleted, EventID: "term-1"})
		close(delivered)
	}()
	sawTerminal := false
	timeout := time.After(3 * time.Second)
	for !sawTerminal {
		select {
		case evt := <-ch:
			if evt.EventType == core.EventTaskCompleted {
				sawTerminal = true
			}
		case <-timeout:
			t.Fatal("terminal event dropped: subscriber would hang forever")
		}
	}
	<-delivered
	b.Unsubscribe("acme", ch)
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
