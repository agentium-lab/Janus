package handler

import (
	"testing"
	"time"

	"github.com/agentium-lab/Janus/core"
	"github.com/stretchr/testify/assert"
)

// The v1.5.0 review found every progress event was delivered to SSE
// subscribers TWICE: once via the fast lane (handler Publish) and once via
// the outbox → queue → broadcaster loopback, with no shared EventID to
// dedupe on. The fix stamps one EventID in ReportProgress and dedupes here.

func TestFanoutBroadcaster_DedupesSameEventID(t *testing.T) {
	inbound := make(chan core.JanusEvent, 4)
	b := NewFanoutBroadcaster(inbound)
	ch := b.Subscribe("acme")

	evt := core.JanusEvent{EventID: "evt-dup-1", TenantID: "acme", TaskID: "t1", EventType: core.EventTaskProgress}
	b.Publish(evt)
	b.Publish(evt) // loopback redelivery of the same event
	b.Publish(core.JanusEvent{EventID: "evt-dup-2", TenantID: "acme", TaskID: "t1", EventType: core.EventTaskProgress})

	var got []string
	deadline := time.After(2 * time.Second)
	for len(got) < 2 {
		select {
		case e := <-ch:
			got = append(got, e.EventID)
		case <-deadline:
			t.Fatalf("timed out; got=%v", got)
		}
	}
	// Drain briefly to prove no third copy arrives.
	time.Sleep(100 * time.Millisecond)
	select {
	case extra := <-ch:
		t.Fatalf("duplicate delivery: %+v", extra)
	default:
	}
	assert.Equal(t, []string{"evt-dup-1", "evt-dup-2"}, got)
	b.Unsubscribe("acme", ch)
}

func TestFanoutBroadcaster_NoEventIDPassesThrough(t *testing.T) {
	// Events without EventID (legacy producers) must not be dropped.
	inbound := make(chan core.JanusEvent, 4)
	b := NewFanoutBroadcaster(inbound)
	ch := b.Subscribe("acme")

	b.Publish(core.JanusEvent{TenantID: "acme", TaskID: "t1", EventType: core.EventTaskProgress})
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("event without EventID must pass through")
	}
	b.Unsubscribe("acme", ch)
}
