package handler

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/agentium-lab/Janus/core"
)

func newIdleBroadcaster() *FanoutBroadcaster {
	return &FanoutBroadcaster{
		fans:       make(map[string][]chan core.JanusEvent),
		inbound:    make(chan core.JanusEvent, 256),
		dedupeSeen: make(map[string]struct{}),
		dedupeRing: make([]string, 1024),
	}
}

func TestFanoutDirect_TerminalOverflowReachesSubscriber(t *testing.T) {
	b := newIdleBroadcaster()
	for i := 0; i < 256; i++ {
		b.inbound <- core.JanusEvent{TenantID: "acme", EventType: core.EventTaskProgress, EventID: fmt.Sprintf("fill-%d", i)}
	}
	sub := b.Subscribe("acme")

	terminal := core.JanusEvent{TenantID: "acme", TaskID: "t1", EventType: core.EventTaskCompleted, EventID: "evt-term"}
	b.Publish(terminal)
	select {
	case got := <-sub:
		assert.Equal(t, core.EventTaskCompleted, got.EventType)
		assert.Equal(t, "t1", got.TaskID)
	default:
		t.Fatal("terminal event must reach the subscriber via the overflow path")
	}
	assert.Len(t, b.inbound, 256, "overflow bypasses the full pipeline")

	b.Publish(terminal)
	select {
	case got := <-sub:
		t.Fatalf("duplicate terminal event must be deduped, got %v", got.EventType)
	default:
	}

	b.Publish(core.JanusEvent{TenantID: "acme", EventType: core.EventTaskProgress, EventID: "evt-prog"})
	select {
	case got := <-sub:
		t.Fatalf("non-terminal overflow must be dropped, got %v", got.EventType)
	default:
	}
}
