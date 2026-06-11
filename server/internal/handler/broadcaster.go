package handler

import (
	"sync"

	"github.com/agentium-lab/Janus/core"
)

type FanoutBroadcaster struct {
	mu      sync.Mutex
	fans    map[string][]chan core.JanusEvent
	inbound <-chan core.JanusEvent
}

func NewFanoutBroadcaster(inbound <-chan core.JanusEvent) *FanoutBroadcaster {
	b := &FanoutBroadcaster{
		fans:    make(map[string][]chan core.JanusEvent),
		inbound: inbound,
	}
	go b.run()
	return b
}

func (b *FanoutBroadcaster) run() {
	for event := range b.inbound {
		b.mu.Lock()
		subs := b.fans[event.TenantID]
		for _, ch := range subs {
			select {
			case ch <- event:
			default:
			}
		}
		b.mu.Unlock()
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
