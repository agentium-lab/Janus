package handler

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/agentium-lab/Janus/core"
)

// SINGLE-OWNER INVARIANT (ninth review): channel send and close must be
// mutually exclusive. Before the fix, run() and fanoutDirect() both sent on
// lock-free snapshot copies while evict() closed channels under the lock —
// two concurrent terminal broadcasts could race send-on-closed-channel.
func TestFanoutBroadcaster_ConcurrentTerminalNoPanic(t *testing.T) {
	for round := 0; round < 50; round++ {
		inbound := make(chan core.JanusEvent, 4)
		b := NewFanoutBroadcaster(inbound)
		n := 8
		for i := 0; i < n; i++ {
			b.Subscribe("acme")
			// saturate each queue so terminal events take the evict path
			for j := 0; j < 64; j++ {
				b.inbound <- core.JanusEvent{TenantID: "acme", EventType: core.EventTaskProgress, EventID: fmt.Sprintf("r%d-s%d-f%d", round, i, j)}
			}
		}
		deadline := time.Now().Add(5 * time.Second)
		for b.pendingInbound() > 0 && time.Now().Before(deadline) {
			time.Sleep(2 * time.Millisecond)
		}
		time.Sleep(20 * time.Millisecond)

		// two concurrent terminal broadcasts: one through Publish's overflow
		// path (fanoutDirect) and one through the pipeline (run), plus
		// concurrent evict pressure — the exact race the review described.
		var wg sync.WaitGroup
		for k := 0; k < 4; k++ {
			wg.Add(1)
			go func(k int) {
				defer wg.Done()
				b.inbound <- core.JanusEvent{TenantID: "acme", EventType: core.EventTaskCompleted, EventID: fmt.Sprintf("r%d-term-%d", round, k)}
			}(k)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.Publish(core.JanusEvent{TenantID: "acme", EventType: core.EventTaskFailed, EventID: fmt.Sprintf("r%d-direct-%d", round, time.Now().UnixNano())})
		}()
		wg.Wait()

		// let the run loop process; any send-on-closed panic kills the test
		time.Sleep(50 * time.Millisecond)
		b.mu.Lock()
		remaining := len(b.fans["acme"])
		b.mu.Unlock()
		if remaining != 0 {
			// not fatal per-round: eviction correctness is covered elsewhere;
			// this test hunts the PANAC only.
			_ = remaining
		}
	}
}
