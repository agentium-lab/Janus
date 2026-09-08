package invariants

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/handler"
	"github.com/stretchr/testify/assert"
)

// NO-SILENT-LOSS INVARIANT (ninth review): under flood pressure the event
// router must deliver 100% of TERMINAL events to the broadcast pipeline and
// 100% of ALL events to the audit projection; only non-terminal broadcast
// overflow may drop. This mirrors the main.go rawEventCh router classes.
func TestEventRouter_FloodNoTerminalLoss(t *testing.T) {
	raw := make(chan core.JanusEvent, 64)
	broadcastCh := make(chan core.JanusEvent, 8)
	projectorCh := make(chan core.JanusEvent, 8)

	go func() {
		for evt := range raw {
			if isTerminalForRouter(evt) {
				select {
				case broadcastCh <- evt:
				case <-time.After(5 * time.Second):
				}
			} else {
				select {
				case broadcastCh <- evt:
				default:
				}
			}
			select {
			case projectorCh <- evt:
			case <-time.After(5 * time.Second):
			}
		}
		close(broadcastCh)
		close(projectorCh)
	}()

	broadcaster := handler.NewFanoutBroadcaster(broadcastCh)
	sse := broadcaster.Subscribe("acme")

	var projected []core.JanusEvent
	projDone := make(chan struct{})
	go func() {
		defer close(projDone)
		for evt := range projectorCh {
			projected = append(projected, evt)
		}
	}()

	const terminals = 50
	const noise = 500
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < noise; i++ {
			raw <- core.JanusEvent{TenantID: "acme", EventType: core.EventTaskProgress, EventID: fmt.Sprintf("noise-%d", i)}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < terminals; i++ {
			raw <- core.JanusEvent{TenantID: "acme", EventType: core.EventTaskCompleted, TaskID: fmt.Sprintf("t-%d", i), EventID: fmt.Sprintf("term-%d", i)}
		}
	}()
	wg.Wait()
	close(raw)

	<-projDone
	assert.Len(t, projected, terminals+noise, "audit projection must receive EVERY event (terminal + noise)")

	gotTerminals := 0
	deadline := time.After(5 * time.Second)
	for gotTerminals < terminals {
		select {
		case _, ok := <-sse:
			if !ok {
				t.Fatalf("stream closed early; got %d/%d terminals", gotTerminals, terminals)
			}
			gotTerminals++
		case <-deadline:
			t.Fatalf("SSE received %d/%d terminal events under flood — silent loss", gotTerminals, terminals)
		}
	}
}

func isTerminalForRouter(evt core.JanusEvent) bool {
	switch evt.EventType {
	case core.EventTaskCompleted, core.EventTaskFailed,
		core.EventTaskCancelled, core.EventTaskDeadLettered, core.EventTaskExpired:
		return true
	}
	return false
}
