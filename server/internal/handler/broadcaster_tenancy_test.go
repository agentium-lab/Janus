package handler

import (
	"testing"
	"time"

	"github.com/agentium-lab/Janus/core"
)

func TestFanoutBroadcaster_SameEventIDDifferentTenantsBothDelivered(t *testing.T) {
	inbound := make(chan core.JanusEvent, 16)
	b := NewFanoutBroadcaster(inbound)
	subA := b.Subscribe("tenant-a")
	subB := b.Subscribe("tenant-b")
	go b.run()
	defer close(inbound)

	// Identical EventIDs across tenants must not cross-dedupe: the key is
	// (tenant, EventID), and a global key used to drop one of these.
	evt := core.JanusEvent{EventID: "evt_collide", EventType: core.EventTaskCreated, TenantID: "tenant-a", TaskID: "t1"}
	inbound <- evt
	inbound <- core.JanusEvent{EventID: "evt_collide", EventType: core.EventTaskCreated, TenantID: "tenant-b", TaskID: "t2"}

	timeout := time.After(2 * time.Second)
	var gotA, gotB bool
	for !(gotA && gotB) {
		select {
		case e := <-subA:
			if e.TenantID == "tenant-a" && e.EventID == "evt_collide" {
				gotA = true
			}
		case e := <-subB:
			if e.TenantID == "tenant-b" && e.EventID == "evt_collide" {
				gotB = true
			}
		case <-timeout:
			t.Fatalf("both tenants must receive their own event (a=%v b=%v)", gotA, gotB)
		}
	}
}
