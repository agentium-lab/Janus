package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/agentium-lab/Janus/core"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFanoutBroadcaster_SubscribeUnsubscribe(t *testing.T) {
	inbound := make(chan core.JanusEvent, 16)
	b := NewFanoutBroadcaster(inbound)

	ch := b.Subscribe("tenant-1")
	require.NotNil(t, ch)

	event := core.JanusEvent{
		TenantID:  "tenant-1",
		EventType: core.EventTaskCreated,
		TaskID:    "task-1",
	}
	inbound <- event

	select {
	case got := <-ch:
		assert.Equal(t, "task-1", got.TaskID)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for event")
	}

	b.Unsubscribe("tenant-1", ch)
}

func TestFanoutBroadcaster_MultipleSubscribers(t *testing.T) {
	inbound := make(chan core.JanusEvent, 16)
	b := NewFanoutBroadcaster(inbound)

	ch1 := b.Subscribe("t1")
	ch2 := b.Subscribe("t1")

	event := core.JanusEvent{TenantID: "t1", EventType: core.EventTaskStarted, TaskID: "t2"}
	inbound <- event

	assert.Equal(t, "t2", (<-ch1).TaskID)
	assert.Equal(t, "t2", (<-ch2).TaskID)

	b.Unsubscribe("t1", ch1)
	b.Unsubscribe("t1", ch2)
}

func TestFanoutBroadcaster_DifferentTenants(t *testing.T) {
	inbound := make(chan core.JanusEvent, 16)
	b := NewFanoutBroadcaster(inbound)

	chA := b.Subscribe("tenant-a")
	chB := b.Subscribe("tenant-b")

	inbound <- core.JanusEvent{TenantID: "tenant-a", TaskID: "t1"}
	inbound <- core.JanusEvent{TenantID: "tenant-b", TaskID: "t2"}

	assert.Equal(t, "t1", (<-chA).TaskID)
	assert.Equal(t, "t2", (<-chB).TaskID)

	select {
	case <-chA:
		t.Fatal("tenant-a should not receive tenant-b events")
	default:
	}

	b.Unsubscribe("tenant-a", chA)
	b.Unsubscribe("tenant-b", chB)
}

func TestWebSocketHandler_BasicConnection(t *testing.T) {
	inbound := make(chan core.JanusEvent, 16)
	b := NewFanoutBroadcaster(inbound)
	h := NewWebSocketHandler(b)

	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?tenant=test"
	ws, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err)
	defer ws.Close()
	if resp != nil {
		resp.Body.Close()
	}

	inbound <- core.JanusEvent{
		TenantID:  "test",
		EventType: core.EventTaskCreated,
		TaskID:    "ws-task-1",
	}

	ws.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, msg, err := ws.ReadMessage()
	require.NoError(t, err)

	var event core.JanusEvent
	require.NoError(t, json.Unmarshal(msg, &event))
	assert.Equal(t, "ws-task-1", event.TaskID)
	assert.Equal(t, core.EventTaskCreated, event.EventType)
}

func TestWebSocketHandler_NoTenant(t *testing.T) {
	inbound := make(chan core.JanusEvent, 16)
	b := NewFanoutBroadcaster(inbound)
	h := NewWebSocketHandler(b)

	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	ws, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err)
	defer ws.Close()
	if resp != nil {
		resp.Body.Close()
	}

	inbound <- core.JanusEvent{
		TenantID:  "default",
		EventType: core.EventTaskCompleted,
		TaskID:    "ws-task-2",
	}

	ws.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, msg, err := ws.ReadMessage()
	require.NoError(t, err)

	var event core.JanusEvent
	require.NoError(t, json.Unmarshal(msg, &event))
	assert.Equal(t, "ws-task-2", event.TaskID)
}

func TestWebSocketHandler_ClientDisconnect(t *testing.T) {
	inbound := make(chan core.JanusEvent, 16)
	b := NewFanoutBroadcaster(inbound)
	h := NewWebSocketHandler(b)

	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?tenant=test"
	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err)

	ws.Close()
	time.Sleep(100 * time.Millisecond)

	inbound <- core.JanusEvent{TenantID: "test", TaskID: "after-close"}

	b.mu.Lock()
	subs := b.fans["test"]
	b.mu.Unlock()
	assert.Empty(t, subs, "subscriber should be removed after disconnect")
}

func TestWebSocketHandler_UpgradeFailure(t *testing.T) {
	inbound := make(chan core.JanusEvent, 16)
	b := NewFanoutBroadcaster(inbound)
	h := NewWebSocketHandler(b)

	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "?tenant=test")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.NotEqual(t, http.StatusSwitchingProtocols, resp.StatusCode)
}

func TestFanoutBroadcaster_BufferFull(t *testing.T) {
	inbound := make(chan core.JanusEvent, 16)
	b := NewFanoutBroadcaster(inbound)

	ch := b.Subscribe("t1")

	for i := 0; i < 100; i++ {
		inbound <- core.JanusEvent{TenantID: "t1", TaskID: "t"}
	}

	received := 0
	timeout := time.After(500 * time.Millisecond)
	for {
		select {
		case <-ch:
			received++
		case <-timeout:
			goto done
		}
	}
done:
	assert.Positive(t, received, "should receive some events even when buffer fills")
	b.Unsubscribe("t1", ch)
}
