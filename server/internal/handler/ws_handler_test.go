package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/auth"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func withWSAuth(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t := r.URL.Query().Get("tenant")
		if t == "" {
			t = "default"
		}
		r = r.WithContext(context.WithValue(r.Context(), auth.TenantCtxKey, t))
		h.ServeHTTP(w, r)
	})
}

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

// wsWaitSubscribed blocks until the handler has actually subscribed: the WS
// upgrade completes BEFORE ServeHTTP subscribes, so an event sent right after
// Dial can be fanned out to zero subscribers and lost. Spin warm-up events
// (unique EventIDs, so the dedupe window never eats them) until one
// round-trips — bounded, because slow CI runners widen the race window.
func wsWaitSubscribed(t *testing.T, ws *websocket.Conn, inbound chan core.JanusEvent, tenantID string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for i := 0; time.Now().Before(deadline); i++ {
		inbound <- core.JanusEvent{
			TenantID:  tenantID,
			EventType: core.EventTaskCreated,
			TaskID:    "warmup",
			EventID:   fmt.Sprintf("warmup-%d", i),
		}
		ws.SetReadDeadline(time.Now().Add(250 * time.Millisecond))
		if _, _, err := ws.ReadMessage(); err == nil {
			return
		}
	}
	t.Fatal("websocket subscription never became ready")
}

func TestWebSocketHandler_BasicConnection(t *testing.T) {
	inbound := make(chan core.JanusEvent, 16)
	b := NewFanoutBroadcaster(inbound)
	h := NewWebSocketHandler(b)

	srv := httptest.NewServer(withWSAuth(h))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?tenant=test"
	hdr := http.Header{}
	hdr.Set("Origin", srv.URL)
	ws, resp, err := websocket.DefaultDialer.Dial(wsURL, hdr)
	require.NoError(t, err)
	defer ws.Close()
	if resp != nil {
		resp.Body.Close()
	}

	wsWaitSubscribed(t, ws, inbound, "test")
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

	srv := httptest.NewServer(withWSAuth(h))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	hdr := http.Header{}
	hdr.Set("Origin", srv.URL)
	ws, resp, err := websocket.DefaultDialer.Dial(wsURL, hdr)
	require.NoError(t, err)
	defer ws.Close()
	if resp != nil {
		resp.Body.Close()
	}

	wsWaitSubscribed(t, ws, inbound, "default")
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

	srv := httptest.NewServer(withWSAuth(h))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?tenant=test"
	hdr := http.Header{}
	hdr.Set("Origin", srv.URL)
	ws, _, err := websocket.DefaultDialer.Dial(wsURL, hdr)
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

	srv := httptest.NewServer(withWSAuth(h))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "?tenant=test")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.NotEqual(t, http.StatusSwitchingProtocols, resp.StatusCode)
}

func TestOriginHostMatchesRequest(t *testing.T) {
	cases := []struct {
		name        string
		origin      string
		requestHost string
		want        bool
	}{
		{"empty origin rejected", "", "example.com", false},
		{"exact host match", "http://example.com", "example.com", true},
		{"exact host:port match", "http://example.com:8080", "example.com:8080", true},
		{"substring not bypassed", "http://evil.example.com", "example.com", false},
		{"substring not bypassed reverse", "http://example.com", "evil.example.com", false},
		{"localhost allowed", "http://localhost:3000", "api.example.com", true},
		{"127.0.0.1 allowed", "http://127.0.0.1:8080", "example.com", true},
		{"unrelated origin rejected", "http://attacker.com", "example.com", false},
		{"invalid origin url rejected", "://bad", "example.com", false},
		{"missing scheme rejected", "example.com", "example.com", false},
		{"loopback to loopback", "http://localhost:8080", "127.0.0.1:8080", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, originHostMatchesRequest(c.origin, c.requestHost))
		})
	}
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
