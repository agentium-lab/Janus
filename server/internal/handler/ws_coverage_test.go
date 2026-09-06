package handler

import (
	"context"
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

func TestWebSocketHandler_PongAndClientMessageKeepConnectionAlive(t *testing.T) {
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

	require.NoError(t, ws.WriteControl(websocket.PongMessage, nil, time.Now().Add(time.Second)))
	require.NoError(t, ws.WriteMessage(websocket.TextMessage, []byte(`{"hello":true}`)))

	wsWaitSubscribed(t, ws, inbound, "test")
	inbound <- core.JanusEvent{TenantID: "test", TaskID: "still-alive"}
	ws.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, msg, err := ws.ReadMessage()
	require.NoError(t, err)
	assert.Contains(t, string(msg), "still-alive")
}

func TestWebSocketHandler_WritePumpExitsOnClientGone(t *testing.T) {
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
	if resp != nil {
		resp.Body.Close()
	}

	ws.Close()

	var big [1 << 16]byte
	for i := 0; i < 64; i++ {
		inbound <- core.JanusEvent{TenantID: "test", TaskID: "t", Payload: big[:]}
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		b.mu.Lock()
		subs := len(b.fans["test"])
		b.mu.Unlock()
		if subs == 0 {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("subscriber not cleaned up after client close")
}

func TestWebSocketHandler_TenantFromContextEmpty(t *testing.T) {
	inbound := make(chan core.JanusEvent, 16)
	b := NewFanoutBroadcaster(inbound)
	h := NewWebSocketHandler(b)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(context.Background())
		h.ServeHTTP(w, r)
	}))
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

	_, _, err = ws.ReadMessage()
	require.Error(t, err, "server should close immediately when tenant is missing")
}

func TestWebSocketHandler_EventJSONRoundTrip(t *testing.T) {
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

	evt := core.JanusEvent{
		TenantID:    "test",
		EventType:   core.EventTaskProgress,
		TaskID:      "roundtrip-1",
		EventID:     "e-42",
		SourceAgent: "agent-1",
	}
	inbound <- evt

	ws.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, msg, err := ws.ReadMessage()
	require.NoError(t, err)

	var got core.JanusEvent
	require.NoError(t, json.Unmarshal(msg, &got))
	assert.Equal(t, evt, got)
}
