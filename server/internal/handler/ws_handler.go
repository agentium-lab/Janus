package handler

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"sync"

	"github.com/agentium-lab/Janus/core"
	"github.com/gorilla/websocket"
)

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		if origin == "" {
			return true
		}
		host := r.Host
		if host == "" {
			return false
		}
		return strings.Contains(origin, host)
	},
}

type EventBroadcaster interface {
	Subscribe(tenantID string) <-chan core.JanusEvent
	Unsubscribe(tenantID string, ch <-chan core.JanusEvent)
}

type WebSocketHandler struct {
	broadcaster EventBroadcaster
	mu          sync.Mutex
	connections map[*wsConn]struct{}
}

type wsConn struct {
	conn     *websocket.Conn
	tenantID string
	events   <-chan core.JanusEvent
	closed   chan struct{}
	closeOnce sync.Once
}

func NewWebSocketHandler(broadcaster EventBroadcaster) *WebSocketHandler {
	return &WebSocketHandler{
		broadcaster: broadcaster,
		connections: make(map[*wsConn]struct{}),
	}
}

func (h *WebSocketHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("ws upgrade: %v", err)
		return
	}

	tenantID := r.URL.Query().Get("tenant")
	if tenantID == "" {
		tenantID = "default"
	}

	events := h.broadcaster.Subscribe(tenantID)
	c := &wsConn{
		conn:     conn,
		tenantID: tenantID,
		events:   events,
		closed:   make(chan struct{}),
	}

	h.mu.Lock()
	h.connections[c] = struct{}{}
	h.mu.Unlock()

	go h.writePump(c)
	go h.readPump(c)
}

func (h *WebSocketHandler) writePump(c *wsConn) {
	defer h.cleanup(c)

	for {
		select {
		case event, ok := <-c.events:
			if !ok {
				return
			}
			data, err := json.Marshal(event)
			if err != nil {
				continue
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
				return
			}
		case <-c.closed:
			return
		}
	}
}

func (h *WebSocketHandler) readPump(c *wsConn) {
	defer h.cleanup(c)
	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
	}
}

func (h *WebSocketHandler) cleanup(c *wsConn) {
	c.closeOnce.Do(func() {
		close(c.closed)
	})
	h.broadcaster.Unsubscribe(c.tenantID, c.events)
	h.removeConn(c)
	c.conn.Close()
}

func (h *WebSocketHandler) removeConn(c *wsConn) {
	h.mu.Lock()
	delete(h.connections, c)
	h.mu.Unlock()
}
