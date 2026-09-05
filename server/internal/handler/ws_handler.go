package handler

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/auth"
	"github.com/gorilla/websocket"
)

// wsAllowedLoopbackHosts lists loopback hosts permitted to originate
// WebSocket connections for dev convenience. Comparison is exact, never a
// substring match, to prevent CSWSH bypass (e.g. "evil.example.com" must
// not match an "example.com" check).
var wsAllowedLoopbackHosts = map[string]bool{
	"localhost": true,
	"127.0.0.1": true,
	"::1":       true,
	"[::1]":     true,
}

// originHostMatchesRequest accepts an Origin only when its host exactly
// matches the request Host (port included) or is a loopback variant.
// Empty Origin is rejected: browsers always send the header on WS handshakes,
// so its absence signals a non-browser or spoofed client.
func originHostMatchesRequest(origin string, requestHost string) bool {
	if origin == "" {
		return false
	}
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" {
		return false
	}
	if u.Host == requestHost {
		return true
	}
	if wsAllowedLoopbackHosts[u.Hostname()] || wsAllowedLoopbackHosts[u.Host] {
		return true
	}
	if isLoopbackHost(requestHost) && isLoopbackHost(u.Host) {
		return true
	}
	return false
}

func isLoopbackHost(hostport string) bool {
	host := hostport
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	return wsAllowedLoopbackHosts[host] || net.ParseIP(host).IsLoopback()
}

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return originHostMatchesRequest(r.Header.Get("Origin"), r.Host)
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
	conn      *websocket.Conn
	tenantID  string
	events    <-chan core.JanusEvent
	closed    chan struct{}
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

	tenantID := auth.TenantFromContext(r.Context())
	if tenantID == "" {
		conn.Close()
		return
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

const (
	wsWriteTimeout = 10 * time.Second
	wsPingInterval = 30 * time.Second
)

func (h *WebSocketHandler) writePump(c *wsConn) {
	defer h.cleanup(c)

	ticker := time.NewTicker(wsPingInterval)
	defer ticker.Stop()
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
			_ = c.conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
			if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
				return
			}
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case <-c.closed:
			return
		}
	}
}

func (h *WebSocketHandler) readPump(c *wsConn) {
	defer h.cleanup(c)
	// Set a read deadline that the peer's pong messages (in response to our
	// pings) refresh via SetPongHandler. If the peer goes away, ReadMessage
	// returns a deadline-exceeded error and we clean up.
	_ = c.conn.SetReadDeadline(time.Now().Add(wsPingInterval * 3))
	c.conn.SetPongHandler(func(string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(wsPingInterval * 3))
		return nil
	})
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
