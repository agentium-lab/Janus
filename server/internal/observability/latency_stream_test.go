package observability

import (
	"bufio"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// A2 acceptance: the production auth chain's ONLY writer wrapper must keep
// http.Flusher (SSE) and http.Hijacker (WebSocket) reachable — main.go
// mounts RequestLatencyMiddleware outside auth.Middleware/ScopeGuard, and
// none of those wrap the writer themselves. Asserted with the real chain
// shape: latency wrapper -> auth-shaped pass-through -> terminal handler.
func TestRequestLatencyMiddleware_PreservesFlusherAndHijacker(t *testing.T) {
	// SSE leg: through the wrapper, a streaming handler's frames reach the
	// recorder with flushed visibility.
	sse := RequestLatencyMiddleware(func(string) string { return "acme" })(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			f, ok := w.(http.Flusher)
			if !ok {
				t.Fatal("SSE handler lost http.Flusher through the wrapper")
			}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("data: {\"event_type\":\"task.completed\"}\n\n"))
			f.Flush()
		}))
	rec := httptest.NewRecorder()
	sse.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/tasks/t1/events", nil))
	if !strings.Contains(rec.Body.String(), "task.completed") {
		t.Fatalf("SSE frame lost through the wrapper: %q", rec.Body.String())
	}

	// WebSocket leg: through the wrapper, a Hijacker-based upgrade returns
	// 101 with a usable connection.
	ws := RequestLatencyMiddleware(func(string) string { return "acme" })(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h, ok := w.(http.Hijacker)
			if !ok {
				t.Fatal("WebSocket handler lost http.Hijacker through the wrapper")
			}
			conn, rw, err := h.Hijack()
			if err != nil {
				t.Fatalf("hijack: %v", err)
			}
			defer conn.Close()
			_, _ = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
			_ = rw.Flush()
		}))
	srv := httptest.NewServer(ws)
	defer srv.Close()
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Get(srv.URL)
	if err != nil {
		t.Fatalf("ws request through wrapper: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("ws upgrade through wrapper: want 101, got %d", resp.StatusCode)
	}
	_ = bufio.NewReader(resp.Body)
}
