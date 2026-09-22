package grpc

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type gatewayMetadataCapture struct {
	gotAPIKey string
	gotAuth   string
	called    bool
}

// TestGateway_ForwardsAuthHeaders drives a real HTTP request through the
// grpc-gateway mux into a real gRPC server and asserts the identity headers
// survive as metadata. Regression test for the missing
// WithIncomingHeaderMatcher: X-API-Key is DROPPED by grpc-gateway's
// DefaultHeaderMatcher, so SDK clients authenticating with it got
// "missing api key" from the interceptor even though outer HTTP auth passed.
func TestGateway_ForwardsAuthHeaders(t *testing.T) {
	capture := &gatewayMetadataCapture{}

	gs := grpc.NewServer(
		grpc.UnknownServiceHandler(func(srv interface{}, stream grpc.ServerStream) error {
			capture.called = true
			if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
				if v := md.Get("x-api-key"); len(v) > 0 {
					capture.gotAPIKey = v[0]
				}
				if v := md.Get("authorization"); len(v) > 0 {
					capture.gotAuth = v[0]
				}
			}
			return status.Error(codes.NotFound, "capture")
		}),
	)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go gs.Serve(ln)
	defer gs.Stop()

	gw, err := RegisterGateway(context.Background(), ln.Addr().String(), nil)
	if err != nil {
		t.Fatalf("register gateway: %v", err)
	}
	ts := httptest.NewServer(gw)
	defer ts.Close()

	// Any /<pkg>.<Service>/<Method> path routes through the gateway into the
	// gRPC dial target; the unknown-service handler captures the metadata.
	// The HTTP status is irrelevant (no real handler is registered).

	resp := postGateway(t, ts.URL+"/v1/tenants/acme/agents", map[string]string{"X-API-Key": "janus_live_key_123"})
	resp.Body.Close()
	if !capture.called {
		t.Fatal("request never reached the gRPC server")
	}
	if capture.gotAPIKey != "janus_live_key_123" {
		t.Fatalf("x-api-key metadata not forwarded through gateway: got %q", capture.gotAPIKey)
	}

	capture.gotAPIKey, capture.gotAuth, capture.called = "", "", false
	resp = postGateway(t, ts.URL+"/v1/tenants/acme/agents", map[string]string{"Authorization": "Bearer janus_bearer_456"})
	resp.Body.Close()
	if !capture.called {
		t.Fatal("second request never reached the gRPC server")
	}
	if !strings.HasPrefix(capture.gotAuth, "Bearer janus_bearer_456") {
		t.Fatalf("authorization metadata not forwarded through gateway: got %q", capture.gotAuth)
	}
}

func postGateway(t *testing.T, url string, headers map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

func TestHeaderMatcher_PassthroughSet(t *testing.T) {
	for _, h := range []string{"X-API-Key", "x-api-key", "X-Api-Key"} {
		mapped, ok := headerMatcher(h)
		if !ok || mapped != "x-api-key" {
			t.Fatalf("header %q must map to x-api-key, got (%q, %v)", h, mapped, ok)
		}
	}
	for _, h := range []string{"Authorization", "authorization"} {
		mapped, ok := headerMatcher(h)
		if !ok || mapped != "authorization" {
			t.Fatalf("header %q must map to authorization, got (%q, %v)", h, mapped, ok)
		}
	}
	if _, ok := headerMatcher("X-Custom-Unrelated"); ok {
		t.Fatal("unrelated custom headers should follow the default matcher")
	}
}
