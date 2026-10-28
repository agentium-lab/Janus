package handler

import (
	"net/http"
	"net/http/httptest"
	"strings"
)

// muxFor builds a ServeMux with one real route-table pattern registered,
// for tests that drive a handler from a goroutine (streaming SSE).
func muxFor(pattern string, h http.HandlerFunc) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle(pattern, h)
	return mux
}

// dispatchViaMux routes a request through a ServeMux registered with a real
// route-table pattern so handlers receive r.PathValue wildcards exactly the
// way cmd/janus-api's router dispatches. It mirrors the router's tenant
// trailing-slash tolerance (withTenantPathNormalization) so the legacy
// tolerated forms still match.
func dispatchViaMux(pattern string, h http.HandlerFunc, r *http.Request) *httptest.ResponseRecorder {
	if p := r.URL.Path; strings.HasPrefix(p, "/v1/tenants/") && p != "/v1/tenants/" {
		r.URL.Path = strings.TrimRight(p, "/")
	}
	rec := httptest.NewRecorder()
	muxFor(pattern, h).ServeHTTP(rec, r)
	return rec
}
