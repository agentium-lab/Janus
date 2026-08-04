package observability

import (
	"net/http"
	"strconv"
	"strings"
)

// HSTSMiddleware adds the HTTP Strict-Transport-Security header to every
// response. It must only be wired in when the server is actually serving
// over TLS, otherwise it would lock clients into HTTPS for a non-TLS deploy.
const hstsHeaderValue = "max-age=31536000; includeSubDomains"

func HSTSMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Strict-Transport-Security", hstsHeaderValue)
		next.ServeHTTP(w, r)
	})
}

// CORSMiddleware enforces a Cross-Origin Resource Sharing allowlist. With an
// empty allowlist (the default) no Access-Control-Allow-Origin header is set,
// which causes browsers to deny cross-origin reads. Requests carrying an
// Origin that exactly matches one of the allowed entries are reflected back.
// Preflight OPTIONS requests are short-circuited with 204.
func CORSMiddleware(allowedOrigins []string) func(http.Handler) http.Handler {
	allowed := make(map[string]bool, len(allowedOrigins))
	for _, o := range allowedOrigins {
		o = strings.TrimSpace(o)
		if o != "" {
			allowed[o] = true
		}
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if origin != "" && allowed[origin] {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Vary", "Origin")
				w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-API-Key")
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
			}
			if r.Method == http.MethodOptions {
				w.Header().Set("Access-Control-Max-Age", strconv.Itoa(600))
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
