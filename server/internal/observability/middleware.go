package observability

import (
	"net/http"
	"strconv"
	"time"

	"github.com/agentium-lab/Janus/server/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var (
	httpRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "janus_http_requests_total", Help: "Total HTTP requests"},
		[]string{"method", "path", "status"},
	)
	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Name: "janus_http_request_duration_seconds", Help: "HTTP request duration"},
		[]string{"method", "path"},
	)
)

func init() {
	prometheus.MustRegister(httpRequestsTotal, httpRequestDuration)
}

func HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ctx, span := otel.Tracer("janus").Start(r.Context(), "HTTP "+r.Method+" "+r.URL.Path,
			trace.WithAttributes(attribute.String("http.method", r.Method), attribute.String("http.url", r.URL.Path)),
		)
		defer span.End()

		ww := &statusWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(ww, r.WithContext(ctx))

		duration := time.Since(start).Seconds()
		path := normalizePath(r.URL.Path)
		status := strconv.Itoa(ww.status)

		httpRequestsTotal.WithLabelValues(r.Method, path, status).Inc()
		httpRequestDuration.WithLabelValues(r.Method, path).Observe(duration)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func normalizePath(path string) string {
	if len(path) > 48 {
		return path[:48]
	}
	return path
}

func RecordTaskLatency(duration time.Duration) {
	metrics.TaskLatency.WithLabelValues().Observe(duration.Seconds())
}
