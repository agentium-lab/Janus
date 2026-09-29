package observability

import (
	"errors"

	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"
)

type CheckFunc func(ctx context.Context) error

// DegradedError marks a check that is not healthy but must NOT fail the
// probe: the dependency is optional (best-effort accelerator) and the data
// plane keeps serving. The result reports "degraded" instead of
// "unavailable" and readiness stays 200.
type DegradedError struct{ Err error }

func (e *DegradedError) Error() string { return e.Err.Error() }
func (e *DegradedError) Unwrap() error { return e.Err }

// Degraded wraps an optional-dependency failure into a probe-visible but
// non-fatal signal.
func Degraded(err error) error { return &DegradedError{Err: err} }

type ReadyChecker struct {
	mu     sync.RWMutex
	checks map[string]CheckFunc
}

func NewReadyChecker() *ReadyChecker {
	return &ReadyChecker{checks: make(map[string]CheckFunc)}
}

func (rc *ReadyChecker) Add(name string, fn CheckFunc) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.checks[name] = fn
}

func (rc *ReadyChecker) Check(ctx context.Context) (bool, map[string]string) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	results := make(map[string]string)
	allReady := true
	for name, fn := range rc.checks {
		checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := fn(checkCtx)
		cancel()
		var degraded *DegradedError
		switch {
		case err == nil:
			results[name] = "ok"
		case errors.As(err, &degraded):
			results[name] = "degraded"
		default:
			log.Printf("readyz: check %s failed: %v", name, err)
			results[name] = "unavailable"
			allReady = false
		}
	}
	return allReady, results
}

func (rc *ReadyChecker) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ready, results := rc.Check(r.Context())
		w.Header().Set("Content-Type", "application/json")
		if !ready {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusOK)
		}
		status := "ready"
		if !ready {
			status = "degraded"
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": status,
			"checks": results,
		})
	}
}
