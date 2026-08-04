package observability

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"
)

type CheckFunc func(ctx context.Context) error

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
		if err != nil {
			log.Printf("readyz: check %s failed: %v", name, err)
			results[name] = "unavailable"
			allReady = false
		} else {
			results[name] = "ok"
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
			"status":   status,
			"checks":   results,
		})
	}
}
