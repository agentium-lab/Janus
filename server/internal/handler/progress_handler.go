package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/agentium-lab/Janus/core"
)

// ProgressService validates and records task progress reports.
type ProgressService interface {
	ReportProgress(ctx context.Context, tenantID, taskID, agentID string, prog core.TaskProgress) error
}

// EventPublisher pushes events into the in-memory fanout channel feeding
// SSE/WebSocket subscribers (the fast path of ADR-0004 dual-path).
type EventPublisher interface {
	Publish(evt core.JanusEvent)
}

// ProgressHandler receives agent progress reports and fans them out through
// the broadcaster + outbox (dual-path, ADR-0004).
type ProgressHandler struct {
	svc         ProgressService
	publisher   EventPublisher
	rateLimiter *progressRateLimiter
}

func NewProgressHandler(svc ProgressService, publisher EventPublisher) *ProgressHandler {
	return &ProgressHandler{
		svc:         svc,
		publisher:   publisher,
		rateLimiter: newProgressRateLimiter(10), // 10/sec per task
	}
}

type progressReq struct {
	Message string          `json:"message"`
	Percent *int            `json:"percent"`
	Data    json.RawMessage `json:"data"`
	AgentID string          `json:"agent_id"`
}

func (h *ProgressHandler) Report(w http.ResponseWriter, r *http.Request) {
	tenantID := tenantIDFromPath(r.URL.Path)
	taskID := lastSegment(strings.TrimSuffix(r.URL.Path, "/progress"))
	if taskID == "" || taskID == "tasks" {
		writeError(w, http.StatusBadRequest, "missing task id")
		return
	}

	var req progressReq
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if req.Message == "" {
		writeError(w, http.StatusBadRequest, "message is required")
		return
	}
	if req.Percent != nil && (*req.Percent < 0 || *req.Percent > 100) {
		writeError(w, http.StatusBadRequest, "percent must be 0-100")
		return
	}
	if req.AgentID == "" {
		writeError(w, http.StatusBadRequest, "agent_id is required")
		return
	}

	// Rate limit per task.
	if !h.rateLimiter.allow(tenantID + "/" + taskID) {
		writeError(w, http.StatusTooManyRequests, "progress rate limit exceeded (10/s per task)")
		return
	}

	prog := core.TaskProgress{
		Message: req.Message,
		Percent: req.Percent,
		Data:    req.Data,
	}

	// Validate: reporter must hold the latest attempt; task must be in progress.
	if err := h.svc.ReportProgress(r.Context(), tenantID, taskID, req.AgentID, prog); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Dual-path: fan out in-memory for SSE/WebSocket subscribers.
	payload, _ := json.Marshal(prog)
	evt := core.JanusEvent{
		EventType:   core.EventTaskProgress,
		TenantID:    tenantID,
		TaskID:      taskID,
		SourceAgent: req.AgentID,
		Payload:     payload,
	}
	h.publisher.Publish(evt)

	writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}

// progressRateLimiter is a simple token bucket per key.
type progressRateLimiter struct {
	mu       sync.Mutex
	limit    int
	lastSeen map[string]time.Time
}

func newProgressRateLimiter(perSec int) *progressRateLimiter {
	return &progressRateLimiter{
		limit:    perSec,
		lastSeen: map[string]time.Time{},
	}
}

func (rl *progressRateLimiter) allow(key string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	now := time.Now()
	if last, ok := rl.lastSeen[key]; ok && now.Sub(last) < time.Second/time.Duration(rl.limit) {
		return false
	}
	rl.lastSeen[key] = now
	// Periodic cleanup to prevent unbounded growth.
	if len(rl.lastSeen) > 10000 {
		for k, t := range rl.lastSeen {
			if now.Sub(t) > time.Minute {
				delete(rl.lastSeen, k)
			}
		}
	}
	return true
}

var _ = fmt.Sprintf
