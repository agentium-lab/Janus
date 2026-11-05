package a2a

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/auth"
)

type createCountingSvc struct{ creates int }

func (s *createCountingSvc) Create(_ context.Context, task core.Task) (*core.Task, error) {
	s.creates++
	return &task, nil
}
func (s *createCountingSvc) Cancel(context.Context, string, string) error { return nil }

type stubSubscriber struct{}

func (stubSubscriber) Subscribe(string) <-chan core.JanusEvent { return make(chan core.JanusEvent) }
func (stubSubscriber) Unsubscribe(string, <-chan core.JanusEvent) {}

type nonFlushingWriter struct {
	header http.Header
	status int
	body   strings.Builder
}

func (w *nonFlushingWriter) Header() http.Header         { return w.header }
func (w *nonFlushingWriter) Write(b []byte) (int, error) { return w.body.Write(b) }
func (w *nonFlushingWriter) WriteHeader(code int)        { w.status = code }

// A2 acceptance: message:stream checks stream capability BEFORE Create — a
// non-streaming writer must get a clean 500 with the task service never
// called (no "task committed but client errored" ambiguity).
func TestA2A_V1StreamMessage_NoFlusher_RejectsBeforeCreate(t *testing.T) {
	svc := &createCountingSvc{}
	gw := NewGateway(nil, svc).WithEventSubscriber(stubSubscriber{})
	req, _ := http.NewRequest(http.MethodPost, "/a2a/message:stream",
		strings.NewReader(`{"message":{"messageId":"m1","role":"user","parts":[{"kind":"text","text":"hi"}]}}`))
	req.Header.Set("Content-Type", "application/json")
	ctx := context.WithValue(req.Context(), auth.TenantCtxKey, "acme")
	w := &nonFlushingWriter{header: http.Header{}}
	gw.handleV1StreamMessage(w, req.WithContext(ctx))
	if w.status != http.StatusInternalServerError {
		t.Fatalf("non-streaming writer: want 500, got %d body=%s", w.status, w.body.String())
	}
	if svc.creates != 0 {
		t.Fatalf("Create must not run when the writer cannot stream (calls=%d)", svc.creates)
	}
	if !strings.Contains(w.body.String(), "streaming") {
		t.Fatalf("error should name the streaming problem: %s", w.body.String())
	}
}
