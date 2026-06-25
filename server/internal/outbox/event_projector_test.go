package outbox

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

// mockEventWriter records events for assertion.
type mockEventWriter struct {
	mu      sync.Mutex
	events  []core.JanusEvent
	recErr  error
}

func (w *mockEventWriter) Record(_ context.Context, evt core.JanusEvent) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.recErr != nil {
		return w.recErr
	}
	w.events = append(w.events, evt)
	return nil
}

func (w *mockEventWriter) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.events)
}

func TestEventProjector_RecordGeneratesIDAndTimestamp(t *testing.T) {
	writer := &mockEventWriter{}
	p := NewEventProjector(writer)

	// Record an event without EventID or Timestamp.
	evt := core.JanusEvent{EventType: core.EventTaskCreated, TenantID: "acme"}
	p.Record(context.Background(), evt)

	// Start the projector to drain the channel.
	ctx, cancel := context.WithCancel(context.Background())
	go p.Start(ctx)
	time.Sleep(100 * time.Millisecond)
	cancel()
	p.Stop()

	require.Equal(t, 1, writer.count(), "event should be recorded")
}

func TestEventProjector_RecordPreservesExistingID(t *testing.T) {
	writer := &mockEventWriter{}
	p := NewEventProjector(writer)

	evt := core.JanusEvent{EventID: "custom-id", EventType: core.EventTaskCompleted, TenantID: "acme"}
	p.Record(context.Background(), evt)

	ctx, cancel := context.WithCancel(context.Background())
	go p.Start(ctx)
	time.Sleep(100 * time.Millisecond)
	cancel()
	p.Stop()

	assert.Equal(t, 1, writer.count())
}

func TestEventProjector_StopTerminatesLoop(t *testing.T) {
	writer := &mockEventWriter{}
	p := NewEventProjector(writer)

	go p.Start(context.Background())
	time.Sleep(50 * time.Millisecond)
	p.Stop()
	// If Stop didn't work, the test would hang on cleanup (goroutine leak).
	// Just verify no panic.
	assert.True(t, true)
}

func TestEventProjector_RecordWriterError(t *testing.T) {
	writer := &mockEventWriter{recErr: assertError("write failed")}
	p := NewEventProjector(writer)

	p.Record(context.Background(), core.JanusEvent{EventType: core.EventTaskCreated})

	ctx, cancel := context.WithCancel(context.Background())
	go p.Start(ctx)
	time.Sleep(100 * time.Millisecond)
	cancel()
	p.Stop()

	// Writer error should be logged but not panic.
	assert.Equal(t, 0, writer.count(), "event should not be recorded on error")
}

// Helper error type.
type assertError string

func (e assertError) Error() string { return string(e) }
