package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/driver/postgres"
)

func TestAuditProjector_WithOptions(t *testing.T) {
	p := NewAuditProjector(nil, nil)
	assert.Equal(t, p.WithInterval(0), p, "non-positive interval keeps default")
	assert.Equal(t, 500*time.Millisecond, p.interval)
	p.WithInterval(time.Second)
	assert.Equal(t, time.Second, p.interval)
	assert.Equal(t, p.WithBatchSize(-1), p, "non-positive batch size keeps default")
	assert.Equal(t, 100, p.batchSize)
	p.WithBatchSize(7)
	assert.Equal(t, 7, p.batchSize)
}

type chWriter struct {
	wrote chan core.JanusEvent
}

func (w *chWriter) RecordIdempotent(_ context.Context, evt core.JanusEvent) error {
	select {
	case w.wrote <- evt:
	default:
	}
	return nil
}

type startReader struct {
	mu      sync.Mutex
	entries []postgres.OutboxEntry
	marked  []string
}

func (f *startReader) FetchUnprojected(_ context.Context, _ int) ([]postgres.OutboxEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.entries, nil
}

func (f *startReader) MarkProjected(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.marked = append(f.marked, id)
	return nil
}

func (f *startReader) FetchByRange(context.Context, string, time.Time, time.Time, int) ([]postgres.OutboxEntry, error) {
	return nil, nil
}

func (f *startReader) markedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.marked...)
}

func TestAuditProjector_StartPlainReader(t *testing.T) {
	evt := core.JanusEvent{EventID: "st-1", TenantID: "acme"}
	payload, _ := json.Marshal(evt)
	reader := &startReader{entries: []postgres.OutboxEntry{
		{ID: "ob-st1", TenantID: "acme", Kind: "event_publish", Payload: payload},
	}}
	writer := &chWriter{wrote: make(chan core.JanusEvent, 1)}
	p := NewAuditProjector(reader, writer).WithInterval(2 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Start(ctx)

	select {
	case got := <-writer.wrote:
		assert.Equal(t, "st-1", got.EventID)
	case <-time.After(2 * time.Second):
		t.Fatal("projector never wrote the entry")
	}
	require.Eventually(t, func() bool {
		return len(reader.markedIDs()) == 1
	}, 2*time.Second, 2*time.Millisecond, "entry must be marked projected")
	assert.Equal(t, []string{"ob-st1"}, reader.markedIDs())
	p.Stop()
}

type tickLockStartReader struct {
	startReader
	mode      atomic.Int32
	acquired  atomic.Int64
	released  atomic.Int64
}

const (
	tickLockErr      int32 = 0
	tickLockOtherOwn int32 = 1
	tickLockOwned    int32 = 2
)

func (f *tickLockStartReader) AcquireTickLock(_ context.Context, _ string) (postgres.Executor, func(), bool, error) {
	f.acquired.Add(1)
	switch f.mode.Load() {
	case tickLockErr:
		return nil, func() {}, false, errors.New("lock backend down")
	case tickLockOtherOwn:
		return nil, func() {}, false, nil
	default:
		return stubExecutor{}, func() { f.released.Add(1) }, true, nil
	}
}

func TestAuditProjector_StartTickLockPaths(t *testing.T) {
	evt := core.JanusEvent{EventID: "tl-1", TenantID: "acme"}
	payload, _ := json.Marshal(evt)
	reader := &tickLockStartReader{startReader: startReader{entries: []postgres.OutboxEntry{
		{ID: "ob-tl1", TenantID: "acme", Kind: "event_publish", Payload: payload},
	}}}
	writer := &chWriter{wrote: make(chan core.JanusEvent, 1)}
	p := NewAuditProjector(reader, writer).WithInterval(2 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Start(ctx)

	require.Eventually(t, func() bool { return reader.acquired.Load() >= 2 }, 2*time.Second, 2*time.Millisecond,
		"lock failures must be retried on later ticks")

	reader.mode.Store(tickLockOtherOwn)
	require.Eventually(t, func() bool { return reader.acquired.Load() >= 3 }, 2*time.Second, 5*time.Millisecond,
		"unowned ticks must keep polling")

	reader.mode.Store(tickLockOwned)
	select {
	case got := <-writer.wrote:
		assert.Equal(t, "tl-1", got.EventID)
	case <-time.After(2 * time.Second):
		t.Fatal("locked tick never projected the entry")
	}
	require.Eventually(t, func() bool { return reader.released.Load() >= 1 }, 2*time.Second, 2*time.Millisecond,
		"the tick lock must be released after each batch")
	p.Stop()
}

func TestAuditProjector_ReplayLimitClampAndFetchError(t *testing.T) {
	t.Run("non-positive limit clamps to 500", func(t *testing.T) {
		reader := &limitRecorder{fakeOutboxReader: fakeOutboxReader{}}
		p := NewAuditProjector(reader, &fakeWriter{})
		_, err := p.Replay(context.Background(), "acme", time.Now().Add(-time.Hour), time.Now(), 0)
		require.NoError(t, err)
		assert.Equal(t, 500, reader.gotLimit)
	})
	t.Run("fetch failure surfaces", func(t *testing.T) {
		p := NewAuditProjector(rangeErrReader{err: errors.New("db down")}, &fakeWriter{})
		_, err := p.Replay(context.Background(), "acme", time.Now().Add(-time.Hour), time.Now(), 10)
		require.ErrorContains(t, err, "replay fetch")
	})
	t.Run("mixed outcomes report partial_failure", func(t *testing.T) {
		good, _ := json.Marshal(core.JanusEvent{EventID: "rp-good", TenantID: "acme"})
		reader := &fakeOutboxReader{rangeEntries: []postgres.OutboxEntry{
			{ID: "ob-good", Kind: "event_publish", Payload: good},
			{ID: "ob-bad", Kind: "event_publish", Payload: json.RawMessage(`{broken`)},
		}}
		writer := &fakeWriter{}
		p := NewAuditProjector(reader, writer)
		res, err := p.Replay(context.Background(), "acme", time.Now().Add(-time.Hour), time.Now(), 10)
		require.NoError(t, err)
		assert.Equal(t, 1, res.Projected)
		assert.Equal(t, 1, res.Failed)
		assert.Equal(t, "partial_failure", res.Status)
	})
}

type limitRecorder struct {
	fakeOutboxReader
	gotLimit int
}

func (f *limitRecorder) FetchByRange(_ context.Context, _ string, _, _ time.Time, limit int) ([]postgres.OutboxEntry, error) {
	f.gotLimit = limit
	return nil, nil
}

type rangeErrReader struct {
	err error
}

func (f rangeErrReader) FetchUnprojected(context.Context, int) ([]postgres.OutboxEntry, error) {
	return nil, nil
}

func (f rangeErrReader) MarkProjected(context.Context, string) error { return nil }

func (f rangeErrReader) FetchByRange(context.Context, string, time.Time, time.Time, int) ([]postgres.OutboxEntry, error) {
	return nil, f.err
}

type recordingDirectWriter struct {
	mu      sync.Mutex
	inserts []string
	err     error
}

func (w *recordingDirectWriter) InsertDirectWithDedupe(_ context.Context, id, _, kind, dedupeKey string, _ json.RawMessage) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	w.inserts = append(w.inserts, kind+":"+id+":"+dedupeKey)
	return nil
}

func (w *recordingDirectWriter) recorded() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.inserts...)
}

func TestOutboxEventRecorder_PublishEvent(t *testing.T) {
	t.Run("generates identity when missing", func(t *testing.T) {
		w := &recordingDirectWriter{}
		rec := NewOutboxEventRecorder(w)
		require.NoError(t, rec.PublishEvent(context.Background(), core.JanusEvent{TenantID: "acme", EventType: core.EventTaskCompleted}))
		recorded := w.recorded()
		require.Len(t, recorded, 1)
		assert.Contains(t, recorded[0], "event_publish:evt_")
	})
	t.Run("preserves existing identity and dedupes on it", func(t *testing.T) {
		w := &recordingDirectWriter{}
		rec := NewOutboxEventRecorder(w)
		require.NoError(t, rec.PublishEvent(context.Background(), core.JanusEvent{EventID: "evt-keep", TenantID: "acme"}))
		assert.Equal(t, []string{"event_publish:evt-keep:evt-keep"}, w.recorded())
	})
	t.Run("writer failure propagates", func(t *testing.T) {
		rec := NewOutboxEventRecorder(&recordingDirectWriter{err: errors.New("outbox down")})
		require.Error(t, rec.PublishEvent(context.Background(), core.JanusEvent{EventID: "evt-x", TenantID: "acme"}))
	})
}
