package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/driver/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeOutboxReader struct {
	entries      []postgres.OutboxEntry
	rangeEntries []postgres.OutboxEntry
	marked       []string
}

func (f *fakeOutboxReader) FetchUnprojected(_ context.Context, _ int) ([]postgres.OutboxEntry, error) {
	return f.entries, nil
}
func (f *fakeOutboxReader) MarkProjected(_ context.Context, id string) error {
	f.marked = append(f.marked, id)
	return nil
}
func (f *fakeOutboxReader) FetchByRange(_ context.Context, _ string, _, _ time.Time, _ int) ([]postgres.OutboxEntry, error) {
	return f.rangeEntries, nil
}

type fakeWriter struct {
	err     error
	written []core.JanusEvent
}

func (f *fakeWriter) Record(_ context.Context, _ core.JanusEvent) error { return f.err }
func (f *fakeWriter) RecordIdempotent(_ context.Context, evt core.JanusEvent) error {
	if f.err != nil {
		return f.err
	}
	f.written = append(f.written, evt)
	return nil
}

func TestAuditProjector_ProjectBatch(t *testing.T) {
	evt := core.JanusEvent{EventID: "e1", TenantID: "acme", EventType: core.EventTaskCompleted}
	payload, _ := json.Marshal(evt)
	reader := &fakeOutboxReader{entries: []postgres.OutboxEntry{
		{ID: "ob1", TenantID: "acme", Kind: "event_publish", Payload: payload},
		{ID: "ob2", TenantID: "acme", Kind: "task_publish", Payload: payload}, // skipped
	}}
	writer := &fakeWriter{}
	p := NewAuditProjector(reader, writer)
	p.projectBatch(context.Background())
	assert.Len(t, writer.written, 1, "only event_publish entries projected")
	assert.Equal(t, []string{"ob1"}, reader.marked, "projected entry marked")
}

func TestAuditProjector_WriteFailureLeavesPending(t *testing.T) {
	evt := core.JanusEvent{EventID: "e-fail", TenantID: "acme"}
	payload, _ := json.Marshal(evt)
	reader := &fakeOutboxReader{entries: []postgres.OutboxEntry{
		{ID: "ob-fail", Kind: "event_publish", Payload: payload},
	}}
	writer := &fakeWriter{err: errors.New("db down")}
	p := NewAuditProjector(reader, writer)
	p.projectBatch(context.Background())
	assert.Empty(t, reader.marked, "failed entry NOT marked projected — stays in outbox for next tick")
	assert.Empty(t, writer.written)
}

func TestAuditProjector_MalformedPayload(t *testing.T) {
	reader := &fakeOutboxReader{entries: []postgres.OutboxEntry{
		{ID: "ob-bad", Kind: "event_publish", Payload: json.RawMessage(`{invalid`)},
	}}
	writer := &fakeWriter{}
	p := NewAuditProjector(reader, writer)
	p.projectBatch(context.Background())
	assert.Len(t, reader.marked, 1, "malformed entry marked projected so it doesn't block the queue")
	assert.Empty(t, writer.written)
}

func TestAuditProjector_NilReaderSafe(t *testing.T) {
	p := NewAuditProjector(nil, &fakeWriter{})
	assert.NotNil(t, p)
	p.Stop()
}

func TestAuditProjector_StopIdempotentOnce(t *testing.T) {
	p := NewAuditProjector(nil, nil)
	p.Stop()
}

func TestAuditProjector_ReplayEmpty(t *testing.T) {
	reader := &fakeOutboxReader{}
	writer := &fakeWriter{}
	p := NewAuditProjector(reader, writer)
	res, err := p.Replay(context.Background(), "acme", time.Now().Add(-time.Hour), time.Now(), 100)
	require.NoError(t, err)
	assert.Equal(t, 0, res.Projected)
}

func TestAuditProjector_StartStop(t *testing.T) {
	reader := &fakeOutboxReader{}
	writer := &fakeWriter{}
	p := NewAuditProjector(reader, writer)
	go p.Start(context.Background())
	time.Sleep(100 * time.Millisecond)
	p.Stop()
}

func TestAuditProjector_ReplayWithEntries(t *testing.T) {
	evt := core.JanusEvent{EventID: "rp1", TenantID: "acme", EventType: core.EventTaskCompleted, Payload: json.RawMessage(`{"s":1}`)}
	payload, _ := json.Marshal(evt)
	reader := &fakeOutboxReader{rangeEntries: []postgres.OutboxEntry{
		{ID: "ob-rp1", Kind: "event_publish", Payload: payload},
		{ID: "ob-rp2", Kind: "task_publish", Payload: payload},
	}}
	writer := &fakeWriter{}
	p := NewAuditProjector(reader, writer)
	res, err := p.Replay(context.Background(), "acme", time.Now().Add(-time.Hour), time.Now(), 100)
	require.NoError(t, err)
	assert.Equal(t, 1, res.Projected, "only event_publish replayed")
	assert.Len(t, writer.written, 1)
}

func TestAuditProjector_EmptyBatch(t *testing.T) {
	reader := &fakeOutboxReader{}
	writer := &fakeWriter{}
	p := NewAuditProjector(reader, writer)
	p.projectBatch(context.Background())
	assert.Empty(t, writer.written)
	assert.Empty(t, reader.marked)
}

func TestAuditProjector_WriteFailureDoesNotMarkProjected(t *testing.T) {
	evt := core.JanusEvent{EventID: "e-err", TenantID: "acme"}
	payload, _ := json.Marshal(evt)
	reader := &fakeOutboxReader{entries: []postgres.OutboxEntry{
		{ID: "ob-err", Kind: "event_publish", Payload: payload},
	}}
	writer := &fakeWriter{err: fmt.Errorf("db unreachable")}
	p := NewAuditProjector(reader, writer)
	p.projectBatch(context.Background())
	assert.Empty(t, reader.marked, "failed write must NOT be marked projected")
	assert.Len(t, reader.entries, 1, "entry stays for next tick")
}

func TestAuditProjector_DefensiveKindFilter(t *testing.T) {
	evt := core.JanusEvent{EventID: "e-k", TenantID: "acme"}
	payload, _ := json.Marshal(evt)
	reader := &fakeOutboxReader{entries: []postgres.OutboxEntry{
		{ID: "ob-k1", Kind: "task_publish", Payload: payload},
		{ID: "ob-k2", Kind: "event_publish", Payload: payload},
	}}
	writer := &fakeWriter{}
	p := NewAuditProjector(reader, writer)
	p.projectBatch(context.Background())
	assert.Len(t, writer.written, 1, "only event_publish despite fake returning both")
	assert.Equal(t, []string{"ob-k2"}, reader.marked)
}

func TestAuditProjector_NilWriterSafe(t *testing.T) {
	reader := &fakeOutboxReader{entries: []postgres.OutboxEntry{
		{ID: "ob-nw", Kind: "event_publish", Payload: json.RawMessage(`{`)},
	}}
	p := NewAuditProjector(reader, nil)
	assert.NotPanics(t, func() { p.projectBatch(context.Background()) })
}

func TestAuditProjector_ReplayMalformed(t *testing.T) {
	reader := &fakeOutboxReader{rangeEntries: []postgres.OutboxEntry{
		{ID: "ob-bad", Kind: "event_publish", Payload: json.RawMessage(`{bad`)},
	}}
	writer := &fakeWriter{}
	p := NewAuditProjector(reader, writer)
	res, err := p.Replay(context.Background(), "acme", time.Now().Add(-time.Hour), time.Now(), 100)
	require.NoError(t, err, "replay itself should succeed")
	assert.Equal(t, 0, res.Projected)
	assert.Equal(t, "all_failed", res.Status)
	assert.Greater(t, res.Failed, 0)
}

func TestAuditProjector_ReplayWriteError(t *testing.T) {
	evt := core.JanusEvent{EventID: "rp-err", TenantID: "acme"}
	payload, _ := json.Marshal(evt)
	reader := &fakeOutboxReader{rangeEntries: []postgres.OutboxEntry{
		{ID: "ob-rp-err", Kind: "event_publish", Payload: payload},
	}}
	writer := &fakeWriter{err: fmt.Errorf("write fail")}
	p := NewAuditProjector(reader, writer)
	res, err := p.Replay(context.Background(), "acme", time.Now().Add(-time.Hour), time.Now(), 100)
	require.NoError(t, err, "replay itself should succeed")
	assert.Equal(t, 0, res.Projected)
	assert.Equal(t, "all_failed", res.Status)
	assert.Greater(t, res.Failed, 0)
}
