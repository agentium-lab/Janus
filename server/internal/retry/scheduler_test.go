package retry

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

func TestNewScheduler(t *testing.T) {
	s := NewScheduler(nil, nil)
	assert.NotNil(t, s)
	assert.False(t, s.useOutbox)
}

func TestScheduler_WithOutbox(t *testing.T) {
	s := NewScheduler(nil, nil)
	s2 := s.WithOutbox()
	assert.Same(t, s, s2)
	assert.True(t, s.useOutbox)
}

func TestScheduler_StartStop_ContextCancel(t *testing.T) {
	s := NewScheduler(nil, nil)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		s.Start(ctx, 1*time.Hour)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not exit after context cancel")
	}
}

func TestScheduler_StartStop_StopMethod(t *testing.T) {
	s := NewScheduler(nil, nil)
	ctx := context.Background()

	done := make(chan struct{})
	go func() {
		s.Start(ctx, 1*time.Hour)
		close(done)
	}()

	s.Stop()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not exit after Stop")
	}
}

func TestBuildRetryDedupeKey(t *testing.T) {
	assert.Equal(t, "task_publish:acme:task-1:3", buildRetryDedupeKey("acme", "task-1", 3))
	assert.Equal(t, "task_publish:t2:task-2:0", buildRetryDedupeKey("t2", "task-2", 0))
}

func TestBuildRetryDedupeKey_StableAndUnique(t *testing.T) {
	k1 := buildRetryDedupeKey("acme", "task-1", 1)
	k2 := buildRetryDedupeKey("acme", "task-1", 1)
	assert.Equal(t, k1, k2, "same inputs → same key")

	k3 := buildRetryDedupeKey("acme", "task-1", 2)
	assert.NotEqual(t, k1, k3, "different attempt → different key")
}

func TestBuildRetryMessage_ValidEnvelope(t *testing.T) {
	env := core.TaskEnvelope{
		JanusVersion: "1", TaskID: "task-1", TenantID: "acme",
		SourceAgent: "a1", Target: core.Target{Type: "mailbox", Value: "mb1"},
	}
	envJSON, _ := json.Marshal(env)

	msg, ok := buildRetryMessage("acme", "task-1", "mb1", core.PriorityNormal, envJSON)
	require.True(t, ok)
	assert.Equal(t, "acme", msg.TenantID)
	assert.Equal(t, "task-1", msg.TaskID)
	assert.Equal(t, "mb1", msg.MailboxID)
	assert.Equal(t, core.PriorityNormal, msg.Priority)
	assert.NotEmpty(t, msg.Payload)
}

func TestBuildRetryMessage_InvalidJSON(t *testing.T) {
	_, ok := buildRetryMessage("acme", "task-1", "mb1", core.PriorityNormal, []byte(`{invalid`))
	assert.False(t, ok)
}

func TestGenerateULID_NotEmpty(t *testing.T) {
	id := generateULID()
	assert.NotEmpty(t, id)
}

func TestGenerateULID_Unique(t *testing.T) {
	ids := make(map[string]bool)
	for i := 0; i < 100; i++ {
		ids[generateULID()] = true
	}
	assert.Len(t, ids, 100, "100 generated ULIDs should all be unique")
}
