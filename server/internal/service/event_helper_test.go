package service

import (
	"testing"

	"github.com/agentium-lab/Janus/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnrichEvent_SetsEventID(t *testing.T) {
	evt := core.JanusEvent{EventType: core.EventTaskCreated}
	err := enrichEvent(&evt)
	require.NoError(t, err)
	assert.NotEmpty(t, evt.EventID)
	assert.Contains(t, evt.EventID, "evt_")
}

func TestEnrichEvent_SetsTimestamp(t *testing.T) {
	evt := core.JanusEvent{EventType: core.EventTaskCreated}
	err := enrichEvent(&evt)
	require.NoError(t, err)
	assert.False(t, evt.Timestamp.IsZero())
}

func TestEnrichEvent_DoesNotOverrideEventID(t *testing.T) {
	evt := core.JanusEvent{
		EventType: core.EventTaskCreated,
		EventID:   "custom_id",
	}
	err := enrichEvent(&evt)
	require.NoError(t, err)
	assert.Equal(t, "custom_id", evt.EventID)
	assert.False(t, evt.Timestamp.IsZero())
}
