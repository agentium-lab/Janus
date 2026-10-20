package lease

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

func TestMarshalLeaseEvent_FillsMissingIdentity(t *testing.T) {
	evt := &core.JanusEvent{EventType: core.EventTaskDeadLettered, TenantID: "acme", TaskID: "t1"}
	payload := marshalLeaseEvent(evt)
	assert.NotEmpty(t, evt.EventID)
	assert.True(t, strings.HasPrefix(evt.EventID, "evt_"))
	assert.False(t, evt.Timestamp.IsZero())

	var back core.JanusEvent
	require.NoError(t, json.Unmarshal(payload, &back))
	assert.Equal(t, evt.EventID, back.EventID)
	assert.Equal(t, core.EventTaskDeadLettered, back.EventType)
	assert.Equal(t, "t1", back.TaskID)
	assert.False(t, back.Timestamp.IsZero())
}

func TestMarshalLeaseEvent_PreservesExistingIdentity(t *testing.T) {
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	evt := &core.JanusEvent{EventID: "evt-fixed", Timestamp: ts, TenantID: "acme"}
	payload := marshalLeaseEvent(evt)
	assert.Equal(t, "evt-fixed", evt.EventID)
	assert.Equal(t, ts, evt.Timestamp)

	var back core.JanusEvent
	require.NoError(t, json.Unmarshal(payload, &back))
	assert.Equal(t, "evt-fixed", back.EventID)
	assert.Equal(t, ts.UTC(), back.Timestamp.UTC())
}
