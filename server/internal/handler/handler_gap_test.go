package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/driver/postgres"
)

func TestConstructSSEData_AllFields(t *testing.T) {
	evt := core.JanusEvent{
		EventType:   core.EventTaskProgress,
		TaskID:      "t1",
		SourceAgent: "a1",
		Payload:     []byte(`{"step":2}`),
	}
	data := constructSSEData(evt)
	var m map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(data), &m))
	assert.Equal(t, string(core.EventTaskProgress), m["event_type"])
	assert.Equal(t, "t1", m["task_id"])
	assert.Equal(t, "a1", m["source_agent"])
	assert.Equal(t, float64(2), m["payload"].(map[string]interface{})["step"])
}

func TestDLQHandler_ReplayNotReplayableMapsTo409(t *testing.T) {
	svc := &dlqHandlerSvcMock{replayErr: postgres.ErrTaskNotReplayable}
	h := NewDLQHandler(svc)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/tenants/acme/dlq/t1/replay", nil)
	h.Replay(w, r)
	assert.Equal(t, http.StatusConflict, w.Code)
	assert.Contains(t, w.Body.String(), "not in a replayable terminal state")
}

func TestDLQServiceAdapter_ReplayFirstUpdateFailure(t *testing.T) {
	repo := &mockDLQTaskRepo{
		tasks: map[string]*core.Task{
			"acme:t1": {ID: "t1", TenantID: "acme", Status: core.TaskStatusDeadLettered, MailboxID: "mb-1"},
		},
		updateErr: errors.New("update failed"),
	}
	adapter := NewDLQServiceAdapter(repo, &mockDLQQueueDriver{})
	_, err := adapter.ReplayDLQ(context.Background(), "acme", "t1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "update failed")
}
