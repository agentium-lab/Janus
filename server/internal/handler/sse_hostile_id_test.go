package handler

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/agentium-lab/Janus/core"
)

// A2 acceptance: SSE data frames must survive hostile ids — quotes,
// backslashes, and newlines in a task id or agent name JSON-decode on the
// client instead of breaking the frame mid-stream.
func TestConstructSSEData_HostileIDsStayDecodable(t *testing.T) {
	evt := core.JanusEvent{
		EventType:   core.EventTaskCompleted,
		TaskID:      `task"quote
newline\\back`,
		SourceAgent: `agent"; DROP FRAME;`,
		Payload:     []byte(`{"ok":true}`),
	}
	frame := constructSSEData(evt)
	if strings.Contains(frame[:len(frame)-1], "\n") {
		t.Fatalf("raw newline escaped into the frame body: %q", frame)
	}
	var decoded struct {
		EventType   string          `json:"event_type"`
		TaskID      string          `json:"task_id"`
		SourceAgent string          `json:"source_agent"`
		Payload     json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal([]byte(frame), &decoded); err != nil {
		t.Fatalf("frame no longer JSON-decodes: %v\nframe=%q", err, frame)
	}
	if decoded.TaskID != evt.TaskID || decoded.SourceAgent != evt.SourceAgent {
		t.Fatalf("id round-trip broken: got %q / %q", decoded.TaskID, decoded.SourceAgent)
	}
}
