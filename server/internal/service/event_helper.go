package service

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/agentium-lab/Janus/core"
)

// enrichEvent ensures every event has event_id and timestamp.
func enrichEvent(event *core.JanusEvent) error {
	if event.EventID == "" {
		id, err := generateEventID()
		if err != nil {
			return fmt.Errorf("generate event id: %w", err)
		}
		event.EventID = id
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	return nil
}

// marshalEvent stamps a stable identity (event_id + timestamp) onto the event
// and serializes it for the outbox. The SAME identity then travels unchanged
// through outbox → queue → SSE/WS → audit projection; a missing event_id
// would collapse audit rows (event repo dedupes on (tenant_id, event_id)).
func MarshalEvent(event *core.JanusEvent) json.RawMessage {
	if err := enrichEvent(event); err != nil {
		// generateEventID only fails when crypto/rand fails; the empty ID
		// still flows through, dedup degrades but delivery continues.
		_ = err
	}
	b, _ := json.Marshal(event)
	return b
}

func generateEventID() (string, error) {
	b := make([]byte, 10)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "evt_" + hex.EncodeToString(b), nil
}
