package service

import (
	"crypto/rand"
	"encoding/hex"
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

func generateEventID() (string, error) {
	b := make([]byte, 10)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "evt_" + hex.EncodeToString(b), nil
}
