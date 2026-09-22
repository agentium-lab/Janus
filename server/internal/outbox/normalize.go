package outbox

import (
	"encoding/json"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/driver/postgres"
)

// NormalizeLegacyEvent stamps a stable identity onto events written before
// write-point enrichment (rows from a rolling upgrade). The identity derives
// from the outbox row ID, so the SAME event_id flows to the queue (SSE/WS),
// the audit projection, and any re-processing — and distinct rows never
// collide on the audit table's (tenant_id, event_id) dedupe key.
// Publish-side and projection-side callers MUST share this function: when
// they derived IDs independently, the live stream and the audit trail
// disagreed on legacy rows.
func NormalizeLegacyEvent(entry postgres.OutboxEntry) (core.JanusEvent, error) {
	var evt core.JanusEvent
	if err := json.Unmarshal(entry.Payload, &evt); err != nil {
		return evt, err
	}
	if evt.EventID == "" {
		evt.EventID = "obx_" + entry.ID
	}
	if evt.Timestamp.IsZero() {
		evt.Timestamp = entry.CreatedAt
	}
	return evt, nil
}
