-- Outbox worker lease: track which worker claimed a 'publishing' row and when
-- its lease expires, so a crashed worker's rows can be reclaimed.
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS locked_by text;
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS locked_at timestamptz;
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS lease_expires_at timestamptz;

-- Index to reclaim expired 'publishing' leases efficiently.
CREATE INDEX IF NOT EXISTS outbox_publishing_lease_idx
    ON outbox_events (status, lease_expires_at)
    WHERE status = 'publishing';
