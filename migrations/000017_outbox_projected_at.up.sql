ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS projected_at timestamptz;
CREATE INDEX IF NOT EXISTS idx_outbox_projected ON outbox_events (kind, projected_at) WHERE kind = 'event_publish' AND projected_at IS NULL;
