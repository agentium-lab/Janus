ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS next_attempt_at timestamptz;

DROP INDEX IF EXISTS outbox_pending_idx;
CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox_events (status, created_at)
    WHERE status IN ('pending', 'retry');
