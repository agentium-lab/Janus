ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS next_attempt_at timestamptz;
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS last_error text;

DROP INDEX IF EXISTS outbox_pending_idx;
CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox_events (status, created_at)
    WHERE status IN ('pending', 'retry', 'publishing');
