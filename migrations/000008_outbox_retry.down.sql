DROP INDEX IF EXISTS outbox_pending_idx;
CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox_events (status, created_at)
    WHERE status = 'pending';
ALTER TABLE outbox_events DROP COLUMN IF EXISTS next_attempt_at;
