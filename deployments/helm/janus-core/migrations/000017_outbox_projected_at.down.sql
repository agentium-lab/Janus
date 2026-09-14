DROP INDEX IF EXISTS idx_outbox_projected;
ALTER TABLE outbox_events DROP COLUMN IF EXISTS projected_at;
