DROP INDEX IF EXISTS outbox_publishing_lease_idx;
ALTER TABLE outbox_events DROP COLUMN IF EXISTS lease_expires_at;
ALTER TABLE outbox_events DROP COLUMN IF EXISTS locked_at;
ALTER TABLE outbox_events DROP COLUMN IF EXISTS locked_by;
