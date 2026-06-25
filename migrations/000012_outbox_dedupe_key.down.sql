DROP INDEX IF EXISTS outbox_dedupe_key_idx;
ALTER TABLE outbox_events DROP COLUMN IF EXISTS dedupe_key;
