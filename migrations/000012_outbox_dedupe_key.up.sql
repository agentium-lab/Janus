-- Outbox dedupe key: stable per-delivery identifier so that retry-driven
-- re-publishes and concurrent schedulers can't double-insert the same delivery.
-- NULL dedupe_key means "no dedup" (backwards compatible).
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS dedupe_key text;

-- Partial unique index: only enforce uniqueness when a dedupe_key is set.
CREATE UNIQUE INDEX IF NOT EXISTS outbox_dedupe_key_idx
    ON outbox_events (tenant_id, dedupe_key)
    WHERE dedupe_key IS NOT NULL;
