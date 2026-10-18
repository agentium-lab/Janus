-- Replay generation in the settlement ledger: attempt numbers restart at 1
-- after a replay, so (task_id, attempt) alone collided across generations —
-- the replay round's usage was silently swallowed by ON CONFLICT DO NOTHING
-- and replayed work never hit the budget. The tasks.replay_count at insert
-- time is the generation.
ALTER TABLE budget_usage_ledger ADD COLUMN IF NOT EXISTS generation integer NOT NULL DEFAULT 0;

ALTER TABLE budget_usage_ledger DROP CONSTRAINT IF EXISTS budget_usage_ledger_pkey;
ALTER TABLE budget_usage_ledger ADD PRIMARY KEY (tenant_id, task_id, generation, attempt, scope_type, scope_id);
