ALTER TABLE budget_usage_ledger DROP CONSTRAINT IF EXISTS budget_usage_ledger_pkey;
ALTER TABLE budget_usage_ledger ADD PRIMARY KEY (tenant_id, task_id, attempt, scope_type, scope_id);
ALTER TABLE budget_usage_ledger DROP COLUMN IF EXISTS generation;
