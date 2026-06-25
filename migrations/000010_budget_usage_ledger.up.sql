-- budget_usage_ledger: idempotent settlement ledger. Each task attempt's budget
-- settlement is recorded exactly once per scope (tenant/agent).
CREATE TABLE IF NOT EXISTS budget_usage_ledger (
    tenant_id         text        NOT NULL,
    task_id           text        NOT NULL,
    attempt           integer     NOT NULL,
    scope_type        text        NOT NULL,
    scope_id          text        NOT NULL,
    prompt_tokens     bigint      NOT NULL DEFAULT 0,
    completion_tokens bigint      NOT NULL DEFAULT 0,
    total_tokens      bigint      NOT NULL DEFAULT 0,
    cost_usd          numeric(18,6) NOT NULL DEFAULT 0,
    recorded_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, task_id, attempt, scope_type, scope_id)
);

CREATE INDEX IF NOT EXISTS budget_usage_ledger_scope_idx
    ON budget_usage_ledger (tenant_id, scope_type, scope_id, recorded_at);
