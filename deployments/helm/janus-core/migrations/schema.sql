-- Janus Core final schema snapshot.
--
-- This file is intentionally named schema.sql, not *.up.sql, so golang-migrate
-- does not treat it as an additional migration. Use the numbered migrations for
-- production upgrades; use this file as a complete fresh-schema reference.

BEGIN;

CREATE TABLE IF NOT EXISTS tenants (
    id text PRIMARY KEY,
    name text NOT NULL,
    status text NOT NULL DEFAULT 'active',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS agents (
    id text NOT NULL,
    tenant_id text NOT NULL REFERENCES tenants(id),
    display_name text NOT NULL,
    team_id text,
    protocol text NOT NULL,
    endpoint text,
    status text NOT NULL DEFAULT 'offline',
    description text,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    max_concurrency integer NOT NULL DEFAULT 1,
    rpm integer,
    tpm integer,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    last_heartbeat_at timestamptz,
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS agent_capabilities (
    tenant_id text NOT NULL,
    agent_id text NOT NULL,
    capability text NOT NULL,
    schema jsonb,
    description text,
    embedding_ref text,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, agent_id, capability)
);

CREATE TABLE IF NOT EXISTS mailboxes (
    tenant_id text NOT NULL,
    id text NOT NULL,
    agent_id text NOT NULL,
    status text NOT NULL DEFAULT 'active',
    priority text NOT NULL DEFAULT 'normal',
    max_concurrency integer NOT NULL DEFAULT 1,
    ack_wait_seconds integer NOT NULL DEFAULT 300,
    max_deliver integer NOT NULL DEFAULT 5,
    retention_seconds integer NOT NULL DEFAULT 604800,
    retry_policy jsonb NOT NULL DEFAULT '{"max_attempts":5,"backoff_type":"exponential","initial_seconds":10,"max_seconds":900,"jitter":true}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS tasks (
    tenant_id text NOT NULL,
    id text NOT NULL,
    idempotency_key text,
    source_agent text NOT NULL,
    target_type text NOT NULL,
    target_value text NOT NULL,
    mailbox_id text,
    status text NOT NULL,
    priority text NOT NULL DEFAULT 'normal',
    deadline timestamptz,
    ttl_seconds integer,
    envelope jsonb NOT NULL,
    result_ref text,
    error jsonb,
    attempt_count integer NOT NULL DEFAULT 0,
    retry_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    PRIMARY KEY (tenant_id, id)
);

CREATE UNIQUE INDEX IF NOT EXISTS tasks_idempotency_idx
    ON tasks (tenant_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS tasks_status_idx
    ON tasks (tenant_id, status, priority, created_at);

CREATE INDEX IF NOT EXISTS tasks_retry_at_idx
    ON tasks (status, retry_at)
    WHERE status = 'retry_scheduled';

CREATE INDEX IF NOT EXISTS tasks_mailbox_backlog_idx
    ON tasks (tenant_id, mailbox_id, status)
    WHERE mailbox_id IS NOT NULL
      AND status IN ('queued', 'retry_scheduled');

CREATE TABLE IF NOT EXISTS task_attempts (
    tenant_id text NOT NULL,
    task_id text NOT NULL,
    attempt integer NOT NULL,
    agent_id text NOT NULL,
    lease_id text NOT NULL,
    delivery_ref text,
    status text NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now(),
    heartbeat_at timestamptz,
    finished_at timestamptz,
    error jsonb,
    token_usage jsonb,
    lease_expires_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, task_id, attempt)
);

CREATE UNIQUE INDEX IF NOT EXISTS task_attempts_lease_idx
    ON task_attempts (tenant_id, lease_id);

CREATE INDEX IF NOT EXISTS task_attempts_lease_expiry_idx
    ON task_attempts (lease_expires_at)
    WHERE status IN ('claimed', 'running');

CREATE TABLE IF NOT EXISTS budgets (
    tenant_id text NOT NULL,
    scope_type text NOT NULL,
    scope_id text NOT NULL,
    rpm integer,
    tpm integer,
    max_concurrency integer,
    daily_cost_usd numeric(18, 6),
    monthly_cost_usd numeric(18, 6),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, scope_type, scope_id)
);

CREATE TABLE IF NOT EXISTS budget_usage (
    tenant_id text NOT NULL,
    scope_type text NOT NULL,
    scope_id text NOT NULL,
    period text NOT NULL,
    period_key text NOT NULL,
    tokens_used integer NOT NULL DEFAULT 0,
    cost_used numeric(18, 6) NOT NULL DEFAULT 0,
    task_count integer NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, scope_type, scope_id, period, period_key)
);

CREATE TABLE IF NOT EXISTS budget_usage_ledger (
    tenant_id text NOT NULL,
    id text NOT NULL,
    task_id text NOT NULL,
    attempt integer NOT NULL,
    scope_type text NOT NULL,
    scope_id text NOT NULL,
    event_type text NOT NULL,
    tokens integer NOT NULL DEFAULT 0,
    cost_usd numeric(18, 6) NOT NULL DEFAULT 0,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    occurred_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, id)
);

CREATE UNIQUE INDEX IF NOT EXISTS budget_usage_ledger_attempt_scope_idx
    ON budget_usage_ledger (tenant_id, task_id, attempt, scope_type, scope_id, event_type);

CREATE INDEX IF NOT EXISTS budget_usage_ledger_scope_idx
    ON budget_usage_ledger (tenant_id, scope_type, scope_id, occurred_at);

CREATE TABLE IF NOT EXISTS policy_rules (
    tenant_id text NOT NULL,
    id text NOT NULL,
    name text NOT NULL,
    status text NOT NULL DEFAULT 'active',
    priority integer NOT NULL DEFAULT 100,
    condition jsonb NOT NULL,
    action jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS approvals (
    tenant_id text NOT NULL,
    id text NOT NULL,
    task_id text NOT NULL,
    status text NOT NULL DEFAULT 'pending',
    requested_by text NOT NULL,
    approver text,
    reason text,
    decision text,
    expires_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    decided_at timestamptz,
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS audit_event_projection (
    tenant_id text NOT NULL,
    event_id text NOT NULL,
    event_type text NOT NULL,
    task_id text,
    agent_id text,
    source_agent text,
    target_agent text,
    actor_type text,
    actor_id text,
    trace_id text,
    occurred_at timestamptz NOT NULL,
    payload jsonb NOT NULL,
    PRIMARY KEY (tenant_id, event_id)
);

CREATE INDEX IF NOT EXISTS audit_event_task_idx
    ON audit_event_projection (tenant_id, task_id, occurred_at);

CREATE INDEX IF NOT EXISTS audit_event_trace_idx
    ON audit_event_projection (tenant_id, trace_id, occurred_at);

CREATE TABLE IF NOT EXISTS api_keys (
    tenant_id text NOT NULL,
    key_hash text NOT NULL,
    name text NOT NULL,
    prefix text NOT NULL,
    scopes text[] NOT NULL DEFAULT '{}',
    id text NOT NULL DEFAULT ('key_' || substr(md5(random()::text || clock_timestamp()::text), 1, 24)),
    last_used_at timestamptz,
    revoked_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, key_hash)
);

CREATE INDEX IF NOT EXISTS api_keys_prefix_idx
    ON api_keys (prefix);

CREATE UNIQUE INDEX IF NOT EXISTS api_keys_tenant_id_idx
    ON api_keys (tenant_id, id);

CREATE INDEX IF NOT EXISTS api_keys_active_hash_idx
    ON api_keys (key_hash)
    WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS context_refs (
    tenant_id text NOT NULL,
    id text NOT NULL,
    type text NOT NULL,
    uri text NOT NULL,
    hash text NOT NULL DEFAULT '',
    classification text NOT NULL DEFAULT '',
    access_scope jsonb NOT NULL DEFAULT '[]'::jsonb,
    expires_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, id)
);

CREATE INDEX IF NOT EXISTS context_refs_task_idx
    ON context_refs (tenant_id, type);

CREATE TABLE IF NOT EXISTS task_context_refs (
    tenant_id text NOT NULL,
    task_id text NOT NULL,
    context_ref_id text NOT NULL,
    attached_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, task_id, context_ref_id)
);

CREATE TABLE IF NOT EXISTS outbox_events (
    id text PRIMARY KEY,
    tenant_id text NOT NULL,
    kind text NOT NULL,
    dedupe_key text NOT NULL,
    payload jsonb NOT NULL,
    status text NOT NULL DEFAULT 'pending',
    attempts integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz,
    last_error text,
    locked_by text,
    locked_at timestamptz,
    lease_expires_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);

CREATE UNIQUE INDEX IF NOT EXISTS outbox_events_dedupe_idx
    ON outbox_events (tenant_id, dedupe_key);

CREATE INDEX IF NOT EXISTS outbox_pending_idx
    ON outbox_events (status, created_at)
    WHERE status IN ('pending', 'retry', 'publishing');

CREATE INDEX IF NOT EXISTS outbox_publishing_lease_idx
    ON outbox_events (lease_expires_at)
    WHERE status = 'publishing';

CREATE OR REPLACE FUNCTION janus_notify_outbox_ready()
RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('janus_outbox_ready', NEW.tenant_id);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS outbox_ready_notify ON outbox_events;
CREATE TRIGGER outbox_ready_notify
AFTER INSERT ON outbox_events
FOR EACH ROW
WHEN (NEW.status IN ('pending', 'retry'))
EXECUTE FUNCTION janus_notify_outbox_ready();

COMMIT;
