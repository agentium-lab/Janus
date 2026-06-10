create table tenants (
    id text primary key,
    name text not null,
    status text not null default 'active',
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table agents (
    id text not null,
    tenant_id text not null references tenants(id),
    display_name text not null,
    protocol text not null,
    endpoint text,
    status text not null default 'offline',
    description text,
    metadata jsonb not null default '{}',
    max_concurrency integer not null default 1,
    rpm integer,
    tpm integer,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    last_heartbeat_at timestamptz,
    primary key (tenant_id, id)
);

create table agent_capabilities (
    tenant_id text not null,
    agent_id text not null,
    capability text not null,
    schema jsonb,
    description text,
    embedding_ref text,
    created_at timestamptz not null default now(),
    primary key (tenant_id, agent_id, capability)
);

create table mailboxes (
    tenant_id text not null,
    id text not null,
    agent_id text not null,
    status text not null default 'active',
    priority text not null default 'normal',
    max_concurrency integer not null default 1,
    ack_wait_seconds integer not null default 300,
    max_deliver integer not null default 5,
    retention_seconds integer not null default 604800,
    retry_policy jsonb not null default '{"max_attempts":5,"backoff_type":"exponential","initial_seconds":10,"max_seconds":900,"jitter":true}',
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (tenant_id, id)
);

create table tasks (
    tenant_id text not null,
    id text not null,
    idempotency_key text,
    source_agent text not null,
    target_type text not null,
    target_value text not null,
    mailbox_id text,
    status text not null,
    priority text not null default 'normal',
    deadline timestamptz,
    ttl_seconds integer,
    envelope jsonb not null,
    result_ref text,
    error jsonb,
    attempt_count integer not null default 0,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    completed_at timestamptz,
    primary key (tenant_id, id)
);

create unique index tasks_idempotency_idx
    on tasks (tenant_id, idempotency_key)
    where idempotency_key is not null;

create index tasks_status_idx
    on tasks (tenant_id, status, priority, created_at);

create table task_attempts (
    tenant_id text not null,
    task_id text not null,
    attempt integer not null,
    agent_id text not null,
    lease_id text not null,
    status text not null,
    started_at timestamptz not null default now(),
    heartbeat_at timestamptz,
    finished_at timestamptz,
    error jsonb,
    token_usage jsonb,
    primary key (tenant_id, task_id, attempt)
);

create table budgets (
    tenant_id text not null,
    scope_type text not null,
    scope_id text not null,
    rpm integer,
    tpm integer,
    max_concurrency integer,
    daily_cost_usd numeric(18, 6),
    monthly_cost_usd numeric(18, 6),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (tenant_id, scope_type, scope_id)
);

create table policy_rules (
    tenant_id text not null,
    id text not null,
    name text not null,
    status text not null default 'active',
    priority integer not null default 100,
    condition jsonb not null,
    action jsonb not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (tenant_id, id)
);

create table approvals (
    tenant_id text not null,
    id text not null,
    task_id text not null,
    status text not null default 'pending',
    requested_by text not null,
    approver text,
    reason text,
    decision text,
    expires_at timestamptz,
    created_at timestamptz not null default now(),
    decided_at timestamptz,
    primary key (tenant_id, id)
);

create table audit_event_projection (
    tenant_id text not null,
    event_id text not null,
    event_type text not null,
    task_id text,
    agent_id text,
    trace_id text,
    occurred_at timestamptz not null,
    payload jsonb not null,
    primary key (tenant_id, event_id)
);

create index audit_event_task_idx
    on audit_event_projection (tenant_id, task_id, occurred_at);

create index audit_event_trace_idx
    on audit_event_projection (tenant_id, trace_id, occurred_at);

create table outbox_events (
    id text primary key,
    tenant_id text not null,
    kind text not null,
    payload jsonb not null,
    status text not null default 'pending',
    attempts integer not null default 0,
    created_at timestamptz not null default now(),
    published_at timestamptz
);
