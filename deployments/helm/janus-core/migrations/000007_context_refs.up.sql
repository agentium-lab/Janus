create table context_refs (
    tenant_id text not null,
    id text not null,
    type text not null,
    uri text not null,
    hash text not null default '',
    classification text not null default '',
    access_scope jsonb not null default '[]',
    expires_at timestamptz,
    created_at timestamptz not null default now(),
    primary key (tenant_id, id)
);

create index context_refs_task_idx on context_refs (tenant_id, type);

create table task_context_refs (
    tenant_id text not null,
    task_id text not null,
    context_ref_id text not null,
    attached_at timestamptz not null default now(),
    primary key (tenant_id, task_id, context_ref_id)
);
