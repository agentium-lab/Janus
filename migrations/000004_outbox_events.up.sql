create table outbox_events (
    id           text primary key,
    tenant_id    text not null,
    kind         text not null,
    payload      jsonb not null,
    status       text not null default 'pending',
    attempts     integer not null default 0,
    created_at   timestamptz not null default now(),
    published_at timestamptz
);

create index outbox_pending_idx on outbox_events (status, created_at) where status = 'pending';
