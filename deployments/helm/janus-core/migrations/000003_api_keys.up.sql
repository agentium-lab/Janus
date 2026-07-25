create table api_keys (
    tenant_id  text not null,
    key_hash   text not null,
    name       text not null,
    prefix     text not null,
    created_at timestamptz not null default now(),
    primary key (tenant_id, key_hash)
);

create index api_keys_prefix_idx on api_keys (prefix);
