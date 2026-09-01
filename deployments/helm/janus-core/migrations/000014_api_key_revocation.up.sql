-- Aligns api_keys with migrations/schema.sql: id / last_used_at / revoked_at
-- existed only in the mirror until now. revoked_at is required by key
-- revocation (ValidatePrincipal rejects non-null) and C.3 management API.
alter table api_keys add column if not exists id text;
alter table api_keys add column if not exists last_used_at timestamptz;
alter table api_keys add column if not exists revoked_at timestamptz;

update api_keys
set id = 'key_' || substr(md5(random()::text || clock_timestamp()::text), 1, 24)
where id is null;

alter table api_keys alter column id set not null;
alter table api_keys
    alter column id set default ('key_' || substr(md5(random()::text || clock_timestamp()::text), 1, 24));

create unique index if not exists api_keys_tenant_id_idx on api_keys (tenant_id, id);
