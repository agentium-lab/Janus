drop index if exists api_keys_tenant_id_idx;
alter table api_keys drop column if exists revoked_at;
alter table api_keys drop column if exists last_used_at;
alter table api_keys drop column if exists id;
