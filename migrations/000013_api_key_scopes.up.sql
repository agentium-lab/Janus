alter table api_keys add column if not exists scopes text[] not null default '{}';
