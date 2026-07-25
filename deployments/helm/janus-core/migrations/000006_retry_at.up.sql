alter table tasks add column retry_at timestamptz;
create index tasks_retry_at_idx on tasks (status, retry_at) where status = 'retry_scheduled';
