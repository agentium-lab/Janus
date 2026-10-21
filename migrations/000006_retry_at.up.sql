alter table tasks add column if not exists retry_at timestamptz;
create index if not exists tasks_retry_at_idx on tasks (status, retry_at) where status = 'retry_scheduled';
