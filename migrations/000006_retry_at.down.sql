drop index if exists tasks_retry_at_idx;
alter table tasks drop column if exists retry_at;
