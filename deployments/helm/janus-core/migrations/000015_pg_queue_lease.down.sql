drop index if exists tasks_queue_ready_idx;
alter table tasks drop column if exists queue_lease_until;
