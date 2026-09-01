alter table tasks add column if not exists queue_lease_until timestamptz;
create index if not exists tasks_queue_ready_idx
    on tasks (mailbox_id, priority desc, created_at)
    where status = 'queued';
