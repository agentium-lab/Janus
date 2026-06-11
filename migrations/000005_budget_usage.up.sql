create table budget_usage (
    tenant_id   text not null,
    scope_type  text not null,
    scope_id    text not null,
    period      text not null,
    period_key  text not null,
    tokens_used  integer not null default 0,
    cost_used    numeric(18,6) not null default 0,
    task_count   integer not null default 0,
    primary key (tenant_id, scope_type, scope_id, period, period_key)
);
