package postgres

import (
	"context"
	"fmt"

	"github.com/agentium-lab/Janus/core"
	"github.com/jackc/pgx/v5/pgxpool"
)

type EventRepo struct {
	pool *pgxpool.Pool
}

func NewEventRepo(pool *pgxpool.Pool) *EventRepo {
	return &EventRepo{pool: pool}
}

func (r *EventRepo) Insert(ctx context.Context, evt core.JanusEvent) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO audit_event_projection (tenant_id, event_id, event_type, task_id, agent_id, trace_id, occurred_at, payload)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		evt.TenantID, evt.EventID, evt.EventType, evt.TaskID, evt.SourceAgent, evt.TraceID, evt.Timestamp, evt.Payload,
	)
	return err
}

func (r *EventRepo) ListByTask(ctx context.Context, tenantID, taskID string, limit int) ([]*core.JanusEvent, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := r.pool.Query(ctx,
		`SELECT event_id, event_type, task_id, agent_id, trace_id, occurred_at, payload
		 FROM audit_event_projection
		 WHERE tenant_id = $1 AND task_id = $2
		 ORDER BY occurred_at ASC
		 LIMIT $3`,
		tenantID, taskID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query audit events: %w", err)
	}
	defer rows.Close()

	var events []*core.JanusEvent
	for rows.Next() {
		var evt core.JanusEvent
		var agentID *string
		if err := rows.Scan(&evt.EventID, &evt.EventType, &evt.TaskID, &agentID, &evt.TraceID, &evt.Timestamp, &evt.Payload); err != nil {
			return nil, fmt.Errorf("scan audit event: %w", err)
		}
		if agentID != nil {
			evt.SourceAgent = *agentID
		}
		evt.TenantID = tenantID
		events = append(events, &evt)
	}
	return events, rows.Err()
}

func (r *EventRepo) ListByTrace(ctx context.Context, tenantID, traceID string, limit int) ([]*core.JanusEvent, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := r.pool.Query(ctx,
		`SELECT event_id, event_type, task_id, agent_id, trace_id, occurred_at, payload
		 FROM audit_event_projection
		 WHERE tenant_id = $1 AND trace_id = $2
		 ORDER BY occurred_at ASC
		 LIMIT $3`,
		tenantID, traceID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query audit events by trace: %w", err)
	}
	defer rows.Close()

	var events []*core.JanusEvent
	for rows.Next() {
		var evt core.JanusEvent
		var agentID *string
		if err := rows.Scan(&evt.EventID, &evt.EventType, &evt.TaskID, &agentID, &evt.TraceID, &evt.Timestamp, &evt.Payload); err != nil {
			return nil, fmt.Errorf("scan audit event: %w", err)
		}
		if agentID != nil {
			evt.SourceAgent = *agentID
		}
		evt.TenantID = tenantID
		events = append(events, &evt)
	}
	return events, rows.Err()
}
