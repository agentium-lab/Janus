package service

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/driver/postgres"
)

type EventRepo interface {
	Insert(ctx context.Context, evt core.JanusEvent) error
	ListByTask(ctx context.Context, tenantID, taskID string, limit int) ([]*core.JanusEvent, error)
	ListByTrace(ctx context.Context, tenantID, traceID string, limit int) ([]*core.JanusEvent, error)
	ListByTenant(ctx context.Context, tenantID string, limit int) ([]*core.JanusEvent, error)
}

type EventService struct {
	repo EventRepo
}

func NewEventService(repo EventRepo) *EventService {
	return &EventService{repo: repo}
}

func (s *EventService) Record(ctx context.Context, evt core.JanusEvent) error {
	if err := enrichEvent(&evt); err != nil {
		return err
	}
	if evt.Payload == nil {
		evt.Payload = []byte(`{}`)
	}
	return s.repo.Insert(ctx, evt)
}

func (s *EventService) QueryByTask(ctx context.Context, tenantID, taskID string, limit int) ([]*core.JanusEvent, error) {
	if limit <= 0 {
		limit = 50
	}
	return s.repo.ListByTask(ctx, tenantID, taskID, limit)
}

func (s *EventService) QueryByTrace(ctx context.Context, tenantID, traceID string, limit int) ([]*core.JanusEvent, error) {
	if limit <= 0 {
		limit = 50
	}
	return s.repo.ListByTrace(ctx, tenantID, traceID, limit)
}

func (s *EventService) QueryByTenant(ctx context.Context, tenantID string, limit int) ([]*core.JanusEvent, error) {
	if limit <= 0 {
		limit = 50
	}
	return s.repo.ListByTenant(ctx, tenantID, limit)
}

func (s *EventService) PublishEvent(ctx context.Context, tenantID string, eventType core.EventType, taskID, traceID, sourceAgent string, payload interface{}) error {
	var payloadBytes []byte
	if payload != nil {
		var err error
		payloadBytes, err = json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("marshal event payload: %w", err)
		}
	} else {
		payloadBytes = []byte(`{}`)
	}
	evt := core.JanusEvent{
		EventType:   eventType,
		TenantID:    tenantID,
		TaskID:      taskID,
		TraceID:     traceID,
		SourceAgent: sourceAgent,
		Payload:     payloadBytes,
	}
	return s.Record(ctx, evt)
}

// RecordIdempotent satisfies the outbox.AuditWriter interface. The
// underlying EventRepo.Insert uses ON CONFLICT (tenant_id, event_id)
// DO NOTHING, making re-projection safe.
func (s *EventService) RecordIdempotent(ctx context.Context, evt core.JanusEvent) error {
	return s.Record(ctx, evt)
}

// execEventRepo is implemented by PG-backed event repos: the insert can
// run through a caller-held executor (the audit projector's tick-lock
// connection) instead of borrowing a second pooled connection.
type execEventRepo interface {
	InsertOn(ctx context.Context, exec postgres.Executor, evt core.JanusEvent) error
}

// RecordIdempotentOn is the executor-scoped AuditWriter path: same
// semantics as RecordIdempotent, but the insert runs on the supplied
// connection (the one holding the caller's advisory lock). A nil
// executor, or a repo without executor support, falls back to the
// pool-scoped path.
func (s *EventService) RecordIdempotentOn(ctx context.Context, exec postgres.Executor, evt core.JanusEvent) error {
	if exec != nil {
		if er, ok := s.repo.(execEventRepo); ok {
			if err := enrichEvent(&evt); err != nil {
				return err
			}
			if evt.Payload == nil {
				evt.Payload = []byte(`{}`)
			}
			return er.InsertOn(ctx, exec, evt)
		}
	}
	return s.RecordIdempotent(ctx, evt)
}
