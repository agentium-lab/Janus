package service

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/agentium-lab/Janus/core"
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
