package grpc

import (
	"context"

	"github.com/agentium-lab/Janus/core"
	pb "github.com/agentium-lab/Janus/proto/gen/janus/v1"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type auditService interface {
	QueryByTask(ctx context.Context, tenantID, taskID string, limit int) ([]*core.JanusEvent, error)
	QueryByTrace(ctx context.Context, tenantID, traceID string, limit int) ([]*core.JanusEvent, error)
	QueryByTenant(ctx context.Context, tenantID string, limit int) ([]*core.JanusEvent, error)
}

type AuditServiceServer struct {
	pb.UnimplementedAuditServiceServer
	svc auditService
}

func NewAuditServiceServer(svc auditService) *AuditServiceServer {
	return &AuditServiceServer{svc: svc}
}

func (s *AuditServiceServer) ListEvents(ctx context.Context, req *pb.ListEventsRequest) (*pb.ListEventsResponse, error) {
	switch {
	case req.TaskId != "":
		events, err := s.svc.QueryByTask(ctx, req.TenantId, req.TaskId, int(req.PageSize))
		if err != nil {
			return nil, err
		}
		return &pb.ListEventsResponse{Events: eventsToProto(events)}, nil
	case req.TraceId != "":
		events, err := s.svc.QueryByTrace(ctx, req.TenantId, req.TraceId, int(req.PageSize))
		if err != nil {
			return nil, err
		}
		return &pb.ListEventsResponse{Events: eventsToProto(events)}, nil
	default:
		events, err := s.svc.QueryByTenant(ctx, req.TenantId, int(req.PageSize))
		if err != nil {
			return nil, err
		}
		return &pb.ListEventsResponse{Events: eventsToProto(events)}, nil
	}
}

func (s *AuditServiceServer) GetTrace(ctx context.Context, req *pb.GetTraceRequest) (*pb.GetTraceResponse, error) {
	events, err := s.svc.QueryByTrace(ctx, req.TenantId, req.TraceId, 100)
	if err != nil {
		return nil, err
	}
	return &pb.GetTraceResponse{Events: eventsToProto(events)}, nil
}

func (s *AuditServiceServer) ListTaskEvents(ctx context.Context, req *pb.ListTaskEventsRequest) (*pb.ListTaskEventsResponse, error) {
	events, err := s.svc.QueryByTask(ctx, req.TenantId, req.TaskId, int(req.PageSize))
	if err != nil {
		return nil, err
	}
	return &pb.ListTaskEventsResponse{Events: eventsToProto(events)}, nil
}

func eventsToProto(events []*core.JanusEvent) []*pb.JanusEvent {
	result := make([]*pb.JanusEvent, 0, len(events))
	for _, e := range events {
		pbEvt := &pb.JanusEvent{
			EventId:     e.EventID,
			EventType:   string(e.EventType),
			Timestamp:   timestamppb.New(e.Timestamp),
			TenantId:    e.TenantID,
			TraceId:     e.TraceID,
			TaskId:      e.TaskID,
			SourceAgent: e.SourceAgent,
		}
		if e.Payload != nil {
			var s structpb.Struct
			if err := s.UnmarshalJSON(e.Payload); err == nil {
				pbEvt.Payload = &s
			}
		}
		result = append(result, pbEvt)
	}
	return result
}
