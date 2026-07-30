package grpc

import (
	"context"
	"fmt"
	"net"
	"time"

	pb "github.com/agentium-lab/Janus/proto/gen/janus/v1"
	"github.com/agentium-lab/Janus/server/internal/handler"
	svc "github.com/agentium-lab/Janus/server/internal/service"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

var (
	grpcRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "janus_grpc_requests_total", Help: "Total gRPC requests"},
		[]string{"method", "status"},
	)
	grpcRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{Name: "janus_grpc_request_duration_seconds", Help: "gRPC request duration"},
		[]string{"method"},
	)
)

type listenFunc func(network, addr string) (net.Listener, error)

var defaultListen listenFunc = net.Listen

type Server struct {
	grpcServer *grpc.Server
	addr       string
	listen     listenFunc
}

func NewServer(port int, agentSvc *svc.AgentService, taskSvc *svc.TaskService, dispatchSvc *svc.DispatchService, eventSvc *svc.EventService, mailboxSvc *svc.MailboxService, dlqSvc *handler.DLQServiceAdapter) *Server {
	s := grpc.NewServer(grpc.UnaryInterceptor(errorMappingInterceptor))
	pb.RegisterAgentServiceServer(s, NewAgentServiceServer(agentSvc))
	pb.RegisterTaskServiceServer(s, NewTaskServiceServer(taskSvc))
	pb.RegisterDispatchServiceServer(s, NewDispatchServiceServer(dispatchSvc))
	pb.RegisterAuditServiceServer(s, NewAuditServiceServer(eventSvc))
	pb.RegisterMailboxServiceServer(s, NewMailboxServiceServer(mailboxSvc))
	pb.RegisterDLQServiceServer(s, NewDLQServiceServer(dlqSvc))
	return &Server{
		grpcServer: s,
		addr:       fmt.Sprintf(":%d", port),
		listen:     defaultListen,
	}
}

// errorMappingInterceptor maps service-layer errors to gRPC status codes via
// toGRPCError, so handlers can return plain errors without explicit mapping.
func errorMappingInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	start := time.Now()
	method := ""
	if info != nil {
		method = info.FullMethod
	}
	resp, err := handler(ctx, req)
	if err != nil {
		mapped := toGRPCError(err)
		st, _ := status.FromError(mapped)
		grpcRequestsTotal.WithLabelValues(method, st.Code().String()).Inc()
		grpcRequestDuration.WithLabelValues(method).Observe(time.Since(start).Seconds())
		return nil, mapped
	}
	grpcRequestsTotal.WithLabelValues(method, "OK").Inc()
	grpcRequestDuration.WithLabelValues(method).Observe(time.Since(start).Seconds())
	return resp, nil
}

func (s *Server) Start() error {
	lis, err := s.listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("grpc listen: %w", err)
	}
	return s.grpcServer.Serve(lis)
}

func (s *Server) Stop() {
	s.grpcServer.GracefulStop()
}
