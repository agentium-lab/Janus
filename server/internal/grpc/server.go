package grpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"

	pb "github.com/agentium-lab/Janus/proto/gen/janus/v1"
	"github.com/agentium-lab/Janus/server/internal/handler"
	svc "github.com/agentium-lab/Janus/server/internal/service"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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
	creds      credentials.TransportCredentials
	grpcServer *grpc.Server
	addr       string
	listen     listenFunc
}

// Option configures the gRPC server at construction time.
type Option func(*Server)

// WithTLS enables transport credentials on the server listener. Without it
// the gRPC endpoint serves plaintext regardless of the HTTP TLS settings.
func WithTLS(cfg *tls.Config) Option {
	return func(s *Server) {
		if cfg != nil {
			s.creds = credentials.NewTLS(cfg.Clone())
		}
	}
}

func NewServer(port int, validator APIKeyValidator, agentSvc *svc.AgentService, taskSvc *svc.TaskService, dispatchSvc *svc.DispatchService, eventSvc *svc.EventService, mailboxSvc *svc.MailboxService, dlqSvc *handler.DLQServiceAdapter, opts ...Option) *Server {
	var srv Server
	for _, opt := range opts {
		opt(&srv)
	}
	serverOpts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(AuthInterceptor(validator), errorMappingInterceptor),
	}
	if srv.creds != nil {
		serverOpts = append(serverOpts, grpc.Creds(srv.creds))
	}
	gs := grpc.NewServer(serverOpts...)
	pb.RegisterAgentServiceServer(gs, NewAgentServiceServer(agentSvc))
	pb.RegisterTaskServiceServer(gs, NewTaskServiceServer(taskSvc))
	pb.RegisterDispatchServiceServer(gs, NewDispatchServiceServer(dispatchSvc))
	pb.RegisterAuditServiceServer(gs, NewAuditServiceServer(eventSvc))
	pb.RegisterMailboxServiceServer(gs, NewMailboxServiceServer(mailboxSvc))
	pb.RegisterDLQServiceServer(gs, NewDLQServiceServer(dlqSvc))
	srv.grpcServer = gs
	srv.addr = fmt.Sprintf(":%d", port)
	if srv.listen == nil {
		srv.listen = defaultListen
	}
	return &srv
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
