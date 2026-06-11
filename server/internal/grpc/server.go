package grpc

import (
	"fmt"
	"net"

	pb "github.com/agentium-lab/Janus/proto/gen/janus/v1"
	svc "github.com/agentium-lab/Janus/server/internal/service"
	"google.golang.org/grpc"
)

type Server struct {
	grpcServer *grpc.Server
	addr       string
}

func NewServer(port int, agentSvc *svc.AgentService, taskSvc *svc.TaskService, dispatchSvc *svc.DispatchService, eventSvc *svc.EventService) *Server {
	s := grpc.NewServer()
	pb.RegisterAgentServiceServer(s, NewAgentServiceServer(agentSvc))
	pb.RegisterTaskServiceServer(s, NewTaskServiceServer(taskSvc))
	pb.RegisterDispatchServiceServer(s, NewDispatchServiceServer(dispatchSvc))
	pb.RegisterAuditServiceServer(s, NewAuditServiceServer(eventSvc))
	return &Server{
		grpcServer: s,
		addr:       fmt.Sprintf(":%d", port),
	}
}

func (s *Server) Start() error {
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("grpc listen: %w", err)
	}
	return s.grpcServer.Serve(lis)
}

func (s *Server) Stop() {
	s.grpcServer.GracefulStop()
}
