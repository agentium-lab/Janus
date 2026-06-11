package grpc

import (
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

func TestServer_StartStop(t *testing.T) {
	s := grpc.NewServer()
	svr := &Server{
		grpcServer: s,
		addr:       ":0",
		listen:     net.Listen,
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- svr.Start()
	}()

	svr.Stop()
	<-errCh
}

func TestServer_Start_ListenError(t *testing.T) {
	s := grpc.NewServer()
	svr := &Server{
		grpcServer: s,
		addr:       ":0",
		listen: func(_, _ string) (net.Listener, error) {
			return nil, fmt.Errorf("port blocked")
		},
	}

	err := svr.Start()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "grpc listen")
}
