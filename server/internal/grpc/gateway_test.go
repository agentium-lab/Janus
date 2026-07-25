package grpc

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestSetHTTPStatus_WithStatus(t *testing.T) {
	md := metadata.Pairs("x-http-status", "201")
	ctx := metadata.NewIncomingContext(context.Background(), md)
	smd := runtime.ServerMetadata{
		HeaderMD: md,
	}
	ctx = runtime.NewServerMetadataContext(ctx, smd)

	w := httptest.NewRecorder()
	err := setHTTPStatus(ctx, w, &emptypb.Empty{})
	assert.NoError(t, err)
	assert.Equal(t, 201, w.Code)
}

func TestSetHTTPStatus_InvalidStatus(t *testing.T) {
	md := metadata.Pairs("x-http-status", "not-a-number")
	ctx := metadata.NewIncomingContext(context.Background(), md)
	smd := runtime.ServerMetadata{
		HeaderMD: md,
	}
	ctx = runtime.NewServerMetadataContext(ctx, smd)

	w := httptest.NewRecorder()
	err := setHTTPStatus(ctx, w, &emptypb.Empty{})
	assert.NoError(t, err)
	assert.Equal(t, 200, w.Code)
}

func TestSetHTTPStatus_NoMetadata(t *testing.T) {
	ctx := context.Background()

	w := httptest.NewRecorder()
	err := setHTTPStatus(ctx, w, &emptypb.Empty{})
	assert.NoError(t, err)
	assert.Equal(t, 200, w.Code)
}

func TestSetHTTPStatus_NoStatusHeader(t *testing.T) {
	md := metadata.Pairs("other-header", "value")
	ctx := metadata.NewIncomingContext(context.Background(), md)
	smd := runtime.ServerMetadata{
		HeaderMD: md,
	}
	ctx = runtime.NewServerMetadataContext(ctx, smd)

	w := httptest.NewRecorder()
	err := setHTTPStatus(ctx, w, &emptypb.Empty{})
	assert.NoError(t, err)
	assert.Equal(t, 200, w.Code)
}
