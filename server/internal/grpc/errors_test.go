package grpc

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestToGRPCError_NilIsNil(t *testing.T) {
	assert.Nil(t, toGRPCError(nil))
}

func TestToGRPCError_NotFound(t *testing.T) {
	cases := []string{
		"task not found",
		"agent not found",
		"no rows in result set",
	}
	for _, msg := range cases {
		st, ok := status.FromError(toGRPCError(errors.New(msg)))
		assert.True(t, ok)
		assert.Equal(t, codes.NotFound, st.Code(), "msg: %s", msg)
	}
}

func TestToGRPCError_InvalidArgument(t *testing.T) {
	cases := []string{
		"tenant id is required",
		"invalid lease",
		"status must be queued",
	}
	for _, msg := range cases {
		st, _ := status.FromError(toGRPCError(errors.New(msg)))
		assert.Equal(t, codes.InvalidArgument, st.Code(), "msg: %s", msg)
	}
}

func TestToGRPCError_PermissionDenied(t *testing.T) {
	st, _ := status.FromError(toGRPCError(errors.New("policy denied")))
	assert.Equal(t, codes.PermissionDenied, st.Code())
}

func TestToGRPCError_ResourceExhausted(t *testing.T) {
	cases := []string{
		"agent concurrency exceeded",
		"budget exhausted",
		"rate throttle",
	}
	for _, msg := range cases {
		st, _ := status.FromError(toGRPCError(errors.New(msg)))
		assert.Equal(t, codes.ResourceExhausted, st.Code(), "msg: %s", msg)
	}
}

func TestToGRPCError_AlreadyExists(t *testing.T) {
	cases := []string{
		"conflict: task changed concurrently",
		"already exists",
		"duplicate key",
	}
	for _, msg := range cases {
		st, _ := status.FromError(toGRPCError(errors.New(msg)))
		assert.Equal(t, codes.AlreadyExists, st.Code(), "msg: %s", msg)
	}
}

func TestToGRPCError_Unavailable(t *testing.T) {
	st, _ := status.FromError(toGRPCError(errors.New("nats connection failed")))
	assert.Equal(t, codes.Unavailable, st.Code())
}

func TestToGRPCError_Internal(t *testing.T) {
	st, _ := status.FromError(toGRPCError(errors.New("unexpected database error")))
	assert.Equal(t, codes.Internal, st.Code())
}

func TestToGRPCError_PreservesMessage(t *testing.T) {
	st, _ := status.FromError(toGRPCError(errors.New("task not found: task-123")))
	assert.Contains(t, st.Message(), "task-123")
}
