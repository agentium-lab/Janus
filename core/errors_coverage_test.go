package core

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPermissionDeniedError_Error(t *testing.T) {
	assert.Equal(t, "permission denied", (&PermissionDeniedError{}).Error())
	var nilErr *PermissionDeniedError
	assert.Equal(t, "permission denied", nilErr.Error())
	assert.Equal(t, "tenant mismatch", (&PermissionDeniedError{Message: "tenant mismatch"}).Error())
}

func TestBackpressureError_Error(t *testing.T) {
	err := &BackpressureError{Reason: ReasonAgentConcurrencyExceeded, Message: "1/1 concurrent tasks"}
	assert.Equal(t, "agent_concurrency_exceeded: 1/1 concurrent tasks", err.Error())
}

func TestIsNotCancelable(t *testing.T) {
	assert.True(t, IsNotCancelable(fmt.Errorf("wrap: %w", ErrTaskInTerminalState)))
	assert.True(t, IsNotCancelable(ErrInvalidTransition))
	assert.False(t, IsNotCancelable(errors.New("unrelated")))
}
