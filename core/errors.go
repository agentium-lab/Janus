package core

import "errors"

type PermissionDeniedError struct {
	Message string
}

// PermissionDeniedError is returned when an authenticated actor cannot access
// a tenant resource.
func (e *PermissionDeniedError) Error() string {
	if e == nil || e.Message == "" {
		return "permission denied"
	}
	return e.Message
}

// ErrTaskInTerminalState is returned when a lifecycle transition is
// attempted on a task already in a terminal state.
var ErrTaskInTerminalState = errors.New("task is in a terminal state")

// ErrInvalidTransition is returned when the requested state transition is
// not permitted by the task state machine.
var ErrInvalidTransition = errors.New("invalid state transition")

// IsNotCancelable reports whether the error indicates the task cannot be
// canceled in its current state (terminal or invalid transition).
func IsNotCancelable(err error) bool {
	return errors.Is(err, ErrTaskInTerminalState) || errors.Is(err, ErrInvalidTransition)
}
