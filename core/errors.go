package core

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