package grpc

import (
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// toGRPCError maps a business error from the service layer to the appropriate
// gRPC status code per docs/Janus-api-contract.md §2.
//
// Mapping:
//
//	validation → InvalidArgument (400)
//	not found  → NotFound (404)
//	policy denied → PermissionDenied (403)
//	budget/quota/concurrency → ResourceExhausted (429)
//	conflict/duplicate → AlreadyExists (409)
//	queue/Redis/NATS unavailable → Unavailable (503)
//	DB/transaction → Internal (500)
func toGRPCError(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	lower := strings.ToLower(msg)

	var code codes.Code
	switch {
	case strings.Contains(lower, "not found") || strings.Contains(lower, "no rows"):
		code = codes.NotFound
	case strings.Contains(lower, "invalid") || strings.Contains(lower, "required") || strings.Contains(lower, "must be"):
		code = codes.InvalidArgument
	case strings.Contains(lower, "denied") || strings.Contains(lower, "permission") || strings.Contains(lower, "forbidden"):
		code = codes.PermissionDenied
	case strings.Contains(lower, "exhausted") || strings.Contains(lower, "throttle") || strings.Contains(lower, "concurrency") || strings.Contains(lower, "budget"):
		code = codes.ResourceExhausted
	case strings.Contains(lower, "conflict") || strings.Contains(lower, "already exists") || strings.Contains(lower, "duplicate"):
		code = codes.AlreadyExists
	case strings.Contains(lower, "unavailable") || strings.Contains(lower, "nats") || strings.Contains(lower, "redis"):
		code = codes.Unavailable
	case strings.Contains(lower, "unauthenticated") || strings.Contains(lower, "api key"):
		code = codes.Unauthenticated
	default:
		code = codes.Internal
	}
	return status.Error(code, msg)
}
