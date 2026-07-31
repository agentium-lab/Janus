package grpc

import (
	"context"
	"errors"
	"strings"

	"github.com/agentium-lab/Janus/server/internal/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// APIKeyValidator is the contract AuthInterceptor depends on. The concrete
// *auth.APIKeyValidator satisfies it; tests may substitute a fake.
type APIKeyValidator interface {
	Validate(ctx context.Context, apiKey string) (tenantID string, err error)
}

// tenantedRequest is implemented by any proto request carrying a tenant_id
// field (the generated GetTenantId getter). Used to enforce that a caller
// authenticated as tenant A cannot target tenant B by setting req.TenantId.
type tenantedRequest interface {
	GetTenantId() string
}

// AuthInterceptor validates the API key present in gRPC metadata, injects the
// authenticated tenant into the call context, and rejects requests whose
// tenant_id field does not match the authenticated tenant. It runs before
// errorMappingInterceptor so authn/authz failures map to the right gRPC codes.
func AuthInterceptor(validator APIKeyValidator) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		apiKey, err := apiKeyFromMetadata(ctx)
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, err.Error())
		}
		tenantID, err := validator.Validate(ctx, apiKey)
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, "invalid api key")
		}
		if tr, ok := req.(tenantedRequest); ok {
			reqTenant := tr.GetTenantId()
			if reqTenant != "" && reqTenant != tenantID {
				return nil, status.Error(codes.PermissionDenied, "tenant mismatch: request tenant_id does not match authenticated tenant")
			}
		}
		ctx = context.WithValue(ctx, auth.TenantCtxKey, tenantID)
		ctx = context.WithValue(ctx, auth.APIKeyCtxKey, apiKey[:8]+"...")
		return handler(ctx, req)
	}
}

// apiKeyFromMetadata extracts an API key from incoming gRPC metadata, accepting
// either the x-api-key header or an Authorization: Bearer <key> header.
// gRPC lowercases all metadata keys.
func apiKeyFromMetadata(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", errors.New("missing api key")
	}
	if vals := md.Get("x-api-key"); len(vals) > 0 && strings.TrimSpace(vals[0]) != "" {
		return strings.TrimSpace(vals[0]), nil
	}
	if vals := md.Get("authorization"); len(vals) > 0 {
		a := vals[0]
		if strings.HasPrefix(a, "Bearer ") {
			if k := strings.TrimSpace(strings.TrimPrefix(a, "Bearer ")); k != "" {
				return k, nil
			}
		}
	}
	return "", errors.New("missing api key")
}
