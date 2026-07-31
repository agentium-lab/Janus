package grpc

import (
	"context"
	"errors"
	"testing"

	"github.com/agentium-lab/Janus/server/internal/auth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type fakeValidator struct {
	tenant string
	err    error
}

func (f *fakeValidator) Validate(_ context.Context, _ string) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	return f.tenant, nil
}

type fakeTenantedReq struct{ tenant string }

func (r *fakeTenantedReq) GetTenantId() string { return r.tenant }

type bareReq struct{}

func ctxWithKey(key string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-api-key", key))
}

func ctxWithBearer(key string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+key))
}

func TestAuthInterceptor_MissingKey(t *testing.T) {
	v := &fakeValidator{tenant: "acme"}
	_, err := AuthInterceptor(v)(context.Background(), &bareReq{}, nil, func(context.Context, interface{}) (interface{}, error) {
		t.Fatal("handler must not run without a key")
		return nil, nil
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("want Unauthenticated, got %v", err)
	}
}

func TestAuthInterceptor_InvalidKey(t *testing.T) {
	v := &fakeValidator{err: errors.New("no rows")}
	_, err := AuthInterceptor(v)(ctxWithKey("janus_bad"), &bareReq{}, nil, func(context.Context, interface{}) (interface{}, error) {
		t.Fatal("handler must not run on invalid key")
		return nil, nil
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("want Unauthenticated, got %v", err)
	}
}

func TestAuthInterceptor_ValidKey_InjectsTenant(t *testing.T) {
	v := &fakeValidator{tenant: "acme"}
	var got context.Context
	_, err := AuthInterceptor(v)(ctxWithKey("janus_good"), &bareReq{}, nil, func(ctx context.Context, _ interface{}) (interface{}, error) {
		got = ctx
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if auth.TenantFromContext(got) != "acme" {
		t.Fatalf("tenant not injected into context: %q", auth.TenantFromContext(got))
	}
}

func TestAuthInterceptor_TenantMismatch(t *testing.T) {
	v := &fakeValidator{tenant: "acme"}
	req := &fakeTenantedReq{tenant: "other"}
	_, err := AuthInterceptor(v)(ctxWithKey("janus_good"), req, nil, func(context.Context, interface{}) (interface{}, error) {
		t.Fatal("handler must not run on tenant mismatch")
		return nil, nil
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("want PermissionDenied, got %v", err)
	}
}

func TestAuthInterceptor_TenantMatch(t *testing.T) {
	v := &fakeValidator{tenant: "acme"}
	req := &fakeTenantedReq{tenant: "acme"}
	called := false
	_, err := AuthInterceptor(v)(ctxWithKey("janus_good"), req, nil, func(context.Context, interface{}) (interface{}, error) {
		called = true
		return nil, nil
	})
	if err != nil || !called {
		t.Fatalf("want success on matching tenant, err=%v called=%v", err, called)
	}
}

func TestAuthInterceptor_EmptyReqTenant_Allowed(t *testing.T) {
	v := &fakeValidator{tenant: "acme"}
	req := &fakeTenantedReq{tenant: ""}
	called := false
	_, err := AuthInterceptor(v)(ctxWithKey("janus_good"), req, nil, func(context.Context, interface{}) (interface{}, error) {
		called = true
		return nil, nil
	})
	if err != nil || !called {
		t.Fatalf("empty request tenant should pass, err=%v", err)
	}
}

func TestAuthInterceptor_BearerToken(t *testing.T) {
	v := &fakeValidator{tenant: "acme"}
	called := false
	_, err := AuthInterceptor(v)(ctxWithBearer("janus_good"), &bareReq{}, nil, func(context.Context, interface{}) (interface{}, error) {
		called = true
		return nil, nil
	})
	if err != nil || !called {
		t.Fatalf("bearer token should authenticate, err=%v", err)
	}
}
