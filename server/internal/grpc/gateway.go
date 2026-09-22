package grpc

import (
	"context"
	"crypto/tls"
	"net/http"
	"strconv"
	"strings"

	pb "github.com/agentium-lab/Janus/proto/gen/janus/v1"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// passthroughHeaders are identity/auth headers the gRPC AuthInterceptor and
// the identity guard read from metadata. grpc-gateway's DefaultHeaderMatcher
// DROPS non-permanent headers (X-API-Key never reaches the backend) and
// prefixes permanent ones (Authorization would become grpcgateway-* without
// its built-in pass-through), so these must be forwarded explicitly.
var passthroughHeaders = map[string]struct{}{
	"x-api-key":           {},
	"authorization":       {},
	"x-tenant-id":         {},
	"x-janus-acting-user": {},
}

func headerMatcher(key string) (string, bool) {
	lower := strings.ToLower(key)
	if _, ok := passthroughHeaders[lower]; ok {
		return lower, true
	}
	return runtime.DefaultHeaderMatcher(key)
}

func RegisterGateway(ctx context.Context, grpcAddr string, tlsCfg *tls.Config) (http.Handler, error) {
	// Use proto field names (snake_case) in JSON, matching the HTTP handlers.
	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{
			MarshalOptions:   protojson.MarshalOptions{UseProtoNames: true},
			UnmarshalOptions: protojson.UnmarshalOptions{DiscardUnknown: true},
		}),
		// Allow handlers to override HTTP status via metadata.
		runtime.WithForwardResponseOption(setHTTPStatus),
		runtime.WithIncomingHeaderMatcher(headerMatcher),
	)
	opts := []grpc.DialOption{}
	if tlsCfg != nil {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg.Clone())))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	if err := pb.RegisterAgentServiceHandlerFromEndpoint(ctx, mux, grpcAddr, opts); err != nil {
		return nil, err
	}
	if err := pb.RegisterTaskServiceHandlerFromEndpoint(ctx, mux, grpcAddr, opts); err != nil {
		return nil, err
	}
	if err := pb.RegisterDispatchServiceHandlerFromEndpoint(ctx, mux, grpcAddr, opts); err != nil {
		return nil, err
	}
	if err := pb.RegisterAuditServiceHandlerFromEndpoint(ctx, mux, grpcAddr, opts); err != nil {
		return nil, err
	}
	if err := pb.RegisterMailboxServiceHandlerFromEndpoint(ctx, mux, grpcAddr, opts); err != nil {
		return nil, err
	}
	if err := pb.RegisterDLQServiceHandlerFromEndpoint(ctx, mux, grpcAddr, opts); err != nil {
		return nil, err
	}

	return mux, nil
}

// setHTTPStatus allows gRPC handlers to set a custom HTTP status code via
// gRPC metadata key "x-http-status" (e.g. "201" for create RPCs).
func setHTTPStatus(ctx context.Context, w http.ResponseWriter, resp proto.Message) error {
	md, ok := runtime.ServerMetadataFromContext(ctx)
	if !ok {
		return nil
	}
	if vals := md.HeaderMD.Get("x-http-status"); len(vals) > 0 {
		if code, err := strconv.Atoi(vals[0]); err == nil {
			w.WriteHeader(code)
		}
	}
	return nil
}
