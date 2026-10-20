package grpc

import (
	"crypto/tls"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScopeForMethod(t *testing.T) {
	assert.Equal(t, "task:write", scopeForMethod("/janus.v1.TaskService/CreateTask"))
	assert.Equal(t, "task:write", scopeForMethod("/janus.v1.DispatchService/AckTask"))
	assert.Equal(t, "task:write", scopeForMethod("/janus.v1.MailboxService/PauseMailbox"))
	assert.Equal(t, "task:write", scopeForMethod("/janus.v1.TaskService/ReplayTask"), "task /replay stays task:write (only dlq and audit replay are admin)")
	assert.Equal(t, "task:read", scopeForMethod("/janus.v1.TaskService/GetTask"))
	assert.Equal(t, "task:read", scopeForMethod("/janus.v1.AuditService/ListEvents"))
	assert.Equal(t, "task:read", scopeForMethod("/janus.v1.DLQService/QueryDLQ"))
	assert.Equal(t, "admin", scopeForMethod("/janus.v1.DLQService/ReplayDLQ"), "gRPC must match the HTTP dlq /replay admin rule")
	assert.Equal(t, "admin", scopeForMethod("/janus.v1.DLQService/DiscardDLQ"), "gRPC must match the HTTP dlq /discard admin rule")
	assert.Equal(t, "audit:read", scopeForMethod("/janus.v1.AuditService/GetTrace"), "gRPC must match the HTTP traces audit:read rule")
	assert.Equal(t, "task:write", scopeForMethod("/janus.v1.TaskService/UndocumentedFutureMutation"), "unmapped methods fail closed")
}

func TestNewServer_WithTLSOption(t *testing.T) {
	srv := NewServer(0, nil, nil, nil, nil, nil, nil, nil,
		WithTLS(&tls.Config{MinVersion: tls.VersionTLS12}),
		WithTLS(nil),
	)
	require.NotNil(t, srv)
	assert.NotNil(t, srv.creds, "TLS config must install transport credentials")
}
