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
	assert.Equal(t, "task:read", scopeForMethod("/janus.v1.TaskService/GetTask"))
	assert.Equal(t, "task:read", scopeForMethod("/janus.v1.AuditService/ListEvents"))
}

func TestNewServer_WithTLSOption(t *testing.T) {
	srv := NewServer(0, nil, nil, nil, nil, nil, nil, nil,
		WithTLS(&tls.Config{MinVersion: tls.VersionTLS12}),
		WithTLS(nil),
	)
	require.NotNil(t, srv)
	assert.NotNil(t, srv.creds, "TLS config must install transport credentials")
}
