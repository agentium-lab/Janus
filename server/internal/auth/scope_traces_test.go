package auth

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRequiredScope_TracesMailbox(t *testing.T) {
	tests := []struct {
		name          string
		path          string
		method        string
		expectedScope string
	}{
		{
			name:          "traces endpoint requires audit:read",
			path:          "/v1/tenants/acme/traces/trace-123",
			method:        "GET",
			expectedScope: ScopeAuditRead,
		},
		{
			name:          "mailbox named traces requires task:write for pull",
			path:          "/v1/tenants/acme/mailboxes/traces/pull",
			method:        "POST",
			expectedScope: ScopeTaskWrite,
		},
		{
			name:          "mailbox named traces requires task:read for get",
			path:          "/v1/tenants/acme/mailboxes/traces",
			method:        "GET",
			expectedScope: ScopeTaskRead,
		},
		{
			name:          "normal mailbox requires task:write for pull",
			path:          "/v1/tenants/acme/mailboxes/mb-1/pull",
			method:        "POST",
			expectedScope: ScopeTaskWrite,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scope, ok := RequiredScope(tt.method, tt.path)
			assert.True(t, ok)
			assert.Equal(t, tt.expectedScope, scope)
		})
	}
}
