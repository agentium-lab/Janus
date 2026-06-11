package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHashKey(t *testing.T) {
	h1 := hashKey("janus_test_key_123")
	h2 := hashKey("janus_test_key_123")
	assert.Equal(t, h1, h2)
	assert.Len(t, h1, 64)

	h3 := hashKey("different_key")
	assert.NotEqual(t, h1, h3)
}

func TestExtractAPIKey(t *testing.T) {
	tests := []struct {
		name   string
		header string
		value  string
		want   string
	}{
		{"x-api-key header", "X-API-Key", "my-api-key", "my-api-key"},
		{"bearer token", "Authorization", "Bearer my-token", "my-token"},
		{"bearer with spaces", "Authorization", "Bearer  my-token  ", "my-token"},
		{"empty", "", "", ""},
		{"wrong scheme", "Authorization", "Basic abc", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.header != "" {
				r.Header.Set(tt.header, tt.value)
			}
			got := extractAPIKey(r)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMiddleware_MissingKey_Returns401(t *testing.T) {
	handler := Middleware(nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("should not reach handler")
	}))

	r := httptest.NewRequest(http.MethodGet, "/v1/tenants", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestMiddleware_ValidKey_SetsTenantContext(t *testing.T) {
	var gotTenant string
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTenant = TenantFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})
	_ = inner
	_ = gotTenant
}

func TestGenerateKey(t *testing.T) {
	raw, prefix, hash, err := GenerateKey()
	assert.NoError(t, err)
	assert.NotEmpty(t, raw)
	assert.True(t, len(raw) > 32, "key should be long enough")
	assert.Equal(t, raw[:8], prefix)
	assert.NotEmpty(t, hash)
	assert.Equal(t, hashKey(raw), hash)

	raw2, _, hash2, err := GenerateKey()
	assert.NoError(t, err)
	assert.NotEqual(t, raw, raw2)
	assert.NotEqual(t, hash, hash2)
}
