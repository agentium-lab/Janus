package llm

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient_Complete_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"choices":[{"message":{"content":"code_review"}}]}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "sk-test", "gpt-4o-mini", 200, 10)
	result, err := c.Complete(context.Background(), "route task", "query")
	require.NoError(t, err)
	assert.Equal(t, "code_review", result)
}

func TestClient_Complete_Non200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "bad-key", "gpt-4o-mini", 200, 10)
	_, err := c.Complete(context.Background(), "route", "query")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "status 401")
}

func TestClient_Complete_EmptyChoices(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"choices":[]}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "sk-test", "gpt-4o-mini", 200, 10)
	_, err := c.Complete(context.Background(), "route", "query")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no choices")
}
