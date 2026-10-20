package janus

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLeaseLostError_MessageVariants(t *testing.T) {
	assert.Equal(t, "lease lost", (&LeaseLostError{}).Error())
	assert.Equal(t, "lease lost for task t1", (&LeaseLostError{TaskID: "t1"}).Error())
	var nilErr *LeaseLostError
	assert.Equal(t, "lease lost", nilErr.Error())
	assert.True(t, errors.Is(&LeaseLostError{TaskID: "t1"}, ErrLeaseLost))
}

func TestPullTask_NoContentReturnsNil(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "a1", r.Header.Get("X-API-Key"))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "acme", APIKey: "a1"})
	res, err := c.PullTask(context.Background(), "mb-1", "agent-1")
	require.NoError(t, err)
	assert.Nil(t, res)
}

func TestPullTask_TransportError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close()
	c := NewClient(Config{BaseURL: url, TenantID: "acme"})
	_, err := c.PullTask(context.Background(), "mb-1", "agent-1")
	require.ErrorContains(t, err, "http request")
}

func TestGetTask_NoContentReturnsZeroTask(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	c := NewClient(Config{BaseURL: srv.URL, TenantID: "acme"})
	task, err := c.GetTask(context.Background(), "t1")
	require.NoError(t, err)
	require.NotNil(t, task)
	assert.Empty(t, task.ID)
}
