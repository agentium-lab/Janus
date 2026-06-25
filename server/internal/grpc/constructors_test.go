package grpc

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	janus "github.com/agentium-lab/Janus/sdk/go"
)

func TestNewAgentServiceServer(t *testing.T) {
	s := NewAgentServiceServer(nil)
	assert.NotNil(t, s)
}

func TestNewDispatchServiceServer(t *testing.T) {
	s := NewDispatchServiceServer(nil)
	assert.NotNil(t, s)
}

func TestNewTaskServiceServer(t *testing.T) {
	s := NewTaskServiceServer(nil)
	assert.NotNil(t, s)
}

func TestNewMailboxServiceServer(t *testing.T) {
	s := NewMailboxServiceServer(nil)
	assert.NotNil(t, s)
}

func TestNewDLQServiceServer(t *testing.T) {
	s := NewDLQServiceServer(nil)
	assert.NotNil(t, s)
}

func TestNewAuditServiceServer(t *testing.T) {
	s := NewAuditServiceServer(nil)
	assert.NotNil(t, s)
}

func TestSDKClient_UpdateMailbox(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPatch, r.Method)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"id": "mb1", "status": "updated"})
	}))
	defer srv.Close()

	c := janus.NewClient(janus.Config{BaseURL: srv.URL, TenantID: "acme"})
	resp, err := c.UpdateMailbox(context.Background(), "mb1", janus.UpdateMailboxRequest{})
	require.NoError(t, err)
	assert.Equal(t, "mb1", resp.ID)
}

func TestSDKClient_PauseMailbox(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "/pause")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"id": "mb1", "status": "paused"})
	}))
	defer srv.Close()

	c := janus.NewClient(janus.Config{BaseURL: srv.URL, TenantID: "acme"})
	resp, err := c.PauseMailbox(context.Background(), "mb1")
	require.NoError(t, err)
	assert.Equal(t, "paused", resp.Status)
}

func TestSDKClient_ResumeMailbox(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "/resume")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"id": "mb1", "status": "active"})
	}))
	defer srv.Close()

	c := janus.NewClient(janus.Config{BaseURL: srv.URL, TenantID: "acme"})
	resp, err := c.ResumeMailbox(context.Background(), "mb1")
	require.NoError(t, err)
	assert.Equal(t, "active", resp.Status)
}

func TestSDKClient_UpdateMailbox_Error(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "internal"})
	}))
	defer srv.Close()

	c := janus.NewClient(janus.Config{BaseURL: srv.URL, TenantID: "acme"})
	_, err := c.UpdateMailbox(context.Background(), "mb1", janus.UpdateMailboxRequest{})
	require.Error(t, err)
}

func TestSDKClient_PauseMailbox_Error(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "not found"})
	}))
	defer srv.Close()

	c := janus.NewClient(janus.Config{BaseURL: srv.URL, TenantID: "acme"})
	_, err := c.PauseMailbox(context.Background(), "mb1")
	require.Error(t, err)
}

func TestSDKClient_ResumeMailbox_Error(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "boom"})
	}))
	defer srv.Close()

	c := janus.NewClient(janus.Config{BaseURL: srv.URL, TenantID: "acme"})
	_, err := c.ResumeMailbox(context.Background(), "mb1")
	require.Error(t, err)
}
