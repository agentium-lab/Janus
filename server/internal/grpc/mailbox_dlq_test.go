package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/status"

	pb "github.com/agentium-lab/Janus/proto/gen/janus/v1"
	"github.com/agentium-lab/Janus/core"
)

// --- Mailbox gRPC handler tests using mock service ---

type mockMailboxSvc struct {
	mb *core.Mailbox
}

func (m *mockMailboxSvc) Create(_ context.Context, mb core.Mailbox) error {
	m.mb = &mb
	return nil
}
func (m *mockMailboxSvc) Get(_ context.Context, tenantID, mailboxID string) (*core.Mailbox, error) {
	if m.mb != nil && m.mb.ID == mailboxID {
		return m.mb, nil
	}
	return &core.Mailbox{ID: mailboxID, TenantID: tenantID, AgentID: "a1", Status: "active"}, nil
}
func (m *mockMailboxSvc) UpdateConfig(_ context.Context, tenantID, mailboxID string, mc, aw, md, rs int) error {
	return nil
}
func (m *mockMailboxSvc) Pause(_ context.Context, tenantID, mailboxID string) error  { return nil }
func (m *mockMailboxSvc) Resume(_ context.Context, tenantID, mailboxID string) error { return nil }

func TestMailboxGRPC_CreateMailbox(t *testing.T) {
	svc := &mockMailboxSvc{}
	srv := &MailboxServiceServer{svc: svc}

	resp, err := srv.CreateMailbox(context.Background(), &pb.CreateMailboxRequest{
		TenantId: "acme", Id: "mb-1", AgentId: "a1",
	})
	require.NoError(t, err)
	assert.Equal(t, "mb-1", resp.Id)
	assert.Equal(t, "acme", resp.TenantId)
}

func TestMailboxGRPC_GetMailbox(t *testing.T) {
	svc := &mockMailboxSvc{}
	srv := &MailboxServiceServer{svc: svc}

	resp, err := srv.GetMailbox(context.Background(), &pb.GetMailboxRequest{
		TenantId: "acme", MailboxId: "mb-1",
	})
	require.NoError(t, err)
	assert.Equal(t, "mb-1", resp.Id)
}

func TestMailboxGRPC_UpdateMailbox(t *testing.T) {
	svc := &mockMailboxSvc{}
	srv := &MailboxServiceServer{svc: svc}

	resp, err := srv.UpdateMailbox(context.Background(), &pb.UpdateMailboxRequest{
		TenantId: "acme", MailboxId: "mb-1", MaxConcurrency: 10,
	})
	require.NoError(t, err)
	assert.Equal(t, "mb-1", resp.Id)
}

func TestMailboxGRPC_PauseMailbox(t *testing.T) {
	svc := &mockMailboxSvc{}
	srv := &MailboxServiceServer{svc: svc}

	resp, err := srv.PauseMailbox(context.Background(), &pb.MailboxActionRequest{
		TenantId: "acme", MailboxId: "mb-1",
	})
	require.NoError(t, err)
	assert.Equal(t, "paused", resp.Status)
}

func TestMailboxGRPC_ResumeMailbox(t *testing.T) {
	svc := &mockMailboxSvc{}
	srv := &MailboxServiceServer{svc: svc}

	resp, err := srv.ResumeMailbox(context.Background(), &pb.MailboxActionRequest{
		TenantId: "acme", MailboxId: "mb-1",
	})
	require.NoError(t, err)
	assert.Equal(t, "active", resp.Status)
}

func TestMailboxToProto(t *testing.T) {
	mb := &core.Mailbox{ID: "mb-1", TenantID: "acme", AgentID: "a1", Status: "active", MaxConcurrency: 5}
	p := mailboxToProto(mb)
	assert.Equal(t, "mb-1", p.Id)
	assert.Equal(t, "acme", p.TenantId)
	assert.Equal(t, int32(5), p.MaxConcurrency)
}

// --- DLQ gRPC handler tests using mock service ---

type mockDLQSvc struct{}

func (m *mockDLQSvc) QueryDLQ(_ context.Context, tenantID, mailboxID string, limit int) ([]*core.Task, error) {
	return []*core.Task{{ID: "task-dlq-1", TenantID: tenantID, Status: core.TaskStatusDeadLettered}}, nil
}
func (m *mockDLQSvc) ReplayDLQ(_ context.Context, tenantID, taskID string) (*core.Task, error) {
	return &core.Task{ID: taskID, TenantID: tenantID, Status: core.TaskStatusCreated}, nil
}
func (m *mockDLQSvc) DiscardDLQ(_ context.Context, tenantID, taskID string) error {
	return nil
}

func TestDLQGRPC_QueryDLQ(t *testing.T) {
	srv := &DLQServiceServer{svc: &mockDLQSvc{}}

	resp, err := srv.QueryDLQ(context.Background(), &pb.DLQQueryRequest{
		TenantId: "acme", MailboxId: "mb-1", Limit: 10,
	})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 1)
	assert.Equal(t, "task-dlq-1", resp.Tasks[0].Id)
}

func TestDLQGRPC_ReplayDLQ(t *testing.T) {
	srv := &DLQServiceServer{svc: &mockDLQSvc{}}

	resp, err := srv.ReplayDLQ(context.Background(), &pb.DLQActionRequest{
		TenantId: "acme", TaskId: "task-dlq-1",
	})
	require.NoError(t, err)
	assert.Equal(t, "task-dlq-1", resp.Id)
}

func TestDLQGRPC_DiscardDLQ(t *testing.T) {
	srv := &DLQServiceServer{svc: &mockDLQSvc{}}

	resp, err := srv.DiscardDLQ(context.Background(), &pb.DLQActionRequest{
		TenantId: "acme", TaskId: "task-dlq-1",
	})
	require.NoError(t, err)
	assert.Equal(t, "discarded", resp.Status)
}

// --- errorMappingInterceptor test ---

func TestErrorMappingInterceptor_MapsError(t *testing.T) {
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, errTestNotFound
	}
	_, err := errorMappingInterceptor(context.Background(), nil, nil, handler)
	require.Error(t, err)
	// Verify it's mapped to a gRPC status error.
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, "NotFound", st.Code().String())
}

func TestErrorMappingInterceptor_PassesThroughSuccess(t *testing.T) {
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "ok", nil
	}
	resp, err := errorMappingInterceptor(context.Background(), nil, nil, handler)
	require.NoError(t, err)
	assert.Equal(t, "ok", resp)
}

var errTestNotFound = errNotFound("task not found")

type errNotFound string

func (e errNotFound) Error() string { return string(e) }
