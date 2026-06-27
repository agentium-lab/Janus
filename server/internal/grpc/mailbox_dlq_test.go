package grpc

import (
	"context"
	"fmt"
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

type mockMailboxSvcErr struct{}

func (m *mockMailboxSvcErr) Create(_ context.Context, mb core.Mailbox) error  { return fmt.Errorf("create error") }
func (m *mockMailboxSvcErr) Get(_ context.Context, _, _ string) (*core.Mailbox, error) {
	return nil, fmt.Errorf("get error")
}
func (m *mockMailboxSvcErr) UpdateConfig(_ context.Context, _, _ string, _, _, _, _ int) error {
	return fmt.Errorf("update error")
}
func (m *mockMailboxSvcErr) Pause(_ context.Context, _, _ string) error  { return fmt.Errorf("pause error") }
func (m *mockMailboxSvcErr) Resume(_ context.Context, _, _ string) error { return fmt.Errorf("resume error") }

func TestMailboxGRPC_CreateMailbox_Error(t *testing.T) {
	srv := &MailboxServiceServer{svc: &mockMailboxSvcErr{}}
	_, err := srv.CreateMailbox(context.Background(), &pb.CreateMailboxRequest{TenantId: "acme", Id: "mb-1"})
	assert.Error(t, err)
}

func TestMailboxGRPC_CreateMailbox_EmptyTenant(t *testing.T) {
	srv := &MailboxServiceServer{svc: &mockMailboxSvc{}}
	_, err := srv.CreateMailbox(context.Background(), &pb.CreateMailboxRequest{Id: "mb-1"})
	assert.Error(t, err)
}

func TestMailboxGRPC_GetMailbox_Error(t *testing.T) {
	srv := &MailboxServiceServer{svc: &mockMailboxSvcErr{}}
	_, err := srv.GetMailbox(context.Background(), &pb.GetMailboxRequest{TenantId: "acme", MailboxId: "mb-1"})
	assert.Error(t, err)
}

func TestMailboxGRPC_UpdateMailbox_Error(t *testing.T) {
	srv := &MailboxServiceServer{svc: &mockMailboxSvcErr{}}
	_, err := srv.UpdateMailbox(context.Background(), &pb.UpdateMailboxRequest{TenantId: "acme", MailboxId: "mb-1"})
	assert.Error(t, err)
}

func TestMailboxGRPC_UpdateMailbox_EmptyTenant(t *testing.T) {
	srv := &MailboxServiceServer{svc: &mockMailboxSvc{}}
	_, err := srv.UpdateMailbox(context.Background(), &pb.UpdateMailboxRequest{MailboxId: "mb-1"})
	assert.Error(t, err)
}

func TestMailboxGRPC_PauseMailbox_Error(t *testing.T) {
	srv := &MailboxServiceServer{svc: &mockMailboxSvcErr{}}
	_, err := srv.PauseMailbox(context.Background(), &pb.MailboxActionRequest{TenantId: "acme", MailboxId: "mb-1"})
	assert.Error(t, err)
}

func TestMailboxGRPC_ResumeMailbox_Error(t *testing.T) {
	srv := &MailboxServiceServer{svc: &mockMailboxSvcErr{}}
	_, err := srv.ResumeMailbox(context.Background(), &pb.MailboxActionRequest{TenantId: "acme", MailboxId: "mb-1"})
	assert.Error(t, err)
}

type mockDLQSvcErr struct{}

func (m *mockDLQSvcErr) QueryDLQ(_ context.Context, _, _ string, _ int) ([]*core.Task, error) {
	return nil, fmt.Errorf("query error")
}
func (m *mockDLQSvcErr) ReplayDLQ(_ context.Context, _, _ string) (*core.Task, error) {
	return nil, fmt.Errorf("replay error")
}
func (m *mockDLQSvcErr) DiscardDLQ(_ context.Context, _, _ string) error {
	return fmt.Errorf("discard error")
}

func TestDLQGRPC_QueryDLQ_Error(t *testing.T) {
	srv := &DLQServiceServer{svc: &mockDLQSvcErr{}}
	_, err := srv.QueryDLQ(context.Background(), &pb.DLQQueryRequest{TenantId: "acme"})
	assert.Error(t, err)
}

func TestDLQGRPC_ReplayDLQ_Error(t *testing.T) {
	srv := &DLQServiceServer{svc: &mockDLQSvcErr{}}
	_, err := srv.ReplayDLQ(context.Background(), &pb.DLQActionRequest{TenantId: "acme", TaskId: "t1"})
	assert.Error(t, err)
}

func TestDLQGRPC_DiscardDLQ_Error(t *testing.T) {
	srv := &DLQServiceServer{svc: &mockDLQSvcErr{}}
	_, err := srv.DiscardDLQ(context.Background(), &pb.DLQActionRequest{TenantId: "acme", TaskId: "t1"})
	assert.Error(t, err)
}

func TestDLQGRPC_DiscardDLQ_EmptyTenant(t *testing.T) {
	srv := &DLQServiceServer{svc: &mockDLQSvc{}}
	_, err := srv.DiscardDLQ(context.Background(), &pb.DLQActionRequest{TaskId: "t1"})
	assert.Error(t, err)
}

type mockAuditSvcErr struct{}

func (m *mockAuditSvcErr) QueryByTask(_ context.Context, _, _ string, _ int) ([]*core.JanusEvent, error) {
	return nil, fmt.Errorf("audit task error")
}
func (m *mockAuditSvcErr) QueryByTrace(_ context.Context, _, _ string, _ int) ([]*core.JanusEvent, error) {
	return nil, fmt.Errorf("audit trace error")
}
func (m *mockAuditSvcErr) QueryByTenant(_ context.Context, _ string, _ int) ([]*core.JanusEvent, error) {
	return nil, fmt.Errorf("audit tenant error")
}

func TestAuditGRPC_ListEvents_Error(t *testing.T) {
	srv := &AuditServiceServer{svc: &mockAuditSvcErr{}}
	_, err := srv.ListEvents(context.Background(), &pb.ListEventsRequest{TenantId: "acme", TraceId: "tr-1"})
	assert.Error(t, err)
}
