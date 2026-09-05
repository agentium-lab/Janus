package service

import (
	"context"
	"fmt"
	"testing"

	"github.com/agentium-lab/Janus/core"
)

type mockContextRefRepo struct {
	refs  map[string]*core.ContextRef
	binds []string
}

func newMockContextRefRepo() *mockContextRefRepo {
	return &mockContextRefRepo{refs: make(map[string]*core.ContextRef)}
}

func (m *mockContextRefRepo) Insert(ctx context.Context, ref core.ContextRef) error {
	m.refs[ref.ID] = &ref
	return nil
}

func (m *mockContextRefRepo) Get(ctx context.Context, tenantID, id string) (*core.ContextRef, error) {
	r, ok := m.refs[id]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return r, nil
}

func (m *mockContextRefRepo) ListByTask(ctx context.Context, tenantID, taskID string) ([]*core.ContextRef, error) {
	return nil, nil
}

func (m *mockContextRefRepo) Delete(ctx context.Context, tenantID, id string) error {
	delete(m.refs, id)
	return nil
}

func (m *mockContextRefRepo) BindToTask(ctx context.Context, tenantID, taskID, contextRefID string) error {
	m.binds = append(m.binds, taskID+":"+contextRefID)
	return nil
}

func (m *mockContextRefRepo) UnbindFromTask(ctx context.Context, tenantID, taskID, contextRefID string) error {
	return nil
}

func TestContextRefService_Attach(t *testing.T) {
	svc := NewContextRefService(newMockContextRefRepo())
	ref, err := svc.Attach(context.Background(), "t1", "git_pr", "github://acme/repo/pull/1", "sha256:abc", "internal", []string{"agent-1"})
	if err != nil {
		t.Fatal(err)
	}
	if ref.ID == "" {
		t.Error("expected id to be generated")
	}
	if ref.TenantID != "t1" {
		t.Errorf("expected t1, got %s", ref.TenantID)
	}
	if ref.Type != "git_pr" {
		t.Errorf("expected git_pr, got %s", ref.Type)
	}
	if len(ref.AccessScope) != 1 || ref.AccessScope[0] != "agent-1" {
		t.Errorf("expected [agent-1], got %v", ref.AccessScope)
	}
}

func TestContextRefService_AttachValidation(t *testing.T) {
	svc := NewContextRefService(newMockContextRefRepo())
	_, err := svc.Attach(context.Background(), "", "git_pr", "uri", "", "", nil)
	if err == nil {
		t.Error("expected error for empty tenant")
	}
	_, err = svc.Attach(context.Background(), "t1", "", "uri", "", "", nil)
	if err == nil {
		t.Error("expected error for empty type")
	}
	_, err = svc.Attach(context.Background(), "t1", "git_pr", "", "", "", nil)
	if err == nil {
		t.Error("expected error for empty uri")
	}
}

func TestContextRefService_GetAndDetach(t *testing.T) {
	repo := newMockContextRefRepo()
	svc := NewContextRefService(repo)
	ref, _ := svc.Attach(context.Background(), "t1", "git_pr", "github://acme/repo/pull/1", "", "", nil)

	got, err := svc.Get(context.Background(), "t1", ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.URI != "github://acme/repo/pull/1" {
		t.Errorf("expected uri, got %s", got.URI)
	}

	if err := svc.Detach(context.Background(), "t1", ref.ID); err != nil {
		t.Fatal(err)
	}
	_, err = svc.Get(context.Background(), "t1", ref.ID)
	if err == nil {
		t.Error("expected error after detach")
	}
}

func TestContextRefService_ListByTask(t *testing.T) {
	svc := NewContextRefService(newMockContextRefRepo())
	refs, err := svc.ListByTask(context.Background(), "t1", "task-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 0 {
		t.Errorf("expected empty, got %d", len(refs))
	}
}

func TestContextRefService_NormalizeAndBind_NewRefs(t *testing.T) {
	repo := newMockContextRefRepo()
	svc := NewContextRefService(repo)

	refs := []core.ContextRef{
		{Type: "git_pr", URI: "github://acme/repo/pull/1", Classification: "internal"},
		{Type: "artifact", URI: "artifact://local/acme/result.json"},
	}

	err := svc.NormalizeAndBind(context.Background(), "t1", "task-1", refs)
	if err != nil {
		t.Fatal(err)
	}
	if len(repo.binds) != 2 {
		t.Errorf("expected 2 binds, got %d", len(repo.binds))
	}
	if refs[0].ID == "" || refs[1].ID == "" {
		t.Error("expected IDs to be generated")
	}
	if refs[0].TenantID != "t1" {
		t.Error("expected tenant_id to be set from task tenant")
	}
}

func TestContextRefService_NormalizeAndBind_ExistingRef(t *testing.T) {
	repo := newMockContextRefRepo()
	repo.refs["ctxref_existing"] = &core.ContextRef{ID: "ctxref_existing", TenantID: "t1", Type: "doc", URI: "file:///doc"}
	svc := NewContextRefService(repo)

	refs := []core.ContextRef{
		{ID: "ctxref_existing", TenantID: "t1"},
	}

	err := svc.NormalizeAndBind(context.Background(), "t1", "task-1", refs)
	if err != nil {
		t.Fatal(err)
	}
	if len(repo.binds) != 1 {
		t.Errorf("expected 1 bind, got %d", len(repo.binds))
	}
}

func TestContextRefService_NormalizeAndBind_CrossTenant(t *testing.T) {
	repo := newMockContextRefRepo()
	svc := NewContextRefService(repo)

	refs := []core.ContextRef{
		{ID: "ctxref_other", TenantID: "other-tenant", Type: "doc", URI: "file:///secret"},
	}

	err := svc.NormalizeAndBind(context.Background(), "t1", "task-1", refs)
	if err == nil {
		t.Fatal("expected cross-tenant error")
	}
}

func TestContextRefService_NormalizeAndBind_NotFoundRef(t *testing.T) {
	repo := newMockContextRefRepo()
	svc := NewContextRefService(repo)

	refs := []core.ContextRef{
		{ID: "ctxref_nonexistent", TenantID: "t1"},
	}

	err := svc.NormalizeAndBind(context.Background(), "t1", "task-1", refs)
	if err == nil {
		t.Fatal("expected not-found error")
	}
}
