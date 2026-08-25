package service

import (
	"context"
	"strings"
	"testing"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/auth"
)

type fakeAPIKeyRepo struct {
	createdHashes []string
	list          []core.APIKey
	revokedID     string
	revokeRes     *core.APIKey
	lastScope     []string
}

func (f *fakeAPIKeyRepo) CreateAPIKey(_ context.Context, tenantID, keyHash, name, prefix string, scopes []string) (core.APIKey, error) {
	f.createdHashes = append(f.createdHashes, keyHash)
	f.lastScope = scopes
	return core.APIKey{TenantID: tenantID, Name: name, Prefix: prefix, Scopes: scopes}, nil
}

func (f *fakeAPIKeyRepo) ListAPIKeys(_ context.Context, _ string) ([]core.APIKey, error) {
	return f.list, nil
}

func (f *fakeAPIKeyRepo) RevokeAPIKey(_ context.Context, _, keyID string) (*core.APIKey, error) {
	f.revokedID = keyID
	return f.revokeRes, nil
}

func TestAPIKeyService_CreateRequiresName(t *testing.T) {
	s := NewAPIKeyService(&fakeAPIKeyRepo{})
	if _, _, err := s.Create(context.Background(), "acme", "", nil); err == nil {
		t.Fatal("empty name must be rejected")
	}
}

func TestAPIKeyService_CreateRejectsUnknownScope(t *testing.T) {
	s := NewAPIKeyService(&fakeAPIKeyRepo{})
	if _, _, err := s.Create(context.Background(), "acme", "n", []string{"task:read", "galaxy:destroy"}); err == nil {
		t.Fatal("unknown scope must be rejected")
	}
}

func TestAPIKeyService_CreateDedupesAndReturnsRawOnce(t *testing.T) {
	repo := &fakeAPIKeyRepo{}
	s := NewAPIKeyService(repo)
	k, raw, err := s.Create(context.Background(), "acme", "n", []string{"task:read", auth.ScopeAdmin, "task:read"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if len(k.Scopes) != 2 {
		t.Fatalf("want 2 deduped scopes, got %v", k.Scopes)
	}
	if k.Scopes[0] != auth.ScopeAdmin || k.Scopes[1] != "task:read" {
		t.Fatalf("scopes must be sorted, got %v", k.Scopes)
	}
	if !strings.HasPrefix(raw, "janus_") || len(raw) < 40 {
		t.Fatalf("raw key shape wrong: %q", raw)
	}
	if len(repo.createdHashes) != 1 || len(repo.createdHashes[0]) != 64 {
		t.Fatalf("repo must receive sha256 hex hash, got %v", repo.createdHashes)
	}
}

func TestAPIKeyService_RevokeNotFound(t *testing.T) {
	repo := &fakeAPIKeyRepo{}
	s := NewAPIKeyService(repo)
	k, err := s.Revoke(context.Background(), "acme", "missing")
	if err != nil || k != nil {
		t.Fatalf("missing revoke must be (nil,nil), got (%v,%v)", k, err)
	}
	want := &core.APIKey{ID: "k1"}
	repo.revokeRes = want
	got, err := s.Revoke(context.Background(), "acme", "k1")
	if err != nil || got != want || repo.revokedID != "k1" {
		t.Fatalf("revoke passthrough broken: (%v,%v)", got, err)
	}
}

func TestAPIKeyService_EmptyScopesStayFullAccess(t *testing.T) {
	repo := &fakeAPIKeyRepo{}
	s := NewAPIKeyService(repo)
	k, _, err := s.Create(context.Background(), "acme", "legacy-style", nil)
	if err != nil {
		t.Fatal(err)
	}
	p := auth.Principal{Scopes: k.Scopes}
	if !p.HasScope(auth.ScopeAdmin) {
		t.Fatal("empty stored scopes must keep full access")
	}
}
