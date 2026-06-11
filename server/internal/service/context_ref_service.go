package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/agentium-lab/Janus/core"
)

type ContextRefRepo interface {
	Insert(ctx context.Context, ref core.ContextRef) error
	Get(ctx context.Context, tenantID, id string) (*core.ContextRef, error)
	ListByTask(ctx context.Context, tenantID, taskID string) ([]*core.ContextRef, error)
	Delete(ctx context.Context, tenantID, id string) error
}

type ContextRefService struct {
	repo ContextRefRepo
}

func NewContextRefService(repo ContextRefRepo) *ContextRefService {
	return &ContextRefService{repo: repo}
}

func (s *ContextRefService) Attach(ctx context.Context, tenantID, refType, uri, hash, classification string, accessScope []string) (*core.ContextRef, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("tenant id is required")
	}
	if refType == "" {
		return nil, fmt.Errorf("context type is required")
	}
	if uri == "" {
		return nil, fmt.Errorf("uri is required")
	}
	id, err := generateContextRefID()
	if err != nil {
		return nil, err
	}
	ref := core.ContextRef{
		TenantID:       tenantID,
		ID:             id,
		Type:           refType,
		URI:            uri,
		Hash:           hash,
		Classification: classification,
		AccessScope:    accessScope,
		CreatedAt:      time.Now().UTC().Format(time.RFC3339),
	}
	if err := s.repo.Insert(ctx, ref); err != nil {
		return nil, fmt.Errorf("attach context ref: %w", err)
	}
	return &ref, nil
}

func (s *ContextRefService) Get(ctx context.Context, tenantID, id string) (*core.ContextRef, error) {
	return s.repo.Get(ctx, tenantID, id)
}

func (s *ContextRefService) ListByTask(ctx context.Context, tenantID, taskID string) ([]*core.ContextRef, error) {
	return s.repo.ListByTask(ctx, tenantID, taskID)
}

func (s *ContextRefService) Detach(ctx context.Context, tenantID, id string) error {
	return s.repo.Delete(ctx, tenantID, id)
}

func generateContextRefID() (string, error) {
	b := make([]byte, 10)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "ctxref_" + hex.EncodeToString(b), nil
}
