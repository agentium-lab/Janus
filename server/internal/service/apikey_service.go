package service

import (
	"context"
	"fmt"
	"sort"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/auth"
)

type APIKeyRepo interface {
	CreateAPIKey(ctx context.Context, tenantID, keyHash, name, prefix string, scopes []string, boundAgentID string) (core.APIKey, error)
	ListAPIKeys(ctx context.Context, tenantID string) ([]core.APIKey, error)
	RevokeAPIKey(ctx context.Context, tenantID, keyID string) (*core.APIKey, error)
}

type APIKeyService struct {
	repo APIKeyRepo
}

func NewAPIKeyService(repo APIKeyRepo) *APIKeyService {
	return &APIKeyService{repo: repo}
}

func validScope(s string) bool {
	switch s {
	case auth.ScopeAdmin, auth.ScopeTaskWrite, auth.ScopeTaskRead, auth.ScopeAuditRead:
		return true
	}
	return false
}

func (s *APIKeyService) Create(ctx context.Context, tenantID, name string, scopes []string, boundAgentID string) (core.APIKey, string, error) {
	if name == "" {
		return core.APIKey{}, "", fmt.Errorf("name is required")
	}
	cleaned := make([]string, 0, len(scopes))
	for _, sc := range scopes {
		if !validScope(sc) {
			return core.APIKey{}, "", fmt.Errorf("unknown scope %q", sc)
		}
		dup := false
		for _, existing := range cleaned {
			if existing == sc {
				dup = true
				break
			}
		}
		if !dup {
			cleaned = append(cleaned, sc)
		}
	}
	sort.Strings(cleaned)

	raw, prefix, keyHash, err := auth.GenerateKey()
	if err != nil {
		return core.APIKey{}, "", fmt.Errorf("generate key: %w", err)
	}
	k, err := s.repo.CreateAPIKey(ctx, tenantID, keyHash, name, prefix, cleaned, boundAgentID)
	if err != nil {
		return core.APIKey{}, "", err
	}
	return k, raw, nil
}

func (s *APIKeyService) List(ctx context.Context, tenantID string) ([]core.APIKey, error) {
	return s.repo.ListAPIKeys(ctx, tenantID)
}

func (s *APIKeyService) Revoke(ctx context.Context, tenantID, keyID string) (*core.APIKey, error) {
	return s.repo.RevokeAPIKey(ctx, tenantID, keyID)
}
