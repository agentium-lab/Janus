package service

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/agentium-lab/Janus/core"
)

type TenantService struct {
	repo TenantRepo
}

func NewTenantService(repo TenantRepo) *TenantService {
	return &TenantService{repo: repo}
}

func (s *TenantService) Create(ctx context.Context, id, name string) error {
	if id == "" {
		return fmt.Errorf("tenant id is required")
	}
	if name == "" {
		return fmt.Errorf("tenant name is required")
	}
	return s.repo.Create(ctx, id, name)
}

func (s *TenantService) Get(ctx context.Context, id string) (*core.Tenant, error) {
	if id == "" {
		return nil, fmt.Errorf("tenant id is required")
	}
	name, err := s.repo.GetName(ctx, id)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("tenant %s not found", id)
		}
		return nil, fmt.Errorf("get tenant: %w", err)
	}
	return &core.Tenant{ID: id, Name: name}, nil
}
