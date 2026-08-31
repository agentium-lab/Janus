package service

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

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

// List returns all tenants (id + name). The name is loaded per tenant so the
// response carries the display name, not just ids.
func (s *TenantService) List(ctx context.Context) ([]core.Tenant, error) {
	ids, err := s.repo.ListIDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("list tenants: %w", err)
	}
	out := make([]core.Tenant, 0, len(ids))
	for _, id := range ids {
		name, err := s.repo.GetName(ctx, id)
		if err != nil {
			name = id
		}
		out = append(out, core.Tenant{ID: id, Name: name})
	}
	return out, nil
}

func (s *TenantService) Get(ctx context.Context, id string) (*core.Tenant, error) {
	if id == "" {
		return nil, fmt.Errorf("tenant id is required")
	}
	name, err := s.repo.GetName(ctx, id)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("tenant %s not found", id)
		}
		return nil, fmt.Errorf("get tenant: %w", err)
	}
	return &core.Tenant{ID: id, Name: name}, nil
}
