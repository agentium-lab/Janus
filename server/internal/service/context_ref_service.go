package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/driver/postgres"
)

type ContextRefRepo interface {
	Insert(ctx context.Context, ref core.ContextRef) error
	Get(ctx context.Context, tenantID, id string) (*core.ContextRef, error)
	ListByTask(ctx context.Context, tenantID, taskID string) ([]*core.ContextRef, error)
	Delete(ctx context.Context, tenantID, id string) error
	BindToTask(ctx context.Context, tenantID, taskID, contextRefID string) error
	UnbindFromTask(ctx context.Context, tenantID, taskID, contextRefID string) error
}

// ContextRefTxRepo is the transactional binding path: refs commit with the
// task in one transaction (see TaskService.createAtomic).
type ContextRefTxRepo interface {
	InsertTx(ctx context.Context, tx pgx.Tx, ref core.ContextRef) error
	BindToTaskTx(ctx context.Context, tx pgx.Tx, tenantID, taskID, contextRefID string) error
}

type ContextRefService struct {
	repo   ContextRefRepo
	txRepo ContextRefTxRepo
}

func NewContextRefService(repo ContextRefRepo) *ContextRefService {
	s := &ContextRefService{repo: repo}
	if pgCtx, ok := repo.(*postgres.ContextRefRepo); ok {
		s.txRepo = pgCtx
	} else {
		s.txRepo = ContextRefRepoTxAdapter{repo}
	}
	return s
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

func (s *ContextRefService) NormalizeAndBind(ctx context.Context, tenantID, taskID string, refs []core.ContextRef) error {
	for i := range refs {
		isNew, err := s.prepareRef(ctx, tenantID, &refs[i])
		if err != nil {
			return err
		}
		if refs[i].ID == "" {
			continue // empty stub (no type/uri) — nothing to bind
		}
		if err := s.writeRef(ctx, nil, tenantID, taskID, &refs[i], isNew); err != nil {
			return err
		}
	}
	return nil
}

// NormalizeAndBindTx is the transactional binding: refs insert+bind commit
// inside the caller's (task-create) transaction. Prepare must have run
// first so IDs are generated and refs validated pre-commit.
func (s *ContextRefService) NormalizeAndBindTx(ctx context.Context, tx pgx.Tx, tenantID, taskID string, refs []core.ContextRef) error {
	for i := range refs {
		if refs[i].ID == "" {
			continue
		}
		if err := s.writeRef(ctx, tx, tenantID, taskID, &refs[i], refs[i].CreatedAt == ""); err != nil {
			return err
		}
	}
	return nil
}

// Prepare validates refs and pre-generates their IDs WITHOUT writing:
// run it BEFORE the task's create transaction so (a) a bad ref rejects the
// create before anything is published, and (b) the generated IDs land in
// the persisted envelope. The insert+bind then happens inside the task's
// transaction via NormalizeAndBindTx.
func (s *ContextRefService) Prepare(ctx context.Context, tenantID string, refs []core.ContextRef) error {
	for i := range refs {
		if _, err := s.prepareRef(ctx, tenantID, &refs[i]); err != nil {
			return err
		}
	}
	return nil
}

// prepareRef is the shared first half of NormalizeAndBind: tenant checks,
// ID generation for new refs, existence+tenant check for referenced IDs.
// It returns whether the ref is newly created (needs an insert).
func (s *ContextRefService) prepareRef(ctx context.Context, tenantID string, ref *core.ContextRef) (bool, error) {
	if ref.TenantID == "" {
		ref.TenantID = tenantID
	}
	if ref.TenantID != tenantID {
		return false, fmt.Errorf("context ref tenant mismatch: ref tenant=%s, task tenant=%s", ref.TenantID, tenantID)
	}
	if ref.ID == "" {
		if ref.Type == "" || ref.URI == "" {
			return false, nil
		}
		id, err := generateContextRefID()
		if err != nil {
			return false, err
		}
		ref.ID = id
		// Stamp created_at on generation: it both feeds the insert and lets
		// the tx path distinguish newly generated refs (need InsertTx) from
		// refs resolved by an existing ID (bind only).
		ref.CreatedAt = time.Now().UTC().Format(time.RFC3339)
		return true, nil
	}
	existing, err := s.repo.Get(ctx, tenantID, ref.ID)
	if err != nil {
		return false, fmt.Errorf("context ref %s not found in tenant %s: %w", ref.ID, tenantID, err)
	}
	if existing.TenantID != tenantID {
		return false, fmt.Errorf("cross-tenant context ref access denied")
	}
	return false, nil
}

// writeRef is the shared second half: insert (new refs only) + bind, on
// the caller's tx when non-nil (ContextRefTxRepo), else on the pool.
func (s *ContextRefService) writeRef(ctx context.Context, tx pgx.Tx, tenantID, taskID string, ref *core.ContextRef, isNew bool) error {
	if isNew {
		if tx != nil && s.txRepo != nil {
			if err := s.txRepo.InsertTx(ctx, tx, *ref); err != nil {
				return fmt.Errorf("insert context ref: %w", err)
			}
		} else {
			if err := s.repo.Insert(ctx, *ref); err != nil {
				return fmt.Errorf("insert context ref: %w", err)
			}
		}
	}
	if tx != nil && s.txRepo != nil {
		if err := s.txRepo.BindToTaskTx(ctx, tx, tenantID, taskID, ref.ID); err != nil {
			return fmt.Errorf("bind context ref %s to task %s: %w", ref.ID, taskID, err)
		}
		return nil
	}
	if err := s.repo.BindToTask(ctx, tenantID, taskID, ref.ID); err != nil {
		return fmt.Errorf("bind context ref %s to task %s: %w", ref.ID, taskID, err)
	}
	return nil
}

func generateContextRefID() (string, error) {
	b := make([]byte, 10)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "ctxref_" + hex.EncodeToString(b), nil
}
