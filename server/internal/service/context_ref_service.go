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
	plan, err := s.Prepare(ctx, tenantID, refs)
	if err != nil {
		return err
	}
	return s.ApplyPlanTx(ctx, nil, tenantID, taskID, plan)
}

// RefWrite is one entry of the explicit write plan returned by Prepare.
// IsNew is the insert-vs-bind decision itself — newness is planned, never
// re-inferred from CreatedAt (prepareRef stamps CreatedAt on generation,
// so the old "CreatedAt == \"\" means new" test inverted for exactly the
// refs it was meant to catch).
type RefWrite struct {
	Ref     core.ContextRef
	IsNew   bool
	Trusted *core.ContextRef // reused ref: the record repo.Get returned
}

// ApplyPlanTx consumes a Prepare write plan: IsNew entries insert, every
// entry binds — on the caller's tx when non-nil (task-create transaction),
// else on the pool (adapter path under MemoryLifecycle).
func (s *ContextRefService) ApplyPlanTx(ctx context.Context, tx pgx.Tx, tenantID, taskID string, plan []RefWrite) error {
	for i := range plan {
		if err := s.applyWrite(ctx, tx, tenantID, taskID, &plan[i]); err != nil {
			return err
		}
	}
	return nil
}

// Prepare validates refs, pre-generates their IDs, and resolves refs that
// reference an existing ID — WITHOUT writing. Run it BEFORE the task's
// create transaction so (a) a bad ref rejects the create before anything
// is published, and (b) the generated IDs land in the persisted envelope.
// It returns the explicit per-ref write plan consumed by ApplyPlanTx inside
// the task's transaction.
func (s *ContextRefService) Prepare(ctx context.Context, tenantID string, refs []core.ContextRef) ([]RefWrite, error) {
	plan := make([]RefWrite, 0, len(refs))
	for i := range refs {
		w, err := s.prepareRef(ctx, tenantID, &refs[i])
		if err != nil {
			return nil, err
		}
		if w.Ref.ID == "" {
			continue // empty stub (no type/uri) — nothing to write
		}
		plan = append(plan, w)
	}
	return plan, nil
}

// prepareRef is the shared first half: tenant checks, ID generation for
// new refs, existence+tenant resolution for referenced IDs. It returns the
// planned write: generated refs are IsNew (insert then bind); refs resolved
// by an existing ID bind to the trusted record repo.Get returned, with
// caller-supplied fields backfilled from it.
func (s *ContextRefService) prepareRef(ctx context.Context, tenantID string, ref *core.ContextRef) (RefWrite, error) {
	if ref.TenantID == "" {
		ref.TenantID = tenantID
	}
	if ref.TenantID != tenantID {
		return RefWrite{}, fmt.Errorf("context ref tenant mismatch: ref tenant=%s, task tenant=%s", ref.TenantID, tenantID)
	}
	if ref.ID == "" {
		if ref.Type == "" || ref.URI == "" {
			return RefWrite{Ref: *ref}, nil
		}
		id, err := generateContextRefID()
		if err != nil {
			return RefWrite{}, err
		}
		ref.ID = id
		ref.CreatedAt = time.Now().UTC().Format(time.RFC3339)
		return RefWrite{Ref: *ref, IsNew: true}, nil
	}
	existing, err := s.repo.Get(ctx, tenantID, ref.ID)
	if err != nil {
		return RefWrite{}, fmt.Errorf("context ref %s not found in tenant %s: %w", ref.ID, tenantID, err)
	}
	if existing.TenantID != tenantID {
		return RefWrite{}, fmt.Errorf("cross-tenant context ref access denied")
	}
	// Backfill from the trusted record: the plan binds (and the persisted
	// envelope carries) stored fields, never caller-declared metadata.
	trusted := *existing
	*ref = trusted
	return RefWrite{Ref: trusted, Trusted: &trusted}, nil
}

// applyWrite is the shared second half of the plan: insert (new refs only)
// + bind, on the caller's tx when non-nil (ContextRefTxRepo), else on the
// pool.
func (s *ContextRefService) applyWrite(ctx context.Context, tx pgx.Tx, tenantID, taskID string, w *RefWrite) error {
	if w.IsNew {
		if tx != nil && s.txRepo != nil {
			if err := s.txRepo.InsertTx(ctx, tx, w.Ref); err != nil {
				return fmt.Errorf("insert context ref: %w", err)
			}
		} else {
			if err := s.repo.Insert(ctx, w.Ref); err != nil {
				return fmt.Errorf("insert context ref: %w", err)
			}
		}
	}
	if tx != nil && s.txRepo != nil {
		if err := s.txRepo.BindToTaskTx(ctx, tx, tenantID, taskID, w.Ref.ID); err != nil {
			return fmt.Errorf("bind context ref %s to task %s: %w", w.Ref.ID, taskID, err)
		}
		return nil
	}
	if err := s.repo.BindToTask(ctx, tenantID, taskID, w.Ref.ID); err != nil {
		return fmt.Errorf("bind context ref %s to task %s: %w", w.Ref.ID, taskID, err)
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
