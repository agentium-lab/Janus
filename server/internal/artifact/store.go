package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type Artifact struct {
	ID          string `json:"id"`
	TenantID    string `json:"tenant_id"`
	Hash        string `json:"hash"`
	Size        int64  `json:"size"`
	ContentType string `json:"content_type"`
}

type Store interface {
	Store(ctx context.Context, tenantID, artifactID string, reader io.Reader, contentType string) (*Artifact, error)
	Get(ctx context.Context, tenantID, artifactID string) (io.ReadCloser, *Artifact, error)
	Delete(ctx context.Context, tenantID, artifactID string) error
}

type LocalArtifactStore struct {
	baseDir string
}

func NewLocalArtifactStore(baseDir string) *LocalArtifactStore {
	return &LocalArtifactStore{baseDir: baseDir}
}

func sanitizeID(id string) error {
	if id == "" {
		return fmt.Errorf("id cannot be empty")
	}
	if strings.ContainsAny(id, "/\\..") {
		return fmt.Errorf("id contains path separator or traversal characters")
	}
	cleaned := filepath.Clean(id)
	if cleaned != id || cleaned == "." || cleaned == ".." {
		return fmt.Errorf("id resolves outside allowed directory")
	}
	return nil
}

func (s *LocalArtifactStore) resolvePath(tenantID, artifactID string) (string, error) {
	if err := sanitizeID(tenantID); err != nil {
		return "", fmt.Errorf("invalid tenant_id: %w", err)
	}
	if err := sanitizeID(artifactID); err != nil {
		return "", fmt.Errorf("invalid artifact_id: %w", err)
	}

	baseAbs, err := filepath.Abs(s.baseDir)
	if err != nil {
		return "", fmt.Errorf("resolve base dir: %w", err)
	}
	fullPath := filepath.Join(baseAbs, tenantID, artifactID)
	fullAbs, err := filepath.Abs(fullPath)
	if err != nil {
		return "", fmt.Errorf("resolve artifact path: %w", err)
	}
	if !strings.HasPrefix(fullAbs, baseAbs+string(filepath.Separator)) {
		return "", fmt.Errorf("path traversal detected")
	}
	return fullAbs, nil
}

func (s *LocalArtifactStore) Store(ctx context.Context, tenantID, artifactID string, reader io.Reader, contentType string) (*Artifact, error) {
	path, err := s.resolvePath(tenantID, artifactID)
	if err != nil {
		return nil, err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create artifact dir: %w", err)
	}

	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create artifact file: %w", err)
	}
	defer f.Close()

	hasher := sha256.New()
	writer := io.MultiWriter(f, hasher)

	written, err := io.Copy(writer, reader)
	if err != nil {
		os.Remove(path)
		return nil, fmt.Errorf("write artifact: %w", err)
	}

	return &Artifact{
		ID:          artifactID,
		TenantID:    tenantID,
		Hash:        hex.EncodeToString(hasher.Sum(nil)),
		Size:        written,
		ContentType: contentType,
	}, nil
}

func (s *LocalArtifactStore) Get(ctx context.Context, tenantID, artifactID string) (io.ReadCloser, *Artifact, error) {
	path, err := s.resolvePath(tenantID, artifactID)
	if err != nil {
		return nil, nil, err
	}

	info, err := os.Stat(path)
	if err != nil {
		return nil, nil, fmt.Errorf("artifact not found: %w", err)
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("open artifact: %w", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		f.Close()
		return nil, nil, fmt.Errorf("read artifact for hash: %w", err)
	}
	hasher := sha256.Sum256(data)

	art := &Artifact{
		ID:       artifactID,
		TenantID: tenantID,
		Hash:     hex.EncodeToString(hasher[:]),
		Size:     info.Size(),
	}

	return f, art, nil
}

func (s *LocalArtifactStore) Delete(ctx context.Context, tenantID, artifactID string) error {
	path, err := s.resolvePath(tenantID, artifactID)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete artifact: %w", err)
	}
	return nil
}
