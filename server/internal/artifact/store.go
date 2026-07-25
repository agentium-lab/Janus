package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type Artifact struct {
	ID        string `json:"id"`
	TenantID  string `json:"tenant_id"`
	Hash      string `json:"hash"`
	Size      int64  `json:"size"`
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

func (s *LocalArtifactStore) Store(ctx context.Context, tenantID, artifactID string, reader io.Reader, contentType string) (*Artifact, error) {
	dir := filepath.Join(s.baseDir, tenantID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create artifact dir: %w", err)
	}

	path := filepath.Join(dir, artifactID)
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
	path := filepath.Join(s.baseDir, tenantID, artifactID)

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
	path := filepath.Join(s.baseDir, tenantID, artifactID)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete artifact: %w", err)
	}
	return nil
}
