package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

	art := &Artifact{
		ID:          artifactID,
		TenantID:    tenantID,
		Hash:        hex.EncodeToString(hasher.Sum(nil)),
		Size:        written,
		ContentType: contentType,
	}
	if err := writeMeta(metaPath(path), art); err != nil {
		os.Remove(path)
		return nil, fmt.Errorf("write artifact metadata: %w", err)
	}
	return art, nil
}

func (s *LocalArtifactStore) Get(ctx context.Context, tenantID, artifactID string) (io.ReadCloser, *Artifact, error) {
	path, err := s.resolvePath(tenantID, artifactID)
	if err != nil {
		return nil, nil, err
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("artifact not found: %w", err)
	}

	// Metadata comes from the sidecar written at Store time so the body can be
	// streamed without loading it into memory. Falls back to a stat-only
	// record for artifacts stored before sidecars existed.
	art, err := readMeta(metaPath(path))
	if err != nil {
		info, statErr := os.Stat(path)
		if statErr != nil {
			f.Close()
			return nil, nil, fmt.Errorf("stat artifact: %w", statErr)
		}
		art = &Artifact{ID: artifactID, TenantID: tenantID, Size: info.Size()}
	}

	return f, art, nil
}

func metaPath(dataPath string) string { return dataPath + ".meta.json" }

func writeMeta(p string, art *Artifact) error {
	b, err := json.Marshal(art)
	if err != nil {
		return err
	}
	return os.WriteFile(p, b, 0o644)
}

func readMeta(p string) (*Artifact, error) {
	b, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	var art Artifact
	if err := json.Unmarshal(b, &art); err != nil {
		return nil, err
	}
	return &art, nil
}

func (s *LocalArtifactStore) Delete(ctx context.Context, tenantID, artifactID string) error {
	path, err := s.resolvePath(tenantID, artifactID)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete artifact: %w", err)
	}
	_ = os.Remove(metaPath(path))
	return nil
}
