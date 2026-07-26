package artifact

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalArtifactStore_StoreAndGet(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalArtifactStore(dir)
	ctx := context.Background()

	data := []byte("hello artifact world")
	reader := bytes.NewReader(data)

	art, err := store.Store(ctx, "acme", "art-1", reader, "text/plain")
	require.NoError(t, err)
	assert.Equal(t, "acme", art.TenantID)
	assert.Equal(t, "art-1", art.ID)
	assert.Equal(t, int64(len(data)), art.Size)
	assert.Len(t, art.Hash, 64)

	rc, gotArt, err := store.Get(ctx, "acme", "art-1")
	require.NoError(t, err)
	defer rc.Close()

	gotData, _ := io.ReadAll(rc)
	assert.Equal(t, data, gotData)
	assert.Equal(t, art.Hash, gotArt.Hash)
}

func TestLocalArtifactStore_GetNotFound(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalArtifactStore(dir)
	ctx := context.Background()

	_, _, err := store.Get(ctx, "acme", "nonexistent")
	assert.Error(t, err)
}

func TestLocalArtifactStore_Delete(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalArtifactStore(dir)
	ctx := context.Background()

	data := []byte("delete me")
	_, err := store.Store(ctx, "acme", "art-del", bytes.NewReader(data), "text/plain")
	require.NoError(t, err)

	err = store.Delete(ctx, "acme", "art-del")
	require.NoError(t, err)

	_, _, err = store.Get(ctx, "acme", "art-del")
	assert.Error(t, err)
}

func TestLocalArtifactStore_DeleteNotFound(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalArtifactStore(dir)
	ctx := context.Background()

	err := store.Delete(ctx, "acme", "nonexistent")
	require.NoError(t, err)
}

func TestLocalArtifactStore_TenantIsolation(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalArtifactStore(dir)
	ctx := context.Background()

	_, err := store.Store(ctx, "tenant-a", "art-1", bytes.NewReader([]byte("a")), "text/plain")
	require.NoError(t, err)
	_, err = store.Store(ctx, "tenant-b", "art-1", bytes.NewReader([]byte("b")), "text/plain")
	require.NoError(t, err)

	rcA, _, err := store.Get(ctx, "tenant-a", "art-1")
	require.NoError(t, err)
	dataA, _ := io.ReadAll(rcA)
	rcA.Close()
	assert.Equal(t, "a", string(dataA))

	pathB := filepath.Join(dir, "tenant-b", "art-1")
	_, err = os.Stat(pathB)
	assert.NoError(t, err, "tenant-b artifact should exist in separate dir")
}

func TestLocalArtifactStore_SHA256Integrity(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalArtifactStore(dir)
	ctx := context.Background()

	data := []byte("integrity check")
	art, err := store.Store(ctx, "acme", "art-hash", bytes.NewReader(data), "text/plain")
	require.NoError(t, err)

	rc, gotArt, err := store.Get(ctx, "acme", "art-hash")
	require.NoError(t, err)
	rc.Close()

	assert.Equal(t, art.Hash, gotArt.Hash, "hash should match between store and get")
}

func TestLocalArtifactStore_PathTraversal_TenantID(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalArtifactStore(dir)
	ctx := context.Background()

	_, err := store.Store(ctx, "../../etc", "passwd", bytes.NewReader([]byte("x")), "text/plain")
	assert.Error(t, err)

	_, _, err = store.Get(ctx, "../../etc", "passwd")
	assert.Error(t, err)
}

func TestLocalArtifactStore_PathTraversal_ArtifactID(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalArtifactStore(dir)
	ctx := context.Background()

	_, err := store.Store(ctx, "acme", "../../../etc/passwd", bytes.NewReader([]byte("x")), "text/plain")
	assert.Error(t, err)

	err = store.Delete(ctx, "acme", "../../secret")
	assert.Error(t, err)
}

func TestLocalArtifactStore_DotDot_ArtifactID(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalArtifactStore(dir)
	ctx := context.Background()

	_, err := store.Store(ctx, "acme", "..", bytes.NewReader([]byte("x")), "text/plain")
	assert.Error(t, err)
}

func TestLocalArtifactStore_EmptyID(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalArtifactStore(dir)
	ctx := context.Background()

	_, err := store.Store(ctx, "", "art-1", bytes.NewReader([]byte("x")), "text/plain")
	assert.Error(t, err)

	_, err = store.Store(ctx, "acme", "", bytes.NewReader([]byte("x")), "text/plain")
	assert.Error(t, err)
}
