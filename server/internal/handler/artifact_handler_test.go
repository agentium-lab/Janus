package handler

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/agentium-lab/Janus/server/internal/artifact"
)

func newArtifactTestHandler(t *testing.T, dir string, maxSizeMB, quotaMB int) *ArtifactHandler {
	t.Helper()
	return NewArtifactHandler(artifact.NewLocalArtifactStore(dir), dir, maxSizeMB, quotaMB)
}

func artifactReq(t *testing.T, method, tenant, id string, body []byte, contentType string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, "/v1/tenants/"+tenant+"/artifacts/"+id, bytes.NewReader(body))
	req.SetPathValue("tenantID", tenant)
	req.SetPathValue("artifactID", id)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return req
}

func TestArtifactHandler_Roundtrip(t *testing.T) {
	h := newArtifactTestHandler(t, t.TempDir(), 10, 1024)

	rec := httptest.NewRecorder()
	h.Upload(rec, artifactReq(t, "PUT", "acme", "rep-1", []byte("hello artifact"), "text/plain"))
	if rec.Code != http.StatusCreated {
		t.Fatalf("upload: %d %s", rec.Code, rec.Body.String())
	}
	var meta artifact.Artifact
	if err := json.Unmarshal(rec.Body.Bytes(), &meta); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte("hello artifact"))
	if meta.Hash != hex.EncodeToString(sum[:]) {
		t.Fatalf("hash mismatch: %s", meta.Hash)
	}

	rec = httptest.NewRecorder()
	h.Download(rec, artifactReq(t, "GET", "acme", "rep-1", nil, ""))
	if rec.Code != http.StatusOK {
		t.Fatalf("download: %d %s", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != "hello artifact" {
		t.Fatalf("body roundtrip mismatch: %q", rec.Body.String())
	}
	if rec.Header().Get("Content-Disposition") == "" ||
		rec.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("download hardening headers missing: %v", rec.Header())
	}
	if rec.Header().Get("ETag") != `"`+meta.Hash+`"` {
		t.Fatalf("etag should carry the artifact hash")
	}

	rec = httptest.NewRecorder()
	h.Delete(rec, artifactReq(t, "DELETE", "acme", "rep-1", nil, ""))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete: %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	h.Download(rec, artifactReq(t, "GET", "acme", "rep-1", nil, ""))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("download after delete: want 404, got %d", rec.Code)
	}
}

func TestArtifactHandler_TenantIsolation(t *testing.T) {
	h := newArtifactTestHandler(t, t.TempDir(), 10, 1024)
	rec := httptest.NewRecorder()
	h.Upload(rec, artifactReq(t, "PUT", "acme", "secret", []byte("acme only"), ""))
	if rec.Code != http.StatusCreated {
		t.Fatalf("upload: %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	h.Download(rec, artifactReq(t, "GET", "evil", "secret", nil, ""))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant download must 404, got %d", rec.Code)
	}
}

func TestArtifactHandler_MaxSizeCap(t *testing.T) {
	h := newArtifactTestHandler(t, t.TempDir(), 1, 1024)
	big := bytes.Repeat([]byte("x"), 2<<20)
	rec := httptest.NewRecorder()
	h.Upload(rec, artifactReq(t, "PUT", "acme", "big", big, ""))
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized upload: want 413, got %d %s", rec.Code, rec.Body.String())
	}
}

func TestArtifactHandler_TenantQuota(t *testing.T) {
	h := newArtifactTestHandler(t, t.TempDir(), 10, 1)
	for i := 0; i < 3; i++ {
		rec := httptest.NewRecorder()
		h.Upload(rec, artifactReq(t, "PUT", "acme", padID(i), bytes.Repeat([]byte("y"), 400<<10), ""))
		// 400KB x 3 > 1MB: third must 413
		if i < 2 && rec.Code != http.StatusCreated {
			t.Fatalf("upload %d: %d %s", i, rec.Code, rec.Body.String())
		}
		if i == 2 && rec.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("over-quota upload: want 413, got %d %s", rec.Code, rec.Body.String())
		}
	}
	// other tenant unaffected
	rec := httptest.NewRecorder()
	h.Upload(rec, artifactReq(t, "PUT", "other", "a", bytes.Repeat([]byte("z"), 400<<10), ""))
	if rec.Code != http.StatusCreated {
		t.Fatalf("other tenant upload: %d", rec.Code)
	}
}

func TestArtifactHandler_PathTraversalRejected(t *testing.T) {
	h := newArtifactTestHandler(t, t.TempDir(), 10, 1024)
	for _, id := range []string{"../escape", "a/b", "..", "a..b"} {
		rec := httptest.NewRecorder()
		h.Upload(rec, artifactReq(t, "PUT", "acme", id, []byte("x"), ""))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("traversal id %q: want 400, got %d", id, rec.Code)
		}
	}
}

func TestArtifactHandler_DisabledWhenUnconfigured(t *testing.T) {
	h := NewArtifactHandler(nil, "", 10, 1024)
	for method, fn := range map[string]func(http.ResponseWriter, *http.Request){
		"PUT":    h.Upload,
		"GET":    h.Download,
		"DELETE": h.Delete,
	} {
		rec := httptest.NewRecorder()
		fn(rec, artifactReq(t, method, "acme", "a", []byte("x"), ""))
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s unconfigured: want 503, got %d", method, rec.Code)
		}
	}
}

// Restart persistence: a fresh handler over the SAME directory (new process
// shape) reads back what the previous one stored — the trial-mode durability
// promise behind GOV-11's smoke obligation.
func TestArtifactHandler_RestartPersistence(t *testing.T) {
	dir := t.TempDir()
	h1 := newArtifactTestHandler(t, dir, 10, 1024)
	rec := httptest.NewRecorder()
	h1.Upload(rec, artifactReq(t, "PUT", "acme", "durable", []byte("survives restarts"), "application/json"))
	if rec.Code != http.StatusCreated {
		t.Fatalf("upload: %d", rec.Code)
	}

	h2 := newArtifactTestHandler(t, dir, 10, 1024)
	rec = httptest.NewRecorder()
	h2.Download(rec, artifactReq(t, "GET", "acme", "durable", nil, ""))
	if rec.Code != http.StatusOK || rec.Body.String() != "survives restarts" {
		t.Fatalf("after restart: %d %q", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("stored content type lost: %q", rec.Header().Get("Content-Type"))
	}
	// quota accounting also survives: sidecar metadata is counted, data files only
	if used, err := h2.tenantUsage("acme"); err != nil || used != int64(len("survives restarts")) {
		t.Fatalf("usage after restart = %d (err %v), want %d", used, err, len("survives restarts"))
	}
}

func padID(i int) string { return "part-" + string(rune('0'+i)) }
