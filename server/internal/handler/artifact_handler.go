package handler

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/agentium-lab/Janus/server/internal/artifact"
)

// ArtifactHandler serves the minimal local-disk artifact store: upload with
// sha256, streamed download, delete. Limits (single-file cap, per-tenant
// quota) are enforced here because the Store is a plain content-addressed
// cabinet with no policy of its own. Small-file/trial mode: production
// storage is bring-your-own (S3 adapters are Enterprise, GOV-15).
type ArtifactHandler struct {
	store       artifact.Store
	dir         string
	maxSize     int64
	tenantQuota int64
}

func NewArtifactHandler(store artifact.Store, dir string, maxSizeMB, tenantQuotaMB int) *ArtifactHandler {
	return &ArtifactHandler{
		store:       store,
		dir:         dir,
		maxSize:     int64(maxSizeMB) << 20,
		tenantQuota: int64(tenantQuotaMB) << 20,
	}
}

func (h *ArtifactHandler) disabled() bool { return h.store == nil || h.dir == "" }

func (h *ArtifactHandler) Upload(w http.ResponseWriter, r *http.Request) {
	if h.disabled() {
		writeError(w, http.StatusServiceUnavailable, "artifact store not configured (artifact.dir)")
		return
	}
	tenantID, artifactID := r.PathValue("tenantID"), r.PathValue("artifactID")
	if tenantID == "" || artifactID == "" {
		writeError(w, http.StatusBadRequest, "tenant id and artifact id are required")
		return
	}

	if h.tenantQuota > 0 {
		used, err := h.tenantUsage(tenantID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "quota check failed")
			return
		}
		if used >= h.tenantQuota {
			writeError(w, http.StatusRequestEntityTooLarge,
				fmt.Sprintf("tenant artifact quota exhausted (%d MB)", h.tenantQuota>>20))
			return
		}
	}

	var body io.Reader = r.Body
	if h.maxSize > 0 {
		body = http.MaxBytesReader(w, r.Body, h.maxSize)
	}
	art, err := h.store.Store(r.Context(), tenantID, artifactID, body, r.Header.Get("Content-Type"))
	if err != nil {
		var maxErr *http.MaxBytesError
		switch {
		case errors.As(err, &maxErr), strings.Contains(err.Error(), "request body too large"):
			writeError(w, http.StatusRequestEntityTooLarge,
				fmt.Sprintf("artifact exceeds max size (%d MB)", h.maxSize>>20))
		default:
			writeError(w, http.StatusBadRequest, err.Error())
		}
		return
	}
	if h.tenantQuota > 0 {
		used, qerr := h.tenantUsage(tenantID)
		if qerr != nil {
			writeError(w, http.StatusInternalServerError, "quota check failed")
			return
		}
		if used > h.tenantQuota {
			// The single-file cap bounded the write; enforce the exact total
			// after it: roll the artifact back and refuse.
			_ = h.store.Delete(r.Context(), tenantID, artifactID)
			writeError(w, http.StatusRequestEntityTooLarge,
				fmt.Sprintf("tenant artifact quota exhausted (%d MB)", h.tenantQuota>>20))
			return
		}
	}
	writeJSON(w, http.StatusCreated, art)
}

func (h *ArtifactHandler) Download(w http.ResponseWriter, r *http.Request) {
	if h.disabled() {
		writeError(w, http.StatusServiceUnavailable, "artifact store not configured (artifact.dir)")
		return
	}
	tenantID, artifactID := r.PathValue("tenantID"), r.PathValue("artifactID")
	if tenantID == "" || artifactID == "" {
		writeError(w, http.StatusBadRequest, "tenant id and artifact id are required")
		return
	}
	body, art, err := h.store.Get(r.Context(), tenantID, artifactID)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) || strings.Contains(err.Error(), "not found") {
			writeError(w, http.StatusNotFound, "artifact not found")
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	defer body.Close()

	if art.ContentType != "" {
		w.Header().Set("Content-Type", art.ContentType)
	}
	w.Header().Set("ETag", `"`+art.Hash+`"`)
	// Never let the browser execute a stored artifact: force download and
	// disable content-type sniffing.
	w.Header().Set("Content-Disposition", "attachment; filename="+strconvQuote(artifactID))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, body)
}

func (h *ArtifactHandler) Delete(w http.ResponseWriter, r *http.Request) {
	if h.disabled() {
		writeError(w, http.StatusServiceUnavailable, "artifact store not configured (artifact.dir)")
		return
	}
	tenantID, artifactID := r.PathValue("tenantID"), r.PathValue("artifactID")
	if tenantID == "" || artifactID == "" {
		writeError(w, http.StatusBadRequest, "tenant id and artifact id are required")
		return
	}
	if err := h.store.Delete(r.Context(), tenantID, artifactID); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// tenantUsage sums stored artifact bytes for the quota check. A directory
// walk per upload is acceptable in the small-file/trial mode this store
// ships as; a production store tracks usage incrementally.
func (h *ArtifactHandler) tenantUsage(tenantID string) (int64, error) {
	root := filepath.Join(h.dir, tenantID)
	var total int64
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() || strings.HasSuffix(path, ".meta.json") {
			return nil
		}
		info, serr := d.Info()
		if serr != nil {
			return serr
		}
		total += info.Size()
		return nil
	})
	if err != nil {
		return 0, err
	}
	return total, nil
}

func strconvQuote(s string) string { return `"` + strings.ReplaceAll(s, `"`, "") + `"` }
