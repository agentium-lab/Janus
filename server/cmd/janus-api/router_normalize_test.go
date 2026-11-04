package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// SECURITY: after trimming a trailing request slash, Path must hold the
// DECODED id with RawPath preserved for round-tripping — writing the
// escaped form into Path would double-encode "t1%2F" into "t1%252F" and
// conflate two distinct resources.
func TestRouter_NormalizationDoesNotDoubleEncode(t *testing.T) {
	var gotPath, gotRawPath string
	capture := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotRawPath = r.URL.EscapedPath()
	})
	h := withTenantPathNormalization(capture)
	mk := func(target string) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
	}

	// Given an encoded slash id with a trailing slash, the trimmed Path is decoded.
	mk("/v1/tenants/acme/tasks/t1%2F/")
	if gotPath != "/v1/tenants/acme/tasks/t1/" {
		t.Fatalf("encoded-slash trim: Path = %q, want %q", gotPath, "/v1/tenants/acme/tasks/t1/")
	}
	if gotRawPath != "/v1/tenants/acme/tasks/t1%2F" {
		t.Fatalf("encoded-slash trim: EscapedPath = %q, want %q", gotRawPath, "/v1/tenants/acme/tasks/t1%2F")
	}

	// Given a plain id with a trailing slash, it is trimmed.
	mk("/v1/tenants/acme/tasks/t1/")
	if gotPath != "/v1/tenants/acme/tasks/t1" || gotRawPath != "/v1/tenants/acme/tasks/t1" {
		t.Fatalf("plain trim: Path=%q EscapedPath=%q, want t1", gotPath, gotRawPath)
	}

	// Given no trailing slash, the path is untouched: Go already decoded
	// %%2F into Path, and EscapedPath must still round-trip to %%2F.
	mk("/v1/tenants/acme/tasks/t1%2F")
	if gotPath != "/v1/tenants/acme/tasks/t1/" || gotRawPath != "/v1/tenants/acme/tasks/t1%2F" {
		t.Fatalf("no-trim case: Path=%q EscapedPath=%q, want decoded t1/ with %%2F round-trip", gotPath, gotRawPath)
	}
}
