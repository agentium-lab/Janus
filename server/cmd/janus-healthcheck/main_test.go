package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// --- ECDSA test PKI (pattern: server/internal/grpc/gateway_tls_test.go) ---

type testCA struct {
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
	certPEM []byte
	caFile  string
}

func newTestCA(t *testing.T, cn string) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	file := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(file, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write CA file: %v", err)
	}
	return &testCA{cert: cert, key: key, certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), caFile: file}
}

// issue signs a leaf certificate; server certs carry localhost/127.0.0.1
// SANs, client certs carry clientAuth EKU, and both land in temp files.
func (ca *testCA) issue(t *testing.T, cn string, isClient bool) (crtPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	if isClient {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	} else {
		tmpl.DNSNames = []string{"localhost"}
		tmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create leaf cert: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal leaf key: %v", err)
	}
	dir := t.TempDir()
	crtPath = filepath.Join(dir, cn+".crt")
	keyPath = filepath.Join(dir, cn+".key")
	if err := os.WriteFile(crtPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write leaf cert: %v", err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatalf("write leaf key: %v", err)
	}
	return crtPath, keyPath
}

func (ca *testCA) pool() *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)
	return pool
}

// newTLSServer starts an httptest TLS server whose certificate the given CA
// signed; requireClient turns on server-side mTLS (client certs mandatory).
func newTLSServer(t *testing.T, ca *testCA, requireClient bool, status int) *httptest.Server {
	t.Helper()
	crt, key := ca.issue(t, "localhost", false)
	cert, err := tls.LoadX509KeyPair(crt, key)
	if err != nil {
		t.Fatalf("load server pair: %v", err)
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}}
	if requireClient {
		cfg.ClientCAs = ca.pool()
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
	}))
	s.TLS = cfg
	s.StartTLS()
	t.Cleanup(s.Close)
	return s
}

// T1: plain http URL, 2xx -> exit 0.
func TestHealthcheck_PlainHTTP_OK(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer s.Close()
	if code := run([]string{"-url", s.URL + "/healthz"}, io.Discard); code != 0 {
		t.Fatalf("plain http 2xx: want exit 0, got %d", code)
	}
}

// T2: https, server demands a client cert, correct CA + client pair -> 0.
func TestHealthcheck_MTLS_ClientCert_OK(t *testing.T) {
	ca := newTestCA(t, "healthcheck-test-ca")
	s := newTLSServer(t, ca, true, http.StatusOK)
	crt, key := ca.issue(t, "healthcheck-client", true)
	args := []string{
		"-url", s.URL + "/healthz",
		"-ca-file", ca.caFile,
		"-client-cert", crt,
		"-client-key", key,
	}
	if code := run(args, io.Discard); code != 0 {
		t.Fatalf("mTLS with correct CA + client pair: want exit 0, got %d", code)
	}
}

// T3: https with the WRONG CA -> handshake verification fails -> exit 1.
func TestHealthcheck_WrongCA_Fails(t *testing.T) {
	ca := newTestCA(t, "healthcheck-test-ca")
	wrong := newTestCA(t, "healthcheck-wrong-ca")
	s := newTLSServer(t, ca, false, http.StatusOK)
	if code := run([]string{"-url", s.URL + "/healthz", "-ca-file", wrong.caFile}, io.Discard); code != 1 {
		t.Fatalf("wrong CA: want exit 1, got %d", code)
	}
}

// T4: server demands a client cert, none presented -> handshake fails -> 1.
func TestHealthcheck_NoClientCert_Fails(t *testing.T) {
	ca := newTestCA(t, "healthcheck-test-ca")
	s := newTLSServer(t, ca, true, http.StatusOK)
	if code := run([]string{"-url", s.URL + "/healthz", "-ca-file", ca.caFile}, io.Discard); code != 1 {
		t.Fatalf("no client cert against mTLS server: want exit 1, got %d", code)
	}
}

// T5: TLS handshake succeeds but the status is non-2xx -> exit 1.
func TestHealthcheck_Non2xx_Fails(t *testing.T) {
	ca := newTestCA(t, "healthcheck-test-ca")
	s := newTLSServer(t, ca, false, http.StatusInternalServerError)
	if code := run([]string{"-url", s.URL + "/healthz", "-ca-file", ca.caFile}, io.Discard); code != 1 {
		t.Fatalf("non-2xx: want exit 1, got %d", code)
	}
}

func TestHealthcheck_FlagAndFileErrors(t *testing.T) {
	ca := newTestCA(t, "healthcheck-test-ca")
	s := newTLSServer(t, ca, false, http.StatusOK)
	crt, key := ca.issue(t, "healthcheck-client", true)
	cases := []struct {
		name string
		args []string
	}{
		{"missing -url", []string{"-ca-file", ca.caFile}},
		{"cert without key", []string{"-url", s.URL, "-client-cert", crt}},
		{"key without cert", []string{"-url", s.URL, "-client-key", key}},
		{"ca-file with insecure", []string{"-url", s.URL, "-ca-file", ca.caFile, "-insecure-skip-verify"}},
		{"unreadable CA file", []string{"-url", s.URL, "-ca-file", filepath.Join(t.TempDir(), "missing.crt")}},
		{"non-PEM CA file", []string{"-url", s.URL, "-ca-file", writeTemp(t, "junk", "not a certificate")}},
		{"bad client pair", []string{"-url", s.URL, "-client-cert", writeTemp(t, "c", "x"), "-client-key", writeTemp(t, "k", "y")}},
		{"unknown flag", []string{"-url", s.URL, "-nope"}},
	}
	for _, tc := range cases {
		if code := run(tc.args, io.Discard); code != 1 {
			t.Errorf("%s: want exit 1, got %d", tc.name, code)
		}
	}
	// insecure path still succeeds against a plain TLS server (no CA given).
	if code := run([]string{"-url", s.URL, "-insecure-skip-verify"}, io.Discard); code != 0 {
		t.Errorf("insecure-skip-verify without CA: want exit 0, got %d", code)
	}
}

func writeTemp(t *testing.T, name, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}
