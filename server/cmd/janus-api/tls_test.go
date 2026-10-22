package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/server/internal/config"
)

// generateTestCert generates a self-signed certificate and private key for testing.
func generateTestCert(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()

	// Generate private key
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	// Create certificate template
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test Org"},
			CommonName:   "localhost",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	// Self-sign the certificate
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	require.NoError(t, err)

	// Encode to PEM
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, err := x509.MarshalECPrivateKey(privateKey)
	require.NoError(t, err)
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	return certPEM, keyPEM
}

func TestBuildTLSConfig_NoClientCA(t *testing.T) {
	// Generate temporary cert/key for testing
	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "server.crt")
	keyFile := filepath.Join(tmpDir, "server.key")

	// Generate self-signed cert
	certPEM, keyPEM := generateTestCert(t)
	require.NoError(t, os.WriteFile(certFile, certPEM, 0644))
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0600))

	cfg := config.TLSConfig{
		Enabled:  true,
		CertFile: certFile,
		KeyFile:  keyFile,
	}
	tlsCfg, err := buildTLSConfig(cfg)
	require.NoError(t, err)
	assert.Equal(t, uint16(0x0303), uint16(tlsCfg.MinVersion), "MinVersion should be TLS 1.2")
	assert.Nil(t, tlsCfg.ClientCAs, "no client CA → no mTLS")
	assert.Equal(t, 0, int(tlsCfg.ClientAuth), "no RequireAndVerifyClientCert")
	assert.Len(t, tlsCfg.Certificates, 1, "should have loaded server certificate")
}

func TestBuildTLSConfig_WithClientCA(t *testing.T) {
	// Generate a minimal self-signed CA cert for testing.
	caPEM := `-----BEGIN CERTIFICATE-----
MIIBkTCB+wIJAJ8X9vYkqX2PMA0GCSqGSIb3DQEBCwUAMBQxEjAQBgNVBAMMCWxv
Y2FsaG9zdDAeFw0yNjA2MDEwMDAwMDBaFw0zNjA1MzAwMDAwMDBaMBQxEjAQBgNV
BAMMCWxvY2FsaG9zdDCBnzANBgkqhkiG9w0BAQEFAAOBjQAwgYkCgYEAxJ5J5p5J
5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J
5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J
5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J
5p5J5p5J5p5J5p5J5p5J5p0CAWEAATANBgkqhkiG9w0BAQsFAAOBgQDd/dx5J5pJ
5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J
5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J
5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J
5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J
5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J
5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J
5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J
5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J
5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J
5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J5p5J
-----END CERTIFICATE-----`

	tmpDir := t.TempDir()
	caPath := filepath.Join(tmpDir, "ca.crt")
	require.NoError(t, os.WriteFile(caPath, []byte(caPEM), 0644))

	// This is an invalid cert, so AppendCertsFromPEM should fail.
	cfg := config.TLSConfig{
		Enabled:      true,
		CertFile:     "server.crt",
		KeyFile:      "server.key",
		ClientCAFile: caPath,
	}
	_, err := buildTLSConfig(cfg)
	require.Error(t, err, "invalid CA cert should fail to parse")
}

func TestBuildTLSConfig_MissingClientCAFile(t *testing.T) {
	cfg := config.TLSConfig{
		Enabled:      true,
		ClientCAFile: "/nonexistent/ca.crt",
	}
	_, err := buildTLSConfig(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read client CA")
}
