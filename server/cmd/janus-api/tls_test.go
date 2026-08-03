package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/server/internal/config"
)

func TestBuildTLSConfig_NoClientCA(t *testing.T) {
	cfg := config.TLSConfig{
		Enabled:  true,
		CertFile: "server.crt",
		KeyFile:  "server.key",
	}
	tlsCfg, err := buildTLSConfig(cfg)
	require.NoError(t, err)
	assert.Equal(t, uint16(0x0303), uint16(tlsCfg.MinVersion), "MinVersion should be TLS 1.2")
	assert.Nil(t, tlsCfg.ClientCAs, "no client CA → no mTLS")
	assert.Equal(t, 0, int(tlsCfg.ClientAuth), "no RequireAndVerifyClientCert")
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
