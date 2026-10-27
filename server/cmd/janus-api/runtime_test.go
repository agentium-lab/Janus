package main

import (
	"bytes"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/server/internal/config"
)

// writeGatewayCA writes a parseable self-signed certificate to a temp file.
// x509.CertPool.AppendCertsFromPEM only requires a parseable PEM certificate
// (not a real CA), which generateTestCert (tls_test.go) produces.
func writeGatewayCA(t *testing.T) string {
	t.Helper()
	certPEM, _ := generateTestCert(t)
	caPath := filepath.Join(t.TempDir(), "gateway-ca.crt")
	require.NoError(t, os.WriteFile(caPath, certPEM, 0644))
	return caPath
}

// T1: no CA + insecure unset → refuse to start, naming the CA + opt-in envs.
func TestBuildGatewayClientTLS_NoCA_RefusesToStart(t *testing.T) {
	_, err := buildGatewayClientTLS(config.TLSConfig{Enabled: true})
	require.Error(t, err, "no CA + no explicit insecure opt-in must refuse to start")
	msg := err.Error()
	assert.Contains(t, msg, "CA", "error must name the missing CA")
	assert.Contains(t, msg, "JANUS_TLS_GATEWAY_CA_CERT_FILE", "error must name the CA env var")
	assert.Contains(t, msg, "JANUS_TLS_GATEWAY_INSECURE", "error must name the explicit opt-in env var")
}

// T2: no CA + insecure=true → explicit opt-in keeps skip-verify, warned.
func TestBuildGatewayClientTLS_NoCA_InsecureExplicitOptIn(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	cfg, err := buildGatewayClientTLS(config.TLSConfig{
		Enabled: true,
		Gateway: config.GatewayTLSConfig{Insecure: true},
	})
	require.NoError(t, err)
	assert.True(t, cfg.InsecureSkipVerify, "explicit opt-in must skip verification")
	assert.Nil(t, cfg.RootCAs, "no CA configured → no root pool")
	assert.Contains(t, buf.String(), "JANUS_TLS_GATEWAY_INSECURE", "the skip must be warned, never silent")
	assert.Contains(t, buf.String(), "NOT verified", "warning must state the identity is unverified")
}

// T3: CA + insecure=true → conflicting config, must error.
func TestBuildGatewayClientTLS_CAAndInsecure_Conflict(t *testing.T) {
	_, err := buildGatewayClientTLS(config.TLSConfig{
		Enabled: true,
		Gateway: config.GatewayTLSConfig{CACertFile: writeGatewayCA(t), Insecure: true},
	})
	require.Error(t, err, "ca_cert_file + insecure=true is a conflicting config")
	assert.Contains(t, err.Error(), "conflicts", "error must say the settings conflict")
}

// T4: CA set + insecure unset → verification exactly as before.
func TestBuildGatewayClientTLS_CAVerifies(t *testing.T) {
	cfg, err := buildGatewayClientTLS(config.TLSConfig{
		Enabled: true,
		Gateway: config.GatewayTLSConfig{CACertFile: writeGatewayCA(t)},
	})
	require.NoError(t, err)
	assert.False(t, cfg.InsecureSkipVerify, "CA configured → verification must stay on")
	assert.NotNil(t, cfg.RootCAs, "CA configured → RootCAs populated")
}
