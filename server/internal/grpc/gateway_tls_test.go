package grpc

import (
	"context"
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

	pb "github.com/agentium-lab/Janus/proto/gen/janus/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func generateTestCert(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

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
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	require.NoError(t, err)

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, err := x509.MarshalECPrivateKey(privateKey)
	require.NoError(t, err)
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	return certPEM, keyPEM
}

// caBundle is a freshly minted CA able to issue leaf certificates. Each
// call to generateTestCA produces an independent trust anchor, so tests can
// simulate "wrong CA" by mixing bundles.
type caBundle struct {
	certPEM []byte
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
}

func generateTestCA(t *testing.T, commonName string) *caBundle {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{Organization: []string{"Janus Test"}, CommonName: commonName},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(certDER)
	require.NoError(t, err)

	return &caBundle{
		certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}),
		cert:    cert,
		key:     key,
	}
}

// issueCert mints a leaf certificate signed by the CA. Server leaves carry
// DNSNames [localhost] + IP 127.0.0.1; client leaves carry clientAuth EKU.
func (ca *caBundle) issueCert(t *testing.T, commonName string, isClient bool) (certPEM, keyPEM []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject: pkix.Name{
			Organization: []string{"Janus Test"},
			CommonName:   commonName,
		},
		NotBefore: time.Now().Add(-time.Minute),
		NotAfter:  time.Now().Add(time.Hour),
		KeyUsage:  x509.KeyUsageDigitalSignature,
	}
	if isClient {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	} else {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		template.DNSNames = []string{"localhost"}
		template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, ca.cert, &key.PublicKey, ca.key)
	require.NoError(t, err)

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	return certPEM, keyPEM
}

// poolFrom returns a cert pool trusting only the given bundle.
func (ca *caBundle) pool(t *testing.T) *x509.CertPool {
	t.Helper()
	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(ca.certPEM), "parse CA certificate")
	return pool
}

// fakeAgentService answers ListAgents so a request through the gateway mux
// exercises a full HTTP -> grpc-gateway -> TLS -> gRPC round trip instead of
// stopping at handler-non-nil.
type fakeAgentService struct {
	pb.UnimplementedAgentServiceServer
}

func (fakeAgentService) ListAgents(context.Context, *pb.ListAgentsRequest) (*pb.ListAgentsResponse, error) {
	// A non-empty agent makes the RPC's JSON body observable: protojson
	// without EmitUnpopulated would render an empty response as "{}",
	// indistinguishable from a mux-level artifact.
	return &pb.ListAgentsResponse{Agents: []*pb.Agent{{Id: "gateway-tls-probe"}}}, nil
}

// startTLSServer runs a gRPC server with the given TLS config and a
// registered fake AgentService, returning its dial address.
func startTLSServer(t *testing.T, serverTLS *tls.Config) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)))
	pb.RegisterAgentServiceServer(srv, fakeAgentService{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	return lis.Addr().String()
}

// getThroughGateway registers the gateway against addr with clientTLS and
// performs one GET /v1/tenants/t1/agents through the handler — the same mux
// production mounts under /grpc/.
func getThroughGateway(t *testing.T, addr string, clientTLS *tls.Config) (*http.Response, string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	handler, err := RegisterGateway(ctx, addr, clientTLS)
	require.NoError(t, err)

	front := httptest.NewServer(handler)
	t.Cleanup(front.Close)

	resp, err := front.Client().Get(front.URL + "/v1/tenants/t1/agents")
	require.NoError(t, err, "HTTP request to the gateway must complete; a transport error means the mux itself is broken")
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	return resp, string(body)
}

func TestRegisterGateway_WithTLS(t *testing.T) {
	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "server.crt")
	keyFile := filepath.Join(tmpDir, "server.key")

	certPEM, keyPEM := generateTestCert(t)
	require.NoError(t, os.WriteFile(certFile, certPEM, 0644))
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0600))

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	require.NoError(t, err)

	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	grpcServer := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)))
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	clientTLS := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true, // Testing with self-signed cert
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	handler, err := RegisterGateway(ctx, lis.Addr().String(), clientTLS)
	require.NoError(t, err, "Gateway should register successfully with TLS")
	assert.NotNil(t, handler, "Handler should not be nil")
}

func TestRegisterGateway_WithoutTLS(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	grpcServer := grpc.NewServer()
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	handler, err := RegisterGateway(ctx, lis.Addr().String(), nil)
	require.NoError(t, err, "Gateway should register successfully without TLS")
	assert.NotNil(t, handler, "Handler should not be nil")
}

// T1: with the correct CA and ServerName the gateway verifies the server,
// and a request through the mux completes a full RPC round trip.
func TestRegisterGateway_TLS_VerifiedFullRoundTrip(t *testing.T) {
	ca := generateTestCA(t, "server-ca")
	serverCertPEM, serverKeyPEM := ca.issueCert(t, "localhost", false)
	serverCert, err := tls.X509KeyPair(serverCertPEM, serverKeyPEM)
	require.NoError(t, err)

	addr := startTLSServer(t, &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		MinVersion:   tls.VersionTLS12,
	})

	clientTLS := &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    ca.pool(t),
		ServerName: "localhost",
	}

	resp, body := getThroughGateway(t, addr, clientTLS)
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusOK, resp.StatusCode, "request must succeed end to end, body: %s", body)
	assert.Contains(t, body, `"id":"gateway-tls-probe"`, "response must come from the ListAgents RPC, body: %s", body)
}

// T2: a CA the server certificate was NOT signed by must be rejected —
// verification is active, not decorative.
func TestRegisterGateway_TLS_WrongCARejected(t *testing.T) {
	ca := generateTestCA(t, "server-ca")
	serverCertPEM, serverKeyPEM := ca.issueCert(t, "localhost", false)
	serverCert, err := tls.X509KeyPair(serverCertPEM, serverKeyPEM)
	require.NoError(t, err)

	addr := startTLSServer(t, &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		MinVersion:   tls.VersionTLS12,
	})

	wrongCA := generateTestCA(t, "untrusted-ca")
	clientTLS := &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    wrongCA.pool(t),
		ServerName: "localhost",
	}

	resp, body := getThroughGateway(t, addr, clientTLS)
	require.NotNil(t, resp)
	assert.NotEqual(t, http.StatusOK, resp.StatusCode, "wrong CA must fail the request, body: %s", body)
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode,
		"handshake failure must surface as an RPC-level 503, got %d, body: %s", resp.StatusCode, body)
}

// T3: the correct CA with the WRONG ServerName must fail certificate
// verification (hostname mismatch).
func TestRegisterGateway_TLS_WrongServerNameRejected(t *testing.T) {
	ca := generateTestCA(t, "server-ca")
	serverCertPEM, serverKeyPEM := ca.issueCert(t, "localhost", false)
	serverCert, err := tls.X509KeyPair(serverCertPEM, serverKeyPEM)
	require.NoError(t, err)

	addr := startTLSServer(t, &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		MinVersion:   tls.VersionTLS12,
	})

	clientTLS := &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    ca.pool(t),
		ServerName: "wrong.example.invalid",
	}

	resp, body := getThroughGateway(t, addr, clientTLS)
	require.NotNil(t, resp)
	assert.NotEqual(t, http.StatusOK, resp.StatusCode, "wrong ServerName must fail the request, body: %s", body)
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode,
		"certificate verification failure must surface as an RPC-level 503, got %d, body: %s", resp.StatusCode, body)
}

// T4: when the server requires a client certificate (mTLS posture from
// buildTLSConfig: ClientCAs + RequireAndVerifyClientCert), a gateway without
// a client keypair is rejected at the handshake, and one with a CA-signed
// keypair completes the full round trip.
func TestRegisterGateway_TLS_ServerRequiresClientCert(t *testing.T) {
	ca := generateTestCA(t, "mtls-ca")
	serverCertPEM, serverKeyPEM := ca.issueCert(t, "localhost", false)
	serverCert, err := tls.X509KeyPair(serverCertPEM, serverKeyPEM)
	require.NoError(t, err)

	clientCertPEM, clientKeyPEM := ca.issueCert(t, "gateway-client", true)
	clientPair, err := tls.X509KeyPair(clientCertPEM, clientKeyPEM)
	require.NoError(t, err)

	addr := startTLSServer(t, &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientCAs:    ca.pool(t),
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	})

	// Without a client certificate: handshake rejected.
	resp, body := getThroughGateway(t, addr, &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    ca.pool(t),
		ServerName: "localhost",
	})
	require.NotNil(t, resp)
	assert.NotEqual(t, http.StatusOK, resp.StatusCode, "missing client certificate must fail the request, body: %s", body)
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode,
		"handshake rejection must surface as an RPC-level 503, got %d, body: %s", resp.StatusCode, body)

	// With the CA-signed client keypair: the mTLS round trip succeeds.
	resp, body = getThroughGateway(t, addr, &tls.Config{
		MinVersion:   tls.VersionTLS12,
		RootCAs:      ca.pool(t),
		ServerName:   "localhost",
		Certificates: []tls.Certificate{clientPair},
	})
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusOK, resp.StatusCode, "mTLS request must succeed end to end, body: %s", body)
	assert.Contains(t, body, `"id":"gateway-tls-probe"`, "response must come from the ListAgents RPC, body: %s", body)
}
