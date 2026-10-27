// Command janus-healthcheck is a TLS-capable health probe for the Janus
// broker. It exists because kubelet httpGet probes cannot present a client
// certificate: under config.tls.clientCaSecret (mTLS) the chart switches
// liveness/readiness probes to exec probes running this binary with the
// mounted probe client pair (config.tls.probeClientCertSecret).
//
// Exit codes: 0 on HTTP 2xx after a full TLS handshake, 1 otherwise
// (non-2xx status, TLS handshake failure, or bad flags/files). Exactly one
// status/error line goes to stderr.
package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

// probeTimeout bounds the whole check: dial + TLS handshake + request.
// Kubelet probe timeouts (probes.*.timeoutSeconds) sit above it.
const probeTimeout = 5 * time.Second

func main() {
	os.Exit(run(os.Args[1:], os.Stderr))
}

func run(args []string, errOut io.Writer) int {
	fs := flag.NewFlagSet("janus-healthcheck", flag.ContinueOnError)
	fs.SetOutput(errOut)
	var (
		url       = fs.String("url", "", "health endpoint URL (required), e.g. https://127.0.0.1:8080/healthz")
		caFile    = fs.String("ca-file", "", "CA bundle used to verify the server certificate")
		clientCrt = fs.String("client-cert", "", "client certificate for mTLS (requires -client-key)")
		clientKey = fs.String("client-key", "", "client key for mTLS (requires -client-cert)")
		insecure  = fs.Bool("insecure-skip-verify", false, "skip server certificate verification (explicit opt-in)")
	)
	if err := fs.Parse(args); err != nil {
		return 1 // the flag package already printed the error
	}
	if *url == "" {
		fmt.Fprintln(errOut, "janus-healthcheck: -url is required")
		return 1
	}
	if (*clientCrt == "") != (*clientKey == "") {
		fmt.Fprintln(errOut, "janus-healthcheck: -client-cert and -client-key must be set together")
		return 1
	}
	if *caFile != "" && *insecure {
		fmt.Fprintln(errOut, "janus-healthcheck: -ca-file and -insecure-skip-verify are mutually exclusive")
		return 1
	}

	tlsCfg := &tls.Config{}
	if *caFile != "" {
		pem, err := os.ReadFile(*caFile)
		if err != nil {
			fmt.Fprintf(errOut, "janus-healthcheck: read CA %s: %v\n", *caFile, err)
			return 1
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			fmt.Fprintf(errOut, "janus-healthcheck: %s contains no PEM certificate\n", *caFile)
			return 1
		}
		tlsCfg.RootCAs = pool
	} else if *insecure {
		tlsCfg.InsecureSkipVerify = true
	}
	if *clientCrt != "" {
		cert, err := tls.LoadX509KeyPair(*clientCrt, *clientKey)
		if err != nil {
			fmt.Fprintf(errOut, "janus-healthcheck: load client pair: %v\n", err)
			return 1
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	client := &http.Client{
		Timeout: probeTimeout,
		Transport: &http.Transport{
			// A health probe must never ride a proxy, least of all to loopback.
			Proxy:           nil,
			TLSClientConfig: tlsCfg,
		},
	}
	resp, err := client.Get(*url)
	if err != nil {
		fmt.Fprintf(errOut, "janus-healthcheck: %s: %v\n", *url, err)
		return 1
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		fmt.Fprintf(errOut, "janus-healthcheck: %s: HTTP %s\n", *url, resp.Status)
		return 1
	}
	fmt.Fprintf(errOut, "janus-healthcheck: %s: HTTP %s\n", *url, resp.Status)
	return 0
}
