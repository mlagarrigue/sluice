package nats_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/messaging/nats"
)

// tlsServer is a NATS server that requires TLS, at SLUICE_NATS_TLS, whose
// self-signed certificate for 127.0.0.1 is in SLUICE_NATS_TLS_CA; CI starts
// it beside the plain one.
func tlsServer(t *testing.T) (addr string, roots *x509.CertPool) {
	t.Helper()
	addr = os.Getenv("SLUICE_NATS_TLS")
	if addr == "" {
		t.Skip("SLUICE_NATS_TLS is not set: no TLS server to run against")
	}
	pem, err := os.ReadFile(os.Getenv("SLUICE_NATS_TLS_CA")) //nolint:gosec // G703: the path is the harness's own configuration, as SLUICE_NATS is
	if err != nil {
		t.Fatalf("SLUICE_NATS_TLS_CA: %v", err)
	}
	roots = x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		t.Fatal("SLUICE_NATS_TLS_CA holds no certificate")
	}
	return addr, roots
}

func connectTLS(t *testing.T, addr string, cfg *tls.Config) (*nats.Conn, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	return nats.Connect(ctx, nats.DialTCP(addr), nats.Config{Name: "sluice-test", TLS: cfg})
}

// The certificate names 127.0.0.1, whatever address the server is reached
// at: ServerName is what the handshake verifies.
func TestTLSConnectsAndVerifiesTheServer(t *testing.T) {
	addr, roots := tlsServer(t)
	c, err := connectTLS(t, addr, &tls.Config{ServerName: "127.0.0.1", RootCAs: roots, MinVersion: tls.VersionTLS12})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Request(t.Context(), "$JS.API.INFO", nil); err != nil {
		t.Fatalf("a request over TLS: %v", err)
	}

	_, err = connectTLS(t, addr, &tls.Config{ServerName: "127.0.0.1", RootCAs: x509.NewCertPool()})
	var unknown x509.UnknownAuthorityError
	if !errors.As(err, &unknown) {
		t.Fatalf("a root that did not sign the certificate: %v", err)
	}
	_, err = connectTLS(t, addr, &tls.Config{ServerName: "nats.invalid", RootCAs: roots})
	var host x509.HostnameError
	if !errors.As(err, &host) {
		t.Fatalf("a name the certificate does not hold: %v", err)
	}
}

func TestTLSRequiredWithoutConfigIsRefused(t *testing.T) {
	addr, _ := tlsServer(t)
	_, err := connectTLS(t, addr, nil)
	if err == nil || !strings.Contains(err.Error(), "requires TLS") {
		t.Fatalf("err %v, want the server's requirement named", err)
	}
}
