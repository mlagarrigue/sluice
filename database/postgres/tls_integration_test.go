package postgres_test

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"net"
	"os"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/database/postgres"
)

// SCRAM-SHA-256-PLUS against a server that hashes its own certificate. The
// scripted server in scram_plus_test.go computes the binding with the same
// tlsServerEndPoint as the client, so it cannot disagree with it; PostgreSQL
// computes it in be_tls_get_certificate_hash (be-secure-openssl.c), from the
// hash OpenSSL's X509_get_signature_info reports — for RSASSA-PSS the hash
// inside the signature parameters, which is what tlsServerEndPoint reads too.
// If the two hashes differ, the server's proof does not verify and Startup
// fails.
//
// It needs a server with ssl = on and the certificate to trust in
// SLUICE_PG_TLS_CA; CI makes both with scripts/pg-tls.sh, once with an RSA
// PKCS #1 v1.5 certificate and once with an RSA-PSS SHA-384 one, and names
// the signature it expects in SLUICE_PG_TLS_SIGALG, so that a server that
// kept its previous certificate cannot pass for the new one.
//
// One RSA-PSS certificate is refused where PostgreSQL and libpq accept it:
// a salt length other than the hash length, which is OpenSSL's default.
// crypto/x509 does not parse that signature, so crypto/tls fails the
// handshake before Startup is called; nothing in this package is reached.
func TestIntegrationChannelBindingOverTLS(t *testing.T) {
	addr, cfg := pgTarget(t)
	caFile := os.Getenv("SLUICE_PG_TLS_CA")
	if caFile == "" {
		t.Skip("SLUICE_PG_TLS_CA is not set: no server certificate to trust")
	}
	ca, err := os.ReadFile(caFile) //nolint:gosec // G304: the path is the harness's own configuration, as SLUICE_PG is
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatalf("%s holds no PEM certificate", caFile)
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}

	nc, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dialling %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = nc.Close() })
	if err := nc.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}

	// SSLRequest: length 8, then 80877103 where a protocol version would be.
	req := binary.BigEndian.AppendUint32(binary.BigEndian.AppendUint32(nil, 8), 80877103)
	if _, err := nc.Write(req); err != nil {
		t.Fatal(err)
	}
	answer := make([]byte, 1)
	if _, err := nc.Read(answer); err != nil {
		t.Fatal(err)
	}
	if answer[0] != 'S' {
		t.Fatalf("the server answered %q to SSLRequest, want 'S': ssl is not on", answer[0])
	}

	tc := tls.Client(nc, &tls.Config{ServerName: host, RootCAs: roots, MinVersion: tls.VersionTLS12})
	if err := tc.Handshake(); err != nil {
		t.Fatalf("TLS handshake: %v", err)
	}
	leaf := tc.ConnectionState().PeerCertificates[0]
	t.Logf("server certificate signed with %v", leaf.SignatureAlgorithm)
	if want := os.Getenv("SLUICE_PG_TLS_SIGALG"); want != "" && leaf.SignatureAlgorithm.String() != want {
		t.Fatalf("the server presented a certificate signed with %v, want %s", leaf.SignatureAlgorithm, want)
	}

	// RequireChannelBinding turns every way of not binding into an error: a
	// server that did not offer -PLUS, a role pg_hba lets in without SCRAM.
	// Success here means the server verified a proof over its own hash of
	// the certificate, and sent a signature that verified over ours.
	cfg.RequireChannelBinding = true
	conn, err := postgres.Startup(tc, cfg)
	if err != nil {
		t.Fatalf("startup with channel binding: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := nc.SetDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}

	// The server's own account of the session: one row only if it is TLS.
	version := queryOne(t, conn, "SELECT version FROM pg_stat_ssl WHERE pid = pg_backend_pid() AND ssl",
		func(row postgres.Row) string {
			v, _ := row.Value(0)
			return postgres.DecodeText(v)
		})
	t.Logf("bound to the certificate over %s", version)
}
