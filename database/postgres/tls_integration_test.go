package postgres_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
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
// If the two hashes differ, the server's proof does not verify and Connect
// fails.
//
// It needs a server with ssl = on and the certificate to trust in
// SLUICE_PG_TLS_CA; CI makes both with scripts/pg-tls.sh, with an RSA
// PKCS #1 v1.5 certificate, then with RSA-PSS SHA-384 ones, and names the
// signature it expects in SLUICE_PG_TLS_SIGALG, so that a server that kept
// its previous certificate cannot pass for the new one. "0" is
// x509.UnknownSignatureAlgorithm, how crypto/x509 leaves a PSS signature
// whose salt is not the hash length — OpenSSL's default — and the one whose
// hash tlsServerEndPoint reads itself.
//
// Each certificate is met twice: through DialTLS's SSLRequest, and through
// DialTLSDirect's immediate handshake, which a server before PostgreSQL 17
// does not take and the subtest skips.
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
	tlsCfg := &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	var major int
	t.Run("SSLRequest", func(t *testing.T) {
		conn := bindOverTLS(t, postgres.DialTLS(addr, tlsCfg), cfg)
		major, _ = strconv.Atoi(strings.SplitN(conn.Parameter("server_version"), ".", 2)[0])
	})
	t.Run("direct", func(t *testing.T) {
		if major != 0 && major < 17 {
			t.Skipf("PostgreSQL %d takes TLS only after an SSLRequest; direct TLS needs 17", major)
		}
		bindOverTLS(t, postgres.DialTLSDirect(addr, tlsCfg), cfg)
	})
	// The CancelRequest opens a second connection by the recipe the first
	// one was made with, so a session over TLS is cancelled over TLS: a
	// cancellation key sent in clear beside it is what an observer on the
	// path would need to cancel the session's statements at will. Every
	// connection the recipe opens is counted, and each must be a TLS one.
	t.Run("cancel", func(t *testing.T) {
		var dials, overTLS atomic.Int32
		viaTLS := postgres.DialTLS(addr, tlsCfg)
		recipe := func(ctx context.Context) (io.ReadWriteCloser, error) {
			rw, err := viaTLS(ctx)
			if err == nil {
				dials.Add(1)
				if _, ok := rw.(*tls.Conn); ok {
					overTLS.Add(1)
				}
			}
			return rw, err
		}
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		conn, err := postgres.Connect(ctx, recipe, cfg)
		if err != nil {
			t.Fatalf("connecting over TLS: %v", err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		cancelInterrupts(t, conn)
		if dials.Load() < 2 {
			t.Fatalf("%d connection(s) opened: the cancellation did not dial", dials.Load())
		}
		if overTLS.Load() != dials.Load() {
			t.Fatalf("%d of %d connections were TLS: a cancellation left in clear", overTLS.Load(), dials.Load())
		}
	})
}

// bindOverTLS connects through dial with ChannelBindingRequire, checks the
// certificate is the one the harness expects and that the server sees a TLS
// session, and returns the connection.
func bindOverTLS(t *testing.T, dial postgres.DialFunc, cfg postgres.Config) *postgres.Conn {
	t.Helper()
	// The wrapper only keeps the *tls.Conn the recipe returns, unchanged, to
	// read the certificate from.
	var tc *tls.Conn
	recipe := func(ctx context.Context) (io.ReadWriteCloser, error) {
		rw, err := dial(ctx)
		if c, ok := rw.(*tls.Conn); ok && tc == nil {
			tc = c
		}
		return rw, err
	}

	// ChannelBindingRequire turns every way of not binding into an error: a
	// server that did not offer -PLUS, a role pg_hba lets in without SCRAM.
	// Success here means the server verified a proof over its own hash of
	// the certificate, and sent a signature that verified over ours.
	cfg.ChannelBinding = postgres.ChannelBindingRequire
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	conn, err := postgres.Connect(ctx, recipe, cfg)
	if err != nil {
		t.Fatalf("connecting with channel binding: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	leaf := tc.ConnectionState().PeerCertificates[0]
	t.Logf("server certificate signed with %v", leaf.SignatureAlgorithm)
	if want := os.Getenv("SLUICE_PG_TLS_SIGALG"); want != "" && leaf.SignatureAlgorithm.String() != want {
		t.Fatalf("the server presented a certificate signed with %v, want %s", leaf.SignatureAlgorithm, want)
	}

	// The server's own account of the session: one row only if it is TLS.
	version := queryOne(t, conn, "SELECT version FROM pg_stat_ssl WHERE pid = pg_backend_pid() AND ssl",
		func(row postgres.Row) string {
			v, _ := row.Value(0)
			return postgres.DecodeText(v)
		})
	t.Logf("bound to the certificate over %s", version)
	return conn
}
