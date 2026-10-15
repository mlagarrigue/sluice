package postgres_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/mlagarrigue/sluice/database/postgres"
)

// Connecting over TLS.
//
// [crypto/tls] is configured by the caller, because the decisions in it —
// which roots to trust, which version to floor, whether to pin — belong to
// whoever runs the system and not to a connector. [postgres.DialTLS] takes
// that configuration and does the one part that is PostgreSQL's own.
//
// # PostgreSQL does not speak TLS on connect
//
// The one part that is not obvious. A PostgreSQL server does not begin with a
// TLS handshake the way an HTTPS server does: the client opens a plain socket,
// sends an **SSLRequest** — eight bytes, no message type, the magic number
// 80877103 — and the server answers with a single byte, `S` to proceed or `N`
// to refuse. Only then does the TLS handshake start.
//
// So a `tls.Dial` straight at port 5432 does not work, and the failure looks
// like a handshake error rather than a protocol mismatch. DialTLS sends the
// request, refuses an `N` rather than continue in clear — the downgrade an
// attacker who can strip one byte is hoping for — and shakes hands.
func Example_tLS() {
	addr := os.Getenv("SLUICE_PG")
	if addr == "" {
		fmt.Println("skipped")
		return
	}

	// Roots and hostname are the caller's decisions; DialTLS fills in the
	// hostname from addr when ServerName is empty. InsecureSkipVerify is not
	// one of them: an encrypted connection to whoever answered the socket
	// protects against nothing an attacker on the path cannot already do.
	roots, err := x509.SystemCertPool()
	if err != nil {
		fmt.Println("system roots:", err)
		return
	}
	dial := postgres.DialTLS(addr, &tls.Config{
		RootCAs:    roots,
		MinVersion: tls.VersionTLS12,
	})

	// From here on nothing is TLS-aware. Connect runs the authentication
	// exchange inside the encrypted connection, and Conn speaks the protocol
	// through it exactly as it would through a plain socket. The recipe is
	// kept: a cancellation opens its second connection over TLS too.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := postgres.Connect(ctx, dial, postgres.Config{
		User:     os.Getenv("SLUICE_PG_USER"),
		Database: os.Getenv("SLUICE_PG_DB"),
		Password: os.Getenv("SLUICE_PG_PASSWORD"),
	})
	if err != nil {
		fmt.Println("connecting:", err)
		return
	}
	defer func() { _ = conn.Close() }()

	fmt.Println("connected over TLS")
}
