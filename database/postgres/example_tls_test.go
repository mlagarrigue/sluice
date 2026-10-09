package postgres_test

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/mlagarrigue/sluice/database/postgres"
)

// Connecting over TLS.
//
// This package does not dial and does not negotiate TLS. That is not a gap: a
// TLS implementation is the last thing anyone should write twice, and
// [crypto/tls] is configured by the caller because the decisions in it —
// which roots to trust, which version to floor, whether to pin — belong to
// whoever runs the system and not to a connector.
//
// What this package needs from you is a connection that is already
// encrypted. Everything from the startup packet onward runs inside it,
// unchanged.
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
// like a handshake error rather than a protocol mismatch. The negotiation is
// eight bytes and one read, shown below.
func Example_tLS() {
	addr := os.Getenv("SLUICE_PG")
	if addr == "" {
		fmt.Println("skipped")
		return
	}

	raw, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		fmt.Println("dial:", err)
		return
	}

	// The SSLRequest packet: length, then the magic number in place of a
	// protocol version.
	req := make([]byte, 8)
	binary.BigEndian.PutUint32(req[0:], 8)
	binary.BigEndian.PutUint32(req[4:], 80877103)
	if _, err := raw.Write(req); err != nil {
		_ = raw.Close()
		fmt.Println("requesting TLS:", err)
		return
	}

	answer := make([]byte, 1)
	if _, err := raw.Read(answer); err != nil {
		_ = raw.Close()
		fmt.Println("reading the TLS answer:", err)
		return
	}
	if answer[0] != 'S' {
		// 'N' means the server will not do TLS. Continuing in plaintext here
		// is the downgrade an attacker who can strip one byte is hoping for,
		// so this refuses rather than falls back.
		_ = raw.Close()
		fmt.Println("the server refused TLS")
		return
	}

	// Roots and hostname are the caller's decisions. InsecureSkipVerify is not
	// one of them: an encrypted connection to whoever answered the socket
	// protects against nothing an attacker on the path cannot already do.
	roots, err := x509.SystemCertPool()
	if err != nil {
		_ = raw.Close()
		fmt.Println("system roots:", err)
		return
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		_ = raw.Close()
		fmt.Println("splitting the address:", err)
		return
	}

	enc := tls.Client(raw, &tls.Config{
		ServerName: host,
		RootCAs:    roots,
		MinVersion: tls.VersionTLS12,
	})

	// From here on nothing is TLS-aware. Startup runs the authentication
	// exchange inside the encrypted connection, and Conn speaks the protocol
	// through it exactly as it would through a plain socket.
	conn, err := postgres.Startup(enc, postgres.StartupConfig{
		User:     os.Getenv("SLUICE_PG_USER"),
		Database: os.Getenv("SLUICE_PG_DB"),
		Password: os.Getenv("SLUICE_PG_PASSWORD"),
	})
	if err != nil {
		fmt.Println("startup:", err)
		return
	}
	defer func() { _ = conn.Close() }()

	fmt.Println("connected over TLS")
}
