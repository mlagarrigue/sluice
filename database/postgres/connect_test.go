package postgres

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// trustScript is a server's whole answer to a startup packet under trust:
// AuthenticationOk, then ReadyForQuery.
func trustScript() []byte {
	return append(beAuth(authOK, ""), beReady()...)
}

// deadlineConn records what Connect does to the socket beyond reading and
// writing it: the deadlines it sets, and whether it closes it.
type deadlineConn struct {
	net.Conn
	deadlines atomic.Int32
	closes    atomic.Int32
}

func (c *deadlineConn) SetDeadline(t time.Time) error {
	c.deadlines.Add(1)
	return c.Conn.SetDeadline(t)
}

func (c *deadlineConn) SetReadDeadline(t time.Time) error {
	c.deadlines.Add(1)
	return c.Conn.SetReadDeadline(t)
}

func (c *deadlineConn) SetWriteDeadline(t time.Time) error {
	c.deadlines.Add(1)
	return c.Conn.SetWriteDeadline(t)
}

func (c *deadlineConn) Close() error {
	c.closes.Add(1)
	return c.Conn.Close()
}

// A configuration Connect refuses costs no connection: it is judged before
// the recipe is called. A nil recipe is refused the same way.
func TestConnectRefusesABadConfigWithoutDialling(t *testing.T) {
	dials := 0
	dial := func(context.Context) (io.ReadWriteCloser, error) {
		dials++
		return nil, errors.New("dialled")
	}
	if _, err := Connect(t.Context(), dial, Config{}); err == nil || !strings.Contains(err.Error(), "User") {
		t.Errorf("Connect without a User = %v, want the refusal to name User", err)
	}
	if _, err := Connect(t.Context(), dial, Config{User: "u", CancelTimeout: -time.Second}); err == nil || !strings.Contains(err.Error(), "CancelTimeout") {
		t.Errorf("Connect with a negative CancelTimeout = %v, want the refusal to name CancelTimeout", err)
	}
	if dials != 0 {
		t.Errorf("the recipe was called %d times for a refused configuration, want 0", dials)
	}
	if _, err := Connect(t.Context(), nil, Config{User: "u"}); err == nil {
		t.Error("Connect with a nil recipe succeeded")
	}
}

// ctx bounds the handshake: a server that accepts the socket and never
// answers is given until ctx ends, then the connection is closed and the
// error is ctx's.
func TestConnectEndsTheHandshakeWithTheContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, server := net.Pipe()
		defer func() { _ = server.Close() }()
		go func() { _, _ = io.Copy(io.Discard, server) }() // reads, never answers
		dc := &deadlineConn{Conn: client}

		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		start := time.Now()
		conn, err := Connect(ctx, func(context.Context) (io.ReadWriteCloser, error) { return dc, nil }, Config{User: "u"})
		if conn != nil || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Connect = %v, %v, want no Conn and an error wrapping context.DeadlineExceeded", conn, err)
		}
		if took := time.Since(start); took != time.Second {
			t.Errorf("Connect returned after %v, want the context's 1s exactly (synctest)", took)
		}
		if dc.closes.Load() == 0 {
			t.Error("the connection was left open")
		}
		if n := dc.deadlines.Load(); n != 0 {
			t.Errorf("Connect set %d deadlines on the socket, want none: ctx is the bound", n)
		}
	})
}

// Nothing of ctx outlives Connect: no deadline was set on the socket, and
// the context ending afterwards does not close the connection. The recipe
// is kept for the cancellations.
func TestConnectLeavesNothingOnTheSocket(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, server := net.Pipe()
		defer func() { _ = server.Close() }()
		go func() {
			if _, err := pgwire.NewReader(server).NextStartup(); err != nil {
				t.Errorf("server reading the startup packet: %v", err)
				return
			}
			_, _ = server.Write(trustScript())
			_, _ = io.Copy(io.Discard, server)
		}()
		dc := &deadlineConn{Conn: client}

		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		conn, err := Connect(ctx, func(context.Context) (io.ReadWriteCloser, error) { return dc, nil }, Config{User: "u"})
		if err != nil {
			t.Fatalf("Connect: %v", err)
		}
		defer func() { _ = conn.Close() }()
		cancel()
		time.Sleep(2 * time.Second) // well past the context, for an AfterFunc that should not run
		synctest.Wait()

		if n := dc.deadlines.Load(); n != 0 {
			t.Errorf("Connect set %d deadlines on the socket, want none", n)
		}
		if n := dc.closes.Load(); n != 0 {
			t.Errorf("the socket was closed %d times after Connect returned", n)
		}
		if err := conn.Err(); err != nil {
			t.Errorf("the connection is broken: %v", err)
		}
		if conn.cancelDial == nil {
			t.Error("the recipe was not kept for the cancellations")
		}
	})
}

// sslServer listens on loopback and answers one SSLRequest with answer,
// then hands the socket to then. It returns the address to dial.
func sslServer(t *testing.T, answer []byte, then func(net.Conn)) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		nc, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = nc.Close() }()
		var req [8]byte
		if _, err := io.ReadFull(nc, req[:]); err != nil {
			t.Errorf("server reading the SSLRequest: %v", err)
			return
		}
		if n, code := binary.BigEndian.Uint32(req[:4]), binary.BigEndian.Uint32(req[4:]); n != 8 || code != 80877103 {
			t.Errorf("SSLRequest = length %d code %d, want 8 and 80877103", n, code)
		}
		if _, err := nc.Write(answer); err != nil {
			t.Errorf("server answering the SSLRequest: %v", err)
			return
		}
		then(nc)
	}()
	return ln.Addr().String()
}

// testCert is a self-signed "db.test" certificate and a pool that trusts it.
func testCert(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := selfSigned(t, key, &key.PublicKey)
	roots := x509.NewCertPool()
	roots.AddCert(cert.Leaf)
	return cert, roots
}

// DialTLS against a server that answers 'S': the handshake runs over the
// same socket, and Connect authenticates inside it.
func TestDialTLSNegotiatesSSLRequest(t *testing.T) {
	cert, roots := testCert(t)
	addr := sslServer(t, []byte{'S'}, func(nc net.Conn) {
		srv := tls.Server(nc, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
		if _, err := pgwire.NewReader(srv).NextStartup(); err != nil {
			t.Errorf("server reading the startup packet inside TLS: %v", err)
			return
		}
		_, _ = srv.Write(trustScript())
		_, _ = io.Copy(io.Discard, srv)
	})

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	conn, err := Connect(ctx, DialTLS(addr, &tls.Config{RootCAs: roots, ServerName: "db.test", MinVersion: tls.VersionTLS12}), Config{User: "u"})
	if err != nil {
		t.Fatalf("Connect over DialTLS: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if _, ok := conn.rw.(*tls.Conn); !ok {
		t.Errorf("the connection is a %T, want *tls.Conn", conn.rw)
	}
}

// tlsDirectServer listens on loopback, runs a TLS handshake on the first
// connection with no SSLRequest before it, and hands the TLS side to then.
func tlsDirectServer(t *testing.T, cfg *tls.Config, then func(*tls.Conn)) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		nc, err := ln.Accept()
		if err != nil {
			return
		}
		srv := tls.Server(nc, cfg)
		defer func() { _ = srv.Close() }()
		if err := srv.Handshake(); err != nil {
			t.Errorf("server handshake: %v", err)
			return
		}
		then(srv)
	}()
	return ln.Addr().String()
}

// DialTLSDirect opens TLS at once, offers the "postgresql" ALPN protocol in
// place of the caller's, and Connect authenticates inside it. The server
// accepts "postgresql" only, so a dial that kept the caller's "h2" would
// fail the handshake.
func TestDialTLSDirectHandshakesAtOnce(t *testing.T) {
	cert, roots := testCert(t)
	addr := tlsDirectServer(t, &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{"postgresql"}, MinVersion: tls.VersionTLS12}, func(srv *tls.Conn) {
		if _, err := pgwire.NewReader(srv).NextStartup(); err != nil {
			t.Errorf("server reading the startup packet inside TLS: %v", err)
			return
		}
		_, _ = srv.Write(trustScript())
		_, _ = io.Copy(io.Discard, srv)
	})

	cfg := &tls.Config{RootCAs: roots, ServerName: "db.test", NextProtos: []string{"h2"}, MinVersion: tls.VersionTLS12}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	conn, err := Connect(ctx, DialTLSDirect(addr, cfg), Config{User: "u"})
	if err != nil {
		t.Fatalf("Connect over DialTLSDirect: %v", err)
	}
	defer func() { _ = conn.Close() }()
	tc, ok := conn.rw.(*tls.Conn)
	if !ok {
		t.Fatalf("the connection is a %T, want *tls.Conn", conn.rw)
	}
	if p := tc.ConnectionState().NegotiatedProtocol; p != "postgresql" {
		t.Errorf("negotiated protocol = %q, want postgresql", p)
	}
	if len(cfg.NextProtos) != 1 || cfg.NextProtos[0] != "h2" {
		t.Errorf("the caller's config was changed: NextProtos = %q", cfg.NextProtos)
	}
}

// A server that completes the handshake without selecting an ALPN protocol
// — Go's crypto/tls client accepts that — is refused, as libpq refuses it:
// it has not said it speaks PostgreSQL.
func TestDialTLSDirectRefusesAServerWithoutALPN(t *testing.T) {
	cert, roots := testCert(t)
	closed := make(chan struct{})
	addr := tlsDirectServer(t, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}, func(srv *tls.Conn) {
		_, _ = io.Copy(io.Discard, srv)
		close(closed)
	})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	rw, err := DialTLSDirect(addr, &tls.Config{RootCAs: roots, ServerName: "db.test", MinVersion: tls.VersionTLS12})(ctx)
	if err == nil {
		_ = rw.Close()
		t.Fatal("DialTLSDirect accepted a server that selected no ALPN protocol")
	}
	if !errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), "ALPN") {
		t.Errorf("DialTLSDirect = %v, want an ErrProtocol that names ALPN", err)
	}
	select {
	case <-closed:
	case <-ctx.Done():
		t.Error("the socket was left open")
	}
}

// DialTLS refuses rather than downgrades, does not repeat what an
// unauthenticated server wrote, and reads exactly one byte before the
// handshake, so bytes stuffed behind the 'S' reach TLS and fail it.
func TestDialTLSRefusals(t *testing.T) {
	_, roots := testCert(t)
	for _, tc := range []struct {
		name     string
		answer   []byte
		want     string
		protocol bool
	}{
		{"N", []byte{'N'}, "does not accept TLS", false},
		{"E", append([]byte{'E'}, "\x00\x00\x00\x20SFATAL\x00Mplease send your password\x00\x00"...), "error message", true},
		{"stuffed", append([]byte{'S'}, beReady()...), "TLS handshake", false},
		{"garbage", []byte{'X'}, "0x58", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			closed := make(chan struct{})
			addr := sslServer(t, tc.answer, func(nc net.Conn) {
				_, _ = io.Copy(io.Discard, nc)
				close(closed)
			})
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			rw, err := DialTLS(addr, &tls.Config{RootCAs: roots, ServerName: "db.test", MinVersion: tls.VersionTLS12})(ctx)
			if err == nil {
				_ = rw.Close()
				t.Fatal("DialTLS succeeded")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("DialTLS = %v, want it to mention %q", err, tc.want)
			}
			if errors.Is(err, ErrProtocol) != tc.protocol {
				t.Errorf("DialTLS = %v, errors.Is(ErrProtocol) = %v, want %v", err, !tc.protocol, tc.protocol)
			}
			if strings.Contains(err.Error(), "password") {
				t.Errorf("DialTLS = %v repeats the unauthenticated server's text", err)
			}
			select {
			case <-closed:
			case <-ctx.Done():
				t.Error("the socket was left open")
			}
		})
	}
}
