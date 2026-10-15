package postgres

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

// DialFunc opens a new transport to the server: a TCP socket, a TLS session
// over one, a UNIX socket, anything that reads and writes the protocol's
// bytes. It is the connection recipe [Connect] is given, and it is called
// again for each cancellation, because a [CancelRequest] travels on a
// connection of its own.
//
// It must return a new connection every time it is called — a recipe that
// hands back the connection being cancelled is refused, and the cancellation
// with it — and it must honour ctx: a dial that ignores it holds [Connect]
// past the caller's deadline, and a cancellation past
// [Config.CancelTimeout]. The connection's Close must be safe to call while
// a Read is blocked, as every net.Conn's is: it is how a cancellation that
// cannot be confirmed releases the statement waiting on the server. [DialTCP] and [DialTLS] are the two recipes most deployments
// need; a recipe of your own — a UNIX socket, a proxy, a custom resolver — is
// any function of this shape.
type DialFunc func(ctx context.Context) (io.ReadWriteCloser, error)

// DialTCP is the recipe for a plain TCP connection to addr, "host:port".
//
// The bytes cross in clear, the password included when the server asks for
// a cleartext one, and the cancellation key with every [CancelRequest]. It
// is the right recipe for a loopback, a UNIX-socket-like private link, or a
// network the deployment already encrypts; anywhere else, use [DialTLS].
func DialTCP(addr string) DialFunc {
	return func(ctx context.Context) (io.ReadWriteCloser, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", addr)
	}
}

// sslRequestCode is the number an SSLRequest carries where a startup packet
// carries its protocol version: 1234 in the high half, 5679 in the low.
const sslRequestCode = 80877103

// DialTLS is the recipe for a TLS connection to addr, "host:port", the way
// PostgreSQL negotiates one: an SSLRequest in clear, the server's one-byte
// answer, then the TLS handshake over the same socket.
//
// cfg is used as given, with one addition: an empty ServerName is filled in
// with the host part of addr, as [tls.Dial] does, so the server's certificate
// is checked against the name the caller dialled. A nil cfg is the zero
// [tls.Config]: the system's root pool, and that name.
//
// The connection is refused, not downgraded, when the server answers that it
// does not do TLS: a caller who asked for encryption and received a clear
// socket would send the password through it. A server that answers with an
// error message is refused the same way, and its text is not repeated — it
// has not been authenticated, and anything it says could be written by
// whoever answered the socket (CVE-2024-10977).
//
// Exactly one byte is read before the handshake starts: bytes injected
// after the server's answer would otherwise be read as if they had come
// through TLS (CVE-2021-23222). Here they reach the TLS library instead,
// which fails the handshake on them.
func DialTLS(addr string, cfg *tls.Config) DialFunc {
	if cfg == nil {
		cfg = &tls.Config{}
	}
	if cfg.ServerName == "" {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			host = addr
		}
		cfg = cfg.Clone()
		cfg.ServerName = host
	}
	return func(ctx context.Context) (io.ReadWriteCloser, error) {
		var d net.Dialer
		nc, err := d.DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, err
		}
		tc, err := negotiateTLS(ctx, nc, cfg)
		if err != nil {
			_ = nc.Close()
			return nil, err
		}
		return tc, nil
	}
}

// negotiateTLS sends the SSLRequest, reads the server's answer and runs the
// handshake, all bounded by ctx. The bound is a close of nc, not a deadline:
// a deadline set here would stay on the socket for the caller to find.
func negotiateTLS(ctx context.Context, nc net.Conn, cfg *tls.Config) (*tls.Conn, error) {
	stop := context.AfterFunc(ctx, func() { _ = nc.Close() })
	defer stop()

	var req [8]byte
	binary.BigEndian.PutUint32(req[0:], 8)
	binary.BigEndian.PutUint32(req[4:], sslRequestCode)
	if _, err := nc.Write(req[:]); err != nil {
		return nil, ctxOr(ctx, fmt.Errorf("postgres: sending SSLRequest: %w", err))
	}
	var answer [1]byte
	if _, err := io.ReadFull(nc, answer[:]); err != nil {
		return nil, ctxOr(ctx, fmt.Errorf("postgres: reading the answer to SSLRequest: %w", err))
	}
	switch answer[0] {
	case 'S':
	case 'N':
		return nil, errors.New("postgres: the server does not accept TLS (ssl = off) — DialTLS refuses to go on in clear; use DialTCP if a clear connection is what you want")
	case 'E':
		return nil, fmt.Errorf("%w: the server answered SSLRequest with an error message, not shown since nothing has authenticated it yet", ErrProtocol)
	default:
		return nil, fmt.Errorf("%w: the server answered SSLRequest with %#x, want 'S' or 'N'", ErrProtocol, answer[0])
	}
	tc := tls.Client(nc, cfg)
	if err := tc.HandshakeContext(ctx); err != nil {
		return nil, ctxOr(ctx, fmt.Errorf("postgres: TLS handshake: %w", err))
	}
	return tc, nil
}

// ctxOr names the context as the cause of err when it has ended: a read that
// failed because ctx closed the socket reports "use of closed network
// connection", which says nothing about why.
func ctxOr(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return fmt.Errorf("%w (%w)", context.Cause(ctx), err)
	}
	return err
}

// Connect opens a connection with dial, performs the startup and
// authentication exchange, and returns a [Conn] ready for queries.
//
//	conn, err := postgres.Connect(ctx,
//	    postgres.DialTLS("db.internal:5432", nil),
//	    postgres.Config{
//	        User:     "orders",
//	        Database: "orders",
//	        Password: os.Getenv("PGPASSWORD"),
//	    })
//
// # The recipe is kept
//
// dial opens the connection, and it is kept: a context that ends while the
// connection waits on a statement the server is still executing —
// [Conn.Query], [Conn.Exec], [Conn.ExecParams], [Copier.WriteTuples],
// [Tx.Pipeline] — calls it again, to send a [CancelRequest] on a connection
// of its own. That is why Connect takes a recipe rather than a socket:
// cancelling needs a second connection to the same server, made the same
// way — over TLS when the main one is, since the packet carries the
// session's secret.
//
// The cancellation is confirmed before the call returns: the server closes
// the second connection once it has signalled the backend, and nothing is
// sent on the main one until then, so a request in flight cannot cancel the
// next statement instead. The server aborts the statement with SQLSTATE
// 57014, the call ends with ctx.Err() wrapped, and the connection stays
// usable. A cancellation not confirmed within [Config.CancelTimeout] — the
// recipe failing, the server not closing — closes the connection instead:
// the call ends with ctx.Err() and [ErrConnBroken], and [Config.Logger]
// receives a warning, the one trace left when the statement had completed
// anyway.
//
// # ctx bounds the connection, not the session
//
// ctx covers the dial and the whole handshake. When it ends first, the
// connection is closed and Connect returns ctx's error wrapped. Nothing of it
// stays on the connection: no deadline is set on the socket, so a query run
// an hour later is bounded by its own context and nothing else.
//
// # Which methods work
//
// SCRAM-SHA-256 (the default on every modern PostgreSQL) and its -PLUS form
// over TLS, trust, and — only when [Config.AllowCleartextPassword] says so —
// cleartext password. The server's final signature is verified, which is
// what makes the exchange mutual — a client that skips that check has
// authenticated itself to whoever answered the socket.
//
// md5 is refused rather than implemented, and the error says so along with
// the server-side fix.
//
// # Channel binding
//
// When dial returns a *tls.Conn — or anything with its ConnectionState
// method — and the server offers SCRAM-SHA-256-PLUS, the exchange is bound
// to the server's certificate with tls-server-end-point (RFC 5929): a man in
// the middle that terminates TLS with a certificate of its own cannot relay
// the proof. Over TLS without -PLUS on offer, the client says it could have
// bound, so a server whose -PLUS was stripped refuses. Over a plain socket
// binding is declined explicitly, and a server offering only -PLUS is
// refused rather than downgraded.
//
// # On failure
//
// A configuration it refuses costs no connection: cfg is judged before dial
// is called. Once dialled, the connection is closed on any failure — the
// socket is at an unknown position in the protocol, and no [Conn] is
// returned to close it through.
func Connect(ctx context.Context, dial DialFunc, cfg Config) (*Conn, error) {
	if dial == nil {
		return nil, errors.New("postgres: Connect needs a DialFunc — DialTCP, DialTLS, or a recipe of your own")
	}
	if _, err := startupBody(cfg); err != nil {
		return nil, err
	}
	rw, err := dial(ctx)
	if err != nil {
		return nil, ctxOr(ctx, fmt.Errorf("postgres: connecting: %w", err))
	}
	// The handshake reads and writes a socket that knows nothing of ctx, so
	// ctx reaches it by closing it: the blocked read fails, startup returns.
	stop := context.AfterFunc(ctx, func() { _ = rw.Close() })
	c, err := startup(rw, cfg)
	if !stop() {
		// ctx ended, and the socket with it — even if the exchange won the
		// race to its last message, it has nothing left to run on.
		if err == nil {
			err = c.breakConn(errors.New("the connection was closed when the context ended"))
		}
		return nil, fmt.Errorf("postgres: connecting: %w (%w)", context.Cause(ctx), err)
	}
	if err != nil {
		return nil, err
	}
	c.cancelDial = dial
	return c, nil
}
