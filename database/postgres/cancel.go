package postgres

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"
)

// cancelRequestCode goes where a startup packet carries its protocol version.
// It is 1234 << 16 | 5678, which is how the protocol distinguishes a
// cancellation from a connection attempt without spending a message type on
// it — the packet has no type byte, because it arrives before there is a
// session to type it against.
const cancelRequestCode = 1234<<16 | 5678

// CancelKey names one backend to the one message allowed to interrupt it.
//
// The server sends it during startup and it is a **secret**: whoever holds it
// can cancel that backend's running query, and the protocol asks for no other
// proof. Treat it the way a session token is treated — do not log it, do not
// put it in an error message, do not hand it to anything that did not open
// the connection.
type CancelKey struct {
	PID    uint32
	Secret uint32
}

// Known reports whether a key was actually received. It is false for a [Conn]
// built by [NewConn], which never saw the startup exchange, and for a server
// that sent no pgwire.BackendKeyData.
func (k CancelKey) Known() bool { return k.PID != 0 || k.Secret != 0 }

// String deliberately hides the secret: a key that reaches a log is a key
// anyone reading the log can cancel with.
func (k CancelKey) String() string {
	if !k.Known() {
		return "postgres.CancelKey{unknown}"
	}
	return fmt.Sprintf("postgres.CancelKey{PID: %d, Secret: <redacted>}", k.PID)
}

// CancelKey returns the cancellation key the server sent during startup, for
// use with [CancelRequest]. It is zero — and [CancelKey.Known] is false — on a
// connection built by [NewConn].
//
// This is the only [Conn] method safe to call from another goroutine while a
// query is running: it reads a field written once during startup and never
// again. Everything else on a Conn is a position in a conversation, and there
// is only one conversation.
func (c *Conn) CancelKey() CancelKey { return c.cancelKey }

// CancelRequest asks the server to interrupt whatever the backend named by key
// is currently running.
//
//	// while another goroutine is inside conn.Query
//	nc, err := net.Dial("tcp", addr) // a *second* connection to the same server
//	if err == nil {
//	    err = postgres.CancelRequest(nc, conn.CancelKey())
//	}
//
// # It needs its own connection, and that is not an inconvenience
//
// rw must be a **new** connection to the same server, and CancelRequest closes
// it. It cannot be the connection being cancelled: that one is in the middle
// of a query, its bytes are spoken for, and writing a cancellation into it
// would corrupt the conversation rather than end it. This is also what keeps
// [Conn]'s "not safe for concurrent use" contract true — the cancelling
// goroutine touches nothing the querying goroutine owns.
//
// # The secret crosses in clear unless rw is encrypted
//
// The packet carries the key's secret, and over a plain socket anyone on the
// path reads it — and with it the power to cancel that backend's queries for
// the rest of the session. The main connection being a *tls.Conn changes
// nothing here: rw is a separate connection, and it is only as protected as
// the caller makes it. Where the main connection needed TLS, negotiate it on
// this one the same way — SSLRequest, the server's 'S', then [crypto/tls] —
// and pass the *tls.Conn; PostgreSQL accepts a CancelRequest inside TLS. This
// package does not do it for you for the reason [Startup] does not: the
// transport and its certificate decisions are the caller's.
//
// # What a nil error means, and what it does not
//
// It means the packet was written and the socket was closed. It does not mean
// the query stopped. Cancellation is advisory in this protocol: the server
// checks for it at points of its own choosing, a query already finishing
// finishes, and a backend between statements has nothing to interrupt. There
// is no reply to wait for — the server answers by closing the connection, and
// says nothing about what it did.
//
// So this is the escape hatch, not the mechanism. The two things that actually
// bound a query's cost are the context passed to [Conn.Query], which stops the
// client pulling, and a server-side statement timeout, which stops the server
// working. Reach for CancelRequest when a backend is stuck inside a statement
// that neither of those will end — or let [StartupConfig.CancelDial] send it
// for you the moment a context ends mid-statement.
func CancelRequest(rw io.ReadWriteCloser, key CancelKey) error {
	defer rw.Close() //nolint:errcheck // the socket is one-shot and the server closes it first

	if !key.Known() {
		return errors.New("postgres: cannot cancel with an unknown key — the connection was built by NewConn, or the server sent no pgwire.BackendKeyData")
	}

	// Sixteen bytes: the length itself, the code, the pid, the secret. Small
	// enough to build on the stack rather than reach for a Writer, and the
	// packet has no type byte for one to add.
	var pkt [16]byte
	binary.BigEndian.PutUint32(pkt[0:], 16)
	binary.BigEndian.PutUint32(pkt[4:], cancelRequestCode)
	binary.BigEndian.PutUint32(pkt[8:], key.PID)
	binary.BigEndian.PutUint32(pkt[12:], key.Secret)

	if _, err := rw.Write(pkt[:]); err != nil {
		return fmt.Errorf("postgres: sending the cancellation: %w", err)
	}
	return nil
}

// parseBackendKeyData reads the pid and secret the server sends once, during
// startup.
func parseBackendKeyData(body []byte) (CancelKey, error) {
	if len(body) != 8 {
		return CancelKey{}, fmt.Errorf("%w: pgwire.BackendKeyData is %d bytes, want 8", ErrProtocol, len(body))
	}
	return CancelKey{
		PID:    binary.BigEndian.Uint32(body[0:4]),
		Secret: binary.BigEndian.Uint32(body[4:8]),
	}, nil
}

// sqlstateQueryCanceled is what the server answers a statement it interrupted
// with, whether on a CancelRequest or on its own statement_timeout.
const sqlstateQueryCanceled = "57014"

// cancelTimeout bounds the second connection a cancellation opens: the dial
// and the write of the packet. The context that triggered the cancellation
// is already over, so it cannot be the bound; a dial that takes longer than
// this is a server that cannot be reached, and the statement runs to its
// end as it would have without the option.
const cancelTimeout = 5 * time.Second

// cancelWatch is the goroutine that answers a context ending while the
// server is still executing a statement: it opens the second connection
// [StartupConfig.CancelDial] provides and sends the [CancelRequest]. The
// statement's own goroutine is blocked in a read all the while, and is
// released by the server's query_canceled error — which is why nothing here
// touches the connection the statement owns.
type cancelWatch struct {
	stop func() bool
	done chan struct{}
	err  error // written by the goroutine before done is closed
}

// watchCancel arms the watch for one statement. It is nil, and costs
// nothing, when there is no dialer to open the second connection with, no
// key to name the backend by, or a context that can never end.
func (c *Conn) watchCancel(ctx context.Context) *cancelWatch {
	if c.cancelDial == nil || ctx.Done() == nil || !c.cancelKey.Known() {
		return nil
	}
	w := &cancelWatch{done: make(chan struct{})}
	w.stop = context.AfterFunc(ctx, func() {
		defer close(w.done)
		w.err = c.sendCancel(ctx)
	})
	return w
}

// end disarms the watch once the statement is over. A cancellation already
// under way is waited for — it is bounded by cancelTimeout — so that no
// goroutine of this statement outlives it and no packet of this statement
// lands on the next one. It reports how the cancellation went; nil when
// none was sent or when the packet was written.
func (w *cancelWatch) end() error {
	if w == nil || w.stop() {
		return nil
	}
	<-w.done
	return w.err
}

// sendCancel opens the second connection and sends the packet. The context
// that fired is kept for its values and nothing else: its deadline is past.
func (c *Conn) sendCancel(ctx context.Context) error {
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cancelTimeout)
	defer cancel()
	rw, err := c.cancelDial(dctx)
	if err != nil {
		return fmt.Errorf("opening the connection for the cancellation: %w", err)
	}
	return CancelRequest(rw, c.cancelKey)
}

// settleCancel folds what the cancellation did into the error the statement
// ends with. The server's query_canceled that answered our request becomes
// ctx.Err(), wrapped, so errors.Is against the context errors answers the way
// [Conn.Query] promises; the server's error stays reachable through
// errors.As. A cancellation that could not be sent is named next to the
// context error it failed to serve, so a deadline that was not honoured
// says why — and says nothing when the statement finished anyway.
func settleCancel(ctx context.Context, w *cancelWatch, err error) error {
	cerr := w.end()
	if ctx.Err() == nil || err == nil {
		return err
	}
	if e, ok := errors.AsType[*Error](err); ok && e.Code == sqlstateQueryCanceled {
		return fmt.Errorf("abandoning the statement: %w (the server cancelled it: %w)", ctx.Err(), err)
	}
	if cerr != nil && errors.Is(err, ctx.Err()) {
		return fmt.Errorf("%w (%w)", err, cerr)
	}
	return err
}
