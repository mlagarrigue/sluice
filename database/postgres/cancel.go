package postgres

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
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
//
// Secret holds the key's bytes as the server sent them. Under protocol 3.0
// they are always four; under 3.2 ([Protocol32]) they are 4
// to 256 — PostgreSQL 18 sends 32, and a pooler in between may send more. A
// string rather than a []byte so that a CancelKey copied to another
// goroutine shares nothing it could change under the connection, and stays
// comparable with ==.
type CancelKey struct {
	PID    uint32
	Secret string
}

// The bounds the protocol puts on a secret: four bytes, the only length
// before 3.2, up to the 256 a 3.2 message may carry.
const (
	minCancelSecret = 4
	maxCancelSecret = 256
)

// Known reports whether a key was actually received. It is false for a [Conn]
// built by [NewConn], which never saw the startup exchange, and for a server
// that sent no pgwire.BackendKeyData.
func (k CancelKey) Known() bool { return k.PID != 0 || k.Secret != "" }

// String deliberately hides the secret: a key that reaches a log is a key
// anyone reading the log can cancel with.
func (k CancelKey) String() string {
	if !k.Known() {
		return "postgres.CancelKey{unknown}"
	}
	return fmt.Sprintf("postgres.CancelKey{PID: %d, Secret: <redacted>}", k.PID)
}

// GoString hides the secret from %#v too, which would otherwise print the
// fields one by one and the secret among them.
func (k CancelKey) GoString() string { return k.String() }

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
// this one the same way — [DialTLS] does: SSLRequest, the server's 'S', then
// [crypto/tls] — and pass the *tls.Conn; PostgreSQL accepts a CancelRequest
// inside TLS.
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
// that neither of those will end. A [Conn] opened by [Connect] sends it for
// you the moment a context ends mid-statement, and waits for the server to
// close the connection before it sends anything else.
func CancelRequest(rw io.ReadWriteCloser, key CancelKey) error {
	defer rw.Close() //nolint:errcheck // the socket is one-shot and the server closes it first
	return writeCancel(rw, key)
}

// writeCancel is [CancelRequest] without the close, for the connection's own
// cancellation, which goes on to wait for the server's.
func writeCancel(rw io.Writer, key CancelKey) error {
	if !key.Known() {
		return errors.New("postgres: cannot cancel with an unknown key — the connection was built by NewConn, or the server sent no pgwire.BackendKeyData")
	}
	if n := len(key.Secret); n < minCancelSecret || n > maxCancelSecret {
		// Refused rather than sent: the server matches the secret byte for
		// byte, so a truncated or padded one cancels nothing, and the caller
		// would read the nil error as a cancellation on its way.
		return fmt.Errorf("postgres: cannot cancel with a %d-byte secret — the protocol carries %d to %d", n, minCancelSecret, maxCancelSecret)
	}

	// The length itself, the code, the pid, then the secret to the end of
	// the packet: 16 bytes under protocol 3.0, at most 268 under 3.2. Built
	// whole and written once, and the packet has no type byte for a Writer
	// to add.
	var buf [12 + maxCancelSecret]byte
	n := 12 + len(key.Secret)
	pkt := buf[:n]
	binary.BigEndian.PutUint32(pkt[0:], uint32(n)) //nolint:gosec // G115: at most 268, bounded above
	binary.BigEndian.PutUint32(pkt[4:], cancelRequestCode)
	binary.BigEndian.PutUint32(pkt[8:], key.PID)
	copy(pkt[12:], key.Secret)

	if _, err := rw.Write(pkt); err != nil {
		return fmt.Errorf("postgres: sending the cancellation: %w", err)
	}
	return nil
}

// parseBackendKeyData reads the pid and secret the server sends once, during
// startup. version is the protocol version in force: before 3.2 the secret
// is exactly four bytes, from 3.2 on it runs to the end of the message, 4 to
// 256 bytes.
func parseBackendKeyData(body []byte, version ProtocolVersion) (CancelKey, error) {
	lo, hi := 4+minCancelSecret, 4+maxCancelSecret
	if version < Protocol32 {
		hi = lo
	}
	if len(body) < lo || len(body) > hi {
		return CancelKey{}, fmt.Errorf("%w: pgwire.BackendKeyData is %d bytes under protocol %v, want %d to %d", ErrProtocol, len(body), version, lo, hi)
	}
	return CancelKey{
		PID:    binary.BigEndian.Uint32(body[0:4]),
		Secret: string(body[4:]),
	}, nil
}

// sqlstateQueryCanceled is what the server answers a statement it interrupted
// with, whether on a CancelRequest or on its own statement_timeout.
const sqlstateQueryCanceled = "57014"

// cancelWatch is the goroutine that answers a context ending while the
// server is still executing a statement: it opens the second connection
// with the recipe [Connect] was given, sends the [CancelRequest] and waits
// for the server to close that connection. The statement's own goroutine
// is blocked in a read all the while, and is released by the server's
// query_canceled error — or by the watch closing the connection under it,
// when the cancellation cannot be confirmed or when, confirmed, it has not
// ended the statement within CancelTimeout.
type cancelWatch struct {
	stop func() bool
	done chan struct{}
	err  error // written by the goroutine before done is closed
	// over is closed once the statement has ended (end, disarm), which is
	// what the goroutine waits for after the server's confirmation.
	over   chan struct{}
	isOver bool // over is closed; read and written by the statement's goroutine only
}

// watchCancel arms the watch for one statement. It is nil, and costs
// nothing, when there is no dialer to open the second connection with, no
// key to name the backend by, or a context that can never end.
func (c *Conn) watchCancel(ctx context.Context) *cancelWatch {
	if c.cancelDial == nil || ctx.Done() == nil || !c.cancelKey.Known() {
		return nil
	}
	w := &cancelWatch{done: make(chan struct{}), over: make(chan struct{})}
	w.stop = context.AfterFunc(ctx, func() { w.run(c, ctx) })
	return w
}

// cancelNow sends the cancellation at once rather than when ctx ends: for a
// statement whose consumer stopped with its context still alive, and whose
// rest would cost more to read than to cancel. Same goroutine, same recipe,
// same confirmation as the watch; end waits for it.
func (c *Conn) cancelNow(ctx context.Context) *cancelWatch {
	w := &cancelWatch{done: make(chan struct{}), over: make(chan struct{}), stop: func() bool { return false }}
	go w.run(c, ctx)
	return w
}

// run sends the cancellation and records how it went.
//
// The server's confirmation says the signal reached the backend, not that
// the statement is over: the backend acts on it at its next check for
// interrupts, and answers on the main connection, which has no deadline
// (Connect). A backend that takes the signal and never gets there, or a
// server that closes the second connection and then says nothing on the
// first, would hold the statement's goroutine, the connection and whatever
// its caller holds for as long as the socket lives. So the statement gets
// CancelTimeout after the confirmation to end, and the connection is
// closed under it when it has not, as for a cancellation not confirmed.
func (w *cancelWatch) run(c *Conn, ctx context.Context) {
	defer close(w.done)
	if w.err = c.sendCancel(ctx); w.err != nil {
		// Closed here rather than when the statement settles: its
		// goroutine may be blocked in a read the server will never
		// answer, and only the close releases it.
		_ = c.rw.Close()
		return
	}
	timeout := c.cancelBound()
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-w.over:
	case <-t.C:
		w.err = fmt.Errorf("the server confirmed the cancellation, and the statement had not ended Config.CancelTimeout (%v) later", timeout)
		_ = c.rw.Close()
	}
}

// cancelBound is Config.CancelTimeout, or its default.
func (c *Conn) cancelBound() time.Duration {
	if c.cancelTimeout == 0 {
		return defaultCancelTimeout
	}
	return c.cancelTimeout
}

// end disarms the watch once the statement is over. A cancellation already
// under way is waited for — to the server's confirmation or to
// CancelTimeout — so that the next statement is not sent before the server
// has acted on this one's request. It reports how the cancellation went:
// nil when none was sent or when the server confirmed it.
func (w *cancelWatch) end() error {
	if w == nil || w.stop() {
		return nil
	}
	w.ended()
	<-w.done
	return w.err
}

// ended tells the goroutine the statement is over, once.
func (w *cancelWatch) ended() {
	if !w.isOver {
		w.isOver = true
		close(w.over)
	}
}

// disarm stops a watch that end will not reach — a consumer's panic
// unwinding through the statement — so that the context ending later does
// not cancel whatever the connection runs next. Deferred beside
// watchCancel; a no-op once end has run.
func (w *cancelWatch) disarm() {
	if w != nil {
		w.stop()
		w.ended()
	}
}

// sendCancel opens the second connection, sends the packet and waits for
// the server to close that connection, all within CancelTimeout.
//
// The close is the confirmation. The server signals the backend before it
// closes, and a backend that takes the signal while it waits for its next
// command drops it: ProcessInterrupts in postgres.c ignores a cancel while
// DoingCommandRead, and PostgresMain checks for interrupts once more before
// it runs the command it read. So once the close is seen, the next
// statement cannot be the one cancelled. Without it nothing says where the
// request is — it may still be travelling — and the connection must not
// carry another statement.
//
// The context that fired is kept for its values and nothing else: its
// deadline is past.
func (c *Conn) sendCancel(ctx context.Context) error {
	timeout := c.cancelBound()
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	rw, err := c.cancelDial(dctx)
	if err != nil {
		return cancelErr(dctx, timeout, fmt.Errorf("opening the connection for the cancellation: %w", err))
	}
	if sameTransport(rw, c.rw) {
		// Neither written nor closed here: it is the statement's own
		// connection, where the packet would be read as a message. The
		// watch closes it, as for any cancellation not confirmed.
		return errors.New("the DialFunc returned the connection being cancelled — it must open a new one on every call")
	}
	defer rw.Close() //nolint:errcheck // the server has closed it, or the attempt is over
	// The bound reaches the socket by closing it, as in [Connect]: a
	// deadline would need a net.Conn, and the recipe returns less.
	stop := context.AfterFunc(dctx, func() { _ = rw.Close() })
	defer stop()
	if err := writeCancel(rw, c.cancelKey); err != nil {
		return cancelErr(dctx, timeout, fmt.Errorf("sending the cancellation: %w", err))
	}
	if _, err := io.Copy(io.Discard, rw); err != nil {
		return cancelErr(dctx, timeout, fmt.Errorf("waiting for the server to close the cancellation connection: %w", err))
	}
	return nil
}

// cancelErr names CancelTimeout when it is what ended the attempt: a read
// cut short by the close reports a closed connection, which says nothing
// about why.
func cancelErr(dctx context.Context, timeout time.Duration, err error) error {
	if dctx.Err() != nil {
		return fmt.Errorf("no confirmation within Config.CancelTimeout (%v): %w", timeout, err)
	}
	return err
}

// sameTransport reports whether a recipe handed back the connection it was
// meant to open a second one beside. Only a comparable dynamic type is
// compared: == on an interface panics otherwise, and a value of such a type
// has no identity to compare.
func sameTransport(a, b io.ReadWriteCloser) bool {
	t := reflect.TypeOf(a)
	return t != nil && t == reflect.TypeOf(b) && t.Comparable() && a == b
}

// settleCancel folds what the cancellation did into the error the statement
// ends with. The server's query_canceled that answered our request becomes
// ctx.Err(), wrapped, so errors.Is against the context errors answers the way
// [Conn.Query] promises; the server's error stays reachable through
// errors.As.
//
// A cancellation that was not confirmed has closed the connection. It is
// broken for whatever comes next, a warning says why, and the statement's
// own outcome decides what this call returns: nil when it completed before
// the close — a statement reported failed after it committed is a
// statement retried — and ctx.Err() with [ErrConnBroken] otherwise.
func (c *Conn) settleCancel(ctx context.Context, w *cancelWatch, err error) error {
	if cerr := w.end(); cerr != nil {
		c.cancelUnconfirmed(ctx, cerr, err == nil)
		if err == nil {
			return nil
		}
		return fmt.Errorf("abandoning the statement: %w (%w)", ctx.Err(), c.broken)
	}
	if ctx.Err() == nil || err == nil {
		return err
	}
	if e, ok := errors.AsType[*Error](err); ok && e.Code == sqlstateQueryCanceled {
		return fmt.Errorf("abandoning the statement: %w (the server cancelled it: %w)", ctx.Err(), err)
	}
	return err
}

// cancelUnconfirmed breaks the connection a cancellation did not settle —
// not confirmed, or confirmed without the statement ending; the watch has
// closed it — and logs one warning saying why.
func (c *Conn) cancelUnconfirmed(ctx context.Context, cerr error, completed bool) {
	c.broken = fmt.Errorf("%w: the cancellation did not end the statement, so the connection was closed: %w", ErrConnBroken, cerr)
	c.log().LogAttrs(context.WithoutCancel(ctx), slog.LevelWarn, "postgres: cancellation did not end the statement, connection closed",
		slog.Uint64("pid", uint64(c.cancelKey.PID)),
		slog.Bool("statement_completed", completed),
		slog.String("error", cerr.Error()))
}

// log is the connection's logger: [Config.Logger], or the default one.
func (c *Conn) log() *slog.Logger {
	if c.logger != nil {
		return c.logger
	}
	return slog.Default()
}
