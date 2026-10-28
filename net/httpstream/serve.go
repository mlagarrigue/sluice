package httpstream

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/ipkey"
)

// Handler answers a batch of requests with a batch of responses, positionally:
// the nth response answers the nth request.
//
// It is a batch-to-batch function rather than a stream-to-stream one, and the
// shape is the same one [github.com/mlagarrigue/sluice/parallel.Ordered] takes — a stage. That is not a
// retreat from the stream model, it is what the model calls a stage; the
// stream is the connection, and [Requests] hands it over whole to a caller
// who wants to compose operators over it directly.
//
// The positional contract is HTTP/1.1's, not this package's: a pipelined
// connection carries no field saying which answer belongs to which request,
// so the order *is* the correlation. [Serve] refuses a batch of the wrong
// length rather than letting a dropped or reordered element answer the wrong
// caller — the same failure the gateway documents when it says not to filter
// a call out of a pipeline.
//
// A handler is called concurrently: across connections, and on one HTTP/2
// or HTTP/3 connection for a batch that arrives while an earlier one is
// still being answered. It is never called twice for the same request.
//
// What it returns is read after it returns, and on HTTP/2 and HTTP/3 while
// its later calls run: a [Response] must stay valid until it is written,
// which the handler is not told.
//
// # What a request looks like, whichever protocol carried it
//
// A handler is not told which protocol a request arrived on, and this is the
// full statement of what it may rely on instead:
//
//   - **The authority is present, under one name.** HTTP/1.1 sends a Host
//     field, HTTP/2 and HTTP/3 send a :authority pseudo-header, and all three
//     reach the handler as req.Get("host"). A request carrying both with a
//     disagreement is refused rather than resolved, because two hops reading
//     a different host out of one request is how a request is routed
//     somewhere it was not addressed.
//   - **The target is raw and origin-form.** It begins with "/", the query is
//     still attached, and nothing has been percent-decoded — decoding before
//     the handler decides what a segment means is how two layers route one
//     request differently. The one other form is the asterisk "*", on an
//     OPTIONS request only (RFC 9112 §3.2.4: the server as a whole, not a
//     resource on it); every other target form is refused at the parser.
//   - **Header names keep the case the protocol sent.** HTTP/1.1 preserves
//     what the client wrote; HTTP/2 and HTTP/3 enforce lowercase on the wire.
//     A handler therefore matches names case-insensitively, which is what
//     [Request.Get] does — reading Headers directly by byte comparison is the
//     mistake this line exists to prevent.
//   - **The cookie arrives whole.** HTTP/2 and HTTP/3 let a client split its
//     Cookie into one field per pair for compression's sake; they reach the
//     handler joined again by "; " as one field, which is what HTTP/1.1
//     delivers, and count as one field under [Config.MaxHeaders].
//   - **The body is streamed, or complete and bounded.** By default
//     ([Config.BodyDelivery] at [BodyStreamed]) a body that was not whole
//     in the read that brought its headers arrives through
//     [Request.Stream], consumed within the call, with [Request.Body] nil.
//     Under [BodyWhole] an element handed over is a whole request: its body
//     has arrived in full, under [Config.MaxBodyBytes], with no partial
//     reads to finish and no framing left to check. [Request.ReadAll]
//     returns either form whole, so a handler written against it needs no
//     change.
//
// # The context
//
// ctx is cancelled when the server stops by force — [Server.Close], or the
// grace of [Serve] or [ServeH3Conn] passing with the handler still running — and
// not when it stops normally, which lets the requests in flight finish. It is
// the context a handler hands to what it calls on the batch's behalf, a
// database query or an upstream request, so that a forced stop reaches them
// rather than leaving them to run with nobody waiting for the answer.
//
// ctx is also cancelled when no request of the batch has a client left. It
// is one context for the whole batch, because the batch is answered as a
// whole: a client that gives up on one request of several does not cancel
// the others' answer. On HTTP/2 a client gives a request up with RST_STREAM,
// on HTTP/3 with RESET_STREAM or STOP_SENDING, and on every protocol by
// closing the connection. On HTTP/1.1, where a batch is one connection's
// requests, the close is seen while the handler runs — a reset, or the end
// of the client's sending side, which RFC 9112 §9.6 notes may be a
// half-close from a client still waiting — from the handler's first call to
// ctx.Done, since seeing it means reading the connection: a handler that
// never asks pays nothing for it. Only the context is cancelled: whatever
// the handler returns is still written, until a write fails. The context
// lives until the batch's responses are written, which is when a streamed
// response's producer stops needing it.
type Handler func(ctx context.Context, b sluice.Batch[Request]) sluice.Batch[Response]

// Serve accepts connections on ln and drives handle over each one until ctx
// is cancelled. It is the shortest way to run a [Server]: the cancellation is
// [Server.Shutdown] bounded by [Config.ShutdownGrace], followed by
// [Server.Close] when the grace passes first.
//
// Every connection is one goroutine reading one stream of request batches.
// There is no worker pool and no queue: the goroutine is the unit the runtime
// already schedules well, and a pool in front of it would add a queue whose
// bound nobody chose.
//
// Serve returns when ctx is cancelled and the connections have drained, or
// when the listener fails. It closes the listener on the way out. A
// temporary Accept failure — a [net.Error] whose Temporary reports true,
// which is how the kernel says it ran out of descriptors (EMFILE, ENFILE)
// or that a peer hung up mid-handshake (ECONNABORTED) — is not the listener
// failing: Serve waits and tries again, 5 ms doubling to 1 s, the policy
// net/http applies, and the connections already accepted keep being
// served. Treating it as fatal would let a burst of descriptor pressure end
// every connection the server holds.
//
// Cancellation drains rather than cuts (docs/design/adr/0017), as
// [Server.Shutdown] describes. What bounds the drain is
// [Config.ShutdownGrace]: when it passes, every connection still open is
// closed under its handler, whose context is cancelled, and Serve returns
// [ErrShutdownForced] without waiting for the goroutines still inside one —
// a handler that has not returned by then is the handler's bug, and holding
// the process up for it would turn the bug into a hang. A drain that ends
// within the grace returns nil.
//
// What Serve speaks on a bare TCP listener is cleartext HTTP/1.1, with
// prior-knowledge HTTP/2 sniffed from the preface. Cleartext is kept for
// interoperability — a terminating proxy, a health check, local tooling —
// not recommended: anything that crosses a network boundary belongs behind
// TLS or on [ServeH3Conn], whose transport encrypts by construction. Over a TLS
// listener the same preface sniff still picks the protocol — Serve does not
// read the negotiated ALPN — so a client that negotiated h2 and sends the
// preface is served HTTP/2, and one that did not is served HTTP/1.1.
//
// A configuration [Config] refuses is returned as an error before the
// listener is read.
func Serve(ctx context.Context, ln net.Listener, cfg Config, handle Handler) error {
	if handle == nil {
		panic("httpstream: Serve requires a handler")
	}
	resolved, err := cfg.resolve()
	if err != nil {
		return err
	}
	srv := &Server{Config: cfg, Handler: handle}
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()
	select {
	case err = <-served:
		// The listener failed; what it accepted is drained all the same.
		served = nil
	case <-ctx.Done():
	}
	grace, cancel := context.WithTimeout(context.Background(), resolved.ShutdownGrace)
	defer cancel()
	if srv.Shutdown(grace) != nil {
		_ = srv.Close()
		err = errors.Join(err, ErrShutdownForced)
	}
	if served != nil {
		// Shutdown closed the listener, so this returns now: nil, or the
		// listener's failure if it came first.
		err = errors.Join(<-served, err)
	}
	return err
}

// connSet is the connections a [Server] has accepted and not yet finished with,
// each with where it stands between two requests. It exists for the
// shutdown: close the ones that owe nothing, then, when the grace has
// passed, the rest. It is also where Config.MaxConns and Config.MaxConnsPerIP
// bind, since it already counts every live connection: the one that would
// take either past its bound is not admitted.
type connSet struct {
	mu     sync.Mutex
	conns  map[net.Conn]*connState
	closed bool
	max    int // Config.MaxConns; zero is unbounded
	// maxPerIP is Config.MaxConnsPerIP, zero unbounded; byIP counts the
	// connections in conns by remote prefix (ipkey.Of, v4Bits or v6Bits
	// long), the ones with an IP address only.
	maxPerIP       int
	v4Bits, v6Bits int
	byIP           map[netip.Prefix]int
	// wg counts the connections admitted and not yet removed. It is added to
	// under mu while the set is open, so a Wait begun after drain races no
	// Add.
	wg sync.WaitGroup
	// draining is cancelled when the shutdown begins; every connection's state
	// shares it, which is how the request reader learns not to read another
	// request and how an HTTP/2 connection learns to say GOAWAY. A context
	// rather than a channel so the latter is a [context.AfterFunc], not a
	// goroutine parked for the life of every connection.
	draining   context.Context
	beginDrain context.CancelFunc
}

// newConnSet takes the resolved Config: its two bounds, zero unbounded, and
// the prefix lengths MaxConnsPerIP counts by.
func newConnSet(c Config) *connSet {
	draining, begin := context.WithCancel(context.Background())
	return &connSet{
		conns: map[net.Conn]*connState{}, max: max(c.MaxConns, 0),
		maxPerIP: max(c.MaxConnsPerIP, 0), byIP: map[netip.Prefix]int{},
		v4Bits: c.IPv4PrefixBits, v6Bits: c.IPv6PrefixBits,
		draining: draining, beginDrain: begin,
	}
}

// remoteIP is the key Config.MaxConnsPerIP counts c under: its remote
// address's prefix (ipkey.Of), the port ignored; false when the address is
// not IP, which no per-IP bound applies to.
func (s *connSet) remoteIP(c net.Conn) (netip.Prefix, bool) {
	if a, ok := c.RemoteAddr().(*net.TCPAddr); ok {
		if ip, ok := netip.AddrFromSlice(a.IP); ok {
			return ipkey.Of(ip, s.v4Bits, s.v6Bits), true
		}
	}
	return netip.Prefix{}, false
}

// connState is what the shutdown needs to know about one connection: whether
// a request is in flight on it. It is written by the goroutine serving the
// connection and read by the shutdown, hence the atomic.
type connState struct {
	phase atomic.Int32
	drain context.Context
	// h2 is set once the preface has been read: an HTTP/2 connection is
	// never closed by the shutdown's first pass, idle or not — it is told
	// with the GOAWAY pair serveH2Conn sends when drain is cancelled, and
	// closes itself at the PING's acknowledgement (RFC 9113 §6.8: a GOAWAY
	// before closing, so that a POST crossing the close is not lost
	// silently). A peer that never acknowledges is closed by the grace.
	h2 atomic.Bool
	ip netip.Prefix // the connection's key in connSet.byIP; invalid when it has none
	// bodyToEOF is set while a close-delimited body — an HTTP/1.0 client's
	// streamed response, see streamedFraming — is on the wire and not yet
	// complete. A close in that state must not carry TLS's close_notify:
	// RFC 9112 §9.8 makes the closure alert that body's only end marker, so
	// a valid alert after a cut body would pass the cut off as the end.
	bodyToEOF atomic.Bool
}

// The phases a connection moves through, as the shutdown sees them.
const (
	// connIdle: nothing has arrived for the next request — the connection
	// is parked, in the preface sniff or between two keep-alive requests.
	// Closing it owes nobody an answer.
	connIdle int32 = iota
	// connBusy: a request has begun to arrive, is being handled or is being
	// answered — on HTTP/2, a stream is open or in flight. The shutdown
	// waits for it, up to the grace.
	connBusy
)

// set records the phase. A nil state is a connection nothing watches — a
// test driving serveConn directly — and takes no note.
func (st *connState) set(phase int32) {
	if st != nil {
		st.phase.Store(phase)
	}
}

// drainingNow reports whether the shutdown has begun; never, for a nil state.
func (st *connState) drainingNow() bool {
	return st != nil && st.drain.Err() != nil
}

// setBodyToEOF records whether a close-delimited body is on the wire and
// incomplete; a nil state takes no note.
func (st *connState) setBodyToEOF(on bool) {
	if st != nil {
		st.bodyToEOF.Store(on)
	}
}

// cutBody reports whether closing now would cut a close-delimited body short.
func (st *connState) cutBody() bool {
	return st != nil && st.bodyToEOF.Load()
}

// add registers a connection, returning nil if it is not admitted — the set
// has already been closed, it is at max, or its remote IP is at maxPerIP —
// and this one therefore has to be closed by its caller. A slot frees in
// remove, which the connection's goroutine runs when it is done with the
// socket, so a client that hung up counts until the server has read its
// close.
func (s *connSet) add(c net.Conn) *connState {
	ip, hasIP := s.remoteIP(c)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || (s.max > 0 && len(s.conns) >= s.max) ||
		(hasIP && s.maxPerIP > 0 && s.byIP[ip] >= s.maxPerIP) {
		return nil
	}
	st := &connState{drain: s.draining}
	if hasIP {
		st.ip = ip
		s.byIP[ip]++
	}
	s.conns[c] = st
	s.wg.Add(1)
	return st
}

func (s *connSet) remove(c net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.conns[c]; ok {
		delete(s.conns, c)
		if st.ip.IsValid() {
			s.byIP[st.ip]--
			if s.byIP[st.ip] <= 0 {
				delete(s.byIP, st.ip)
			}
		}
		s.wg.Done()
	}
}

// busy is the number of connections with a request in flight.
func (s *connSet) busy() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, st := range s.conns {
		if st.phase.Load() == connBusy {
			n++
		}
	}
	return n
}

// drain begins the shutdown: it refuses new connections, tells the readers
// not to take another request, and closes every connection that has none in
// flight. The order matters against the reader, which marks itself idle
// before it looks at draining: a reader that marked itself after this scan
// sees the context cancelled; one that marked itself before is closed here.
// Either way no idle HTTP/1.1 connection outlives the call. An HTTP/2
// connection is told instead, idle or not (see serveH2Conn): the
// cancellation runs its GOAWAY pair, and it closes itself once the PING is
// acknowledged and every stream answered — one round trip for an idle one,
// where closing it here raced the GOAWAY its own goroutine was queuing.
func (s *connSet) drain() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.beginDrain()
	for c, st := range s.conns {
		if st.phase.Load() != connBusy && !st.h2.Load() {
			_ = c.Close()
		}
	}
}

// closeAll closes what the grace did not drain. The map is kept: remove
// still runs for each connection when its goroutine ends.
func (s *connSet) closeAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for c, st := range s.conns {
		if st.cutBody() {
			closeCut(c)
			continue
		}
		_ = c.Close()
	}
}

// closeCut closes a connection whose close-delimited body is incomplete. On
// TLS the transport is closed under the record layer, so no close_notify goes
// out and the client reads the cut as an incomplete close (crypto/tls returns
// io.ErrUnexpectedEOF) rather than the valid closure alert that RFC 9112
// §9.8 makes the body's end marker. On a plain socket a FIN is all there is:
// cleartext HTTP/1.0 cannot tell a cut body from a complete one, which is the
// weakness §9.8 names. Close on the TLS connection afterwards finds the
// socket gone and sends nothing.
func closeCut(c net.Conn) {
	if tc, ok := c.(*tls.Conn); ok {
		_ = tc.NetConn().Close()
	}
	_ = c.Close()
}

// serveConn runs one connection to its end, answering whatever it can and
// closing when it cannot.
//
// c is a [prefixConn] even when nothing was peeked: it is where a streamed
// response's peer watch (see watchPeer) puts back what it read ahead. st is
// the shutdown's view of the connection, nil when nothing is watching it.
func serveConn(ctx context.Context, c *prefixConn, st *connState, cfg Config, handle Handler) {
	src := newRequestReader(c, st, cfg)
	// Reused for every batch on this connection, growing toward whatever the
	// largest one needed and never shrinking back — the ordinary trade of a
	// reused buffer against a per-batch allocation, paid once per connection
	// rather than once per batch.
	out := make([]byte, 0, 4<<10)

	// Read once the first batch is in, by which time a TLS handshake is done.
	var who peer
	known := false
	stopped := false
	src.Stream()(func(b sluice.Batch[Request]) bool {
		if !known {
			who, known = peerOf(c), true
		}
		who.stamp(b.Items)
		call := newClientCtx(ctx, c, cfg.maxBuffer())
		defer call.written()
		body := streamedBody(b.Items)
		if body != nil && !body.done {
			call.bodyPending = true
			body.onDone = call.bodyEnded
		}
		res, err := apply(call, handle, b, cfg.EarlyData)
		call.returned()
		if body != nil {
			body.closed = true
			if !body.done {
				// The body was left unread, or broke: the next request is
				// behind it, so this response is the connection's last.
				b.Items[len(b.Items)-1].KeepAlive = false
			}
		}
		if err != nil {
			reportFailure(cfg.logger(), err)
			_ = writeStatus(c, cfg, firstMethod(b.Items), 500)
			stopped = true
			return false
		}
		if st.drainingNow() && len(b.Items) > 0 {
			// The shutdown began while the handler ran: this batch is the
			// connection's last, and its last response says so (RFC 9112
			// §9.6) the way a request's own Connection: close would — the
			// reader reads the same field and stops after it. A shutdown
			// that begins once the header is out cannot be announced; the
			// client finds the close at its next request instead.
			b.Items[len(b.Items)-1].KeepAlive = false
		}
		// A streamed response is written as it is produced rather than
		// gathered first, which is the whole point of it: the batch's
		// buffered responses go out together, and a streaming one takes the
		// connection for as long as it runs.
		if streamedIn(res) {
			var closed bool
			var werr error
			if out, closed, werr = writeStreamedBatch(c, st, cfg, b.Items, res, out); werr != nil || closed || asksToClose(res) {
				// closed without error is a close-delimited body: EOF is its
				// end marker, so the close *is* the last byte of the answer.
				stopped = true
				return false
			}
			return true
		}
		var werr error
		if out, werr = WriteBatch(out[:0], cfg, b.Items, res); werr != nil {
			// A response this package refuses to put on the wire — an
			// injected header, a framing header the caller set, trailers
			// that panicked. The caller's bug, answered as one rather than
			// written out.
			reportPanic(cfg.logger(), werr)
			_ = writeStatus(c, cfg, firstMethod(b.Items), 500)
			stopped = true
			return false
		}
		if err := writeAll(c, cfg, out); err != nil || asksToClose(res) {
			// A handler's Connection: close went out on the last response;
			// nothing more is read from a client that was told so.
			stopped = true
			return false
		}
		return true
	})

	if !stopped {
		// The read side ended. A clean close needs no answer; a refused
		// request gets the status its refusal maps to, on the connection
		// that is about to close anyway.
		if err := src.Err(); err != nil && !errors.Is(err, io.EOF) {
			if status := statusFor(err); status != 0 {
				_ = writeStatus(c, cfg, refusedMethod(err), status)
			}
		}
	}
	if st.cutBody() {
		// A close-delimited body ended short of its producer's end — the
		// producer failed, or the client left. The two-stage close would
		// send close_notify over TLS, and the client would take the cut
		// body for complete (RFC 9112 §9.8).
		closeCut(c.Conn)
		return
	}
	tearDown(c, cfg)
}

// tearDown ends the connection in the two stages RFC 9112 §9.6 asks for,
// so that the last response reaches the client.
//
// Closing a socket with unread bytes in it makes the TCP stack answer the
// client's next segment with a reset, and a reset can erase what the client
// had received and not yet read — the 413 an upload was refused with, the
// last response before a pipelined request the shutdown will not serve. So
// the write side is closed first, which is the client's EOF, and whatever
// it still sends is read and dropped until it closes in turn or a short
// deadline passes; the caller's Close then finds the socket empty. The
// deadline is [Config.LingerTimeout]. A connection that
// cannot half-close (a pipe, a wrapper without CloseWrite) is closed in one
// stage as before.
func tearDown(c *prefixConn, cfg Config) {
	linger(c.Conn, cfg)
}

// linger is tearDown's two stages on any connection: the write side closed,
// then what the peer still sends read and dropped until it closes in turn
// or [Config.LingerTimeout] passes. A connection put behind a prefixConn is
// unwrapped first, since the half-close is the underlying socket's.
func linger(c net.Conn, cfg Config) {
	if pc, ok := c.(*prefixConn); ok {
		c = pc.Conn
	}
	cw, ok := c.(interface{ CloseWrite() error })
	if !ok || cw.CloseWrite() != nil {
		return
	}
	if err := c.SetReadDeadline(time.Now().Add(cfg.LingerTimeout)); err != nil {
		return
	}
	var sink [4 << 10]byte
	for {
		if _, err := c.Read(sink[:]); err != nil {
			return
		}
	}
}

// firstMethod is the method of the request a single-response answer to a
// whole batch is written against — the batch failed together, so there is no
// single request it answers, and the first is as good a guess at "does this
// connection expect a body" as any.
func firstMethod(reqs []Request) []byte {
	if len(reqs) == 0 {
		return nil
	}
	return reqs[0].Method
}

// apply runs the handler and checks what it returned, since a batch of the
// wrong length would answer the wrong caller. A request carrying Early-Data
// that early does not allow is answered 425 in its place without reaching
// the handler (RFC 8470 §5.2), which sees the others in their order.
func apply(ctx context.Context, handle Handler, b sluice.Batch[Request], early func(*Request) bool) (res []Response, err error) {
	if tooEarly(b.Items, early) {
		return applyTooEarly(ctx, handle, b, early)
	}
	return call(ctx, handle, b)
}

// call is apply once no request is left to refuse: the handler, its panic
// turned into an error, and the length of its answer checked.
func call(ctx context.Context, handle Handler, b sluice.Batch[Request]) (res []Response, err error) {
	defer func() {
		if r := recover(); r != nil {
			// A panic in the handler is the caller's bug and must not take
			// the process down; it becomes a 500 on this connection, the same
			// bargain [web.Recover] makes, and its value and stack go to the
			// log where the 500 is written (reportFailure).
			err = recovered("handler", r)
		}
	}()
	out := handle(ctx, b)
	if len(out.Items) != b.Len() {
		return nil, fmt.Errorf("httpstream: the handler answered %d of %d requests; "+
			"a pipelined connection correlates by position, so a dropped or added "+
			"element answers the wrong caller", len(out.Items), b.Len())
	}
	return out.Items, nil
}

// tooEarly reports whether any request carries an Early-Data field that
// early does not allow. Any value counts: RFC 8470 §5.1 has an invalid or
// repeated field read as a single "1".
func tooEarly(reqs []Request, early func(*Request) bool) bool {
	for i := range reqs {
		if refusedEarly(&reqs[i], early) {
			return true
		}
	}
	return false
}

func refusedEarly(r *Request, early func(*Request) bool) bool {
	for _, h := range r.Headers {
		if equalFold(h.Name, "early-data") {
			return early == nil || !early(r)
		}
	}
	return false
}

// applyTooEarly is apply for a batch where some requests are answered 425:
// the handler is called on the others, and its answers are put back in
// their positions.
func applyTooEarly(ctx context.Context, handle Handler, b sluice.Batch[Request], early func(*Request) bool) ([]Response, error) {
	res := make([]Response, b.Len())
	var rest []Request
	var at []int
	for i := range b.Items {
		if refusedEarly(&b.Items[i], early) {
			res[i] = transportError(425)
			continue
		}
		rest = append(rest, b.Items[i])
		at = append(at, i)
	}
	if len(rest) == 0 {
		return res, nil
	}
	out, err := call(ctx, handle, sluice.Batch[Request]{Items: rest})
	if err != nil {
		return nil, err
	}
	for k, i := range at {
		res[i] = out[k]
	}
	return res, nil
}

// isTimeout reports whether err, or an error it wraps, is a deadline that
// expired on the connection.
func isTimeout(err error) bool {
	ne, ok := errors.AsType[net.Error](err)
	return ok && ne.Timeout()
}

// statusFor maps a read failure to what the client should be told. A timeout
// mid-request is answered 408, so a slow client learns it was slow; a
// timeout between requests (errIdleTimeout) answers nothing — the connection
// simply had no request to be slow about, and a 408 there is a reply to a
// question nobody asked, which some strict clients take as a protocol
// violation rather than a close.
func statusFor(err error) int {
	switch {
	case errors.Is(err, errIdleTimeout):
		return 0
	case isTimeout(err):
		return 408
	case errors.Is(err, errUnsupportedCoding):
		// RFC 9112 §6.1: a transfer coding this server does not implement is
		// answered 501, not 400 — the request may be well-formed; the server
		// is the one missing the feature.
		return 501
	case errors.Is(err, errRequestLineTooLong):
		// RFC 9112 §3: a request-target longer than the server will parse
		// MUST be answered 414 — the client learns which part was too long.
		return 414
	case errors.Is(err, errHeadersTooLarge):
		// RFC 6585 §5: header fields too large in total or singly are 431.
		return 431
	case errors.Is(err, ErrTooLarge):
		return 413
	case errors.Is(err, errVersionUnsupported):
		// RFC 9110 §15.6.6: a version this server does not speak is 505.
		return 505
	case errors.Is(err, errConnect):
		// RFC 9110 §15.6.2: a tunnel this server does not open is 501, the
		// request being well formed. The connection closes with the
		// refusal: the client meant to switch it to a tunnel.
		return 501
	case errors.Is(err, ErrMalformed):
		return 400
	}
	return 0
}

// writeStatus answers an error of the transport's own, see [transportError].
// method is the request's when it was read before the refusal, nil when
// the refusal came first; a HEAD that reached the refusal gets no body.
func writeStatus(c net.Conn, cfg Config, method []byte, status int) error {
	// closing is always true here: every caller is about to end the
	// connection, so appendResponse writes the Connection: close itself
	// rather than have it duplicated as a caller-supplied header.
	buf, err := appendResponse(nil, method, true, false, nil, transportError(status), dateFor(time.Now()))
	if err != nil {
		return err
	}
	return writeAll(c, cfg, buf)
}

func writeAll(c net.Conn, cfg Config, buf []byte) error {
	return writeBoundOf(cfg).write(c.SetWriteDeadline, c.Write, buf)
}

// writeBound is [Config.WriteTimeout] read as a time without progress: a
// write that times out having put progress bytes or more on the wire is
// given the timeout again, from where it stopped, and one that times out
// short of them fails. HTTP/1.1 and HTTP/3 write through it; the HTTP/2
// writer applies the same rule to a stream's credit itself.
type writeBound struct {
	timeout  time.Duration
	progress int
	log      *slog.Logger // where a producer's or trailers' failure is reported
}

func writeBoundOf(cfg Config) writeBound {
	return writeBound{timeout: cfg.WriteTimeout, progress: cfg.WriteProgressBytes, log: cfg.logger()}
}

// write puts p on the wire with write, under deadlines it sets through
// setDeadline.
//
// The bytes are handed over whole rather than in progress-sized pieces: a
// client that is reading takes them in one write, one syscall, and only one
// that is not costs a second. That relies on a timed-out write reporting
// what it wrote and taking a later deadline, which net.Conn and quic.Stream
// both do. crypto/tls does not — it makes the timeout fatal — so over TLS
// the second write fails at once and the first timeout is the last, as it
// was before progress counted.
func (b writeBound) write(setDeadline func(time.Time) error, write func([]byte) (int, error), p []byte) error {
	for {
		if err := setDeadline(time.Now().Add(b.timeout)); err != nil {
			return err
		}
		n, err := write(p)
		if err == nil || n < b.progress || !errors.Is(err, os.ErrDeadlineExceeded) {
			return err
		}
		p = p[n:]
	}
}

// serveAny picks the protocol from what the client says first.
//
// Prior-knowledge HTTP/2 opens with a 24-byte preface chosen to be invalid
// HTTP/1.1, which is what makes the sniff safe rather than a guess: no
// well-formed HTTP/1.1 request begins with those bytes, and a client sending
// them has been told to speak HTTP/2. The sniff is also what decides over
// TLS: a client that negotiated h2 by ALPN sends the same preface, and this
// function reads that rather than the negotiated protocol. TLS itself is not
// this package's to terminate.
//
// The peeked bytes are handed on rather than re-read, since a connection
// cannot be rewound.
func serveAny(ctx context.Context, c net.Conn, st *connState, cfg Config, handle Handler) {
	peek, isH2, err := sniffPreface(c, cfg)
	if err != nil {
		return // silent past ReadTimeout, or gone: nothing to hand on
	}
	if isH2 {
		// Idle until the reader opens a stream: the preface promises none,
		// and a client that sends it and stops has nothing in flight — busy
		// from here made 24 silent bytes count as a request a forced stop
		// cut. Marked HTTP/2: from here the shutdown tells it, never cuts it
		// (see connState.h2); one still short of its SETTINGS is told and
		// closed after LingerTimeout (see beginDrain).
		if st != nil {
			st.h2.Store(true)
		}
		err := serveH2Conn(ctx, prefixed(c, peek), st, cfg, handle)
		reportConnEnd(cfg.logger(), "h2", c.RemoteAddr(), err)
		return
	}
	// The peeked bytes are the start of a request: busy from here, and the
	// reader takes over the phase from its first read.
	st.set(connBusy)
	serveConn(ctx, &prefixConn{Conn: c, head: peek}, st, cfg, handle)
}

// sniffPreface reads only as far as it must to tell the protocols apart.
//
// It compares as it goes and stops at the first byte that diverges, which is
// what keeps it from waiting for twenty-four bytes a client is never going to
// send: an ordinary request differs from the preface by its second byte, so
// the decision costs one read. Waiting for the whole preface instead would
// hang every request shorter than it — and "GET /x HTTP/1.1" with a blank
// line is nineteen bytes, which is how this was found.
//
// One deadline covers the whole sniff, not one per read: re-armed per read, a
// client dribbling the preface a byte at a time held the connection for up to
// twenty-four idle timeouts. A read error ends the connection here rather
// than handing an undecided prefix to the HTTP/1.1 reader, which would wait
// a second IdleTimeout on a client already found silent.
func sniffPreface(c net.Conn, cfg Config) (peek []byte, isH2 bool, err error) {
	// A connection that has not spoken yet has ReadTimeout to do so — the
	// bound a request gets, not the IdleTimeout a connection may rest
	// between two requests: nothing has been asked yet, and two minutes of
	// a goroutine, a buffer and a descriptor per silent dial is what a
	// slowloris that never sends a byte would buy otherwise. The TLS
	// handshake, driven by this first Read, is under the same bound.
	if err := c.SetReadDeadline(time.Now().Add(cfg.ReadTimeout)); err != nil {
		return nil, false, err
	}
	buf := make([]byte, len(Preface))
	for len(peek) < len(Preface) {
		n, err := c.Read(buf[:len(Preface)-len(peek)])
		peek = append(peek, buf[:n]...)
		if !bytes.HasPrefix(prefaceBytes, peek) {
			return peek, false, nil // diverged: this is not HTTP/2
		}
		if err != nil {
			return peek, false, err
		}
	}
	return peek, true, nil
}

// prefaceBytes is [Preface], kept as []byte so sniffPreface compares without
// allocating a string from what it has read so far on every read.
var prefaceBytes = []byte(Preface)

// prefixConn puts bytes already read back in front of a connection, since
// a connection cannot be rewound.
type prefixConn struct {
	net.Conn
	head []byte
	// scratch is the watch's read buffer, kept for the connection: one watch
	// runs at a time, and a buffer per watch went to the heap every batch.
	scratch []byte
}

func prefixed(c net.Conn, head []byte) net.Conn {
	if len(head) == 0 {
		return c
	}
	return &prefixConn{Conn: c, head: head}
}

// peer is what a connection knows of its client, written into each request
// before the handler sees it ([Request.RemoteAddr], [Request.TLS]).
type peer struct {
	addr netip.AddrPort
	tls  *tls.ConnectionState
}

// peerOf reads the client's address and TLS state off a connection whose
// handshake is done: one ConnectionState copy per connection, shared by its
// requests.
func peerOf(c net.Conn) peer {
	if pc, ok := c.(*prefixConn); ok {
		c = pc.Conn
	}
	p := peer{addr: addrPortOf(c.RemoteAddr())}
	if t, ok := c.(interface{ ConnectionState() tls.ConnectionState }); ok {
		s := t.ConnectionState()
		p.tls = &s
	}
	return p
}

// addrPortOf is a TCP or UDP address as an [netip.AddrPort], an IPv4 address
// in its four-byte form; the zero value for any other kind of address.
func addrPortOf(a net.Addr) netip.AddrPort {
	var ap netip.AddrPort
	switch a := a.(type) {
	case *net.TCPAddr:
		ap = a.AddrPort()
	case *net.UDPAddr:
		ap = a.AddrPort()
	default:
		return ap
	}
	return netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port())
}

// stamp writes the peer into each request of a batch.
func (p peer) stamp(reqs []Request) {
	for i := range reqs {
		reqs[i].RemoteAddr, reqs[i].TLS = p.addr, p.tls
	}
}

// batchCtx is the context of one batch's call: its parent's — the forced
// stop — and also the batch's clients all leaving.
//
// It is allocated once per batch and costs nothing more until someone asks
// for Done: the channel, and the parent's hold on it through
// [context.AfterFunc] (which runs no goroutine of its own), are made then.
// It covers the batch until its responses are written, so that a streamed
// response's producer keeps it; past that it is neither watched nor
// cancelled, and the parent lets go of it.
//
// live counts the requests of the batch whose client may still want an
// answer, on HTTP/2 and HTTP/3: a stream the client resets takes one away,
// the connection's end takes them all, and the context is cancelled at
// zero — not before, since the batch is answered whole and one request
// given up on does not cost the others their answers (ADR 0008). HTTP/1.1
// has one client per batch, and clientCtx watches it.
type batchCtx struct {
	context.Context // the parent

	mu     sync.Mutex
	ended  bool            // the batch is written
	done   <-chan struct{} // what Done returns, once asked
	cancel chan struct{}   // done when this context owns it
	err    error
	unlink func() bool // ends the parent's hold on cancel
	live   int
}

func (x *batchCtx) Done() <-chan struct{} {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.doneLocked()
}

func (x *batchCtx) doneLocked() <-chan struct{} {
	if x.done != nil {
		return x.done
	}
	if x.ended && x.err == nil {
		// Asked after the batch was written: nothing left to watch.
		x.done = x.Context.Done()
		return x.done
	}
	x.cancel = make(chan struct{})
	x.done = x.cancel
	if x.err != nil {
		close(x.cancel) // the clients left before anyone asked
		return x.done
	}
	x.unlink = context.AfterFunc(x.Context, func() { x.close(x.Context.Err()) })
	return x.done
}

func (x *batchCtx) Err() error {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.err != nil || x.cancel != nil {
		return x.err
	}
	return x.Context.Err()
}

// close cancels the context, unless the batch is written or it already is.
func (x *batchCtx) close(err error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.closeLocked(err)
}

func (x *batchCtx) closeLocked(err error) {
	if x.err != nil || x.ended {
		return
	}
	x.err = err
	if x.cancel != nil {
		close(x.cancel)
	}
}

// leave counts one request's client gone: the last one cancels.
func (x *batchCtx) leave() {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.live--
	if x.live <= 0 {
		x.closeLocked(context.Canceled)
	}
}

// written is called once the batch's responses are written, or abandoned.
func (x *batchCtx) written() {
	x.mu.Lock()
	x.ended = true
	unlink := x.unlink
	x.mu.Unlock()
	if unlink != nil {
		unlink()
	}
}

// streamCalls maps each stream handed to a handler to its batch's context,
// until the batch's responses are written: on HTTP/2 and HTTP/3, where a
// client cancels one request by resetting its stream.
type streamCalls[K comparable] struct {
	mu sync.Mutex
	m  map[K]*batchCtx
}

// register makes the context of a batch about to be handed to the handler.
func (k *streamCalls[K]) register(parent context.Context, ids []K) *batchCtx {
	x := &batchCtx{Context: parent, live: len(ids)}
	k.mu.Lock()
	if k.m == nil {
		k.m = make(map[K]*batchCtx)
	}
	for _, id := range ids {
		k.m[id] = x
	}
	k.mu.Unlock()
	return x
}

// leave takes a cancelled stream's request out of its batch; a stream not
// handed over, or already taken out, is nothing.
func (k *streamCalls[K]) leave(id K) {
	k.mu.Lock()
	x := k.m[id]
	delete(k.m, id)
	k.mu.Unlock()
	if x != nil {
		x.leave()
	}
}

// leaveAll cancels every batch still in flight: the connection is over.
func (k *streamCalls[K]) leaveAll() {
	k.mu.Lock()
	defer k.mu.Unlock()
	for id, x := range k.m {
		x.close(context.Canceled)
		delete(k.m, id)
	}
}

// release ends a batch's context once its responses are written.
func (k *streamCalls[K]) release(x *batchCtx, ids []K) {
	k.mu.Lock()
	for _, id := range ids {
		if k.m[id] == x {
			delete(k.m, id)
		}
	}
	k.mu.Unlock()
	x.written()
}

// clientCtx is the handler's context on HTTP/1.1: a [batchCtx] whose client
// is watched while the handler runs.
//
// Watching costs a goroutine reading the connection, and the wake-ups to
// start and stop it; most handlers never ask, so nothing is watched until
// the first call to Done. A handler that answers from memory pays one
// allocation per batch; one that hands the context to a query pays the
// watch. Under load that is 1.2 µs of work a batch (BenchmarkServeH1RoundTrip/
// parallel: 4.0 µs a request watched, 2.8 plain), little beside the query
// that made the handler ask; one request at a time on an idle machine it is
// the wake-up of an idle thread the watch's goroutine needs, 24 µs on a
// GitHub Linux runner (serial: 46 µs watched, 23 plain). Starting it later
// — a timer, so that only slow handlers pay — was measured and costs more:
// arming a timer wakes the network poller. Keeping one reader for the
// connection's life was measured too, and costs more again: every request,
// watched or not, then pays the wake-up one at a time (15.5 to 98.4 µs
// serial, ADR 0008). The read is [watchPeer]'s, whose
// bytes are put back for the request reader, and it ends when the handler
// returns: a streamed response has its own watch.
//
// An EOF counts as the client leaving. RFC 9112 §9.6 notes that a client's
// half-close does not by itself say it lost interest in the response; but a
// client that closes sends exactly that EOF, and is by far the common case.
// What is cancelled is the context only: whatever the handler returns is
// still written, until a write fails.
type clientCtx struct {
	batchCtx

	c       *prefixConn
	limit   int
	calling bool // the handler is running: the watch may start
	watch   *peerWatch

	// bodyPending holds the watch back while a streamed request body is
	// still on the wire: the watch reads the connection, and would take the
	// body's bytes. A Done asked for meanwhile is remembered in wanted, and
	// the watch starts when the body ends; until then a client leaving shows
	// in the body's own read.
	bodyPending, wanted bool
}

func newClientCtx(parent context.Context, c *prefixConn, limit int) *clientCtx {
	return &clientCtx{batchCtx: batchCtx{Context: parent}, c: c, limit: limit, calling: true}
}

func (x *clientCtx) Done() <-chan struct{} {
	x.mu.Lock()
	defer x.mu.Unlock()
	done := x.doneLocked()
	if x.bodyPending {
		x.wanted = true
		return done
	}
	x.armLocked()
	return done
}

// armLocked starts the watch, once, while the handler runs.
func (x *clientCtx) armLocked() {
	if x.calling && x.watch == nil && x.err == nil && x.cancel != nil {
		x.watch = watchPeerThen(x.c, x.limit, func(w *peerWatch) {
			if w.gone.Load() || w.halfClosed.Load() {
				x.close(context.Canceled)
			}
		})
	}
}

// bodyEnded lets the watch start, if a Done was asked for while the body was
// still being read.
func (x *clientCtx) bodyEnded() {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.bodyPending = false
	if x.wanted {
		x.armLocked()
	}
}

// returned is called when the handler returns: the watch stops, and the
// connection has a single reader again.
func (x *clientCtx) returned() {
	x.mu.Lock()
	x.calling = false
	w := x.watch
	x.mu.Unlock()
	if w != nil {
		w.stop()
	}
}

// buffered reports whether bytes put back in front of the connection are
// waiting to be read — the start of a request, as far as the shutdown is
// concerned.
func (c *prefixConn) buffered() bool { return len(c.head) > 0 }

func (c *prefixConn) Read(p []byte) (int, error) {
	if len(c.head) > 0 {
		n := copy(p, c.head)
		c.head = c.head[n:]
		return n, nil
	}
	return c.Conn.Read(p)
}

// errPeerGone ends a streamed HTTP/1.1 response whose client reset the
// connection, or whose connection was closed under it by Serve's shutdown.
var errPeerGone = errors.New("httpstream: the client left during a streamed response")

// peerWatch reads the connection while a streamed response holds it, which is
// how a producer learns the client left without a write failing first.
//
// Nothing else reads an HTTP/1.1 connection while a response streams — the
// serving goroutine is the producer's — so a client that resets is invisible
// until a write fails, and a producer that only yields empty batches never
// writes. The watch is one goroutine and one blocked read per streamed
// response, not per batch: the producer's check is an atomic load.
//
// EOF is not the client leaving. A client that half-closes after its
// request — shutdown(SHUT_WR), what netcat and the simplest HTTP/1.0
// clients do — still reads the answer, and RFC 9112 §9.6 says so in as many
// words: a half-close neither delimits the request nor means the client lost
// interest. So EOF ends the read and nothing else; gone is for a reset, the
// socket closed by Serve's shutdown, and a TLS connection cut without its
// close_notify, which [crypto/tls] reports as [io.ErrUnexpectedEOF]. Past
// the half-close the peer has nothing more to say, so the producer's check
// becomes one read per batch (see left), which returns at once: EOF again,
// or the reset or close that ends the response.
//
// What it reads is not lost. A client that pipelines its next request while
// this one streams has those bytes kept, up to limit, and put back in front
// of the connection when the watch stops, for the request reader to find
// where it would have read them. Past limit the watch stops reading and the
// client is not watched for the rest of this response.
type peerWatch struct {
	c          *prefixConn
	gone       atomic.Bool
	halfClosed atomic.Bool
	stopping   atomic.Bool
	done       chan struct{}
	got        []byte
}

// left reports whether the client is gone. Asked on every batch, empty ones
// included: an empty batch writes nothing, so no write failure would ever
// tell a producer of cadence ticks that the client left.
func (w *peerWatch) left() bool {
	if w.gone.Load() {
		return true
	}
	if !w.halfClosed.Load() {
		return false
	}
	// The watching goroutine is done and the read side drained: a read here
	// does not block. A client that half-closed and then went away is told
	// apart from one still reading only by a segment it answers with a
	// reset, which only a write provokes; until then its EOF is all there is.
	var scratch [1]byte
	_, err := w.c.Conn.Read(scratch[:])
	if err == nil || errors.Is(err, io.EOF) {
		return false
	}
	w.gone.Store(true)
	return true
}

func watchPeer(c *prefixConn, limit int) *peerWatch {
	return watchPeerThen(c, limit, nil)
}

// watchPeerThen is watchPeer with a function run when the watch ends, on the
// watching goroutine, before done is closed: the handler's context learns
// the client left without a goroutine of its own waiting on done.
func watchPeerThen(c *prefixConn, limit int, ended func(*peerWatch)) *peerWatch {
	w := &peerWatch{c: c, done: make(chan struct{})}
	// No deadline while watching: the request reader set one for the request
	// it read, and that clock says nothing about how long a response may
	// take to produce. stop sets it to the past to end the read, and the
	// request reader sets its own again before its next read.
	if err := c.SetReadDeadline(time.Time{}); err != nil {
		w.gone.Store(true) // a connection that refuses a deadline is closed
		if ended == nil {
			close(w.done)
			return w
		}
		// Not on this goroutine: its caller may hold the lock ended takes.
		go func() {
			defer close(w.done)
			ended(w)
		}()
		return w
	}
	go func() {
		defer close(w.done)
		if ended != nil {
			defer ended(w)
		}
		if c.scratch == nil {
			c.scratch = make([]byte, 512)
		}
		scratch := c.scratch
		for len(w.got) < limit {
			// c.Conn, not c: c.head belongs to the request reader, and this
			// goroutine only appends to it once stop has waited for it.
			n, err := c.Conn.Read(scratch[:min(len(scratch), limit-len(w.got))])
			w.got = append(w.got, scratch[:n]...)
			if err != nil {
				switch {
				case errors.Is(err, io.EOF):
					// The client half-closed: it is still reading.
					w.halfClosed.Store(true)
				case !w.stopping.Load():
					// A reset, or the socket closed by Serve's shutdown —
					// unless stop's own deadline is what ended the read.
					w.gone.Store(true)
				}
				return
			}
		}
	}()
	return w
}

// stop ends the watch and puts back what it read. It returns only once the
// watching goroutine has, so the connection has a single reader again.
func (w *peerWatch) stop() {
	w.stopping.Store(true)
	_ = w.c.SetReadDeadline(time.Unix(1, 0))
	<-w.done
	if len(w.got) > 0 {
		w.c.head = append(w.c.head, w.got...)
	}
}

// streamedIn reports whether any response in a batch produces its payload as
// it goes, which is what decides between one write for the batch and a write
// per chunk.
func streamedIn(res []Response) bool {
	for _, r := range res {
		if r.streamed() {
			return true
		}
	}
	return false
}

// writeStreamedBatch writes responses in order, streaming the ones that
// stream.
//
// The order is HTTP/1.1's and is not negotiable: the nth response answers the
// nth request, so a streaming response holds the connection until it ends and
// the ones behind it wait. That is the cost of streaming on a protocol with
// no stream identifiers, and it is why HTTP/2 and HTTP/3 do not pay it.
// buf is the connection's scratch, the same one serveConn's buffered path
// reuses: streaming a batch is not the common case, but it runs on the same
// connection and there is no reason for it to allocate its own 4 KiB where
// the caller already has one.
//
// closed reports that a close-delimited body was written: a streamed body
// for an HTTP/1.0 client has no chunk framing (RFC 9112 §6.1 forbids
// Transfer-Encoding there) and, without a ContentLength, no end but EOF, so
// the caller must
// close the connection now — a response written after it would be read as
// more of the body.
//
// Consecutive buffered responses coalesce in buf and go out together, the
// same one-write-per-run the all-buffered path gets from [WriteBatch]: what
// forces a flush is a streamed body taking over the connection, not a
// response boundary. A streamed response's header rides the same flush as
// the buffered responses before it.
//
// A failure that strikes before any byte of the failing response went out —
// a refused header, a Body beside a Stream — is answered with a 500 here,
// the same answer the buffered path gives, and what was owed to the earlier
// requests is flushed first. Once part of a response is on the wire there is
// no such repair: a 500 appended to a half-written body would be read as
// body bytes, so a mid-stream write failure closes the connection with
// nothing more said.
//
// writeStreamedBatch runs in this order: prepare the keep-alive field and the
// 500 that answers a refused response; then each response of the batch —
// checked; a buffered one appended to buf; a streamed one has the buffered
// responses before it flushed with its own header, then its producer pulled at
// the socket's pace, chunk by chunk or delimited by the close, a panic in it
// closing the connection; and last, the buffered responses after the last
// streamed one, in one write.
func writeStreamedBatch(c *prefixConn, st *connState, cfg Config, requests []Request, res []Response, buf []byte) (_ []byte, closed bool, _ error) {
	var field [keepAliveFieldMax]byte
	keepAlive := appendKeepAlive(field[:0], cfg.IdleTimeout)
	buf = buf[:0]
	// fail answers response i's refusal with a 500, after flushing the
	// responses coalesced ahead of it — they were owed whatever happened to
	// this one. The connection closes after it, which the 500 says.
	fail := func(i int, err error) ([]byte, bool, error) {
		if len(buf) > 0 {
			if werr := writeAll(c, cfg, buf); werr != nil {
				return buf[:0], false, err
			}
		}
		var method []byte
		if i < len(requests) {
			method = requests[i].Method
		}
		_ = writeStatus(c, cfg, method, 500)
		return buf[:0], false, err
	}
	closeAfter := asksToClose(res)
	for i, r := range res {
		if err := r.check(); err != nil {
			return fail(i, err)
		}
		closing := i == len(res)-1 && (closeAfter || i < len(requests) && !requests[i].KeepAlive)
		if !r.streamed() {
			var oneReq []Request
			if i < len(requests) {
				oneReq = requests[i : i+1]
			}
			// Appended behind whatever is already pending, not written on its
			// own: WriteBatch returns nil on refusal and buf keeps the
			// responses gathered so far, which is what lets fail flush them.
			out, err := WriteBatch(buf, cfg, oneReq, []Response{r})
			if err != nil {
				reportPanic(cfg.logger(), err)
				return fail(i, err)
			}
			buf = out
			continue
		}

		var method []byte
		http10 := false
		if i < len(requests) {
			method = requests[i].Method
			http10 = requests[i].http10
		}
		status := r.Status
		if status == 0 {
			status = 200
		}
		noLength, noBody := noLengthAndBody(status, method)
		chunked, size, closeDelimited := streamedFraming(r, http10, noLength, noBody)
		if closeDelimited {
			// The 1.0 client gets the body to EOF, so this response is the
			// connection's last whatever the request's Connection field said.
			closing = true
		}
		out, err := appendStreamedHeader(buf, closing, chunked, size, keepAlive, r, dateFor(time.Now()))
		if err != nil {
			return fail(i, err)
		}
		buf = out
		if err := writeAll(c, cfg, buf); err != nil {
			return buf, false, err
		}
		buf = buf[:0]
		if closeDelimited && !noBody {
			// From here until the producer ends, a close cuts the body:
			// the connection's closer reads this (serveConn, closeAll) and
			// closes without close_notify.
			st.setBodyToEOF(true)
		}
		if noBody {
			// A HEAD or 204/304 response states no body and sends none —
			// the producer is never pulled, the same way the buffered path
			// never writes res.Body for these.
			continue
		}
		// The transport pulls: the producer runs at the socket's pace, and
		// what it holds is one batch rather than the whole answer.
		var werr error
		var sent int64
		watch := watchPeer(c, cfg.maxBuffer())
		// A producer that panics has already had its header and perhaps
		// some chunks sent: there is no status left to change, so the
		// connection closes with the body unterminated, which a client reads
		// as the failure it is.
		perr := pull(r.Stream, func(b sluice.Batch[[]byte]) bool {
			if watch.left() {
				werr = errPeerGone
				return false
			}
			if overLength(&sent, r.ContentLength, b.Items) {
				// The batch that would take the body past its declared
				// length is not written: the connection closes short of it,
				// or, if the length is already out, with no surplus behind.
				werr = errLengthMismatch
				return false
			}
			buf = buf[:0]
			for _, chunk := range b.Items {
				if chunked {
					buf = appendChunk(buf, chunk)
				} else {
					buf = append(buf, chunk...)
				}
			}
			if len(buf) == 0 {
				return true // an empty batch is cadence, not an end
			}
			if werr = writeAll(c, cfg, buf); werr != nil {
				return false
			}
			return true
		})
		watch.stop()
		if perr != nil {
			reportFailure(cfg.logger(), perr)
		}
		if werr == nil {
			werr = perr
		}
		if werr == nil && sent < r.ContentLength {
			// Short of the declared length: no end may be written, so the
			// connection closes and the client reads the body as incomplete
			// (RFC 9112 §6.3), the chunked one left unterminated.
			werr = errLengthMismatch
		}
		if werr != nil {
			return buf, false, werr
		}
		if closeDelimited {
			// The producer ended: the close that follows is the body's end,
			// and close_notify says so.
			st.setBodyToEOF(false)
			return buf, true, nil
		}
		if !chunked {
			buf = buf[:0] // the declared length was the end
			continue
		}
		if r.Trailers == nil {
			buf = appendLastChunk(buf[:0])
		} else {
			// The body is out and a refusal can no longer become a 500:
			// the chunked body is left unterminated and the connection
			// closes, as for a producer that panicked.
			fields, err := r.trailers()
			if err != nil {
				reportPanic(cfg.logger(), err)
				return buf, false, err
			}
			buf = appendLastChunkTrailers(buf[:0], fields)
		}
		if err := writeAll(c, cfg, buf); err != nil {
			return buf, false, err
		}
		buf = buf[:0]
	}
	// The buffered responses after the last streamed one, in one write.
	if len(buf) > 0 {
		if err := writeAll(c, cfg, buf); err != nil {
			return buf, false, err
		}
	}
	return buf, false, nil
}
