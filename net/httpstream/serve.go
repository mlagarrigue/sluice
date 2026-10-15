package httpstream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mlagarrigue/sluice"
)

// Handler answers a batch of requests with a batch of responses, positionally:
// the nth response answers the nth request.
//
// It is a batch-to-batch function rather than a stream-to-stream one, and the
// shape is the same one [parallel.Ordered] takes — a stage. That is not a
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
//     request differently. Other target forms are refused at the parser.
//   - **Header names keep the case the protocol sent.** HTTP/1.1 preserves
//     what the client wrote; HTTP/2 and HTTP/3 enforce lowercase on the wire.
//     A handler therefore matches names case-insensitively, which is what
//     [Request.Get] does — reading Headers directly by byte comparison is the
//     mistake this line exists to prevent.
//   - **The cookie arrives whole.** HTTP/2 and HTTP/3 let a client split its
//     Cookie into one field per pair for compression's sake; they reach the
//     handler joined again by "; " as one field, which is what HTTP/1.1
//     delivers, and count as one field under [Config.MaxHeaders].
//   - **The body is complete and bounded.** An element handed over is a whole
//     request: its body has arrived in full, under [Config.MaxBodyBytes].
//     There are no partial reads to finish and no framing left to check.
type Handler func(sluice.Batch[Request]) sluice.Batch[Response]

// Serve accepts connections and drives handle over each one until ctx is
// cancelled.
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
// Cancellation drains rather than cuts (docs/design/adr/0017). The listener
// closes at once, so nothing new is admitted. A connection with no request
// on it — parked between two keep-alive requests, or accepted and silent —
// is closed at once too: nothing is owed to it, and waiting for it instead
// means Serve returns when the last idle client happens to time out, which
// is [Config.IdleTimeout] after the cancellation rather than at it. A
// connection with a request in flight is left to finish it: the handler
// runs to its end, the response goes out whole with Connection: close, and
// the connection closes after it without reading another request. What
// bounds that is [Config.ShutdownGrace]: when it passes, every connection
// still open is closed under its handler and Serve returns without waiting
// for the goroutines still inside one — a handler that has not returned by
// then is the handler's bug, and holding the process up for it would turn
// the bug into a hang. An HTTP/2 connection admitted by Serve is closed at
// the cancellation, as before, until it learns to say GOAWAY first.
//
// What Serve speaks on a bare TCP listener is cleartext HTTP/1.1, with
// prior-knowledge HTTP/2 sniffed from the preface. Cleartext is kept for
// interoperability — a terminating proxy, a health check, local tooling —
// not recommended: anything that crosses a network boundary belongs behind
// TLS or on [ServeH3], whose transport encrypts by construction. Over a TLS
// listener the same preface sniff still picks the protocol — Serve does not
// read the negotiated ALPN — so a client that negotiated h2 and sends the
// preface is served HTTP/2, and one that did not is served HTTP/1.1.
func Serve(ctx context.Context, ln net.Listener, cfg Config, handle Handler) error {
	if handle == nil {
		panic("httpstream: Serve requires a handler")
	}
	// Taken before the defaults: a zero MaxConcurrentStreams is HTTP/2's own
	// default of 100, not the 64 Config's default is sized to HTTP/3 by.
	h2Streams := cfg.MaxConcurrentStreams
	cfg = cfg.withDefaults()
	h2cfg := h2ConfigOf(cfg, h2Streams)

	var wg sync.WaitGroup
	live := newConnSet()
	stop := make(chan struct{})
	var once sync.Once
	shutdown := func() {
		once.Do(func() {
			close(stop)
			_ = ln.Close()
			live.drain()
		})
	}
	defer func() {
		shutdown()
		// The drain's end: every connection finished, or the grace passed
		// and what is left is closed under its handler. The wait after
		// that is not resumed — see the note on cancellation above.
		finished := make(chan struct{})
		go func() { wg.Wait(); close(finished) }()
		grace := time.NewTimer(cfg.ShutdownGrace)
		defer grace.Stop()
		select {
		case <-finished:
		case <-grace.C:
			live.closeAll()
		}
	}()
	go func() {
		select {
		case <-ctx.Done():
			shutdown()
		case <-stop:
		}
	}()

	var tempDelay time.Duration // how long to wait after a temporary Accept failure
	for {
		c, err := ln.Accept()
		if err != nil {
			select {
			case <-stop:
				return nil // asked to stop: not a failure
			default:
			}
			if ne, ok := errors.AsType[net.Error](err); ok && ne.Temporary() { //nolint:staticcheck // SA1019: Temporary is what Accept still reports EMFILE and ECONNABORTED through, as net/http reads it
				// The same shape as the QUIC read loop's readBackoff and
				// deliberately not shared with it: four lines are cheaper
				// than a dependency between the two packages.
				tempDelay = min(max(tempDelay*2, 5*time.Millisecond), time.Second)
				t := time.NewTimer(tempDelay)
				select {
				case <-stop:
					t.Stop()
					return nil
				case <-t.C:
				}
				continue
			}
			return fmt.Errorf("httpstream: accepting: %w", err)
		}
		tempDelay = 0
		st := live.add(c)
		if st == nil {
			// The shutdown ran between Accept and here, so this connection
			// would never be closed by it.
			_ = c.Close()
			continue
		}
		wg.Go(func() {
			defer func() { live.remove(c); _ = c.Close() }()
			serveAny(ctx, c, st, cfg, h2cfg, handle)
		})
	}
}

// connSet is the connections Serve has accepted and not yet finished with,
// each with where it stands between two requests. It exists for the
// shutdown: close the ones that owe nothing, then, when the grace has
// passed, the rest.
type connSet struct {
	mu     sync.Mutex
	conns  map[net.Conn]*connState
	closed bool
	// draining is cancelled when the shutdown begins; every connection's state
	// shares it, which is how the request reader learns not to read another
	// request and how an HTTP/2 connection learns to say GOAWAY. A context
	// rather than a channel so the latter is a [context.AfterFunc], not a
	// goroutine parked for the life of every connection.
	draining   context.Context
	beginDrain context.CancelFunc
}

func newConnSet() *connSet {
	draining, begin := context.WithCancel(context.Background())
	return &connSet{conns: map[net.Conn]*connState{}, draining: draining, beginDrain: begin}
}

// connState is what the shutdown needs to know about one connection: whether
// a request is in flight on it. It is written by the goroutine serving the
// connection and read by the shutdown, hence the atomic.
type connState struct {
	phase atomic.Int32
	drain context.Context
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

// add registers a connection, returning nil if the set has already been
// closed and this one therefore has to be closed by its caller.
func (s *connSet) add(c net.Conn) *connState {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	st := &connState{drain: s.draining}
	s.conns[c] = st
	return st
}

func (s *connSet) remove(c net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.conns, c)
}

// drain begins the shutdown: it refuses new connections, tells the readers
// not to take another request, and closes every connection that has none in
// flight. The order matters against the reader, which marks itself idle
// before it looks at draining: a reader that marked itself after this scan
// sees the context cancelled; one that marked itself before is closed here.
// Either way no idle connection outlives the call. An HTTP/2 connection with
// a stream open is told instead (see serveH2Conn): the cancellation runs its
// GOAWAY, and it closes itself once the stream is answered.
func (s *connSet) drain() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.beginDrain()
	for c, st := range s.conns {
		if st.phase.Load() != connBusy {
			_ = c.Close()
		}
	}
}

// closeAll closes what the grace did not drain. The map is kept: remove
// still runs for each connection when its goroutine ends.
func (s *connSet) closeAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for c := range s.conns {
		_ = c.Close()
	}
}

// serveConn runs one connection to its end, answering whatever it can and
// closing when it cannot.
//
// c is a [prefixConn] even when nothing was peeked: it is where a streamed
// response's peer watch (see watchPeer) puts back what it read ahead. st is
// the shutdown's view of the connection, nil when nothing is watching it.
func serveConn(c *prefixConn, st *connState, cfg Config, handle Handler) {
	src := newRequestReader(c, st, cfg)
	// Reused for every batch on this connection, growing toward whatever the
	// largest one needed and never shrinking back — the ordinary trade of a
	// reused buffer against a per-batch allocation, paid once per connection
	// rather than once per batch.
	out := make([]byte, 0, 4<<10)

	stopped := false
	src.Stream()(func(b sluice.Batch[Request]) bool {
		res, err := apply(handle, b)
		if err != nil {
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
			if out, closed, werr = writeStreamedBatch(c, cfg, b.Items, res, out); werr != nil || closed || asksToClose(res) {
				// closed without error is a close-delimited body: EOF is its
				// end marker, so the close *is* the last byte of the answer.
				stopped = true
				return false
			}
			return true
		}
		var werr error
		if out, werr = WriteBatch(out[:0], b.Items, res); werr != nil {
			// A response this package refuses to put on the wire — an
			// injected header, a framing header the caller set. The caller's
			// bug, answered as one rather than written out.
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

	if stopped {
		return
	}
	// The read side ended. A clean close needs no answer; a refused request
	// gets the status its refusal maps to, on the connection that is about to
	// close anyway.
	if err := src.Err(); err != nil && !errors.Is(err, io.EOF) {
		if status := statusFor(err); status != 0 {
			_ = writeStatus(c, cfg, nil, status)
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
// wrong length would answer the wrong caller.
func apply(handle Handler, b sluice.Batch[Request]) (res []Response, err error) {
	defer func() {
		if r := recover(); r != nil {
			// A panic in the handler is the caller's bug and must not take
			// the process down; it becomes a 500 on this connection, the same
			// bargain [web.Recover] makes.
			err = fmt.Errorf("httpstream: the handler panicked: %v", r)
		}
	}()
	out := handle(b)
	if len(out.Items) != b.Len() {
		return nil, fmt.Errorf("httpstream: the handler answered %d of %d requests; "+
			"a pipelined connection correlates by position, so a dropped or added "+
			"element answers the wrong caller", len(out.Items), b.Len())
	}
	return out.Items, nil
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
	case errors.Is(err, ErrTooLarge):
		return 413
	case errors.Is(err, ErrMalformed):
		return 400
	}
	return 0
}

func writeStatus(c net.Conn, cfg Config, method []byte, status int) error {
	// closing is always true here: every caller is about to end the
	// connection, so appendResponse writes the Connection: close itself
	// rather than have it duplicated as a caller-supplied header.
	buf, err := appendResponse(nil, method, true, Response{Status: status})
	if err != nil {
		return err
	}
	return writeAll(c, cfg, buf)
}

func writeAll(c net.Conn, cfg Config, buf []byte) error {
	if err := c.SetWriteDeadline(time.Now().Add(cfg.WriteTimeout)); err != nil {
		return err
	}
	_, err := c.Write(buf)
	return err
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
func serveAny(ctx context.Context, c net.Conn, st *connState, cfg Config, h2cfg H2Config, handle Handler) {
	peek, isH2, err := sniffPreface(c, cfg)
	if err != nil {
		return // silent past IdleTimeout, or gone: nothing to hand on
	}
	if isH2 {
		// Busy until the reader has seen the first batch: a preface is
		// followed by a request, and the reader keeps the phase from there.
		st.set(connBusy)
		_ = serveH2Conn(ctx, prefixed(c, peek), st, h2cfg, handle)
		return
	}
	// The peeked bytes are the start of a request: busy from here, and the
	// reader takes over the phase from its first read.
	st.set(connBusy)
	serveConn(&prefixConn{Conn: c, head: peek}, st, cfg, handle)
}

// h2ConfigOf is the [H2Config] a [Serve] connection that turns out to be
// HTTP/2 runs under. streams is Config.MaxConcurrentStreams as the caller set
// it, zero included.
//
// MaxFrameSize is left to its default on purpose. It is a transport bound
// with a protocol floor of 16384 (RFC 9113 §4.2/§6.5.2), not a body bound:
// wiring MaxBodyBytes into it made a small body limit advertise an illegal
// SETTINGS value, and a large one hand the peer a megabyte-sized frame
// ceiling to amplify SETTINGS processing against. Bodies are bounded by
// MaxBodyBytes across frames, however large each frame is allowed to be.
func h2ConfigOf(cfg Config, streams int) H2Config {
	return H2Config{
		MaxBodyBytes:         cfg.MaxBodyBytes,
		MaxHeaderBlockBytes:  cfg.MaxHeaderBytes,
		MaxHeaders:           cfg.MaxHeaders,
		MaxConcurrentStreams: streams,
		IdleTimeout:          cfg.IdleTimeout,
		ReadTimeout:          cfg.ReadTimeout,
		WriteTimeout:         cfg.WriteTimeout,
	}
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
	if err := c.SetReadDeadline(time.Now().Add(cfg.IdleTimeout)); err != nil {
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
}

func prefixed(c net.Conn, head []byte) net.Conn {
	if len(head) == 0 {
		return c
	}
	return &prefixConn{Conn: c, head: head}
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

// errPeerGone ends a streamed HTTP/1.1 response whose client closed the
// connection, or whose connection was closed under it by Serve's shutdown.
var errPeerGone = errors.New("httpstream: the client left during a streamed response")

// peerWatch reads the connection while a streamed response holds it, which is
// how a producer learns the client left without a write failing first.
//
// Nothing else reads an HTTP/1.1 connection while a response streams — the
// serving goroutine is the producer's — so a client that closes is invisible
// until a write fails, and a producer that only yields empty batches never
// writes. The watch is one goroutine and one blocked read per streamed
// response, not per batch: the producer's check is an atomic load.
//
// What it reads is not lost. A client that pipelines its next request while
// this one streams has those bytes kept, up to limit, and put back in front
// of the connection when the watch stops, for the request reader to find
// where it would have read them. Past limit the watch stops reading and the
// client is not watched for the rest of this response.
type peerWatch struct {
	c        *prefixConn
	gone     atomic.Bool
	stopping atomic.Bool
	done     chan struct{}
	got      []byte
}

func watchPeer(c *prefixConn, limit int) *peerWatch {
	w := &peerWatch{c: c, done: make(chan struct{})}
	// No deadline while watching: the request reader set one for the request
	// it read, and that clock says nothing about how long a response may
	// take to produce. stop sets it to the past to end the read, and the
	// request reader sets its own again before its next read.
	if err := c.SetReadDeadline(time.Time{}); err != nil {
		w.gone.Store(true) // a connection that refuses a deadline is closed
		close(w.done)
		return w
	}
	go func() {
		defer close(w.done)
		var scratch [512]byte
		for len(w.got) < limit {
			// c.Conn, not c: c.head belongs to the request reader, and this
			// goroutine only appends to it once stop has waited for it.
			n, err := c.Conn.Read(scratch[:min(len(scratch), limit-len(w.got))])
			w.got = append(w.got, scratch[:n]...)
			if err != nil {
				// EOF, a reset, or the socket closed by Serve's shutdown —
				// unless stop's own deadline is what ended the read.
				if !w.stopping.Load() {
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
// Transfer-Encoding there) and therefore no end but EOF, so the caller must
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
func writeStreamedBatch(c *prefixConn, cfg Config, requests []Request, res []Response, buf []byte) (_ []byte, closed bool, _ error) {
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
			out, err := WriteBatch(buf, oneReq, []Response{r})
			if err != nil {
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
		_, noBody := noLengthAndBody(status, method)
		closeDelimited := http10 && !noBody
		if closeDelimited {
			// The 1.0 client gets the body to EOF, so this response is the
			// connection's last whatever the request's Connection field said.
			closing = true
		}
		out, err := appendStreamedHeader(buf, closing, http10, noBody, r)
		if err != nil {
			return fail(i, err)
		}
		buf = out
		if err := writeAll(c, cfg, buf); err != nil {
			return buf, false, err
		}
		buf = buf[:0]
		if noBody {
			// A HEAD or 204/304 response states no body and sends none —
			// the producer is never pulled, the same way the buffered path
			// never writes res.Body for these.
			continue
		}
		// The transport pulls: the producer runs at the socket's pace, and
		// what it holds is one batch rather than the whole answer.
		var werr error
		watch := watchPeer(c, cfg.maxBuffer())
		// A producer that panics has already had its header and perhaps
		// some chunks sent: there is no status left to change, so the
		// connection closes with the body unterminated, which a client reads
		// as the failure it is.
		perr := pull(r.Stream, func(b sluice.Batch[[]byte]) bool {
			// Asked on every batch, empty ones included: an empty batch
			// writes nothing, so no write failure would ever tell a producer
			// of cadence ticks that the client left.
			if watch.gone.Load() {
				werr = errPeerGone
				return false
			}
			buf = buf[:0]
			for _, chunk := range b.Items {
				if closeDelimited {
					buf = append(buf, chunk...)
				} else {
					buf = appendChunk(buf, chunk)
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
		if werr == nil {
			werr = perr
		}
		if werr != nil {
			return buf, false, werr
		}
		if closeDelimited {
			return buf, true, nil
		}
		buf = appendLastChunk(buf[:0])
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
