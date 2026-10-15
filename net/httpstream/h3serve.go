package httpstream

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/quic"
)

// ServeH3 serves HTTP/3 over a QUIC connection, with the same [Handler] the
// other two protocols take.
//
// This is where the three meet. A batch of requests goes in and a batch of
// responses comes out, and the pipeline behind it never learns whether the
// bytes arrived pipelined on one HTTP/1.1 connection, multiplexed over an
// HTTP/2 one, or in a QUIC datagram. The transport is a stage.
//
// The batch comes from [quic.Conn.OnStreamFrames]: a datagram's stream frames
// are handed over together, so requests that arrived together are served
// together, with nothing waited for.
//
// # Two goroutines, and why the second one exists
//
// [quic.Conn.OnStreamFrames] runs on the connection's read goroutine — the one
// that decrypts packets, drives the handshake and files stream data. What runs
// there is assembly and nothing else: the frames of a datagram are folded into
// requests, and the completed ones are handed to a **responder** goroutine
// that runs the handler, writes the answers and pulls the streamed bodies.
//
// Everything about that split is forced. Running the handler on the read
// goroutine stops every packet from being processed while it runs, CRYPTO
// included; draining a streamed body there stops them for as long as the
// producer takes, and a producer that pauses trips the read loop's own
// deadline and tears the connection down. It is the same argument the HTTP/2
// side makes, and this transport makes it more sharply because the read loop
// is also the handshake.
//
// The hand-off is one bounded channel and the bound is
// [Config.MaxConcurrentStreams] batches. When it fills, the read callback
// blocks, the read loop stops, and the pressure arrives back at the peer
// through QUIC's own flow control — which is what a transport's flow control
// is for. Nothing is dropped, because a dropped request is a caller with no
// answer.
//
// # Concurrency, stated rather than guarded
//
// [h3Assembler] is not safe for concurrent use and is touched by the read
// goroutine only. Streams are taken from [quic.Conn.Stream], which is safe
// for concurrent use, and each response stream is written by exactly one
// writer goroutine — writes on different streams may interleave, which
// [quic.Stream.Write]'s connection lock makes safe, while one goroutine per
// stream keeps that stream's own bytes ordered.
//
// It returns when the connection ends. Everything it does not serve is the
// connection's own limitation and is documented there — the transport
// retransmits, enforces flow control and interoperates with quic-go, whose
// HTTP/3 client reads this layer's responses too (the interop/ module, in
// CI) — and the server side is one connection per packet socket.
//
// The connection must have negotiated "h3" (RFC 9114 §3.1): the server's
// [crypto/tls.Config] announces it in NextProtos, and a connection that
// settled on any other protocol is refused and closed rather than served.
func ServeH3(ctx context.Context, c *quic.Conn, cfg Config, handle Handler) error {
	if handle == nil {
		panic("httpstream: ServeH3 requires a handler")
	}
	cfg = cfg.withDefaults()

	// The hand-off to the responder below is provably non-blocking only while
	// the transport admits no more request streams than the channel can hold,
	// and h3MaxServeStreams is where that proof is capped. A connection whose
	// announced stream limit exceeds it is refused outright: serving it would
	// carry a latent read-loop deadlock instead of a visible error. The
	// connection is closed with the refusal, as on every other way out of
	// ServeH3: left open, the client would wait on a server nobody runs.
	if lim := c.LocalParameters().InitialMaxStreamsBidi; lim > h3MaxServeStreams {
		_ = c.CloseWithError(h3InternalError)
		return fmt.Errorf(
			"httpstream: InitialMaxStreamsBidi %d is over the %d ServeH3 can serve without deadlock",
			lim, h3MaxServeStreams)
	}
	// RFC 9114 §3.1: HTTP/3 is the protocol identified by the ALPN token
	// "h3". A peer that settled on anything else is waiting for that other
	// protocol and would read this layer's SETTINGS and frames as garbage;
	// refused and closed, like the limit above.
	if proto := c.ConnectionState().NegotiatedProtocol; proto != "h3" {
		_ = c.CloseWithError(h3InternalError)
		return fmt.Errorf("httpstream: ServeH3 on a connection that negotiated %q, not h3", proto)
	}
	a := newH3Assembler(cfg)
	// Manual flow-control credit closes the security report's M-3 residue:
	// an auto-crediting window reopens for bytes this layer still retains,
	// so retention is bounded only by this layer's own caps instead of by
	// the windows. Under manual credit a retained byte keeps its window
	// debt until released — which also means a request body must fit the
	// *initial* stream window, since a body is retained until the handler
	// takes it. So the mode turns on only when the connection's announced
	// windows cover the layer's admission bounds (MaxBodyBytes per stream,
	// 4× across the connection, headroom for headers); on smaller windows
	// the transport keeps crediting on delivery and the caps bound the
	// exposure, exactly as before. [Config.TransportParameters] produces
	// windows this check accepts.
	if p := c.LocalParameters(); p.InitialMaxStreamDataBidiRemote >= uint64(cfg.MaxBodyBytes+cfg.MaxHeaderBytes)+h3FrameSlack && //nolint:gosec // G115: positive config, set by withDefaults
		p.InitialMaxData >= uint64(4*cfg.MaxBodyBytes+cfg.MaxHeaderBytes)+h3FrameSlack { //nolint:gosec // G115: positive config, set by withDefaults
		c.SetManualCredit(true)
		a.Credit = func(id, n uint64) { h3ReleaseStreamBytes(c, id, n) }
	}

	// RFC 9114 §6.2.1: each endpoint opens a control stream and sends
	// SETTINGS as its first frame. A peer is entitled to wait for it, so a
	// server that never sends one is a server a conformant client hangs on.
	// Stream 3 is the first server-initiated unidirectional identifier.
	control := c.Stream(3)
	if _, err := control.Write(appendH3Control(nil, uint64(cfg.MaxHeaderBytes))); err != nil { //nolint:gosec // G115: positive config, set by withDefaults
		// Closed on the way out, like the two refusals above. The one
		// failure a live connection produces here is the client's
		// STOP_SENDING on the control stream landing between Stream(3)
		// and this write: the §6.2.1 error the callback below names,
		// which used to leave the connection open with nobody serving it.
		if code, reset := control.WasReset(); reset {
			_ = c.CloseWithError(h3ClosedCriticalStream)
			return controlStopped(code)
		}
		_ = c.CloseWithError(h3InternalError)
		return err
	}

	// A cancelled server drains (ADR 0017), the way RFC 9114 §5.2 has it:
	// GOAWAY on the control stream naming the first request stream this
	// connection rejects — the one after the highest it admitted, decided
	// under the assembler's lock so no stream is both admitted and named —
	// then every admitted request is answered, then the connection closes
	// H3_NO_ERROR (or §8.1's grease) once the last answer is acknowledged,
	// which is what [quic.Conn.ShutdownWithError] waits for: a close sent at
	// once could reach the client before the response it follows. A request
	// stream opened from the named identifier on is rejected on arrival
	// (REQUEST_REJECTED, see the assembler's admit). Config.ShutdownGrace
	// bounds the whole: when it passes, what is still open is closed under
	// its handler and ServeH3 returns, as Serve does. The goroutine this
	// runs on lives for the drain only — nothing is parked for the life of
	// the connection.
	//
	// The grace is a timer armed at the cancellation, not a context scoped
	// to the drain: the wait at the bottom of ServeH3 reads it too, after
	// the connection has ended, and the drain can end — client gone, grace
	// passed — before the handler has returned.
	drained := make(chan struct{})
	var finishOnce sync.Once
	a.Finish = func() { finishOnce.Do(func() { close(drained) }) }
	graceOver := make(chan struct{})
	var graceTimer atomic.Pointer[time.Timer]
	stop := context.AfterFunc(ctx, func() {
		deadline := time.Now().Add(cfg.ShutdownGrace)
		graceTimer.Store(time.AfterFunc(cfg.ShutdownGrace, func() { close(graceOver) }))
		from := a.beginDrain()
		if _, err := control.Write(appendH3Frame(nil, h3GoAway, quic.AppendVarint(nil, from))); err != nil {
			_ = c.CloseWithError(h3GracefulCode())
			return
		}
		if a.inFlight.Load() == 0 {
			a.Finish()
		}
		select {
		case <-drained:
			// The grace left bounds the acknowledgement too; past it the
			// connection closes anyway, as Shutdown promises.
			grace, cancel := context.WithDeadline(context.Background(), deadline)
			defer cancel()
			_ = c.ShutdownWithError(grace, h3GracefulCode())
		case <-graceOver:
			_ = c.CloseWithError(h3GracefulCode())
		case <-c.Done():
		}
	})
	defer func() {
		stop()
		// Stopped once ServeH3 has returned, when nobody reads it; one
		// that fires anyway closes a channel nobody waits on.
		if t := graceTimer.Load(); t != nil {
			t.Stop()
		}
	}()

	// The channel is never closed, and both ends select on the connection
	// instead. Closing it would be the natural thing and it is a panic waiting
	// to happen: the callback runs on the connection's read goroutine, which
	// can still be inside a datagram when the connection ends, and a send on a
	// closed channel takes the process down. Selecting on Done gives both ends
	// a way out that does not depend on which of them notices first.
	//
	// The capacity is the transport's own stream limit, and that it can never
	// block matters more than it looks: the read goroutine sending here is
	// the goroutine that delivers the flow-control credit a stalled response
	// is waiting for, so a read loop blocked on the responder would be a
	// deadlock, not a slowdown. The bound holds because a request occupies a
	// stream slot from admission until its response retires the stream — so
	// batches queued here can never outnumber the streams the transport
	// admits. The same invariant, with the same proof, sizes the HTTP/2
	// hand-off (ADR 0004). The cap guards a caller who announced an
	// astronomical stream limit; past it, admission control is theirs.
	streamLimit := min(c.LocalParameters().InitialMaxStreamsBidi, h3MaxServeStreams)
	in := make(chan h3Batch, max(uint64(cfg.MaxConcurrentStreams), streamLimit)) //nolint:gosec // G115: positive config, set by withDefaults
	answered := make(chan struct{})
	// One responder runs the handler, batch by batch, in order — handlers
	// are not asked to be concurrency-safe. The *writes* fan out: each
	// response leaves on its own goroutine, so a response stalled on
	// stream flow control — a client withholding MAX_STREAM_DATA for one
	// stream — stalls that stream alone, bounded by the idle timeout,
	// instead of every answer behind it on the connection (the
	// head-of-line block this responder used to accept). Writer goroutines
	// are bounded by the same invariant that sizes the channel: one
	// unfinished response per admitted stream. quic.Stream.Write is safe
	// across streams (the connection lock serialises packets), and one
	// goroutine per stream keeps that stream's own writes ordered.
	var writers sync.WaitGroup
	go func() {
		defer close(answered)
		for {
			select {
			case b := <-in:
				answerH3(c, b, handle, cfg.WriteTimeout, &writers)
			case <-c.Done():
				return
			}
		}
	}()

	c.OnStreamFrames(func(frames []quic.Frame) error {
		// RFC 9114 §6.2.1: "the receiver MUST NOT request that the sender
		// close the control stream", and a closed one is a connection error
		// of type H3_CLOSED_CRITICAL_STREAM. The transport has answered the
		// STOP_SENDING already (RESET_STREAM, RFC 9000 §3.5) and hands it up
		// in the datagram that carried it; the connection ends before any
		// request beside it is admitted. A STOP_SENDING on a request stream
		// needs nothing here: the transport abandoned the response, whose
		// writer fails on its next write.
		for _, f := range frames {
			if f.Type == quic.FrameStopSending && f.StreamID == control.ID() {
				code, _ := control.WasReset()
				_ = c.CloseWithError(h3ClosedCriticalStream)
				return controlStopped(code)
			}
		}
		// RESET_STREAM is the assembler's to fold in, inside Frames' own
		// loop: a separate pass here used to run every Forget before the
		// datagram's STREAM deltas, which resurrected assembly state for
		// streams the transport had already abandoned — entries that could
		// never complete, each burning an admission slot for good.
		batch, ids, failed, err := a.Frames(frames)
		if err != nil {
			// A connection error carries the code RFC 9114 §8 assigns it;
			// anything else ends the connection through the transport's own
			// teardown when this return reaches it.
			if ce, ok := errors.AsType[h3ConnectionError](err); ok {
				_ = c.CloseWithError(ce.Code)
			}
			return err
		}
		for _, se := range failed {
			// One stream's refusal, on that stream. The connection and
			// every other request the datagram carried are untouched, which
			// is the whole of what a multiplexed transport offers over a
			// sequential one.
			s := c.Stream(se.StreamID)
			if se.Status != 0 {
				// A request over a bound with a status: RFC 9114 §4.1 has
				// the server stop reading with H3_NO_ERROR, send the
				// complete response and close its side cleanly — a client
				// behind a proxy acts on a 431 or 413 where a bare reset
				// tells it nothing. STOP_SENDING leaves the read loop now;
				// the head leaves on a writer goroutine like any response,
				// since a Write waits on the peer's credit.
				if err := s.StopSending(h3NoError); err != nil {
					return err
				}
				writers.Add(1)
				go func(id uint64, status int, fieldSection uint64) {
					defer writers.Done()
					defer a.retire()
					writeH3Response(c, id, Response{Status: status}, nil, fieldSection, cfg.WriteTimeout, dateFor(time.Now()), func() {})
				}(se.StreamID, se.Status, a.peerMaxFieldSection())
				continue
			}
			// RESET_STREAM abandons the answer and STOP_SENDING tells the
			// peer to stop supplying the request (§4.1.1 uses both).
			if err := s.Reset(se.Code); err != nil {
				return err
			}
			if err := s.StopSending(se.Code); err != nil {
				return err
			}
		}
		if batch.Len() == 0 {
			return nil
		}
		// The batch slice is the assembler's and is reused on the next
		// datagram; its elements are not, because each request owns the
		// bytes it was assembled from. Copying the slice — not the requests —
		// is what makes the hand-off safe, and it is one small copy per
		// datagram rather than one per request.
		select {
		case in <- h3Batch{
			reqs: append([]Request(nil), batch.Items...),
			ids:  append([]uint64(nil), ids...),
			// The peer's MAX_FIELD_SECTION_SIZE, snapshotted here because
			// the assembler belongs to this goroutine and the responder
			// must not reach into it.
			fieldSection: a.peerMaxFieldSection(),
			credit:       a.Credit,
			retire:       a.retire,
		}:
		case <-c.Done():
		}
		return nil
	})

	<-c.Done()
	// The responder exits at its next select, and every writer that is
	// writing exits once the connection is done: its writes fail. A writer
	// inside a streamed producer is not waited for — the producer is the
	// caller's code, and one blocked on something other than its yield
	// would hold this return forever. It learns the connection is gone at
	// its next yield, which returns false (see the package documentation),
	// and its goroutine ends then. A handler that has not returned is the
	// same case one level up: after a cancellation the wait for it ends
	// with the grace, as Serve's does (ADR 0017 §2), and the goroutine
	// below ends when the handler does. A connection the client ended is
	// waited for in full — there is no grace to count from until the
	// server is cancelled, which arms one.
	finished := make(chan struct{})
	go func() {
		<-answered
		writers.Wait()
		close(finished)
	}()
	select {
	case <-finished:
	case <-graceOver:
	}
	return h3CloseErr(c.Err())
}

// controlStopped is the connection error for a client that asked the control
// stream to stop (RFC 9114 §6.2.1): a critical stream closed, whenever the
// refusal is noticed.
func controlStopped(code uint64) error {
	return h3ConnectionError{Code: h3ClosedCriticalStream, Err: fmt.Errorf(
		"%w: the client asked the control stream to stop (STOP_SENDING %#x)", ErrH3Protocol, code)}
}

// h3CloseErr is what ServeH3 reports once the connection ended: nil for
// the ends an HTTP/3 server treats as the client leaving — its H3_NO_ERROR
// (RFC 9114 §8.1), its transport NO_ERROR, or silence past the idle timeout
// — and the transport's cause for everything else. The transport reports
// every peer close as a [quic.ConnectionError] because it does not know
// what the codes mean; this is the layer that does.
func h3CloseErr(err error) error {
	if errors.Is(err, quic.ErrIdleTimeout) {
		return nil
	}
	if ce, ok := errors.AsType[*quic.ConnectionError](err); ok {
		if (ce.Application && ce.Code == h3NoError) || (!ce.Application && ce.Code == 0) {
			return nil
		}
	}
	return err
}

// h3ReleaseStreamBytes is the manual-credit release, a variable so a test
// can count what this layer gives back to the transport.
var h3ReleaseStreamBytes = (*quic.Conn).ReleaseStreamBytes

// h3Batch is what the read goroutine hands the responder: the requests one
// datagram completed, and the stream each arrived on. HTTP/3 correlates by
// stream rather than by order, which is the one thing it does not inherit
// from HTTP/1.1's pipelining.
type h3Batch struct {
	reqs []Request
	ids  []uint64
	// fieldSection is the peer's SETTINGS_MAX_FIELD_SECTION_SIZE at hand-off
	// time, zero for unbounded; the responder holds every response's field
	// section under it (RFC 9114 §4.2.2).
	fieldSection uint64
	// credit is the assembler's manual-credit release, nil when the
	// transport credits on delivery. Releasing a body in that mode as well
	// credited every byte twice, so MAX_DATA ran ahead of what this server
	// had consumed without bound.
	credit func(streamID, n uint64)
	// retire, when set, is called once per request when its response has
	// left — or its writer has given up — which is how a draining connection
	// learns it owes nothing more.
	retire func()
}

// TransportParameters returns QUIC transport parameters sized to this
// configuration's own admission bounds, for the connection ServeH3 will
// serve: stream windows that fit a full request (MaxBodyBytes plus header
// room), a connection window that fits the 4×MaxBodyBytes aggregate
// assembly cap, the stream count ServeH3 admits, and [Config.IdleTimeout]
// as the connection's max_idle_timeout — HTTP/3 has no idle clock of its
// own above the transport's, so this is the one place the field reaches an
// HTTP/3 connection, and a zero field is the 30s every protocol defaults
// to. Serving a connection built with these lets ServeH3 run the transport
// under manual flow-control credit, where a peer's window only reopens for
// bytes this server has actually stopped retaining — the strongest memory
// bound the layer offers. Any other parameters work too; below these
// windows ServeH3 falls back to delivery-time crediting, bounded by the
// assembler's caps, and the idle timeout is whatever those parameters say.
func (c Config) TransportParameters() quic.TransportParameters {
	c = c.withDefaults()
	p := quic.DefaultParameters()
	p.MaxIdleTimeout = c.IdleTimeout
	p.InitialMaxStreamDataBidiRemote = uint64(c.MaxBodyBytes+c.MaxHeaderBytes) + h3FrameSlack //nolint:gosec // G115: positive config
	p.InitialMaxData = uint64(4*c.MaxBodyBytes+c.MaxHeaderBytes) + h3FrameSlack               //nolint:gosec // G115: positive config
	p.InitialMaxStreamsBidi = min(uint64(c.MaxConcurrentStreams), h3MaxServeStreams)          //nolint:gosec // G115: positive config
	return p
}

// h3MaxServeStreams caps the stream limit ServeH3 will serve: the responder
// hand-off channel holds this many batches, which is what makes the read
// loop's send on it provably non-blocking (see the capacity comment in
// ServeH3). Past it the proof fails, so ServeH3 refuses the connection.
const h3MaxServeStreams = 4096

// h3GracefulCode is H3_NO_ERROR — except, one close in eight, a code from the
// reserved 0x1f*N+0x21 space: RFC 9114 §8.1 says implementations SHOULD
// grease their graceful closes with some probability, so a peer that fails
// §9's rule — unknown error codes are to be treated as H3_NO_ERROR — is
// found out early.
func h3GracefulCode() uint64 {
	if rand.Uint64N(8) == 0 { //nolint:gosec // G404: grease only has to vary, not be secret
		return 0x21 + 0x1f*rand.Uint64N(1<<16) //nolint:gosec // G404: grease only has to vary, not be secret
	}
	return h3NoError
}

// answerH3 runs the handler over one batch and writes the answers back on the
// streams their requests arrived on.
//
// A response that cannot be framed resets its own stream rather than failing
// the connection: the caller's bug costs the caller's request.
func answerH3(c *quic.Conn, b h3Batch, handle Handler, writeTimeout time.Duration, writers *sync.WaitGroup) {
	res, err := apply(handle, sluice.Batch[Request]{Items: b.reqs})
	if err != nil {
		// The handler panicked, or answered a number of requests that does
		// not match what it was given. Every request of the batch is answered
		// 500 and the connection carries on.
		res = make([]Response, len(b.reqs))
		for i := range res {
			res[i] = Response{Status: 500}
		}
	}

	// The request bodies stop being retained by this layer the moment the
	// handler returns — the release half of the manual credit ServeH3 runs
	// the transport under. Held across the handler call deliberately: a
	// slow handler holding a batch of bodies is exactly the memory the
	// window should not re-open for.
	if b.credit != nil {
		for i, id := range b.ids {
			if n := uint64(len(b.reqs[i].Body)); n > 0 {
				b.credit(id, n)
			}
		}
	}

	date := dateFor(time.Now())
	for i, id := range b.ids {
		writers.Add(1)
		go func(id uint64, r Response, method []byte) {
			detached := false
			defer func() {
				if !detached {
					writers.Done()
				}
				// Retired at the goroutine's very end, a streamed producer's
				// pull included: the drain waits for the body it is writing,
				// bounded by the grace, where ServeH3's own return does not.
				if b.retire != nil {
					b.retire()
				}
			}()
			writeH3Response(c, id, r, method, b.fieldSection, writeTimeout, date, func() {
				detached = true
				writers.Done()
			})
		}(id, res[i], b.reqs[i].Method)
	}
}

// h3ScratchPool recycles the per-response framing scratch across writer
// goroutines; each writer holds one for exactly one response.
var h3ScratchPool = sync.Pool{New: func() any { return &h3Scratch{} }}

// writeH3Response frames and writes one response on its own goroutine — the
// lift of the old single-writer design's head-of-line block. A write stalled
// on this stream's flow control stalls this goroutine alone; packets keep
// arriving on the read loop and every other response keeps leaving. Each
// write has writeTimeout to complete — [Config.WriteTimeout], the bound the
// HTTP/2 writer puts on a job — so a client that keeps the connection alive
// but never grants credit holds the goroutine, and a streamed producer's
// body, for that long and no longer: the stream is then reset and the
// goroutine returned.
//
// detach is called once the response's own writes are done and a streamed
// producer is about to be pulled: from there on the goroutine runs the
// caller's code, which ServeH3 does not wait for.
func writeH3Response(c *quic.Conn, id uint64, r Response, method []byte, fieldSection uint64, writeTimeout time.Duration, date []byte, detach func()) {
	s := c.Stream(id)
	sc := h3ScratchPool.Get().(*h3Scratch)
	defer h3ScratchPool.Put(sc)
	head, err := appendH3Response(sc.head[:0], r, method, fieldSection, sc, date)
	if err != nil {
		if head, err = appendH3Response(sc.head[:0], Response{Status: 500}, method, fieldSection, sc, date); err != nil {
			// Only reachable when the peer's MAX_FIELD_SECTION_SIZE is under
			// a bare 500's own (:status, date and content-length, ~155
			// bytes): no
			// answer fits what the client said it accepts, so the stream is
			// reset rather than sent a section §4.2.2 says it will refuse.
			_ = s.Reset(h3InternalError)
			return
		}
		r = Response{Status: 500}
	}
	sc.head = head
	status := r.Status
	if status == 0 {
		status = 200
	}
	_, noBody := noLengthAndBody(status, method)
	if !r.streamed() || noBody {
		// The whole response is in head: it and the FIN leave in one write,
		// one datagram for a response that fits one, where a write and then
		// CloseWrite sent the FIN in a datagram of its own — one more per
		// request, measured at 1.0 per request over 20,000 small responses.
		_ = writeH3Bounded(s, head, true, writeTimeout)
		return
	}
	if !writeH3Bounded(s, head, false, writeTimeout) {
		return // the stream is gone or given up on; its neighbours are not
	}
	// The payload follows as DATA frames on the same stream, at the pace the
	// producer supplies them — off the read goroutine, so packets and the
	// credit this write waits on keep arriving. A HEAD or 204/304 response
	// states no body and sends none: it took the branch above, and the
	// producer is never pulled, the same way the HTTP/1.1 and HTTP/2 paths
	// never do for these.
	detach()
	if !streamH3Body(c, s, r.Stream, writeTimeout) {
		return
	}
	_ = s.CloseWrite()
}

// writeH3Bounded writes p on s with writeTimeout to finish, and the FIN with
// it when fin is set, reporting whether it all went out. A write the peer
// starves of credit past the timeout is given up on and its stream reset
// with H3_REQUEST_CANCELLED (RFC 9114 §8.1): the peer granted less than the
// response needs, which it is entitled to do, and this end is the one that
// stopped waiting — the same reading as the HTTP/2 writer's CANCEL after its
// write timeout. Any other failure means the stream is already gone.
func writeH3Bounded(s *quic.Stream, p []byte, fin bool, writeTimeout time.Duration) bool {
	_ = s.SetWriteDeadline(time.Now().Add(writeTimeout))
	write := s.Write
	if fin {
		write = s.WriteClose
	}
	if _, err := write(p); err != nil {
		if errors.Is(err, os.ErrDeadlineExceeded) {
			_ = s.Reset(h3RequestCancelled)
		}
		return false
	}
	return true
}

// streamH3Body pulls a streamed response onto its stream, reporting whether it
// ran to the end. A stream the peer reset stops the pull, which is a cancelled
// request reaching the handler that is still working for it; so does a
// connection that ended, even on an empty batch that would write nothing.
func streamH3Body(c *quic.Conn, s *quic.Stream, stream sluice.Stream[[]byte], writeTimeout time.Duration) bool {
	ok := true
	var out []byte // reused across batches; Stream.Write copies before returning
	perr := pull(stream, func(b sluice.Batch[[]byte]) bool {
		select {
		case <-c.Done():
			ok = false
			return false
		default:
		}
		if _, reset := s.WasReset(); reset {
			ok = false
			return false
		}
		out = appendH3StreamedBody(out[:0], b.Items)
		if len(out) == 0 {
			return true // an empty batch is cadence, not an end
		}
		if !writeH3Bounded(s, out, false, writeTimeout) {
			ok = false
			return false
		}
		return true
	})
	if perr != nil {
		// The producer panicked: its stream is reset, the same answer a
		// response that cannot be framed gets, and nothing else is touched.
		_ = s.Reset(h3InternalError)
		return false
	}
	return ok
}
