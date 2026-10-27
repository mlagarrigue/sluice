package httpstream

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/varint"
	"github.com/mlagarrigue/sluice/net/quic"
)

// ServeH3Conn serves HTTP/3 over a QUIC connection, with the same [Handler] the
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
// # Why the handler runs off the read goroutine
//
// [quic.Conn.OnStreamFrames] runs on the connection's read goroutine — the one
// that decrypts packets, drives the handshake and files stream data. What runs
// there is assembly and nothing else: the frames of a datagram are folded into
// requests, and each batch of completed ones is handed to a goroutine of its
// own that calls the handler, writes the answers and pulls the streamed
// bodies.
//
// Everything about that split is forced. Running the handler on the read
// goroutine stops every packet from being processed while it runs, CRYPTO
// included; draining a streamed body there stops them for as long as the
// producer takes, and a producer that pauses trips the read loop's own
// deadline and tears the connection down. It is the same argument the HTTP/2
// side makes, and this transport makes it more sharply because the read loop
// is also the handshake.
//
// The hand-off never blocks the read loop, and needs no queue to bound it: a
// request holds a stream slot from admission until its response retires it,
// so the goroutines a connection runs are bounded by the streams the
// transport admits, and a peer that wants more waits on QUIC's own stream
// limit — which is what a transport's flow control is for. Nothing is
// dropped, because a dropped request is a caller with no answer.
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
// settled on any other protocol is refused and closed rather than served,
// as is any connection when [Config] refuses the configuration.
//
// The cancellation of ctx stops the connection normally, as below; when
// [Config.ShutdownGrace] passes before the drain ends, the connection is
// closed under its handler, whose context is cancelled, and ServeH3Conn returns
// [ErrShutdownForced]. The handler's context carries ctx's values but not
// its cancellation, which is the normal stop and not the handler's to see.
//
// Experimental: it takes net/quic's types, and net/quic is experimental, so
// this entry point is outside the v1 promise the rest of the package makes
// (ADR 0020, ADR 0021): it may change with net/quic, announced with a `!`.
func ServeH3Conn(ctx context.Context, c *quic.Conn, cfg Config, handle Handler) error {
	if handle == nil {
		panic("httpstream: ServeH3Conn requires a handler")
	}
	cfg, err := cfg.resolve()
	if err != nil {
		_ = c.CloseWithError(h3InternalError)
		return err
	}
	// The grace is a timer armed at the cancellation, not a context scoped
	// to the drain: the wait at the bottom of serveH3 reads it too, after
	// the connection has ended, and the drain can end — client gone, grace
	// passed — before the handler has returned.
	forced, force := context.WithCancel(context.WithoutCancel(ctx))
	defer force()
	var graceTimer atomic.Pointer[time.Timer]
	stop := context.AfterFunc(ctx, func() {
		graceTimer.Store(time.AfterFunc(cfg.ShutdownGrace, force))
	})
	defer func() {
		stop()
		// Stopped once ServeH3Conn has returned, when nobody reads it.
		if t := graceTimer.Load(); t != nil {
			t.Stop()
		}
	}()
	err = serveH3(ctx, forced, c, cfg, handle)
	if errors.Is(err, ErrShutdownForced) {
		cfg.logger().Warn("httpstream: the shutdown grace passed with requests in flight",
			"connections", 1, "grace", cfg.ShutdownGrace)
	}
	return err
}

// serveH3 is [ServeH3Conn] with its two stops apart, as a [Server] drives them:
// the cancellation of drain is the normal stop, that of forced the forced
// one, and forced is also the handler's context. cfg is resolved.
//
// serveH3 runs in this order: refuse a connection it cannot serve (a stream
// limit over h3MaxServeStreams, fewer than three unidirectional streams for
// the client, an ALPN other than h3); choose the credit mode and build the
// assembler; open the control stream and send SETTINGS; arm the drain
// (GOAWAY, PING, GOAWAY, close); then serve, through onFrames on the
// transport's read goroutine — control-stream checks, assembly (Frames),
// the interim 100s owed, the refusals, and one handler call per batch,
// held for the handshake when it arrived in 0-RTT; and last, at the
// connection's end, wake what is still waiting and wait for the calls and
// writers, the forced stop bounding that wait.
func serveH3(drain, forced context.Context, c *quic.Conn, cfg Config, handle Handler) error {
	// The goroutines a connection runs are bounded by the request streams
	// the transport admits, and h3MaxServeStreams is where that bound is
	// capped. A connection whose announced stream limit exceeds it is
	// refused outright rather than served with a bound nobody chose. The
	// connection is closed with the refusal, as on every other way out of
	// ServeH3Conn: left open, the client would wait on a server nobody runs.
	// The handshake is done: one copy for every request of the connection.
	state := c.ConnectionState()
	if lim := c.LocalParameters().InitialMaxStreamsBidi; lim > h3MaxServeStreams {
		_ = c.CloseWithError(h3InternalError)
		return fmt.Errorf(
			"httpstream: InitialMaxStreamsBidi %d is over the %d streams ServeH3Conn serves on one connection",
			lim, h3MaxServeStreams)
	}
	// RFC 9114 §6.2: the transport parameters MUST let the peer create at
	// least three unidirectional streams — its control stream and QPACK's
	// two — and SHOULD give each at least 1,024 bytes of credit. The
	// defaults hold both (quic.DefaultParameters, Config.TransportParameters);
	// parameters written by hand are checked for the MUST, and for a credit
	// of zero, under which the client's control stream could never carry
	// even its type byte: this end grants more credit only as it reads, and
	// it would never read. Below 1,024 the stream merely goes slowly, which
	// is the operator's to choose.
	if p := c.LocalParameters(); p.InitialMaxStreamsUni < 3 || p.InitialMaxStreamDataUni == 0 {
		_ = c.CloseWithError(h3InternalError)
		return fmt.Errorf(
			"httpstream: InitialMaxStreamsUni %d with InitialMaxStreamDataUni %d leaves the peer short of the three unidirectional streams RFC 9114 §6.2 requires",
			p.InitialMaxStreamsUni, p.InitialMaxStreamDataUni)
	}
	// RFC 9114 §3.1: HTTP/3 is the protocol identified by the ALPN token
	// "h3". A peer that settled on anything else is waiting for that other
	// protocol and would read this layer's SETTINGS and frames as garbage;
	// refused and closed, like the limit above.
	if proto := c.ConnectionState().NegotiatedProtocol; proto != "h3" {
		_ = c.CloseWithError(h3InternalError)
		return fmt.Errorf("httpstream: ServeH3Conn on a connection that negotiated %q, not h3", proto)
	}
	if cfg.KeepAlive > 0 {
		_ = c.SetKeepAlive(cfg.KeepAlive) // positive: SetKeepAlive refuses only a negative period
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
	//
	// When request bodies stream (Config.MaxStreamedBodyBytes), a body is
	// retained whole only when it ended in the delivery that completed its
	// head — a packet's worth at most — and every other one is handed over
	// as a stream whose bytes are released as it is read, so the windows
	// need only cover a request's head; streaming takes manual credit, and
	// on windows too small for that it is off and bodies arrive whole.
	credit := quic.CreditOnDelivery
	streamWindow, connWindow := uint64(cfg.MaxBodyBytes+cfg.MaxHeaderBytes)+h3FrameSlack, uint64(4*cfg.MaxBodyBytes+cfg.MaxHeaderBytes)+h3FrameSlack //nolint:gosec // G115: positive config, set by withDefaults
	if cfg.BodyDelivery == BodyStreamed {
		streamWindow = uint64(cfg.MaxHeaderBytes) + h3FrameSlack //nolint:gosec // G115: positive config, set by withDefaults
		connWindow = streamWindow
	}
	if p := c.LocalParameters(); p.InitialMaxStreamDataBidiRemote >= streamWindow && p.InitialMaxData >= connWindow {
		credit = quic.CreditOnRelease
		a.Credit = func(id, n uint64) { h3ReleaseStreamBytes(c, id, n) }
		if cfg.BodyDelivery == BodyStreamed {
			a.CreditConn = c.ReleaseConnectionCredit
			a.CreditStream = c.ReleaseStreamCredit
			a.Interim = func(id, fieldSection uint64) { writeH3Interim(c, id, fieldSection, writeBoundOf(cfg)) }
		}
	}

	// RFC 9114 §6.2.1: each endpoint opens a control stream and sends
	// SETTINGS as its first frame. A peer is entitled to wait for it, so a
	// server that never sends one is a server a conformant client hangs on.
	// Stream 3 is the first server-initiated unidirectional identifier. A
	// client that grants no unidirectional stream breaks §6.2's "MUST permit
	// at least three": no control stream can open, and the connection is
	// closed with the generic protocol error, §6.2 naming no specific one.
	control, err := c.Stream(3)
	if err != nil {
		_ = c.CloseWithError(h3GeneralProtocolError)
		return fmt.Errorf("httpstream: the client grants no unidirectional stream for the control stream: %w", err)
	}
	// The forced stop closes the connection whatever this end is doing when
	// it comes — writing SETTINGS, draining, serving — and the close ends
	// every write still waiting. A write on the control stream waits for
	// the client's credit like any other, and a client that withholds it
	// must not keep the connection, nor ServeH3Conn, past the grace.
	stopForced := context.AfterFunc(forced, func() { _ = c.CloseWithError(h3GracefulCode()) })
	defer stopForced()
	// The three writes on the control stream — SETTINGS, the two GOAWAYs —
	// are bounded as a response's are (Config.WriteTimeout, read as a time
	// without progress): §6.2 only asks the client for 1,024 bytes of
	// credit there by a SHOULD, and a client that grants less than SETTINGS
	// needs would otherwise park this write for the life of the connection.
	wb := writeBoundOf(cfg)
	if err := wb.write(control.SetWriteDeadline, control.Write, appendH3Control(nil, uint64(cfg.MaxHeaderBytes))); err != nil { //nolint:gosec // G115: positive config, set by withDefaults
		// Closed on the way out, like the two refusals above. The one
		// failure a live connection produces here is the client's
		// STOP_SENDING on the control stream landing between Stream(3)
		// and this write: the §6.2.1 error the callback below names,
		// which, unanswered, would leave the connection open with nobody
		// serving it.
		//
		// A client that withheld the credit until the write timeout passed
		// is closed the same way, with H3_INTERNAL_ERROR: a connection whose
		// control stream cannot open is one nobody can serve. Under the
		// forced stop, the close above ended the write, and the stop is
		// what ServeH3Conn reports.
		if code, reset := control.WasReset(); reset {
			_ = c.CloseWithError(h3ClosedCriticalStream)
			return controlStopped(code)
		}
		_ = c.CloseWithError(h3InternalError)
		if forced.Err() != nil {
			return ErrShutdownForced
		}
		if errors.Is(err, os.ErrDeadlineExceeded) {
			return fmt.Errorf("httpstream: the client's credit on the control stream took no SETTINGS within the write timeout: %w", err)
		}
		return err
	}

	// A cancelled server drains (ADR 0017), the way RFC 9114 §5.2 has it
	// "without losing requests": a first GOAWAY at 2^62−4, the highest
	// identifier a server may send, which tells the client to open nothing
	// more while every request it already sent is still admitted; a PING
	// whose acknowledgement measures the round trip in which those requests
	// arrive; then a second GOAWAY naming the first request stream this
	// connection rejects — the one after the highest it admitted, decided
	// under the assembler's lock so no stream is both admitted and named.
	// A request the client sent before it read the first GOAWAY is thereby
	// served, not rejected, as on HTTP/2. Then every admitted request is
	// answered, then the connection closes
	// H3_NO_ERROR (or §8.1's grease) once the last answer is acknowledged,
	// which is what [quic.Conn.ShutdownWithError] waits for: a close sent at
	// once could reach the client before the response it follows. A request
	// stream opened from the named identifier on is rejected on arrival
	// (REQUEST_REJECTED, see the assembler's admit). The forced stop bounds
	// the whole: when it comes, what is still open is closed under its
	// handler (stopForced, above) and serveH3 returns, as Serve does. A
	// GOAWAY the client's credit holds past the write timeout closes the
	// connection, as a failed one does. The goroutine this runs on
	// lives for the drain only — nothing is parked for the life of the
	// connection.
	drained := make(chan struct{})
	var finishOnce sync.Once
	a.Finish = func() { finishOnce.Do(func() { close(drained) }) }
	stop := context.AfterFunc(drain, func() {
		if err := wb.write(control.SetWriteDeadline, control.Write, appendH3Frame(nil, h3GoAway, varint.Append(nil, h3MaxServerGoAway))); err != nil {
			_ = c.CloseWithError(h3GracefulCode())
			return
		}
		// The forced stop bounds the round trip, as it bounds the rest: a
		// client that never acknowledges is closed at the grace.
		if err := c.Ping(forced); err != nil {
			_ = c.CloseWithError(h3GracefulCode())
			return
		}
		from, idle := a.beginDrain()
		if err := wb.write(control.SetWriteDeadline, control.Write, appendH3Frame(nil, h3GoAway, varint.Append(nil, from))); err != nil {
			_ = c.CloseWithError(h3GracefulCode())
			return
		}
		if idle {
			// Decided under the assembler's lock, with the admission: a
			// request stream arriving from here on is rejected, not served
			// into a connection that is closing.
			a.Finish()
		}
		// The forced stop needs no case here: it closes the connection,
		// which ends this wait.
		select {
		case <-drained:
			// The forced stop bounds the acknowledgement too; at it the
			// connection closes anyway, as Shutdown promises.
			_ = c.ShutdownWithError(forced, h3GracefulCode())
		case <-c.Done():
		}
	})
	defer stop()

	// Each batch is one call to the handler on a goroutine of its own,
	// launched from the read callback: a request that arrives while the
	// handler is busy with an earlier batch is not held behind it — the
	// head-of-line block one responder per connection would impose, which
	// HTTP/2 lifts the same way (ADR 0008). The handler is called
	// concurrently on one connection, as [Handler] states. Launching never
	// blocks, and that matters more than it looks: the read goroutine is the
	// goroutine that delivers the flow-control credit a stalled response is
	// waiting for, so a read loop that waited on the handler would be a
	// deadlock, not a slowdown. The calls are bounded because a request is
	// in flight, in the assembler's own count, from its first frame until
	// its response has been written and the stream retired — so the
	// requests held by unfinished calls never outnumber MaxConcurrentStreams.
	// The transport's stream slot is not that bound: it is given back once
	// both directions are done, which a client's STOP_SENDING on a complete
	// request does at once (RFC 9114 §4.1.1), the call still running; the
	// assembler refuses the stream over its own count, and closes a
	// connection that cancels more than it lets complete
	// (h3Assembler.countCancel). Starting the goroutine
	// costs 1.7% of a request answered alone (BenchmarkH3FromQuicGo/plain,
	// interop module), against a p99 under load that went from 1.48-2.32
	// times HTTP/1.1's to 1.13-1.57 (ADR 0008).
	//
	// Within a call the *writes* fan out: each response leaves on its own
	// goroutine, so a response stalled on stream flow control — a client
	// withholding MAX_STREAM_DATA for one stream — stalls that stream alone,
	// bounded by the write timeout, instead of every answer behind it on the
	// connection. Writer goroutines are bounded by the same invariant: one
	// unfinished response per admitted stream. quic.Stream.Write is safe
	// across streams (the connection lock serialises packets), and one
	// goroutine per stream keeps that stream's own writes ordered.
	//
	// launch starts f counted in wg unless the connection has ended. The
	// callback runs on the connection's read goroutine, which can still be
	// inside a datagram when the connection ends; a WaitGroup that gains a
	// goroutine once its Wait has begun at zero is a misuse, and the wait
	// below must not miss a call that started after it looked.
	var (
		calls, writers sync.WaitGroup
		launchMu       sync.Mutex
		ended          bool
		// cancels maps each request stream handed to a handler to its
		// batch's context: a RESET_STREAM or STOP_SENDING from the client
		// takes the request out of it, the connection's end takes them all.
		cancels streamCalls[uint64]
	)
	// launch runs f on wg unless the connection has ended, and reports
	// whether it did: a caller that owes a signal from f sends it itself
	// when f never runs.
	launch := func(wg *sync.WaitGroup, f func()) bool {
		launchMu.Lock()
		defer launchMu.Unlock()
		if !ended {
			wg.Go(f)
		}
		return !ended
	}

	// The assembler belongs to the delivery callback and to the expiry timer
	// below, which take amu in turn; without the timer the read timeout only
	// ran when stream frames arrived (see expire). The timer is armed at the
	// assembler's earliest deadline after every delivery and every sweep, and
	// re-armed only when that deadline moved, which on the ordinary path is
	// once per first partial stream: one timer per connection, not per
	// stream, and no timer at all while nothing is assembling.
	var (
		amu       sync.Mutex
		sweepT    *time.Timer
		sweepAt   time.Time
		sweepDone bool
		sweepNow  func()
	)
	arm := func() { // under amu
		next := a.nextExpiry
		if sweepDone || next.Equal(sweepAt) {
			return
		}
		sweepAt = next
		switch {
		case next.IsZero():
			if sweepT != nil {
				sweepT.Stop()
			}
		case sweepT == nil:
			sweepT = time.AfterFunc(time.Until(next), sweepNow)
		default:
			sweepT.Reset(time.Until(next))
		}
	}
	sweepNow = func() {
		amu.Lock()
		if sweepDone {
			amu.Unlock()
			return
		}
		failed := append([]h3StreamError(nil), a.sweep(time.Now())...)
		sweepAt = time.Time{} // fired: whatever the next deadline is, arm it
		arm()
		amu.Unlock()
		for _, se := range failed {
			// Reset on both halves, as a stream failure inside a delivery
			// is (below); the connection may have ended meanwhile, which
			// leaves nothing to reset.
			if s, err := c.Stream(se.StreamID); err == nil {
				_ = s.Reset(se.Code)
				_ = s.StopSending(se.Code)
			}
		}
	}
	onFrames := func(frames []quic.Frame) error {
		// RFC 9114 §6.2.1: "the receiver MUST NOT request that the sender
		// close the control stream", and a closed one is a connection error
		// of type H3_CLOSED_CRITICAL_STREAM. The transport has answered the
		// STOP_SENDING already (RESET_STREAM, RFC 9000 §3.5) and hands it up
		// in the datagram that carried it; the connection ends before any
		// request beside it is admitted. A STOP_SENDING on a request stream
		// is the assembler's: the transport abandoned the response, whose
		// writer fails on its next write, and Frames charges the
		// cancellation against the rapid-reset allowance.
		for _, f := range frames {
			if f.Type == quic.FrameStopSending && f.StreamID == control.ID() {
				_ = c.CloseWithError(h3ClosedCriticalStream)
				return controlStopped(f.Code())
			}
			// A client that resets a request stream, or asks this end to
			// stop sending on it, has given the request up (RFC 9114
			// §4.1.1): it leaves its batch, whose context is cancelled once
			// none is left. A stream not yet handed over is not in cancels.
			if f.Type == quic.FrameResetStream || f.Type == quic.FrameStopSending {
				cancels.leave(f.StreamID)
			}
		}
		// RESET_STREAM is the assembler's to fold in, inside Frames' own
		// loop: a separate pass here would run every Forget before the
		// datagram's STREAM deltas, and resurrect assembly state for streams
		// the transport has already abandoned — entries that could never
		// complete, each burning an admission slot for good.
		batch, ids, failed, err := a.Frames(frames)
		if err != nil {
			// A connection error carries the code RFC 9114 §8 assigns it;
			// anything else ends the connection through the transport's own
			// teardown when this return reaches it. No GOAWAY before this
			// close: a connection error leaves nothing a GOAWAY would let
			// the client retry (§5.3 MAY) — the drain above is where a
			// GOAWAY earns its frame, when every request is still served.
			if ce, ok := errors.AsType[h3ConnectionError](err); ok {
				_ = c.CloseWithError(ce.Code)
			}
			return err
		}
		for _, ct := range a.continues {
			// A head with Expect: 100-continue and no FIN yet: the interim
			// 100 leaves now, RFC 9110 §10.1.1, on a writer goroutine like
			// any response, since a Write waits on the peer's credit. The
			// gate is closed whatever became of the write — the final
			// response waits on it, not on the write's success — and here
			// when nothing runs, the connection having ended. Before this,
			// the assembler wrote nothing before the FIN, and a conformant
			// client waited out its own timer before sending the body.
			id, done, fieldSection, wb := ct.StreamID, ct.Done, a.peerMaxFieldSection(), writeBoundOf(cfg)
			if !launch(&writers, func() {
				defer close(done)
				writeH3Interim(c, id, fieldSection, wb)
			}) {
				close(done)
			}
		}
		for _, se := range failed {
			// One stream's refusal, on that stream. The connection and
			// every other request the datagram carried are untouched, which
			// is the whole of what a multiplexed transport offers over a
			// sequential one.
			s, err := c.Stream(se.StreamID)
			if err != nil {
				continue // unreachable: the peer opened it, within the limit
			}
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
				id, status, method, fieldSection, after := se.StreamID, se.Status, se.Method, a.peerMaxFieldSection(), se.After
				launch(&writers, func() {
					defer a.retire()
					if after != nil {
						<-after // the interim 100 the head was owed leaves first
					}
					writeH3Response(c, id, transportError(status), method, fieldSection, writeBoundOf(cfg), dateFor(time.Now()), func() {})
				})
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
		held := append([]uint64(nil), ids...)
		b := h3Batch{
			ctx:   cancels.register(forced, held),
			calls: &cancels,
			reqs:  append([]Request(nil), batch.Items...),
			ids:   held,
			after: append([]<-chan struct{}(nil), a.after...),
			// The peer's MAX_FIELD_SECTION_SIZE, snapshotted here because
			// the assembler belongs to this goroutine and the call must not
			// reach into it.
			fieldSection: a.peerMaxFieldSection(),
			credit:       a.Credit,
			retire:       a.retire,
			answered:     a.answered,
			early:        cfg.EarlyData,
		}
		select {
		case <-c.HandshakeComplete():
		default:
			// Arrived in 0-RTT, before the handshake completed: it may be
			// a replay (ADR 0019). A batch Config.EarlyData does not allow
			// in full waits for the handshake — a replay never completes
			// it, so it is never handled — and is dropped with the
			// connection otherwise. The wait is a call like any other, in
			// calls, so the drain below waits for it too.
			if !allowedEarly(b.reqs, cfg.EarlyData) {
				launch(&calls, func() {
					select {
					case <-c.HandshakeComplete():
						answerH3(forced, c, &state, b, handle, writeBoundOf(cfg), &writers)
					case <-c.Done():
					}
				})
				return nil
			}
		}
		launch(&calls, func() { answerH3(forced, c, &state, b, handle, writeBoundOf(cfg), &writers) })
		return nil
	}
	c.OnStreamFrames(credit, func(frames []quic.Frame) error {
		amu.Lock()
		defer amu.Unlock()
		defer arm()
		return onFrames(frames)
	})

	<-c.Done()
	// Nobody is left to answer: the handlers still running learn it before
	// they are waited for, and so does one waiting for a body's bytes.
	cancels.leaveAll()
	amu.Lock()
	a.failBodies(errBodyConnClosed)
	amu.Unlock()
	amu.Lock()
	sweepDone = true
	if sweepT != nil {
		sweepT.Stop()
	}
	amu.Unlock()
	// No call starts once the connection is done, and every writer that is
	// writing exits then: its writes fail. A writer
	// inside a streamed producer is not waited for — the producer is the
	// caller's code, and one blocked on something other than its yield
	// would hold this return forever. It learns the connection is gone at
	// its next yield, which returns false (see the package documentation),
	// and its goroutine ends then. A handler that has not returned is the
	// same case one level up: after a cancellation the wait for it ends
	// with the forced stop, as Serve's does (ADR 0017 §2), and the goroutine
	// below ends when the handler does. A connection the client ended is
	// waited for in full — there is no grace to count from until the
	// server is cancelled, which arms one.
	finished := make(chan struct{})
	go func() {
		launchMu.Lock()
		ended = true
		launchMu.Unlock()
		// Every writer is counted before its call returns.
		calls.Wait()
		writers.Wait()
		close(finished)
	}()
	select {
	case <-finished:
	case <-forced.Done():
		select {
		case <-finished:
		default:
			return ErrShutdownForced
		}
	}
	return h3CloseErr(c.Err())
}

// allowedEarly reports whether early allows every request of a batch that
// arrived before the handshake completed. Holding the whole batch for one
// request it does not allow costs the others the rest of the handshake, a
// round trip at most, and keeps every request of a batch in one call.
func allowedEarly(reqs []Request, early func(*Request) bool) bool {
	if early == nil {
		return false
	}
	for i := range reqs {
		if !early(&reqs[i]) {
			return false
		}
	}
	return true
}

// controlStopped is the connection error for a client that asked the control
// stream to stop (RFC 9114 §6.2.1): a critical stream closed, whenever the
// refusal is noticed.
func controlStopped(code uint64) error {
	return h3ConnectionError{Code: h3ClosedCriticalStream, Err: fmt.Errorf(
		"%w: the client asked the control stream to stop (STOP_SENDING %#x)", ErrH3Protocol, code)}
}

// h3CloseErr is what ServeH3Conn reports once the connection ended: nil for
// the ends an HTTP/3 server treats as the client leaving — its H3_NO_ERROR
// (RFC 9114 §8.1), any application code this layer does not know (§8:
// "receipt of an unknown error code MUST be treated as equivalent to
// H3_NO_ERROR", which is how the reserved 0x1f·N+0x21 codes a peer SHOULD
// send in its place arrive — h3GracefulCode sends them too), its transport
// NO_ERROR, or silence past the idle timeout — and the transport's cause
// for everything else. The transport reports every peer close as a
// [quic.ConnectionError] because it does not know what the codes mean;
// this is the layer that does.
func h3CloseErr(err error) error {
	if errors.Is(err, quic.ErrIdleTimeout) {
		return nil
	}
	if ce, ok := errors.AsType[*quic.ConnectionError](err); ok {
		if ce.Application && (ce.Code == h3NoError || !h3KnownCode(ce.Code)) {
			return nil
		}
		if !ce.Application && ce.Code == 0 {
			return nil
		}
	}
	return err
}

// h3KnownCode reports whether code is an error code RFC 9114 §8.1
// (H3_NO_ERROR to H3_VERSION_FALLBACK) or RFC 9204 §8.3 (the three QPACK
// codes) defines. Every other value is unknown, H3_NO_ERROR's equivalent
// on receipt (RFC 9114 §8).
func h3KnownCode(code uint64) bool {
	return (code >= h3NoError && code <= h3VersionFallback) ||
		(code >= qpackDecompressionFailed && code <= qpackDecoderStreamError)
}

// h3ReleaseStreamBytes is the manual-credit release, a variable so a test
// can count what this layer gives back to the transport.
var h3ReleaseStreamBytes = (*quic.Conn).ReleaseStreamBytes

// h3Batch is what the read goroutine hands a handler call: the requests one
// datagram completed, and the stream each arrived on. HTTP/3 correlates by
// stream rather than by order, which is the one thing it does not inherit
// from HTTP/1.1's pipelining.
type h3Batch struct {
	reqs []Request
	ids  []uint64
	// after runs parallel to ids: the gate behind the interim 100 each
	// request's head was owed, nil for none. A response is written only
	// once its gate is closed, so the final HEADERS follows the interim one
	// on the stream (RFC 9114 §4.1); the gate closes when the 100 has left
	// or its writer gave up, so the wait is bounded as that write is.
	after []<-chan struct{}
	// fieldSection is the peer's SETTINGS_MAX_FIELD_SECTION_SIZE at hand-off
	// time, zero for unbounded; the call holds every response's field
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
	// answered, when set, is called once per response written in full,
	// before retire: what the rapid-reset allowance grows with.
	answered func()
	// early is Config.EarlyData, for the requests carrying Early-Data.
	early func(*Request) bool
	// ctx is the batch's context, released through calls once the last of
	// its responses is written (see batchCtx).
	ctx   *batchCtx
	calls *streamCalls[uint64]
}

// TransportParameters returns QUIC transport parameters sized to this
// configuration's own admission bounds, for the connection ServeH3Conn will
// serve: stream windows that fit a full request (MaxBodyBytes plus header
// room) — or, when bodies stream, the transport's default stream window,
// which is then what bounds a streamed body (see
// [Config.MaxStreamedBodyBytes]) — a connection window that fits the
// 4×MaxBodyBytes aggregate assembly cap, the stream count ServeH3Conn
// admits, and [Config.IdleTimeout]
// as the connection's max_idle_timeout — HTTP/3 has no idle clock of its
// own above the transport's, so this is the one place the field reaches an
// HTTP/3 connection, and a zero field is the two minutes every protocol
// defaults to. Serving a connection built with these lets ServeH3Conn run the transport
// under manual flow-control credit, where a peer's window only reopens for
// bytes this server has actually stopped retaining — the strongest memory
// bound the layer offers. Any other parameters work too; below these
// windows ServeH3Conn falls back to delivery-time crediting, bounded by the
// assembler's caps, and the idle timeout is whatever those parameters say.
//
// It returns an error for a configuration [Config] refuses, and for a
// MaxConcurrentStreams over the 4096 streams ServeH3Conn serves on one
// connection: announced, that limit would have ServeH3Conn refuse every
// connection built on these parameters.
//
// Experimental: it takes net/quic's types, and net/quic is experimental, so
// this entry point is outside the v1 promise the rest of the package makes
// (ADR 0020, ADR 0021): it may change with net/quic, announced with a `!`.
func (c Config) TransportParameters() (quic.TransportParameters, error) {
	c, err := c.resolve()
	if err != nil {
		return quic.TransportParameters{}, err
	}
	if c.MaxConcurrentStreams > h3MaxServeStreams {
		return quic.TransportParameters{}, fmt.Errorf("httpstream: Config.MaxConcurrentStreams %d is over the %d streams ServeH3Conn serves on one connection",
			c.MaxConcurrentStreams, h3MaxServeStreams)
	}
	p := quic.DefaultParameters()
	p.MaxIdleTimeout = c.IdleTimeout
	p.InitialMaxData = uint64(4*c.MaxBodyBytes+c.MaxHeaderBytes) + h3FrameSlack //nolint:gosec // G115: positive config
	if c.BodyDelivery == BodyStreamed {
		// A streamed body is held to the stream window, not to MaxBodyBytes:
		// the transport's own window, or the room for a head if that is
		// larger.
		p.InitialMaxStreamDataBidiRemote = max(p.InitialMaxStreamDataBidiRemote, uint64(c.MaxHeaderBytes)+h3FrameSlack) //nolint:gosec // G115: positive config
	} else {
		p.InitialMaxStreamDataBidiRemote = uint64(c.MaxBodyBytes+c.MaxHeaderBytes) + h3FrameSlack //nolint:gosec // G115: positive config
	}
	p.InitialMaxStreamsBidi = uint64(c.MaxConcurrentStreams) //nolint:gosec // G115: positive config
	return p, nil
}

// h3MaxServeStreams caps the stream limit ServeH3Conn will serve. One connection
// runs up to a handler call and a writer goroutine per admitted stream (see
// the hand-off comment in ServeH3Conn); past this cap ServeH3Conn refuses the
// connection rather than let one stream limit pin that many goroutines.
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
func answerH3(ctx context.Context, c *quic.Conn, state *tls.ConnectionState, b h3Batch, handle Handler, wb writeBound, writers *sync.WaitGroup) {
	// The address as it stands now: a QUIC connection may have moved.
	peer{addr: addrPortOf(c.RemoteAddr()), tls: state}.stamp(b.reqs)
	// The batch's context, released by the last of its writers below.
	var pending atomic.Int32
	done := func() {}
	if b.ctx != nil {
		ctx = b.ctx
		pending.Store(int32(len(b.ids))) //nolint:gosec // G115: a batch is bounded far below 2^31
		done = func() {
			if pending.Add(-1) == 0 {
				b.calls.release(b.ctx, b.ids)
			}
		}
	}
	res, err := apply(ctx, handle, sluice.Batch[Request]{Items: b.reqs}, b.early)
	closeBodies(b.reqs)
	if err != nil {
		reportFailure(wb.log, err)
		// The handler panicked, or answered a number of requests that does
		// not match what it was given. Every request of the batch is answered
		// 500 and the connection carries on.
		res = make([]Response, len(b.reqs))
		for i := range res {
			res[i] = transportError(500)
		}
	}

	// The request bodies stop being retained by this layer the moment the
	// handler returns — the release half of the manual credit ServeH3Conn runs
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
		// A streamed body the client may still be sending: once the response
		// is out, STOP_SENDING with H3_NO_ERROR tells it to stop (RFC 9114
		// §4.1).
		unread := false
		if rb := b.reqs[i].body; rb != nil {
			unread = rb.q.unread()
		}
		go func(id uint64, r Response, method []byte, after <-chan struct{}) {
			if after != nil {
				<-after // the interim 100 the head was owed leaves first
			}
			detached := false
			defer func() {
				if !detached {
					writers.Done()
				}
				// Retired at the goroutine's very end, a streamed producer's
				// pull included: the drain waits for the body it is writing,
				// bounded by the grace, where ServeH3Conn's own return does not.
				if b.retire != nil {
					b.retire()
				}
				done()
			}()
			whole := writeH3Response(c, id, r, method, b.fieldSection, wb, date, func() {
				detached = true
				writers.Done()
			})
			if whole && b.answered != nil {
				b.answered()
			}
			if unread {
				if s, err := c.Stream(id); err == nil {
					_ = s.StopSending(h3NoError)
				}
			}
		}(id, res[i], b.reqs[i].Method, b.after[i])
	}
}

// h3Interim100 is the HEADERS frame of an interim 100 (Continue): :status
// alone, as the HTTP/2 writer's interim has it — a 1xx owes no date
// (RFC 9110 §6.6.1) and carries no content (RFC 9114 §4.1). Built once;
// nothing in it is per connection.
var h3Interim100 = appendH3Frame(nil, h3Headers, appendQPACK(nil, h3Interim100Fields))

// h3Interim100Fields is the field section h3Interim100 encodes, kept for the
// size check against the peer's SETTINGS_MAX_FIELD_SECTION_SIZE.
var h3Interim100Fields = []Header{{Name: h3StatusName, Value: []byte("100")}}

// writeH3Interim writes the interim 100 a request head with Expect:
// 100-continue is owed on its stream, without a FIN: the final response
// follows on the same stream (RFC 9114 §4.1). It is bounded as every write
// is, and gives up without a word when the peer's field-section limit is
// under the 100's own 42 bytes — no final response fits such a peer either,
// and writeH3Response answers that case. A write that fails has nothing to
// report: the stream is reset or gone, and the final response's writer
// meets the same end.
func writeH3Interim(c *quic.Conn, id, fieldSection uint64, wb writeBound) {
	if checkH3FieldSection(h3Interim100Fields, fieldSection) != nil {
		return
	}
	s, err := c.Stream(id)
	if err != nil {
		return // unreachable: the head arrived on it, within the limit
	}
	writeH3Bounded(s, h3Interim100, false, wb)
}

// h3ScratchPool recycles the per-response framing scratch across writer
// goroutines; each writer holds one for exactly one response.
var h3ScratchPool = sync.Pool{New: func() any { return &h3Scratch{} }}

// writeH3Response frames and writes one response on its own goroutine — the
// lift of the old single-writer design's head-of-line block. A write stalled
// on this stream's flow control stalls this goroutine alone; packets keep
// arriving on the read loop and every other response keeps leaving. Each
// write is bounded by wb — [Config.WriteTimeout] without progress, the bound
// the HTTP/2 writer puts on a job — so a client that keeps the connection
// alive but never grants credit holds the goroutine, and a streamed
// producer's body, for that long and no longer: the stream is then reset and
// the goroutine returned. One that keeps granting holds it as long as it
// reads.
//
// detach is called once the response's own writes are done and a streamed
// producer is about to be pulled: from there on the goroutine runs the
// caller's code, which ServeH3Conn does not wait for.
//
// It reports whether the whole response left, FIN included — a request
// answered, in the rapid-reset allowance's terms. A stream the peer reset or
// starved, a producer that stopped short and a response that could not be
// framed are not.
func writeH3Response(c *quic.Conn, id uint64, r Response, method []byte, fieldSection uint64, wb writeBound, date []byte, detach func()) bool {
	s, err := c.Stream(id)
	if err != nil {
		return false // unreachable: the request arrived on it, within the limit
	}
	sc := h3ScratchPool.Get().(*h3Scratch)
	defer h3ScratchPool.Put(sc)
	head, err := appendH3Response(sc.head[:0], r, method, fieldSection, sc, date)
	if err != nil {
		reportPanic(wb.log, err)
		r = transportError(500)
		if head, err = appendH3Response(sc.head[:0], r, method, fieldSection, sc, date); err != nil {
			// The peer's MAX_FIELD_SECTION_SIZE is under the line's own two
			// fields: the status alone, as before the body.
			r = Response{Status: 500}
			head, err = appendH3Response(sc.head[:0], r, method, fieldSection, sc, date)
		}
		if err != nil {
			// Only reachable when the peer's MAX_FIELD_SECTION_SIZE is under
			// a bare 500's own (:status, date and content-length, ~155
			// bytes): no answer fits what the client said it accepts, so
			// the stream is reset rather than sent a section §4.2.2 says it
			// will refuse.
			_ = s.Reset(h3InternalError)
			return false
		}
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
		return writeH3Bounded(s, head, true, wb)
	}
	if !writeH3Bounded(s, head, false, wb) {
		return false // the stream is gone or given up on; its neighbours are not
	}
	// The payload follows as DATA frames on the same stream, at the pace the
	// producer supplies them — off the read goroutine, so packets and the
	// credit this write waits on keep arriving. A HEAD or 204/304 response
	// states no body and sends none: it took the branch above, and the
	// producer is never pulled, the same way the HTTP/1.1 and HTTP/2 paths
	// never do for these.
	detach()
	if !streamH3Body(c, s, r.Stream, r.ContentLength, wb) {
		return false
	}
	if r.Trailers == nil {
		return s.CloseWrite() == nil
	}
	tail, err := appendH3Trailers(sc.head[:0], r, fieldSection, sc)
	if err != nil {
		reportPanic(wb.log, err)
		// The body is out: a refused trailer resets the stream, as a
		// producer that panicked does, so the client never reads the
		// answer as whole.
		_ = s.Reset(h3InternalError)
		return false
	}
	sc.head = tail
	// The trailer section and the FIN leave in one write, as a buffered
	// response's last frame and its FIN do.
	return writeH3Bounded(s, tail, true, wb)
}

// writeH3Bounded writes p on s under [Config.WriteTimeout], read as a time
// without progress (see writeBound), and the FIN with it when fin is set,
// reporting whether it all went out. A write the peer starves of credit is
// given up on and its stream reset with H3_REQUEST_CANCELLED, the code
// RFC 9114 §4.1.1 says a server SHOULD abort with when it "abandons a
// response after partial processing": the peer granted less than the
// response needs, which it is entitled to do, and this end is the one that
// stopped waiting — the same reading as the HTTP/2 writer's CANCEL after its
// write timeout. Any other failure means the stream is already gone.
func writeH3Bounded(s *quic.Stream, p []byte, fin bool, wb writeBound) bool {
	write := s.Write
	if fin {
		write = s.WriteFinal
	}
	if err := wb.write(s.SetWriteDeadline, write, p); err != nil {
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
//
// length is the response's ContentLength, zero for none. A producer that
// yields more — the batch that would cross it is not written — or ends short
// of it has its stream reset, never ended: RFC 9114 §4.1.2 makes the
// mismatch a malformed response.
func streamH3Body(c *quic.Conn, s *quic.Stream, stream sluice.Stream[[]byte], length int64, wb writeBound) bool {
	ok, wrong := true, false
	var sent int64
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
		if overLength(&sent, length, b.Items) {
			wrong = true
			return false
		}
		out = appendH3StreamedBody(out[:0], b.Items)
		if len(out) == 0 {
			return true // an empty batch is cadence, not an end
		}
		if !writeH3Bounded(s, out, false, wb) {
			ok = false
			return false
		}
		return true
	})
	if perr != nil {
		reportFailure(wb.log, perr)
	}
	if perr != nil || wrong || ok && sent < length {
		// The producer panicked or missed its length: its stream is reset,
		// the same answer a response that cannot be framed gets, and nothing
		// else is touched.
		_ = s.Reset(h3InternalError)
		return false
	}
	return ok
}
