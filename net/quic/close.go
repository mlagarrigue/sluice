package quic

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"time"
)

// Connection lifecycle: how it ends, and what the peer is told.

// Transport error codes (RFC 9000 §20.1), the ones this package sends.
const (
	transportNoError              = 0x0
	transportInternalError        = 0x1
	transportConnectionRefused    = 0x2
	transportFlowControl          = 0x3
	transportStreamLimit          = 0x4
	transportStreamState          = 0x5
	transportFinalSize            = 0x6
	transportFrameEncoding        = 0x7
	transportParameterError       = 0x8
	transportConnectionIDLimit    = 0x9
	transportInvalidToken         = 0xb
	transportApplicationError     = 0xc
	transportProtocolViolation    = 0xa
	transportCryptoBufferExceeded = 0xd
	transportKeyUpdateError       = 0xe
	transportAEADLimitReached     = 0xf
	// transportCryptoErrorBase is the start of the range RFC 9001 §4.8
	// reserves for TLS alerts: 0x0100 plus the alert's own code, which is
	// how ALPN failure (0x0178), a missing extension (0x016d) and the rest
	// travel without QUIC inventing names for them.
	transportCryptoErrorBase = 0x0100
)

// transportError carries the CONNECTION_CLOSE code a violation deserves
// alongside the error that describes it.
type transportError struct {
	code uint64
	err  error
}

func (e *transportError) Error() string { return e.err.Error() }
func (e *transportError) Unwrap() error { return e.err }

// Why a connection ended, as [Conn.Err] reports it (and [Conn.Dial] or
// [Conn.Accept] when it ended during the handshake). Each cause has its own
// value, so a caller distinguishes them with [errors.Is] and [errors.As]
// rather than by reading the text: a server whose clients leave silently
// sees [ErrIdleTimeout] once per client, a peer that rebooted shows as
// [ErrStatelessReset], a peer that said goodbye shows as a
// [*ConnectionError] carrying its code. Nothing here judges whether an end
// was clean — what an application code means belongs to the layer that
// knows the codes, and whether a silent peer is a failure depends on who
// was expected to speak — so [Conn.Err] is nil only while the connection
// is open or after this end closed it.
var (
	// ErrIdleTimeout: neither end sent anything for the negotiated idle
	// period (RFC 9000 §10.1). The peer is presumed gone; it was told
	// nothing.
	ErrIdleTimeout = errors.New("quic: idle timeout")
	// ErrStatelessReset: the peer answered with a stateless reset (RFC 9000
	// §10.3) — it once held this connection and holds no state for it any
	// more, as after a restart.
	ErrStatelessReset = errors.New("quic: stateless reset, the peer no longer holds state for this connection")
	// ErrVersionNegotiation: the server answered the first flight with a
	// Version Negotiation packet that does not list version 1 (RFC 9000
	// §6), the only version this package speaks.
	ErrVersionNegotiation = errors.New("quic: the server does not speak QUIC version 1")
)

// ConnectionError is the peer's CONNECTION_CLOSE (RFC 9000 §10.2), whatever
// it said: a transport error, a handshake it refused, an application code
// such as HTTP/3's H3_NO_ERROR, or NO_ERROR itself. A handshake this end's
// own TLS refused is not one of these: it is reported as the
// [crypto/tls.AlertError] in the error's chain.
type ConnectionError struct {
	// Code is the transport error code (RFC 9000 §20.1) — the crypto
	// range, 0x0100 to 0x01ff, is a TLS alert plus 0x0100 (RFC 9001 §4.8),
	// which is how a peer refuses a handshake: 0x0178 for no common ALPN —
	// or, when Application is set, the application's own code (§20.2).
	Code uint64
	// Application says the frame was the application flavour (type 0x1d):
	// Code is the application's, and the transport does not know what it
	// means.
	Application bool
	// Reason is the reason phrase the frame carried, usually empty; this
	// package never sends one.
	Reason string
}

func (e *ConnectionError) Error() string {
	kind := "transport"
	if e.Application {
		kind = "application"
	}
	if e.Reason == "" {
		return fmt.Sprintf("quic: the peer closed the connection: %s code %#x", kind, e.Code)
	}
	return fmt.Sprintf("quic: the peer closed the connection: %s code %#x %q", kind, e.Code, e.Reason)
}

// closeCodeFor picks the CONNECTION_CLOSE code an error deserves. A
// transportError anywhere in the chain carries its own — anywhere, because
// the sites that know the code (a stream's flow-control account, the
// parameter parser) sit below callers that add context with %w, and a
// direct assertion on the outermost error turned every one of those into
// INTERNAL_ERROR. A TLS alert anywhere in the chain maps to RFC 9001 §4.8's
// crypto range (0x0100 + alert), which is how the codes that RFC mandates —
// 0x0178 for ALPN failure (§8.1), 0x016d for missing transport parameters
// (§8.2), 0x010a for a KeyUpdate message (§6) — all reach the wire without
// this package naming each alert; everything else is INTERNAL_ERROR.
func closeCodeFor(err error) uint64 {
	if te, ok := errors.AsType[*transportError](err); ok {
		return te.code
	}
	if alert, ok := errors.AsType[tls.AlertError](err); ok {
		return transportCryptoErrorBase + uint64(uint8(alert))
	}
	return transportInternalError
}

// abort ends the connection over a protocol violation: record why, tell the
// peer with a CONNECTION_CLOSE, and close. The close carries the code, not
// the text — error strings are for this end's operator, and what they reveal
// about internals is nobody else's business (the GOAWAY on the HTTP/2 side
// made the same decision).
func (c *Conn) abort(err error) {
	code := closeCodeFor(err)
	c.fail(err)
	c.mu.Lock()
	c.sendCloseLocked(code, false)
	c.mu.Unlock()
	_ = c.Close()
}

// Close ends the connection, telling the peer with a CONNECTION_CLOSE frame
// (NO_ERROR) so it can drop its state now rather than wait out an idle
// period (§10.2). Close never blocks on the peer: the frame is sent once,
// best effort. If it is lost, a connection that holds the closing period —
// a [Listener]'s, or one [Conn.SetClosingPeriod] asked — sends it again to
// the next packet the peer sends; otherwise the peer's idle timer is the
// fallback the protocol designed for.
//
// The error return always comes back nil, kept for [io.Closer] rather than
// for anything it could report: a failed write here is already "best
// effort" and swallowed upstream (sendCloseLocked), not a reason for the
// caller to think closing failed.
func (c *Conn) Close() error {
	c.closeOnce(transportNoError, false)
	return nil
}

// CloseWithError ends the connection with an application-level code, which
// is how HTTP/3 says H3_NO_ERROR (§10.2's application close). The reason
// phrase is deliberately not sent; codes travel, prose stays home.
//
// Like [Conn.Close], the error return always comes back nil.
func (c *Conn) CloseWithError(code uint64) error {
	c.closeOnce(code, true)
	return nil
}

// Shutdown closes the connection once every packet this end sent has been
// acknowledged, so that a peer still reading the last stream bytes does not
// see the CONNECTION_CLOSE before them. [Conn.Close] sends the close at
// once, and what is still in flight — sent and not yet acknowledged, or
// queued for retransmission after a loss — goes down with the connection:
// correct for the transport, which promises nothing about delivery once
// closed, and a lost tail for the application above it. Shutdown is the
// close an application calls when it has finished writing and wants what it
// wrote to arrive, the way [net/http.Server.Shutdown] waits for its
// connections.
//
// Shutdown returns nil once the flight is empty and the connection closed,
// and the context's error when the context ends first — the connection is
// closed in both cases, Shutdown never leaves it open. A context that is
// already done closes at once, without looking at the flight, and reports
// its error: the caller did not want to wait, and nothing confirmed delivery.
// A connection the peer closed meanwhile, or one already closed by this
// end, returns nil: there is nothing left to deliver. Shutdown does not wait
// for the peer to read, only for it to acknowledge; a peer that
// acknowledges and then crashes is beyond any close's reach.
func (c *Conn) Shutdown(ctx context.Context) error {
	err := c.drain(ctx)
	c.closeOnce(transportNoError, false)
	return err
}

// ShutdownWithError is [Conn.Shutdown] with an application-level close code,
// which is how HTTP/3 ends a graceful shutdown with H3_NO_ERROR (RFC 9114
// §5.2) once its last response has been acknowledged. It waits as Shutdown
// does and closes in every case, like it; only the frame the peer reads
// differs, as between [Conn.Close] and [Conn.CloseWithError].
func (c *Conn) ShutdownWithError(ctx context.Context, code uint64) error {
	err := c.drain(ctx)
	c.closeOnce(code, true)
	return err
}

// drain waits under mu until no space holds an ack-eliciting packet in
// flight or a payload queued for retransmission. The waits ride c.cond,
// which handleAck broadcasts on every new acknowledgement and closeOnce on
// the way out; the context's expiry broadcasts too, so the wait never
// outlives the deadline. A retransmission queued by loss detection goes
// out as a new packet whose acknowledgement wakes the wait like any other.
func (c *Conn) drain(ctx context.Context) error {
	stop := context.AfterFunc(ctx, func() {
		c.mu.Lock()
		c.cond.Broadcast()
		c.mu.Unlock()
	})
	defer stop()
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		select {
		case <-c.closed:
			return nil
		default:
		}
		if c.draining {
			return nil // the peer closed first; its close is on its way to closeOnce
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if c.flightEmptyLocked() {
			return nil
		}
		c.cond.Wait()
	}
}

// Ping sends a PING frame (RFC 9000 §19.2) in a packet of its own and
// waits until the peer acknowledges that packet or a later one: one round
// trip, measured on the path rather than estimated from it. What this end
// wrote before the call left in earlier packets, so the acknowledgement
// says the peer has had a round trip to see it — which is how an HTTP/3
// server gives the requests already on their way time to arrive between
// its two GOAWAY frames (RFC 9114 §5.2), as an HTTP/2 server does with its
// own PING. Any packet sent after the PING counts, so a lost PING ends the
// wait at the acknowledgement of whatever follows it, a probe included.
//
// Ping returns nil at the acknowledgement and the context's error when the
// context ends first. On a connection that is closed, closing, or without
// 1-RTT keys yet, it returns an error at once or as soon as the close is
// known. Safe from any goroutine.
func (c *Conn) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, func() {
		c.mu.Lock()
		c.cond.Broadcast()
		c.mu.Unlock()
	})
	defer stop()
	c.mu.Lock()
	defer c.mu.Unlock()
	sp := &c.spaces[spaceApplication]
	// Every packet from here on is numbered from first up, skipped numbers
	// included, so an acknowledgement at or past it is one of them.
	first := sp.nextPN
	if err := c.sendPacketLocked(spaceApplication, []byte{framePing}, sendOpts{}); err != nil {
		return err
	}
	for {
		select {
		case <-c.closed:
			return errClosed
		default:
		}
		if c.draining {
			return errClosed
		}
		if sp.sent.hasLargestAcked && sp.sent.largestAcked >= first {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		c.cond.Wait()
	}
}

// flightEmptyLocked reports whether every live space has nothing left to
// deliver: no ack-eliciting packet awaiting acknowledgement and no payload
// waiting to go again. ACK-only packets do not count — the peer never
// acknowledges them (RFC 9000 §13.2.1), and nothing of the application's
// rides in them.
func (c *Conn) flightEmptyLocked() bool {
	for space := range c.spaces {
		sp := &c.spaces[space]
		if sp.discarded {
			continue
		}
		if _, ok := sp.sent.oldestEliciting(); ok || len(sp.retrans) > 0 {
			return false
		}
	}
	return true
}

// closeOnce is the body Close and CloseWithError share: send the
// CONNECTION_CLOSE (transport or application flavour), then release
// everyone waiting on the connection. Only the first call does anything.
func (c *Conn) closeOnce(code uint64, app bool) {
	c.once.Do(func() {
		c.mu.Lock()
		c.sendCloseLocked(code, app)
		c.enterClosingLocked()
		c.mu.Unlock()
		close(c.closed)
		c.wakeOwnedReadLoop()
		c.mu.Lock()
		c.cond.Broadcast() // writers parked on credit learn it is over
		c.mu.Unlock()
	})
}

// wakeOwnedReadLoop jolts the exclusive-socket readLoop out of its blocking
// ReadFrom with an already-expired read deadline — without it the goroutine
// and its receive buffer would linger until the next idle wakeup, as long as
// the idle period away. Called after close(c.closed): the loop re-checks
// c.closed after arming each deadline, so whichever SetReadDeadline lands
// last, it still exits promptly. A listener-fed connection selects on
// c.closed directly and needs no jolt.
func (c *Conn) wakeOwnedReadLoop() {
	if c.incoming == nil {
		c.mu.Lock()
		pc := c.pc
		c.mu.Unlock()
		_ = pc.SetReadDeadline(time.Now())
	}
}

// sendCloseLocked writes the CONNECTION_CLOSE, once, unless the peer closed
// first — an endpoint in the draining state sends nothing at all (§10.2.2).
//
// Before the handshake is confirmed the close goes out at *every* level that
// can still seal, not only the newest (§10.2.3): the newest keys are the ones
// the peer is least certain to have — a server closing during the handshake
// may hold Handshake keys the client has not derived yet — and a close the
// peer cannot read is a client that eats its whole handshake timeout instead.
func (c *Conn) sendCloseLocked(code uint64, app bool) {
	if c.closeSent || c.draining {
		return
	}
	c.closeSent = true

	// The most recent level with a sealer is the one the peer most likely
	// reads; everything below it is sealable insurance pre-confirmation.
	newest := -1
	for s := spaceApplication; s >= spaceInitial; s-- {
		if !c.spaces[s].discarded && c.spaces[s].sealer != nil {
			newest = s
			break
		}
	}
	if newest < 0 {
		return
	}
	lowest := newest
	if !c.handshakeConfirmed {
		lowest = spaceInitial
	}
	// The close leaves on its own, now: whatever a writer gathered or
	// batched before it goes first, and nothing of the close joins a batch
	// — the closing state below keeps each of its datagrams as written.
	c.flushDatagramLocked()
	c.flushBatchLocked()
	batching := c.batching
	c.batching = false
	c.closing.capturing = c.holdsClosingLocked()
	defer func() {
		c.closing.capturing = false
		c.batching = batching
	}()
	now := time.Now()
	for space := newest; space >= lowest; space-- {
		sp := &c.spaces[space]
		if sp.discarded || sp.sealer == nil {
			continue
		}
		typ, spaceCode := uint64(frameConnectionClose), code
		if app {
			if space == spaceApplication {
				typ = frameConnectionClose | 1
			} else {
				// §10.2.3: an application close leaving in an Initial or
				// Handshake packet would leak application state under keys
				// anyone (Initial) or any observer of the handshake may hold;
				// it travels as the transport form with APPLICATION_ERROR.
				spaceCode = transportApplicationError
			}
		}
		frame := AppendVarint(nil, typ)
		frame = AppendVarint(frame, spaceCode)
		if typ == frameConnectionClose {
			frame = AppendVarint(frame, 0) // the frame type being answered; none
		}
		frame = AppendVarint(frame, 0) // no reason phrase, deliberately

		// closeSent is already set, so sendPacketLocked would refuse; write
		// the packet directly, marked as the close so writePacketLocked
		// lets it through. Best effort: a loss is repaired by the closing
		// state when the connection holds one, and by the peer's idle
		// timer otherwise.
		_ = c.writePacketLocked(sp, space, frame, nil, sendOpts{ackOnly: true, close: true}, now)
	}
}

// idleExpired closes silently: an idle timeout is the one end of a
// connection that sends nothing, because the whole premise is that the peer
// is gone (§10.1). Err reports [ErrIdleTimeout]: the protocol expects an
// unused connection to end this way, and whether that was expected here is
// for the caller to say.
func (c *Conn) idleExpired() {
	c.fail(ErrIdleTimeout)
	c.mu.Lock()
	c.closeSent = true // suppress the CONNECTION_CLOSE a Close would send
	c.mu.Unlock()
	_ = c.Close()
}

// The closing state (RFC 9000 §10.2.1). Once this end's CONNECTION_CLOSE
// has left, the peer may still be sending: the close itself may have been
// lost, or packets were in flight when it went. A connection that simply
// vanished would answer those with a stateless reset (§10.3) — the listener
// no longer routing its identifier — and the peer would learn that this end
// lost its state, never the code it closed with. So a connection that holds
// the state stays routable for three probe timeouts after its close, and
// every datagram attributed to it in that time earns the same sealed
// CONNECTION_CLOSE again, at a bounded rate.
//
// The draining state (§10.2.2) is the same period entered the other way,
// when the peer closed first — or reset the connection statelessly, which
// §10.3.1 treats alike: nothing is kept to send, since a draining endpoint
// sends nothing at all, and what arrives is absorbed so that the late
// packet earns neither a reply nor a stateless reset.
//
// A [Listener]'s connections always hold these states: §10.2 asks servers
// that keep their socket open not to end them early. A connection that
// owns its socket ([Dial], the standalone [Accept]) holds them only when
// [Conn.SetClosingPeriod] asked: by default it gives the socket back to its
// caller the moment Close returns — the caller may close it or dial again
// on it — which §10.2 allows an endpoint able to close its socket. When it
// holds them, its own read loop keeps reading the socket for the period,
// and [Conn.Wait] tells the caller when the socket is free again.
//
// What is kept is what §10.2.1 says suffices — the sealed datagram(s) of
// the close, where they went, and a count of what arrived — not the keys:
// the datagrams are never opened. The packet goes out again byte for byte,
// which §10.2.1 names as the exception to the one-packet-number-per-packet
// rule (§12.3) made for exactly this. Two bounds on what leaves: the reply
// goes out on the first datagram received, then on the second, the fourth,
// the eighth — the "progressively increasing number of received packets"
// §10.2.1 suggests, since the first reply is the repair and the rest are
// insurance against its own loss — and, because nothing received is
// authenticated, no more than three times the bytes received from the
// peer's address (§10.2.1's rule for an endpoint without keys), from the
// peer's address only (a new address is unvalidated, and discarded).
type closingState struct {
	// active is read by Listener.dispatch on every datagram without
	// taking the connection's mu: the hot path of every open connection
	// is one atomic load, and only a closing connection's datagrams pay
	// for the lock below.
	active atomic.Bool
	// capturing makes writeLocked keep a copy of what it writes, for the
	// duration of sendCloseLocked.
	capturing bool
	datagrams [][]byte
	via       net.PacketConn
	to        net.Addr
	received  int   // datagrams from the peer since the close left
	nextReply int   // the count at which the close goes out again
	sent      int64 // bytes sent while closing, against 3× received
	recv      int64
	// hold is [Conn.SetClosingPeriod]'s choice for a connection that owns
	// its socket; end is when its read loop stops answering — zeroed by a
	// [Conn.Wait] whose context ended — and left records that the state is
	// over and gone closed or about to be. loop says a socket-owning read
	// loop runs: then the loop's exit, not the close, is what frees the
	// socket, so it is the loop that closes gone (releaseSocket).
	hold bool
	end  time.Time
	left bool
	loop bool
}

// holdsClosingLocked reports whether a close keeps the closing or draining
// state: always for a Listener's connection, on request for one that owns
// its socket.
func (c *Conn) holdsClosingLocked() bool {
	return c.incoming != nil || c.closing.hold
}

// closingPeriodLocked is how long the closing state lasts: three probe
// timeouts (§10.2), computed at the moment the close leaves.
func (c *Conn) closingPeriodLocked() time.Duration {
	return 3 * c.rtt.pto(c.peerMaxAckDelayLocked())
}

// keepClosingDatagramLocked is writeLocked's hook: a copy of each datagram
// of the close, and the socket and address it left by.
func (c *Conn) keepClosingDatagramLocked(pc net.PacketConn, b []byte, to net.Addr) {
	c.closing.datagrams = append(c.closing.datagrams, append([]byte(nil), b...))
	c.closing.via, c.closing.to = pc, to
}

// enterClosingLocked runs in closeOnce after the close left: when a
// datagram of it was kept, or the peer's close put the connection in the
// draining state, a connection that holds these states stays routable for
// the period and answers (or absorbs); otherwise — a connection that gave
// its socket back, or one that ended without a close on either side: idle
// timeout — there is nothing to keep and gone closes now, or, on a socket
// a read loop still reads, as soon as that loop returns: until then the
// loop may still arm a deadline on the socket or read from it, and Wait
// would hand the caller a socket that is not yet free. A Listener's
// connection leaves on a timer; one that owns its socket leaves when its
// read loop, which keeps answering until end, returns (holdClosing).
func (c *Conn) enterClosingLocked() {
	if len(c.closing.datagrams) == 0 && (!c.draining || !c.holdsClosingLocked()) {
		c.closing.left = true
		if !c.closing.loop {
			close(c.gone)
		}
		return
	}
	c.closing.nextReply = 1
	c.closing.active.Store(true)
	if c.incoming != nil {
		time.AfterFunc(c.closingPeriodLocked(), c.leaveClosing)
		return
	}
	c.closing.end = time.Now().Add(c.closingPeriodLocked())
}

// leaveClosing ends the closing state: the kept datagrams are released,
// and gone tells the listener it may forget the connection, and
// [Conn.Wait] that the socket is free — after which a late packet gets the
// stateless reset §10.2 allows "once its closing state ends". Idempotent:
// the period's timer and a Wait cutting it short may both call it.
func (c *Conn) leaveClosing() {
	c.mu.Lock()
	if c.closing.left {
		c.mu.Unlock()
		return
	}
	c.closing.left = true
	c.closing.active.Store(false)
	c.closing.datagrams = nil
	c.mu.Unlock()
	close(c.gone)
}

// holdClosing is the end of a socket-owning read loop: when the close left
// the connection in a held closing or draining state, keep reading the
// socket until the period ends and hand each datagram to answerClosing,
// then leave. A read error other than the deadline ends the state at once:
// it is the caller closing the socket — the early end §10.2 allows an
// endpoint that can — or a socket that no longer reads, and spinning on it
// for three probe timeouts would answer nobody.
func (c *Conn) holdClosing(buf []byte) {
	if !c.closing.active.Load() {
		c.releaseSocket()
		return
	}
	defer c.leaveClosing()
	for {
		c.mu.Lock()
		pc, end := c.pc, c.closing.end
		c.mu.Unlock()
		if !time.Now().Before(end) {
			return
		}
		if err := pc.SetReadDeadline(end); err != nil {
			return
		}
		// Re-read after arming, as readLoop does: a Wait that zeroed end
		// and expired the deadline before the line above would otherwise
		// leave this read parked until end.
		c.mu.Lock()
		end = c.closing.end
		c.mu.Unlock()
		if !time.Now().Before(end) {
			return
		}
		n, from, err := pc.ReadFrom(buf)
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				continue
			}
			return
		}
		c.answerClosing(buf[:n], from)
	}
}

// releaseSocket is the exit of a socket-owning read loop that held no
// closing state: the close already ran (enterClosingLocked) and left gone
// for the loop to close, now that it has stopped touching the socket.
func (c *Conn) releaseSocket() {
	c.mu.Lock()
	release := c.closing.loop && c.closing.left
	c.closing.loop = false
	c.mu.Unlock()
	if release {
		close(c.gone)
	}
}

// SetClosingPeriod makes a connection that owns its socket ([Dial], the
// standalone [Accept]) hold the closing and draining states of RFC 9000
// §10.2 after it closes, as a [Listener]'s connections always do: for
// three probe timeouts its read loop keeps reading the socket, answers a
// peer that missed this end's CONNECTION_CLOSE with the close again, and
// absorbs what arrives after the peer's own close. Without it — the
// default — the socket is the caller's again the moment Close returns, and
// a late packet meets whatever the caller does with it next.
//
// A server that keeps the socket open to accept further connections should
// turn it on: §10.2 asks such servers not to end these states early. While
// the state lasts the socket is not the caller's; [Conn.Wait] reports when
// it is, and closing the socket ends the state early, which §10.2 allows.
// The choice is read when the connection closes; it changes nothing on a
// Listener's connection. Safe from any goroutine.
func (c *Conn) SetClosingPeriod(on bool) {
	c.mu.Lock()
	c.closing.hold = on
	c.mu.Unlock()
}

// Wait blocks until the connection has closed and released its socket:
// at the close for a connection that holds no closing period, at the end
// of the period for one that does ([Conn.SetClosingPeriod], a [Listener]'s
// connection). It returns nil then. Wait does not close the connection.
//
// When ctx ends first, Wait returns its error. If the connection has closed
// by then, Wait first ends its closing period early, so that a closed
// connection has released its socket whenever Wait returns; an open one is
// left as it is. Safe from any goroutine.
func (c *Conn) Wait(ctx context.Context) error {
	select {
	case <-c.gone:
		return nil
	case <-ctx.Done():
	}
	select {
	case <-c.gone: // both were ready, and select picks at random
		return nil
	default:
	}
	select {
	case <-c.closed:
	default:
		return ctx.Err()
	}
	if c.incoming != nil {
		c.leaveClosing()
	} else {
		c.mu.Lock()
		c.closing.end = time.Time{}
		pc := c.pc
		c.mu.Unlock()
		_ = pc.SetReadDeadline(time.Now())
	}
	<-c.gone
	return ctx.Err()
}

// answerClosing is what Listener.dispatch does with a datagram it routed
// to a connection in the closing state: count it and, at the counts and
// within the byte budget described on closingState, send the kept close
// again. The return says whether the datagram was the closing state's to
// handle; false means the connection is open and the datagram goes to its
// read loop as usual. Nothing in the datagram is parsed or opened.
func (c *Conn) answerClosing(datagram []byte, from net.Addr) bool {
	if !c.closing.active.Load() {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cs := &c.closing
	if len(cs.datagrams) == 0 || !sameAddr(from, cs.to) {
		return true // the state ended under us, or an unvalidated address
	}
	cs.received++
	cs.recv += int64(len(datagram))
	if cs.received != cs.nextReply {
		return true
	}
	cs.nextReply *= 2
	for _, d := range cs.datagrams {
		if cs.sent+int64(len(d)) > 3*cs.recv {
			break
		}
		cs.sent += int64(len(d))
		_, _ = cs.via.WriteTo(d, cs.to)
	}
	return true
}
