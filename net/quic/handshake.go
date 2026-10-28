package quic

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"time"

	"github.com/mlagarrigue/sluice/internal/varint"
)

// installInitial derives the Initial keys, which need no handshake: they come
// from the connection identifier the client picked, so anyone who saw the
// first packet can read them. They authenticate, they do not hide.
func (c *Conn) installInitial(dcid []byte) error {
	s, err := initialSecrets(dcid)
	if err != nil {
		return err
	}
	return c.installInitialKeys(s, nil)
}

// installInitialKeys installs already-derived Initial secrets; opener, when
// non-nil, is one already built for the peer's direction.
func (c *Conn) installInitialKeys(s secrets, opener *packetOpener) error {
	mine, theirs := s.Client, s.Server
	if !c.isClient {
		mine, theirs = s.Server, s.Client
	}
	sealer, err := newPacketSealer(mine)
	if err != nil {
		return err
	}
	if opener == nil {
		if opener, err = newPacketOpener(theirs); err != nil {
			return err
		}
	}
	c.mu.Lock()
	c.spaces[spaceInitial].sealer = sealer
	c.spaces[spaceInitial].opener = opener
	c.mu.Unlock()
	return nil
}

func (c *Conn) start() error {
	c.tls.SetTransportParameters(appendParameters(nil, c.params))
	if err := c.tls.Start(context.Background()); err != nil {
		return fmt.Errorf("quic: starting the handshake: %w", err)
	}
	return nil
}

// endTLS stops crypto/tls's handshake goroutine. Start runs the handshake on
// a goroutine of its own, which returns when the handshake completes and
// otherwise waits for data until [tls.QUICConn.Close]: a connection
// abandoned mid-handshake — a client that sent one Initial and left, a
// Dial that timed out — kept that goroutine and the TLS state it holds for
// the life of the process. Called by the goroutine that feeds TLS (QUICConn
// is not safe for concurrent use), once nothing more will be fed; a no-op
// on a completed handshake, one never started, or a second call.
func (c *Conn) endTLS() {
	if c.tls != nil {
		_ = c.tls.Close()
	}
}

// handshake runs until TLS says it is done, carrying CRYPTO both ways. Lost
// flights are the timer loop's problem — its probe timeout retransmits them —
// so this loop only has to keep reading.
//
// ctx's deadline replaces the default one, and its cancellation closes the
// connection, which every wait below already watches for.
func (c *Conn) handshake(ctx context.Context) error {
	err := c.driveHandshake(ctx)
	if err != nil {
		c.endTLS()
	}
	return err
}

// driveHandshake is handshake's body; every error it returns has closed the
// connection.
//
// driveHandshake runs in this order: start TLS on a client; set the
// handshake's deadline — the default or the handshake budget, replaced by
// ctx's, extendable only when this package chose it; tie ctx's cancellation to
// the close; then loop until the handshake completes or fails — pump TLS's
// events, send the flight they produce as one datagram, send the ticket a
// server owes once done, read and process the next datagram, give up after too
// many refusals heard from nobody or past the deadline, and report a
// connection the peer closed meanwhile as itself.
func (c *Conn) driveHandshake(ctx context.Context) error {
	if c.isClient {
		if err := c.start(); err != nil {
			_ = c.Close()
			return err
		}
	}
	start := time.Now()
	c.mu.Lock()
	budget := DefaultHandshakeTimeout
	if c.handshakeBudget > 0 {
		budget = c.handshakeBudget
	}
	deadline := start.Add(budget)
	ctxDeadline, fromCtx := ctx.Deadline()
	if fromCtx {
		deadline = ctxDeadline
	}
	// Only a deadline this package chose may move: a caller's context says
	// what the caller wants, and a client has no address of the peer's to
	// see validated.
	c.hsStart, c.hsDeadline, c.hsExtendable = start, deadline, !fromCtx && !c.isClient
	c.mu.Unlock()
	fired := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		c.fail(abandonedErr(ctx))
		_ = c.Close()
		close(fired)
	})
	defer stop()
	c.mu.Lock()
	c.hsSyncRead = true
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.hsSyncRead = false
		c.mu.Unlock()
	}()
	// ctxErr turns a deadline the context set into the context's error,
	// rather than a bare timeout — even when the socket's read deadline
	// fires a hair before the context's own timer marks it done.
	ctxErr := func(err error) error {
		if ctx.Err() != nil {
			return abandonedErr(ctx)
		}
		if fromCtx && !time.Now().Before(deadline) {
			return fmt.Errorf("%w: the handshake was abandoned: %w", ErrQUIC, context.DeadlineExceeded)
		}
		return err
	}
	var (
		heard     bool  // a datagram was read from the socket
		sockErr   error // the last socket read error
		sockErrs  int   // socket read errors so far
		readFails int   // consecutive ones, for the backoff
	)
	for {
		// The flight TLS produces leaves as one datagram where it fits
		// (§12.2): gathered across pump, flushed before the next read.
		c.mu.Lock()
		c.gatherLocked(true)
		deadline = c.hsDeadline // extendHandshakeLocked may have moved it
		c.mu.Unlock()
		done, err := c.pump()
		if done && err == nil && c.sendsTicket {
			err = c.issueTicket()
		}
		c.mu.Lock()
		if done {
			c.handshakeComplete()
		}
		c.gatherLocked(false)
		c.mu.Unlock()
		if err != nil {
			c.abort(err)
			return err
		}
		if done {
			if !stop() {
				// The context ended it in the same instant; the
				// connection is closed, or about to be.
				<-fired
				return c.endedDuringHandshakeErr()
			}
			// From here the read loop, not the close, frees the socket
			// (enterClosingLocked) — unless the close already ran.
			c.mu.Lock()
			c.closing.loop = c.incoming == nil && !c.closing.left
			c.mu.Unlock()
			go c.readLoop()
			return nil
		}
		c.mu.Lock()
		wRefusals, wRefusal := c.writeRefusals, c.writeRefusal
		c.mu.Unlock()
		if sockErr == nil {
			sockErr = wRefusal
		}
		if !heard && sockErrs+wRefusals >= handshakeRefusalLimit {
			c.abort(sockErr)
			return sockErr
		}
		if time.Now().After(deadline) {
			err := fmt.Errorf("%w: the handshake did not complete", ErrQUIC)
			if sockErr != nil && !heard {
				// Refused and never heard from: the refusal is the
				// better diagnosis than a bare timeout.
				err = sockErr
			}
			err = ctxErr(err)
			c.abort(err)
			return err
		}
		// A connection that ended under the handshake — a Version
		// Negotiation, the peer's CONNECTION_CLOSE (a listener's stateless
		// refusal included) — is reported now and as itself: reading on
		// would only wait out the deadline and call it a timeout.
		select {
		case <-c.closed:
			return c.endedDuringHandshakeErr()
		default:
		}
		got, err := c.readOne(deadline)
		if got {
			heard = true
			readFails = 0
		}
		if err == nil {
			continue
		}
		select {
		case <-c.closed:
			return c.endedDuringHandshakeErr()
		default:
		}
		if !got && !errors.Is(err, os.ErrDeadlineExceeded) &&
			!errors.Is(err, net.ErrClosed) && !errors.Is(err, errClosed) {
			// A socket read error under the handshake is, on UDP, an ICMP
			// surfacing: ECONNREFUSED on a connected Linux socket,
			// WSAECONNRESET on Windows. Unauthenticated, so read past like
			// readLoop does — one forged ICMP must not kill a handshake —
			// but a port that is really closed answers every flight with
			// one, and Dial should say so, not wait out the deadline.
			// The rule: fail with the socket's error once
			// handshakeRefusalLimit of them arrived with no datagram ever
			// read; once something has been heard, only the deadline ends
			// the handshake.
			c.noteDiscard()
			sockErr = err
			sockErrs++
			readFails++
			if !heard && sockErrs+wRefusals >= handshakeRefusalLimit {
				c.abort(err)
				return err
			}
			if readFails > 1 && !c.readBackoff(readFails) {
				return c.endedDuringHandshakeErr()
			}
			continue
		}
		c.mu.Lock()
		deadline = c.hsDeadline
		c.mu.Unlock()
		if !got && errors.Is(err, os.ErrDeadlineExceeded) && ctx.Err() == nil && time.Now().Before(deadline) {
			// Not the deadline: a send refusal woke the read (hsWake) so
			// the refusal count at the top of the loop is re-taken now,
			// or the deadline moved while the read waited on the old one.
			continue
		}
		if !got && sockErr != nil && !heard && errors.Is(err, os.ErrDeadlineExceeded) {
			err = sockErr // as at the deadline check above
		}
		if errors.Is(err, os.ErrDeadlineExceeded) {
			err = ctxErr(err)
		}
		c.abort(err)
		return err
	}
}

// extendHandshakeLocked moves a server's handshake deadline, once, to the
// handshake's start plus the connection's idle timeout, when that is later:
// called when the client's first Handshake packet authenticates, which
// validates its address (RFC 9000 §8.1). From then on the client is as
// proven as any established peer, and RFC 9000 §10.1 ends those by the idle
// timeout, not by a clock started at their first packet. The fixed deadline
// closed handshakes that were alive: under 30 % loss a client's Finished
// went missing, its next probe was due 12 s in, and the server had closed
// with INTERNAL_ERROR at 10 s (runner handshakeloss against ngtcp2 and
// quinn). The pending slot a client can hold grows with it, from the
// HandshakeTimeout to the idle timeout — but only for a client that
// answered from its own address with the handshake keys, which could as
// well complete and hold a connection that long. Callers hold mu.
func (c *Conn) extendHandshakeLocked() {
	if !c.hsExtendable {
		return
	}
	c.hsExtendable = false
	if c.idleTimeout == 0 {
		return
	}
	if d := c.hsStart.Add(c.idleTimeout); d.After(c.hsDeadline) {
		c.hsDeadline = d
	}
}

// abandonedErr is the error a handshake ended by its context reports.
func abandonedErr(ctx context.Context) error {
	return fmt.Errorf("%w: the handshake was abandoned: %w", ErrQUIC, context.Cause(ctx))
}

// handshakeRefusalLimit is how many socket refusals, read or send side, a
// handshake that has heard nothing yet takes as "the port is closed". Two,
// not one, so a single forged ICMP is survived; not more, because a closed
// port answers one refusal per flight and flights are PTO-spaced: counting
// the send-side one too, a Dial to a closed port on Linux fails at the
// first probe, ~1 s, where a third would wait to ~3 s.
const handshakeRefusalLimit = 2

// endedDuringHandshakeErr is why a connection closed before its handshake
// finished: the recorded cause — the peer's close is one, whatever its
// code, so a refused ALPN reaches the dialer as the [*ConnectionError]
// carrying 0x0178 — else this end's own Close, which is still a handshake
// that did not happen.
func (c *Conn) endedDuringHandshakeErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closeErr != nil {
		return c.closeErr
	}
	return fmt.Errorf("%w: the connection closed during the handshake", ErrQUIC)
}

// issueTicket has crypto/tls write the connection's one session ticket —
// marked for 0-RTT, with what earlyGate needs to admit it, when the
// listener has EarlyData — and sends it, in the application space where RFC 9001 §4.5 puts it, beside
// the HANDSHAKE_DONE handshakeComplete adds — gathered into one datagram.
// Called once the handshake is complete, on a server: before then
// crypto/tls refuses, and on a client it has no ticket to send.
func (c *Conn) issueTicket() error {
	opts, ok := c.ticketOptions(time.Now())
	if !ok {
		return nil // the chain is over: the client's next connection is a full handshake
	}
	if err := c.tls.SendSessionTicket(opts); err != nil {
		return err
	}
	_, err := c.pump()
	return err
}

// installEarly installs the 0-RTT read keys of a server whose resumption's
// 0-RTT was accepted. Nothing is read with them yet: releaseEarly does that
// once this end can also write in 1-RTT.
func (c *Conn) installEarly(suiteID uint16, secret []byte) error {
	s, err := suiteFor(suiteID)
	if err != nil {
		return err
	}
	opener, err := newOpenerSuite(secret, s)
	if err != nil {
		return err
	}
	// The ticket was reserved when crypto/tls asked about the resumption
	// (eligible): the 0-RTT it has just accepted is the one the reservation
	// is for, and the reservation is now spent for good.
	c.mu.Lock()
	c.earlyOpener = opener
	c.earlyTaken = true
	c.mu.Unlock()
	return nil
}

// releaseEarlyTicketLocked gives the listener's register back a ticket this
// connection reserved (eligible) and never used: the handshake ended
// without the early keys, or the connection ends before it did. Idempotent;
// a reservation installEarly confirmed stays spent. Callers hold mu.
func (c *Conn) releaseEarlyTicketLocked() {
	if c.earlyTicket == nil {
		return
	}
	if !c.earlyTaken && c.early != nil {
		c.early.release(c.earlyTicket)
	}
	c.earlyTicket = nil
}

// releaseEarly runs on a server whose 0-RTT was accepted, once its 1-RTT
// write keys are in: crypto/tls hands them out in the same flight as the
// early secret, and nothing — an ACK for a 0-RTT packet, the application's
// first write — can leave in the application space before them. It reads
// the 0-RTT packets that arrived before the early secret and tells the
// listener the connection may be accepted now.
func (c *Conn) releaseEarly() error {
	c.mu.Lock()
	if c.earlyOpener == nil || c.earlyReleased {
		c.mu.Unlock()
		return nil
	}
	c.earlyReleased = true
	held := c.deferredEarly
	c.deferredEarly = nil
	peer, rxECN := c.peer, c.rxECN
	c.mu.Unlock()
	close(c.earlyAccepted)
	for _, pkt := range held {
		c.mu.Lock()
		c.rxECN, c.rxReplay = pkt.ecn, true
		c.mu.Unlock()
		err := c.receiveAt(pkt.raw, peer, pkt.at)
		c.mu.Lock()
		c.rxReplay = false
		c.mu.Unlock()
		if err != nil {
			return err
		}
	}
	c.mu.Lock()
	c.rxECN = rxECN
	c.mu.Unlock()
	return nil
}

// HandshakeComplete returns a channel closed when the handshake completes.
// A connection from [Dial] has completed it already, and so has one from
// [Listener.Accept] unless the listener has [ListenerConfig.EarlyData]:
// that one may not have, and what it received until then may be a replay of
// another client's first flight. A connection whose handshake fails never closes the channel:
// select on [Conn.Done] beside it.
func (c *Conn) HandshakeComplete() <-chan struct{} { return c.hsComplete }

// handshakeComplete is the transition TLS reports: the server confirms at
// once and tells the client with HANDSHAKE_DONE; the client confirms when
// that frame arrives (§4.1.2 of RFC 9001). Handshake keys are dropped on
// confirmation (§4.9.2). Called under mu.
func (c *Conn) handshakeComplete() {
	close(c.hsComplete)
	if !c.isClient {
		c.control = varint.Append(c.control, frameHandshakeDone)
		c.confirmLocked()
		c.flushControlLocked()
	}
}

// confirmLocked marks the handshake confirmed and discards handshake keys.
func (c *Conn) confirmLocked() {
	if c.handshakeConfirmed {
		return
	}
	c.rxProgress = true
	c.handshakeConfirmed = true
	c.peerAddrValidated = true
	c.discardSpaceLocked(spaceHandshake)
	// Both ends now offer the alternatives a migrating, rebinding or
	// rotation-forcing peer will need.
	c.issueLocalCIDsLocked()
	// A client whose server would rather be reached elsewhere starts
	// validating that path now (§9.6.2) — the earliest moment the section
	// allows, and the connection keeps using the handshake's path until
	// the answer arrives.
	if c.isClient && c.preferred != nil && !sameAddr(c.preferred, c.peer) {
		c.probePreferredAddressLocked(time.Now())
	}
	// DPLPMTUD's BASE state (RFC 9000 §14.3.1): the path may now be probed
	// for more than 1200 bytes.
	c.maybeProbePMTULocked()
}

// discardSpaceLocked drops a space's keys and every timer and in-flight
// packet with them (RFC 9001 §4.9): what was sent there is either implicitly
// delivered — the handshake moved past it — or gone for good, and
// retransmitting it under discarded keys would be garbage on the wire.
func (c *Conn) discardSpaceLocked(space int) {
	sp := &c.spaces[space]
	if sp.discarded {
		return
	}
	sp.discarded = true
	sp.sealer, sp.opener = nil, nil
	_, inFlight := sp.sent.takeAll()
	sp.retrans = nil
	c.cc.onLifted(inFlight)
	c.deferred[space] = nil
	sp.ack = ackTracker{}
	// RFC 9002 A.11: the backoff was about this space's unanswered
	// probes, and the handshake just moved past it.
	c.ptoCount = 0
	c.kickTimer()
}

// pump drains the events crypto/tls has ready, sending what it asks to send
// and installing what it hands back. done reports that the handshake
// completed somewhere in the drain; the drain runs to the end of the queue
// regardless, because crypto/tls queues the 1-RTT read secret right behind
// QUICHandshakeDone (RFC 9001 §5.7: no 1-RTT packet is decrypted before the
// handshake completes), and a pump that stopped at the first left the key
// in the queue: every client, and every server without tickets, ended its
// handshake with no 1-RTT opener, and the 1-RTT packets already deferred —
// the request a client coalesces with its Finished, a server's 0.5-RTT data
// — waited for the peer's next datagram, a round trip plus its
// max_ack_delay later.
func (c *Conn) pump() (done bool, err error) {
	for {
		e := c.tls.NextEvent()
		if e.Kind == tls.QUICNoEvent {
			return done, nil
		}
		d, err := c.handleTLSEvent(e)
		if err != nil {
			return done, err
		}
		done = done || d
	}
}

// handleTLSEvent acts on one crypto/tls event; done reports the handshake
// complete.
func (c *Conn) handleTLSEvent(e tls.QUICEvent) (done bool, err error) {
	switch e.Kind {
	case tls.QUICSetReadSecret, tls.QUICSetWriteSecret:
		if e.Level == tls.QUICEncryptionLevelEarly {
			// The 0-RTT secret, never filed under the application space
			// (spaceFor would, and it would seal 1-RTT packets with a key
			// the peer holds only for 0-RTT). On a server, crypto/tls hands
			// out the read secret only for a resumption whose 0-RTT it
			// accepted, which the QUICResumeSession case below allows only
			// when the listener admits the ticket: the client's 0-RTT
			// packets are read with it (installEarly). On a client, the
			// write secret comes only when DialEarly offered 0-RTT — the
			// same case declines it for Dial — and seals what is sent
			// before the 1-RTT keys (installEarlySealer).
			switch {
			case !c.isClient && e.Kind == tls.QUICSetReadSecret:
				return false, c.installEarly(e.Suite, e.Data)
			case c.isClient && e.Kind == tls.QUICSetWriteSecret && c.wantsEarly:
				return false, c.installEarlySealer(e.Suite, e.Data)
			}
			return false, nil
		}
		if err := c.install(e.Level, e.Suite, e.Data, e.Kind == tls.QUICSetWriteSecret); err != nil {
			return false, err
		}
		if e.Level == tls.QUICEncryptionLevelApplication && e.Kind == tls.QUICSetWriteSecret {
			if !c.isClient {
				return false, c.releaseEarly()
			}
			// RFC 9001 §4.9.3: a client SHOULD discard its 0-RTT keys as
			// soon as it installs 1-RTT keys; from here everything leaves
			// in 1-RTT, retransmissions of 0-RTT data included.
			c.mu.Lock()
			c.earlySealer = nil
			c.cond.Broadcast() // a writer waiting since a refused 0-RTT (awaitSealLocked)
			c.mu.Unlock()
		}
		return false, nil
	case tls.QUICResumeSession:
		if !c.isClient {
			// The chain this resumption continues, for the ticket this
			// connection issues (ticketOptions).
			c.ticketChainOrigin = ticketChainOrigin(e.SessionState.Extra)
			// A client resumes. Its 0-RTT stands only if this listener
			// admits the ticket (ADR 0019); otherwise crypto/tls answers
			// without early_data, and no 0-RTT packet is read (RFC 9001
			// §4.6.2). The ticket is reserved here; installEarly confirms
			// the reservation when the 0-RTT is accepted, and
			// releaseEarlyTicketLocked gives it back otherwise.
			if e.SessionState.EarlyData {
				var id []byte
				ok := false
				if c.early != nil {
					id, ok = c.early.eligible(e.SessionState.Extra, time.Now())
				}
				if !ok {
					e.SessionState.EarlyData = false
				}
				c.earlyTicket = append([]byte(nil), id...)
			}
			return false, nil
		}
		// This client resumes from a ticket. It offers 0-RTT only under
		// DialEarly, and only with the server's parameters remembered
		// beside the ticket: they are what 0-RTT must be sent under (RFC
		// 9000 §7.4.1). Dial offers none, so its ticket costs the server
		// no 0-RTT admission.
		if e.SessionState.EarlyData {
			p, ok := rememberedParams(e.SessionState.Extra)
			if !c.wantsEarly || !ok {
				e.SessionState.EarlyData = false
			} else {
				c.mu.Lock()
				c.remembered = p
				c.earlyOffered = true
				c.mu.Unlock()
			}
		}
		return false, nil
	case tls.QUICStoreSession:
		// A ticket from the server, stored with the parameters this
		// connection's handshake brought, for a later DialEarly to send
		// 0-RTT under. Session events being on, crypto/tls stores nothing
		// itself. A ticket that fails to store is one resumption less, not
		// a reason to end this connection.
		if raw := c.peerParamsRaw; raw != nil {
			e.SessionState.Extra = append(e.SessionState.Extra, rememberedEntry(raw))
		}
		_ = c.tls.StoreSession(e.SessionState)
		return false, nil
	case tls.QUICRejectedEarlyData:
		// The server refused 0-RTT (§4.6.2). Under DialEarly, what went
		// out in it is undone (rejectEarlyLocked); otherwise nothing was
		// sent under the early secret, and the rejection is a non-event,
		// not one of the kinds UnhandledTLSEvents counts.
		c.mu.Lock()
		if c.wantsEarly {
			c.rejectEarlyLocked()
		}
		c.mu.Unlock()
		return false, nil
	case tls.QUICWriteData:
		return false, c.sendCrypto(e.Level, e.Data)
	case tls.QUICTransportParameters:
		if c.isClient {
			c.peerParamsRaw = append([]byte(nil), e.Data...)
		}
		return false, c.handlePeerParameters(e.Data)
	case tls.QUICHandshakeDone:
		// RFC 9001 §8.1: ALPN is mandatory. NewListener refuses a Config
		// without NextProtos, but not one GetConfigForClient returns, and
		// crypto/tls negotiates the empty protocol without complaint when
		// neither side names one. The server ends the handshake as a
		// failed negotiation would: no_application_protocol, 0x0178 on
		// the wire (closeCodeFor). A client needs no check: Dial refuses
		// a Config without NextProtos, and crypto/tls refuses a server
		// that selects nothing from a client that offered something.
		if !c.isClient && c.tls.ConnectionState().NegotiatedProtocol == "" {
			return false, fmt.Errorf("quic: TLS: the handshake negotiated no application protocol (RFC 9001 §8.1): %w",
				tls.AlertError(120))
		}
		// A server that reserved a ticket and installed no early keys
		// served a resumption without 0-RTT: the ticket keeps its 0-RTT.
		c.mu.Lock()
		c.releaseEarlyTicketLocked()
		c.mu.Unlock()
		return true, nil
	case tls.QUICErrorEvent:
		// crypto/tls reports a failed handshake here (Go ≥1.25) as
		// well as through HandleData's return; either way the
		// handshake is over. e.Err wraps the AlertError closeCodeFor
		// maps to the §4.8 crypto range.
		return false, fmt.Errorf("quic: TLS: %w", e.Err)
	default:
		// An event this package does not act on — session events it
		// never enables, or a kind a newer crypto/tls added — is not
		// fatal, but it is not silent either: UnhandledTLSEvents.
		c.unhandledTLSEvents.Add(1)
		return false, nil
	}
}

// handlePeerParameters validates and adopts what the peer announced. The
// checks are RFC 9000 §7.3's authentication of the handshake against the
// packets that carried it: the identifiers each side saw on the wire must be
// the ones the handshake names, or someone spliced two conversations.
// idleDuration converts an announced max_idle_timeout (max_ack_delay uses
// it too) to a Duration, saturating. The field is a varint of up to 2^62-1
// milliseconds, far past what a Duration holds, and a wrapped product turns
// a peer's "practically never" into a deadline already passed. The
// saturation point is a whole number of milliseconds: appendParameters
// re-encodes the Duration in milliseconds, and a value that was not one
// (math.MaxInt64 is not) would parse back different — the drift
// FuzzParseParameters caught.
func idleDuration(ms uint64) time.Duration {
	const most = uint64(math.MaxInt64 / int64(time.Millisecond))
	if ms > most {
		ms = most
	}
	return time.Duration(ms) * time.Millisecond //nolint:gosec // G115: bounded by most just above
}

// earlyWriteStillLocked reports that this end would put application data in
// a 0-RTT packet: the early write keys are in and the application space has
// none yet, which is exactly writePacketLocked's own test. Callers hold mu.
func (c *Conn) earlyWriteStillLocked() bool {
	return c.earlySealer != nil && c.spaces[spaceApplication].sealer == nil
}

// applyPeerLimitsLocked takes the peer's flow-control and stream limits as
// this end's bounds on what it may send: the connection credit, the stream
// counts, and the per-stream credit of every stream already open. Held back
// while 0-RTT packets are still being written, where the remembered ones
// apply (§7.4.1). Callers hold mu.
func (c *Conn) applyPeerLimitsLocked(p TransportParameters) {
	c.connSend.grant(p.InitialMaxData)
	c.peerMaxStreamsBidi = p.InitialMaxStreamsBidi
	c.peerMaxStreamsUni = p.InitialMaxStreamsUni
	// Streams opened before the grants arrived: the 0-RTT ones, which wrote
	// under the ticket's per-stream limit, and otherwise none in the
	// ordinary order of things, but harmless to repair. §7.4.1 names this
	// limit with the others — "A server might provide larger initial stream
	// flow control limits for streams than the remembered values that a
	// client applies when sending 0-RTT" — so it waits with them.
	for _, s := range c.streams {
		s.sendQ.grant(initialStreamSendLimit(p, s.id, c.isClient))
	}
	// Whoever raises the limits wakes what the old ones stopped: a writer
	// parked on the remembered credit has room now. Held back, the raise no
	// longer rides handlePeerParameters' own broadcast.
	c.cond.Broadcast()
}

// sendLimitsLocked is the parameter set this end's sending is bounded by:
// the peer's, or the ticket's remembered ones for as long as the peer's
// are held back behind 0-RTT packets still being written (§7.4.1,
// limitsHeld). A stream born in that window takes its credit from here, not
// from c.peerParams, which already holds the peer's values. Callers hold mu.
func (c *Conn) sendLimitsLocked() TransportParameters {
	if c.limitsHeld {
		return c.remembered
	}
	return c.peerParams
}

func (c *Conn) handlePeerParameters(raw []byte) error {
	p, err := parseParameters(raw)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	// Every refusal below is TRANSPORT_PARAMETER_ERROR (parameterError says
	// which section names it for which check).
	if !p.hasInitialSourceCID {
		// §7.3: mandatory for both endpoints. Checked apart from the
		// comparison below: against a peer whose identifier is zero-length
		// an absent parameter compared equal to the empty identifier.
		return parameterError(fmt.Errorf("%w: the peer omitted initial_source_connection_id", ErrQUIC))
	}
	if !bytes.Equal(p.initialSourceCID, c.dcid) {
		return parameterError(fmt.Errorf("%w: the peer's initial_source_connection_id is not the identifier its packets carry", ErrQUIC))
	}
	switch {
	case c.isClient:
		if !bytes.Equal(p.originalDCID, c.initialDCID) {
			return parameterError(fmt.Errorf("%w: the server did not echo the original destination identifier", ErrQUIC))
		}
		// retry_source_connection_id binds this handshake to the Retry that
		// redirected it (RFC 9000 §7.3): present iff a Retry actually
		// happened, and then exactly the identifier that Retry carried —
		// kept in retrySCID, because c.dcid has usually already moved on to
		// the server's own identifier by the time the parameters arrive.
		if c.retried {
			if !p.hasRetrySourceCID || !bytes.Equal(p.retrySourceCID, c.retrySCID) {
				return parameterError(fmt.Errorf("%w: the server did not echo the Retry's source identifier", ErrQUIC))
			}
		} else if p.hasRetrySourceCID {
			return parameterError(fmt.Errorf("%w: retry_source_connection_id without a Retry", ErrQUIC))
		}
	// §18.2: the four server-only announcements a client must not send.
	case p.originalDCID != nil:
		return parameterError(fmt.Errorf("%w: a client sent original_destination_connection_id", ErrQUIC))
	case p.hasRetrySourceCID:
		return parameterError(fmt.Errorf("%w: a client sent retry_source_connection_id", ErrQUIC))
	case p.sawStatelessResetToken:
		return parameterError(fmt.Errorf("%w: a client sent stateless_reset_token", ErrQUIC))
	case p.sawPreferredAddress:
		return parameterError(fmt.Errorf("%w: a client sent preferred_address", ErrQUIC))
	}
	// §18.2's own bounds: above 20 and 2^14 or more are "invalid"
	// (max_udp_payload_size below 1200 is refused by the parser, which is
	// where its default is filled in too).
	if p.AckDelayExponent > 20 {
		return parameterError(fmt.Errorf("%w: ack_delay_exponent %d is over 20", ErrQUIC, p.AckDelayExponent))
	}
	if p.MaxAckDelay >= 1<<14*time.Millisecond {
		return parameterError(fmt.Errorf("%w: max_ack_delay %v is over the protocol maximum", ErrQUIC, p.MaxAckDelay))
	}

	c.peerParams = p
	c.hasPeerParams = true
	c.paramsRemembered = false
	if c.isClient && len(p.statelessResetToken) == 16 {
		// §10.3.1: the handshake parameter's token belongs to the peer's
		// sequence-0 identifier, exactly as a NEW_CONNECTION_ID's token
		// belongs to the identifier it rides with.
		for i := range c.peerCIDs {
			if c.peerCIDs[i].seq == 0 {
				copy(c.peerCIDs[i].token[:], p.statelessResetToken)
			}
		}
	}
	if c.isClient && p.preferredAddress != nil {
		// §18.2: a server that chose a zero-length identifier for itself
		// cannot offer a preferred address — nothing in its packets would
		// tell the connection apart on the new path — and a client treats
		// the offer as TRANSPORT_PARAMETER_ERROR. Refused before anything
		// of the offer is kept.
		if len(c.peerSCID) == 0 {
			return parameterError(fmt.Errorf("%w: preferred_address from a server using a zero-length connection ID", ErrQUIC))
		}
		// §5.1.1: the identifier offered with preferred_address is sequence
		// 1 of the peer's pool, token included, whether or not this end
		// ever moves there. The move itself waits for confirmation
		// (confirmLocked), as §9.6 requires.
		pa := p.preferredAddress
		c.peerCIDs = append(c.peerCIDs, peerCID{seq: 1, cid: append([]byte(nil), pa.cid...), token: pa.statelessResetToken})
		// The address is probed at 1200 bytes with no amplification
		// limit, so one nothing could answer from is not taken: loopback
		// offered by a server reached elsewhere (§21.5.6), the unspecified
		// address, a multicast group, port 0. The client stays where it is.
		if addr := pa.addrFor(c.peer); usablePreferredAddr(c.peer, addr) {
			c.preferred = addr
		}
	}
	// RFC 9000 §7.4.1: "Remembered transport parameters apply to the new
	// connection until the handshake completes and the client starts
	// sending 1-RTT packets. Once the handshake completes, the client uses
	// the transport parameters established in the handshake." A client that
	// offered 0-RTT is still writing 0-RTT packets here — the server's
	// parameters arrive in EncryptedExtensions, while this end's 1-RTT
	// write keys come only after the rest of the server's flight and this
	// client's own Finished, and writePacketLocked picks a 0-RTT packet for
	// as long as the application space has no sealer. Raised at once, the
	// limit let 300 bytes of stream data leave in a real 0-RTT packet
	// against a remembered limit of 100 (measured). The
	// window is open whenever the server's flight takes more than one
	// packet, which a certificate chain and §8.1's amplification limit
	// make ordinary. The raise is therefore held for the first 1-RTT
	// packet, which install does; §7.4.1 has the server's values no
	// smaller than the remembered ones, so nothing is lost by waiting.
	if c.earlyWriteStillLocked() {
		c.limitsHeld = true
	} else {
		c.applyPeerLimitsLocked(p)
	}
	// c.maxDatagram does not follow p.MaxUDPPayloadSize: the peer announces
	// what it reads, not what the *path* carries, and a datagram the path
	// swallows would be retransmitted at the same size into the same black
	// hole until the connection stalled. It bounds the search instead:
	// once the handshake is confirmed the path is probed up to
	// min(MaxDatagramSize, p.MaxUDPPayloadSize) (pmtu.go), and maxDatagram
	// rises only on a probe's acknowledgement.
	// The effective idle timeout is the smaller of the two announcements,
	// zero meaning "none" on either side (§10.1).
	peerIdle := p.MaxIdleTimeout
	switch {
	case c.idleTimeout == 0:
		c.idleTimeout = peerIdle
	case peerIdle != 0 && peerIdle < c.idleTimeout:
		c.idleTimeout = peerIdle
	}
	if c.idleTimeout != 0 {
		c.idleDeadline = time.Now().Add(c.idleTimeout)
	}
	c.cond.Broadcast()
	return nil
}

func (c *Conn) install(l tls.QUICEncryptionLevel, suiteID uint16, secret []byte, write bool) error {
	s, err := suiteFor(suiteID)
	if err != nil {
		return err
	}
	space := spaceFor(l)
	c.mu.Lock()
	defer c.mu.Unlock()
	if write {
		sealer, err := newSealerSuite(secret, s)
		if err != nil {
			return err
		}
		c.spaces[space].sealer = sealer
		if space == spaceApplication && c.limitsHeld {
			// The first 1-RTT packet can leave from here on, so the
			// server's own limits take over from the remembered ones
			// (§7.4.1, handlePeerParameters).
			c.limitsHeld = false
			c.applyPeerLimitsLocked(c.peerParams)
		}
		return nil
	}
	var opener *packetOpener
	if space == spaceApplication {
		// 1-RTT keys rotate (RFC 9001 §6): the opener carries the next
		// phase from the start, so the peer's first rotation costs no
		// derivation on the receive path.
		opener, err = newPhasedOpener(secret, s)
	} else {
		opener, err = newOpenerSuite(secret, s)
	}
	if err != nil {
		return err
	}
	c.spaces[space].opener = opener
	held := c.deferred[space]
	c.deferred[space] = nil
	peer := c.peer
	rxECN := c.rxECN
	c.mu.Unlock()
	// The keys just arrived, so whatever was waiting on them can be read
	// now — stamped with when each packet actually arrived, not with now,
	// and with the ECN mark of its own datagram, not the current one's.
	// Unlocked first: opening a packet takes the lock again.
	for _, pkt := range held {
		c.mu.Lock()
		c.rxECN, c.rxReplay = pkt.ecn, true
		c.mu.Unlock()
		err := c.receiveAt(pkt.raw, peer, pkt.at)
		c.mu.Lock()
		c.rxReplay = false
		c.mu.Unlock()
		if err != nil {
			c.mu.Lock()
			return err
		}
	}
	c.mu.Lock()
	c.rxECN = rxECN
	return nil
}
